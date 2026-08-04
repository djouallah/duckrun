"""Column introspection on a command that never populates dbt's relation cache (issue #24).

A duckrun model is a `delta_scan` view that only exists once discovery has registered it, and
discovery only runs while dbt populates its relation cache. `dbt run-operation` is not a graph
task, so it populates nothing — and `adapter.get_columns_in_relation()` (how dbt-codegen's
generate_model_yaml, dbt-osmosis and any project macro resolve columns) came back EMPTY for a
model sitting on disk. Silently: an empty list reads as "this model has no columns", not as a
failure, so a generated schema.yml just had no columns in it.

impl.get_columns_in_relation now binds the one relation being asked about when the base answer is
empty. These tests drive the real `dbt run-operation` command, because the whole bug lives in
which task dbt is running — a direct adapter call can't see it.
"""
import os
import types
from pathlib import Path

import duckrun
import pytest

from dbt.cli.main import dbtRunner


MODEL_SQL = """{{ config(materialized='table') }}
select 1 as id, 'a' as name, cast(1.5 as double) as amount, current_date as d
"""

# The adapter call that dbt-codegen / dbt-osmosis make, reduced to two parseable log lines.
MACRO_SQL = """{% macro show_cols(model_name, schema=none) %}
  {% set rel = api.Relation.create(database=target.database,
                                   schema=schema or target.schema,
                                   identifier=model_name) %}
  {% set cols = adapter.get_columns_in_relation(rel) %}
  {{ log("COLS: " ~ (cols | map(attribute='name') | join(",")), info=True) }}
  {{ log("TYPES: " ~ (cols | map(attribute='data_type') | join(",")), info=True) }}
{% endmacro %}
"""

# Ad-hoc SQL against a model, the other thing a run-operation macro does (issue #24 part B).
RUN_SQL_MACRO = """{% macro run_sql(query) %}
  {% set res = run_query(query) %}
  {{ log("RESULT: " ~ (res.columns[0].values() | join(",")), info=True) }}
{% endmacro %}
"""


@pytest.fixture
def project(tmp_path):
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "macros").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        "name: ops\nversion: '1.0'\nconfig-version: 2\nprofile: ops\n"
        "model-paths: [models]\nmacro-paths: [macros]\n", encoding="utf-8")
    (proj / "profiles.yml").write_text(
        "ops:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckrun\n      root_path: \"{{ env_var('OPS_PATH') }}\"\n",
        encoding="utf-8")
    (proj / "models" / "events.sql").write_text(MODEL_SQL, encoding="utf-8")
    (proj / "macros" / "show_cols.sql").write_text(MACRO_SQL, encoding="utf-8")
    (proj / "macros" / "run_sql.sql").write_text(RUN_SQL_MACRO, encoding="utf-8")
    root = (tmp_path / "wh").as_posix()
    os.environ["OPS_PATH"] = root
    return proj, root


def _fresh_process():
    """Drop the process-global DuckDB so the next dbt command starts from an empty in-memory
    catalog.

    dbt-duckdb keeps ONE Environment per process (DuckDBConnectionManager._ENV, reused whenever
    the credentials match), so an in-process `dbt run` followed by `dbt run-operation` — which is
    how this suite invokes dbt — leaves the run's `delta_scan` views standing and information_schema
    answers from them. Real usage runs the two as separate processes, where nothing is left. Without
    this reset the tests below would pass with or without the fix.
    """
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    from dbt.adapters.duckrun.impl import DuckrunConnectionManager

    DuckDBConnectionManager.close_all_connections()
    DuckrunConnectionManager.close_all_connections()


def _dbt(proj: Path, *args: str):
    """Invoke dbt, returning (result, logged_messages). Jinja `log(..., info=True)` lands in the
    event stream, which is the only way to read a macro's result out of `run-operation`."""
    msgs = []
    res = dbtRunner(callbacks=[lambda e: msgs.append(e.info.msg)]).invoke(
        [*args, "--project-dir", str(proj), "--profiles-dir", str(proj)])
    return res, msgs


def _logged(msgs, prefix):
    """The value logged under `prefix`, as a list ([] when the macro logged nothing)."""
    line = next(m for m in msgs if m.startswith(prefix))
    value = line[len(prefix):].strip()
    return value.split(",") if value else []


def _show_cols(proj, model_name, schema=None):
    _fresh_process()  # a `dbt run-operation` of one's own, with nothing left over from the build
    args = f"{{model_name: {model_name}"
    if schema:
        args += f", schema: {schema}"
    res, msgs = _dbt(proj, "run-operation", "show_cols", "--args", args + "}")
    assert res.success
    return _logged(msgs, "COLS:"), _logged(msgs, "TYPES:")


def test_run_operation_sees_the_models_columns(project):
    """The bug, through the command that has it: `dbt run-operation` on a materialized model."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    cols, types = _show_cols(proj, "events")
    assert cols == ["id", "name", "amount", "d"]        # order preserved (ordinal_position)
    # Types come from the bound delta_scan view, so they are the Delta table's real types
    # (spelled the way dbt's sql_convert_columns_in_relation renders them).
    assert types == ["INTEGER", "character varying(256)", "DOUBLE", "DATE"]


def test_run_operation_columns_for_a_custom_schema_model(project):
    """A model in a custom schema has no schema in a fresh in-memory DuckDB either, so the bind
    has to create it before the view — otherwise this is still an empty list."""
    proj, _ = project
    (proj / "models" / "other.sql").write_text(
        "{{ config(materialized='table', schema='x2') }}\nselect 42 as answer", encoding="utf-8")
    assert _dbt(proj, "run")[0].success

    cols, _ = _show_cols(proj, "other", schema="main_x2")
    assert cols == ["answer"]


def test_dropped_table_still_reports_no_columns(project):
    """The tombstone contract survives the new path. duckrun's `drop table` overwrites the table
    to a one-column marker; binding that would report a `__duckrun_deleted__` column as if the
    model still existed."""
    proj, root = project
    assert _dbt(proj, "run")[0].success
    duckrun.connect(root, schema="main", read_only=False).sql("drop table events")
    assert (Path(root) / "main" / "events" / "_delta_log").is_dir()  # tombstoned, not deleted

    cols, _ = _show_cols(proj, "events")
    assert cols == []


def test_missing_relation_reports_no_columns_without_failing(project):
    """Nothing on disk → the empty list the caller already had, and no error: introspection must
    not turn loud on a relation that simply isn't there."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    cols, _ = _show_cols(proj, "no_such_model")
    assert cols == []


def test_normal_run_never_reaches_the_fallback(project, monkeypatch):
    """The fallback is gated on an empty base answer, so a `dbt run` — whose introspection targets
    real DuckDB temp tables — must never pay for it. Pins the "costs nothing where it already
    worked" claim, which is what makes the extra store round trip acceptable."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success  # build once so run 2 introspects an existing model too

    from dbt.adapters.duckrun.impl import DuckrunAdapter

    binds = []
    real_bind = DuckrunAdapter._bind_delta_view
    monkeypatch.setattr(DuckrunAdapter, "_bind_delta_view", lambda self, relation: (
        binds.append(str(relation)), real_bind(self, relation))[1])

    _fresh_process()  # the harder case: run 2 rediscovers everything from disk
    assert _dbt(proj, "run")[0].success
    assert binds == []


# --- the branch a real store reaches but a local one never does ---------------------------------

def _bare_adapter(monkeypatch, registered):
    """A DuckrunAdapter shell: _bind_delta_view only needs credentials, a cursor and the
    registration hook. Used to drive the delta-rs-can't-open branch, which needs a store whose
    credential DuckDB has and delta-rs doesn't (az:// with a `secrets:` block) — not reachable
    from a local-path project."""
    from dbt.adapters.duckrun.impl import DuckrunAdapter

    adapter = DuckrunAdapter.__new__(DuckrunAdapter)
    adapter.config = types.SimpleNamespace(
        credentials=types.SimpleNamespace(root_for=lambda database: ("az://wh", None)))
    monkeypatch.setattr(DuckrunAdapter, "_cursor", lambda self: None)
    monkeypatch.setattr(DuckrunAdapter, "create_schema", lambda self, relation: None)
    monkeypatch.setattr(DuckrunAdapter, "_register_delta_view",
                        lambda self, relation, dt=None, cursor=None: registered.append(relation))
    return adapter, DuckrunAdapter.Relation.create(
        database="memory", schema="main", identifier="events")


@pytest.mark.parametrize("tombstoned, expected", [(True, False), (False, True)])
def test_bind_falls_back_to_the_duckdb_tombstone_probe(monkeypatch, tombstoned, expected):
    """When delta-rs can't open the table, existence is unsettled and the bind goes ahead — but
    the tombstone check has to go with it, via the DuckDB-side probe discovery uses in the same
    situation (_live_relations). Otherwise a dropped table would report its `__duckrun_deleted__`
    marker column as if the model were still there."""
    from dbt.adapters.duckrun import delta_dml, engine

    registered = []
    adapter, relation = _bare_adapter(monkeypatch, registered)
    monkeypatch.setattr(engine, "open_if_exists",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("no delta-rs credential")))
    monkeypatch.setattr(delta_dml, "is_dropped", lambda cursor, loc, so: tombstoned)

    assert adapter._bind_delta_view(relation) is expected
    assert registered == ([] if tombstoned else [relation])


# --- part B: raw SQL against a model, lazy-bound on the catalog error (issue #24) ---------------
# `run_query("select … from main.events")` in a run-operation macro died with
# `Catalog Error: Table with name events does not exist!` — same root cause as above, but the SQL
# goes straight to DuckDB, so the adapter never sees which relation it needs. The cursor wrapper
# now catches the catalog error, binds the missing relation's delta_scan view, and retries; a
# genuinely missing table re-raises the original error unchanged.

def _run_sql(proj, query):
    """`dbt run-operation run_sql` from a fresh process, returning (result, first column values,
    logged messages — where a failed operation reports its error)."""
    import json
    _fresh_process()
    res, msgs = _dbt(proj, "run-operation", "run_sql", "--args", json.dumps({"query": query}))
    values = _logged(msgs, "RESULT:") if res.success else []
    return res, values, msgs


def test_run_operation_raw_sql_reads_the_model(project):
    """The bug itself: ad-hoc SQL against a materialized model, qualified as macros write it."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    res, values, _ = _run_sql(proj, "select count(*) as n from main.events")
    assert res.success
    assert values == ["1"]


def test_run_operation_raw_sql_unqualified_name(project):
    """An unqualified name resolves under the profile's default schema — DuckDB's error names the
    bare table, and the statement itself carries no schema to recover it from."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    res, values, _ = _run_sql(proj, "select count(*) as n from events")
    assert res.success
    assert values == ["1"]


def test_run_operation_raw_sql_join_binds_every_model(project):
    """A join of two unbound models errors on one table at a time, so the bind-and-retry has to
    loop until the statement stops naming new relations."""
    proj, _ = project
    (proj / "models" / "clicks.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1 as id", encoding="utf-8")
    assert _dbt(proj, "run")[0].success

    res, values, _ = _run_sql(
        proj, "select count(*) as n from main.events e join main.clicks c on e.id = c.id")
    assert res.success
    assert values == ["1"]


def test_run_operation_raw_sql_custom_schema_model(project):
    """A custom-schema model fails with DuckDB's other error shape — `Table with name
    \"main_x2.other\" does not exist because schema \"main_x2\" does not exist.` — and the bind
    has to create the schema before the view."""
    proj, _ = project
    (proj / "models" / "other.sql").write_text(
        "{{ config(materialized='table', schema='x2') }}\nselect 42 as answer", encoding="utf-8")
    assert _dbt(proj, "run")[0].success

    res, values, _ = _run_sql(proj, "select answer from main_x2.other")
    assert res.success
    assert values == ["42"]


def test_run_operation_raw_sql_missing_table_still_errors(project):
    """Nothing on disk → the original catalog error surfaces unchanged; the lazy bind must not
    swallow or reshape a genuine failure."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    res, _, msgs = _run_sql(proj, "select count(*) as n from main.no_such_model")
    assert not res.success
    assert any("no_such_model" in m and "does not exist" in m for m in msgs)


def test_run_operation_raw_sql_dropped_table_still_errors(project):
    """The drop-tombstone contract holds on this path too: a dropped model must not come back as
    a queryable one-column `__duckrun_deleted__` view."""
    proj, root = project
    assert _dbt(proj, "run")[0].success
    duckrun.connect(root, schema="main", read_only=False).sql("drop table events")

    res, _, _msgs = _run_sql(proj, "select count(*) as n from main.events")
    assert not res.success


def test_show_no_populate_cache_works(project):
    """`--no-populate-cache` skips discovery on every command, failing through the identical
    path — the lazy bind fixes it for free, so pin that."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success

    _fresh_process()
    res, _ = _dbt(proj, "show", "--inline", "select * from {{ ref('events') }}",
                  "--no-populate-cache")
    assert res.success


def test_normal_run_never_lazy_binds(project, monkeypatch):
    """A `dbt run` raises no catalog errors, so the lazy bind — like the introspection fallback
    above — must cost a working run nothing."""
    proj, _ = project
    assert _dbt(proj, "run")[0].success  # build once so run 2 rediscovers an existing model too

    from dbt.adapters.duckrun.environment import DuckrunCursorWrapper

    binds = []
    real_bind = DuckrunCursorWrapper._lazy_bind_delta_view
    monkeypatch.setattr(DuckrunCursorWrapper, "_lazy_bind_delta_view",
                        lambda self, *c: (binds.append(c), real_bind(self, *c))[1])

    _fresh_process()
    assert _dbt(proj, "run")[0].success
    assert binds == []
