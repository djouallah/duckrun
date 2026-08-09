"""The read-only debug cursor behind `duckrun.dbt_project` (issue #29).

Two claims are pinned here, both of which the feature is unusable — or unsafe — without.

**1. `sql()` lazy-binds.** The whole point of issue #29 is getting a `DuckDBPyRelation` back, so a
debug session calls `cursor.sql()`. But dbt-duckdb's `DuckDBCursorWrapper.__getattr__` forwards
every attribute EXCEPT `execute` straight to the raw DuckDB cursor — so a plain `cursor.sql()`
goes around duckrun's lazy bind and dies on the first model dbt's relation cache never bound,
which in a fresh debug session is every model. `DuckrunDebugCursor.sql` shadows the forwarding and
routes through the same bind-and-retry the run path uses.

**2. Read-only is structural.** A debug session is built from the real profile, so it sits one
typo away from a production write — and a `DELETE` through the run path's cursor is genuinely
routed to delta_rs and genuinely destroys data. `DuckrunDebugCursor` is a SIBLING of
`DuckrunCursorWrapper`, not a subclass: `delta_dml.handle` is not called anywhere in its MRO, so
the write route does not exist to be re-enabled. `test_debug_mro_never_calls_delta_dml_handle`
checks that against bytecode rather than prose, so a later refactor that quietly re-parents the
class fails here instead of in someone's lakehouse.
"""
import os
from pathlib import Path

import pytest
from deltalake import DeltaTable

from dbt.cli.main import dbtRunner


MODEL_SQL = """{{ config(materialized='table') }}
select 1 as id, 'a' as name, cast(1.5 as double) as amount, current_date as d
union all
select 2, 'b', 2.5, current_date
"""


@pytest.fixture
def project(tmp_path):
    """A one-model project on a local-fs Delta warehouse, already built."""
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        "name: dbg\nversion: '1.0'\nconfig-version: 2\nprofile: dbg\nmodel-paths: [models]\n",
        encoding="utf-8")
    (proj / "profiles.yml").write_text(
        "dbg:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckrun\n      root_path: \"{{ env_var('DBG_PATH') }}\"\n",
        encoding="utf-8")
    (proj / "models" / "events.sql").write_text(MODEL_SQL, encoding="utf-8")
    root = (tmp_path / "wh").as_posix()
    os.environ["DBG_PATH"] = root

    res = dbtRunner().invoke(["run", "--project-dir", str(proj), "--profiles-dir", str(proj)])
    assert res.success
    return proj, root


@pytest.fixture
def creds(project):
    from dbt.config.runtime import load_profile
    proj, _ = project
    return load_profile(str(proj), {}, profile_name_override=None, target_override=None).credentials


def _fresh_debug_cursor(creds):
    """A debug cursor on a BRAND-NEW environment: no discovery has run on it, so nothing is bound —
    exactly the state a notebook debug session starts in. The env is returned too and must be held
    by the caller, or it is collected and its connection closed mid-test."""
    from dbt.adapters.duckrun.environment import DuckrunEnvironment
    env = DuckrunEnvironment(creds)
    return env, env.debug_handle().cursor()


# ── 1. the relation path ───────────────────────────────────────────────────────────────────────

def test_sql_returns_a_relation_and_lazy_binds(creds):
    """`cursor.sql()` on an unbound model must bind it and hand back a relation. Before the fix
    this raised `Catalog Error: Table with name events does not exist!` — the forwarding to the raw
    cursor skipped the bind entirely."""
    from dbt.adapters.duckrun.environment import DuckrunDebugCursor

    env, cur = _fresh_debug_cursor(creds)
    assert isinstance(cur, DuckrunDebugCursor)
    assert _bound(cur) == []                                    # nothing bound yet

    rel = cur.sql('select * from "memory"."main"."events" order by id')

    assert _bound(cur) == ["main.events"]                       # the bind fired
    assert rel.columns == ["id", "name", "amount", "d"]
    # Real DuckDB types end to end — the JSON round trip issue #29 is about would have lost these.
    assert [str(t) for t in rel.types] == ["INTEGER", "VARCHAR", "DOUBLE", "DATE"]


def test_relation_stays_lazy(creds):
    """Filtering must push into the delta_scan rather than materialize first — the second reason
    issue #29 wants a relation instead of a dataframe."""
    env, cur = _fresh_debug_cursor(creds)
    rel = cur.sql('select * from "memory"."main"."events"')

    narrowed = rel.filter("id = 2")
    assert type(narrowed).__name__ == "DuckDBPyRelation"        # still a relation, not rows
    assert [r[0] for r in narrowed.fetchall()] == [2]


def test_sql_binds_every_relation_a_join_names(creds):
    """A join of two unbound models errors on one table at a time, so the retry loop has to keep
    binding. Same guarantee the run path has — it is literally the same loop now."""
    env, cur = _fresh_debug_cursor(creds)
    rel = cur.sql('select a.id from "memory"."main"."events" a '
                  'join "memory"."main"."events" b on a.id = b.id')
    assert sorted(r[0] for r in rel.fetchall()) == [1, 2]


def test_a_genuinely_missing_table_still_raises(creds):
    """The lazy bind must not turn a real mistake into silence: a table that isn't on disk
    re-raises DuckDB's original catalog error."""
    import duckdb

    env, cur = _fresh_debug_cursor(creds)
    with pytest.raises(duckdb.CatalogException):
        cur.sql('select * from "memory"."main"."no_such_model"')


# ── 2. read-only, structurally ─────────────────────────────────────────────────────────────────

WRITES = [
    'delete from "memory"."main"."events"',
    'insert into "memory"."main"."events" values (9, \'z\', 9.5, current_date)',
    'update "memory"."main"."events" set name = \'zzz\'',
    'drop table "memory"."main"."events"',
    'create or replace table "memory"."main"."events" as select 1 as id',
]


@pytest.mark.parametrize("stmt", WRITES)
@pytest.mark.parametrize("api", ["sql", "execute"])
def test_writes_are_rejected_and_delta_is_untouched(project, creds, stmt, api):
    """Both entry points refuse, and — the part that actually matters — the Delta table on disk
    does not move a version."""
    from dbt.adapters.duckrun.environment import DuckrunReadOnlyError
    _, root = project
    before = DeltaTable(f"{root}/main/events").version()

    env, cur = _fresh_debug_cursor(creds)
    with pytest.raises(DuckrunReadOnlyError):
        getattr(cur, api)(stmt)

    assert DeltaTable(f"{root}/main/events").version() == before


def test_debug_mro_never_calls_delta_dml_handle():
    """The structural claim, checked against bytecode rather than trusted from a docstring.

    `DuckrunDebugCursor` must not be able to reach delta_rs through ANY class it inherits from.
    A refactor that re-parents it onto the run-path cursor — the obvious "just reuse the wrapper"
    cleanup — silently restores the write route, and this is what catches it."""
    from dbt.adapters.duckrun.environment import DuckrunCursorWrapper, DuckrunDebugCursor

    def callers(cls):
        return [name for name, fn in vars(cls).items()
                if getattr(fn, "__code__", None) is not None
                and "delta_dml" in fn.__code__.co_names and "handle" in fn.__code__.co_names]

    assert not any(callers(c) for c in DuckrunDebugCursor.__mro__)
    assert not issubclass(DuckrunDebugCursor, DuckrunCursorWrapper)
    # …and the run path still does call it, so the assertion above is testing something real.
    assert any(callers(c) for c in DuckrunCursorWrapper.__mro__)


def test_scratch_objects_are_still_allowed(creds):
    """Read-only means "can't write your Delta tables", not "can't work": temp tables and views are
    how you actually take a model apart, and they classify as passthrough."""
    env, cur = _fresh_debug_cursor(creds)
    cur.sql("create temp table scratch as select 1 as a")
    cur.sql("create or replace view v_scratch as select 2 as b")
    assert cur.sql("select a from scratch").fetchall() == [(1,)]
    assert cur.sql("select b from v_scratch").fetchall() == [(2,)]


FILE_WRITES = [
    "copy (select 1 as x) to '{out}' (format parquet)",
    "copy \"memory\".\"main\".\"events\" to '{out}'",
    "export database '{out}'",
]


@pytest.mark.parametrize("stmt", FILE_WRITES)
def test_file_exports_are_rejected_and_nothing_lands_on_disk(tmp_path, creds, stmt):
    """COPY ... TO / EXPORT DATABASE classify as passthrough — native DuckDB, no delta_rs — but
    they write files wherever this session's live store credentials reach, and no read-only
    delta_scan view sits downstream to refuse them. Here the check IS the safety, so the file must
    not exist afterwards."""
    from dbt.adapters.duckrun.environment import DuckrunReadOnlyError

    out = tmp_path / "leak.out"
    env, cur = _fresh_debug_cursor(creds)
    with pytest.raises(DuckrunReadOnlyError):
        cur.sql(stmt.format(out=out.as_posix()))
    assert not out.exists()


def test_copy_from_into_scratch_is_still_allowed(tmp_path, creds):
    """COPY <table> FROM loads a file INTO the catalog — scratch territory, the same contract as
    CREATE TEMP TABLE. Only the writing direction (a top-level TO) is refused."""
    src = tmp_path / "in.csv"
    src.write_text("a\n1\n2\n", encoding="utf-8")
    env, cur = _fresh_debug_cursor(creds)
    cur.sql("create temp table loaded (a int)")
    cur.sql(f"copy loaded from '{src.as_posix()}' (header)")
    assert cur.sql("select count(*) from loaded").fetchall() == [(2,)]


# ── 3. the run path is unchanged ───────────────────────────────────────────────────────────────

def test_run_cursor_still_writes_through_delta_rs(project, creds):
    """The counterpart to the rejection tests: the same DELETE on the RUN cursor must still reach
    delta_rs. Without this, "the debug cursor doesn't write" would be satisfiable by breaking
    writing everywhere."""
    from dbt.adapters.duckrun.environment import DuckrunEnvironment
    _, root = project
    before = DeltaTable(f"{root}/main/events")

    env = DuckrunEnvironment(creds)                  # held: a collected env closes its connection
    cur = env.handle().cursor()
    cur.execute('delete from "memory"."main"."events" where id = 1')

    after = DeltaTable(f"{root}/main/events")
    assert after.version() > before.version()
    assert after.to_pyarrow_table().num_rows == before.to_pyarrow_table().num_rows - 1


def _bound(cur):
    """The relations bound on this cursor's catalog. Reads through the RAW cursor so the lazy bind
    can't fire while we are measuring whether it fired."""
    return sorted(r[0] for r in cur._cursor.sql(
        "select table_schema || '.' || table_name from information_schema.tables").fetchall())
