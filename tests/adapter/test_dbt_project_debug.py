"""`duckrun.dbt_project()` — the notebook debug session of issue #29.

The feature's whole claim is that what you get back is trustworthy: real types, lazily evaluated,
compiled by dbt itself, executed on the connection a real run uses, and never able to write. Each
of those is pinned below, but two deserve saying out loud, because they are the ones that would
fail silently rather than loudly:

* **Freshness.** A warm manifest is what makes the session usable in a notebook, and it is also
  what would happily hand back the SQL from before your last edit. `test_an_edited_model_is_not_
  served_stale` is the guard, and `test_a_compile_alone_does_not_trigger_a_reparse` is its
  counterweight — a freshness check that fires on every call would have thrown the warm manifest
  away and nobody would have noticed except through the clock.

* **One DuckDB.** Every DuckDB instance pins `memory_limit` to a large share of RAM, so quietly
  opening a second environment beside dbt's is an OOM in a Fabric notebook, not a style problem.
"""
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

import duckrun
from duckrun.dbt_debug import DbtProjectError

from dbt.cli.main import dbtRunner


# materialized='table' on purpose: dbt's default is `view`, which duckrun materializes as a plain
# DuckDB view and therefore writes NOTHING to the lakehouse. These tests are about reading real
# Delta tables through the lazy bind, so the models have to actually be Delta tables.
STG = ("{{ config(materialized='table') }}\n"
       "select * from (values (1, 'a', 10.5), (2, 'b', 20.5)) as t(id, name, amount)\n")
MART = """{{ config(materialized='table') }}
select id, name, cast(amount * 2 as double) as doubled
from {{ ref('stg_items') }}
"""
# Branches on is_incremental(), so the two compiles genuinely differ.
INCR = """{{ config(materialized='incremental', unique_key='id') }}
select * from {{ ref('stg_items') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
"""


@pytest.fixture
def project(tmp_path):
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        "name: dbg\nversion: '1.0'\nconfig-version: 2\nprofile: dbg\nmodel-paths: [models]\n",
        encoding="utf-8")
    (proj / "profiles.yml").write_text(
        "dbg:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckrun\n      root_path: \"{{ env_var('DBG2_PATH') }}\"\n",
        encoding="utf-8")
    (proj / "models" / "stg_items.sql").write_text(STG, encoding="utf-8")
    (proj / "models" / "mart_items.sql").write_text(MART, encoding="utf-8")
    (proj / "models" / "incr_items.sql").write_text(INCR, encoding="utf-8")
    os.environ["DBG2_PATH"] = (tmp_path / "wh").as_posix()

    assert dbtRunner().invoke(
        ["run", "--project-dir", str(proj), "--profiles-dir", str(proj), "--quiet"]).success

    # Drop the process-global DuckDB the build left behind, so the debug session starts from an
    # empty catalog. Without this the build's own relations stay bound and every read below would
    # pass whether or not the lazy bind works — and the real scenario IS a cold catalog: you run
    # dbt in one process and open the notebook in another.
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    DuckDBConnectionManager.close_all_connections()
    return proj


@pytest.fixture
def p(project):
    return duckrun.dbt_project(project)


# ── construction ───────────────────────────────────────────────────────────────────────────────

def test_construction_is_cheap_and_defers_the_parse(project):
    """The constructor must not pay for a parse: in a notebook it is a cell of its own, and the
    parse belongs to the first thing you actually ask for."""
    started = time.perf_counter()
    session = duckrun.dbt_project(project)
    assert time.perf_counter() - started < 0.5
    assert session._manifest is None
    assert "not parsed yet" in repr(session)


def test_a_wrong_directory_fails_immediately(tmp_path):
    with pytest.raises(DbtProjectError, match="no dbt_project.yml"):
        duckrun.dbt_project(tmp_path)


def test_a_typoed_target_fails_immediately_and_lists_the_real_ones(project):
    """Caught at construction rather than several cells later inside a show(), where "your target
    is wrong" and "your model is wrong" would arrive as the same kind of failure."""
    with pytest.raises(DbtProjectError, match="no target 'nope'.*dev"):
        duckrun.dbt_project(project, target="nope")


def test_profiles_yml_next_to_dbt_project_yml_is_found(p, project):
    assert p.profiles_dir == Path(project).resolve()


# ── the relation ───────────────────────────────────────────────────────────────────────────────

def test_show_returns_a_relation_with_real_types(p):
    """The premise of issue #29: no JSON round trip, so no type inference."""
    rel = p.show("mart_items")
    assert rel.columns == ["id", "name", "doubled"]
    assert [str(t) for t in rel.types] == ["INTEGER", "VARCHAR", "DOUBLE"]
    assert sorted(rel.fetchall()) == [(1, "a", 21.0), (2, "b", 41.0)]


def test_the_relation_is_lazy(p):
    """A filter must stay a relation — i.e. push into the delta_scan — rather than materialize."""
    rel = p.show("mart_items")
    narrowed = rel.filter("id = 2")
    assert type(narrowed).__name__ == "DuckDBPyRelation"
    assert narrowed.fetchall() == [(2, "b", 41.0)]


def test_sql_renders_refs(p):
    rel = p.sql("select count(*) as n from {{ ref('stg_items') }}")
    assert rel.fetchall() == [(2,)]
    assert "stg_items" in p.last_compile.sql
    assert "{{" not in p.last_compile.sql


def test_compiled_returns_the_sql_text(p):
    sql = p.compiled("mart_items")
    assert "stg_items" in sql and "{{" not in sql
    assert p.last_compile.model == "mart_items"
    assert p.last_compile.full_refresh is False


def test_a_path_selector_works(p, project):
    """Selectors are handed to dbt untouched, so dbt's full syntax comes along for free."""
    assert p.compiled("path:models/mart_items.sql") == p.compiled("mart_items")


# ── freshness ──────────────────────────────────────────────────────────────────────────────────

def test_an_edited_model_is_not_served_stale(p, project):
    """The trap this design exists to close: with a warm manifest, dbt re-compiles from the code it
    parsed earlier, so an edited model compiles to its OLD sql — silently. Debugging an edit you
    cannot see is worse than a slow tool."""
    assert "EDITED" not in p.compiled("mart_items")

    model = project / "models" / "mart_items.sql"
    model.write_text(MART + "-- EDITED\n", encoding="utf-8")
    time.sleep(0.05)                                   # mtime granularity

    assert "EDITED" in p.compiled("mart_items")


def test_a_compile_alone_does_not_trigger_a_reparse(p):
    """The counterweight: `dbt compile` rewrites target/, so watching it would re-parse on every
    call and quietly undo the warm manifest."""
    p.compiled("mart_items")
    manifest, signature = p._manifest, p._signature
    p.compiled("mart_items")
    assert p._manifest is manifest and p._signature == signature


def test_the_reparse_message_names_the_file(p, project, capsys):
    p.compiled("mart_items")
    capsys.readouterr()

    (project / "models" / "mart_items.sql").write_text(MART + "-- EDITED\n", encoding="utf-8")
    time.sleep(0.05)
    p.compiled("mart_items")

    assert "mart_items.sql changed" in capsys.readouterr().out


def test_reload_reparses_on_demand(p):
    p.compiled("mart_items")
    manifest = p._manifest
    p.reload()
    assert p._manifest is not manifest


# ── the is_incremental() branch ────────────────────────────────────────────────────────────────

def test_incremental_false_compiles_the_full_refresh_branch(p):
    """A branching model has two compiled forms and the SQL does not say which one you got. Being
    able to ask for the other one is what turns that hazard into a comparison."""
    default = p.compiled("incr_items")
    full = p.compiled("incr_items", incremental=False)

    assert "max(id)" in default            # the table exists, so is_incremental() was true
    assert "max(id)" not in full
    assert p.last_compile.full_refresh is True


def test_incremental_true_cannot_be_forced_and_says_why(p):
    with pytest.raises(DbtProjectError, match="incremental=True cannot be forced"):
        p.compiled("incr_items", incremental=True)


# ── read-only, through the session API ─────────────────────────────────────────────────────────

def test_writes_through_the_session_are_rejected(p, project):
    """The cursor's structural guarantee has to survive the wrapper around it."""
    from dbt.adapters.duckrun.environment import DuckrunReadOnlyError
    from deltalake import DeltaTable

    root = os.environ["DBG2_PATH"]
    before = DeltaTable(f"{root}/main/stg_items").version()

    with pytest.raises(DuckrunReadOnlyError):
        p.sql("delete from {{ ref('stg_items') }}")

    assert DeltaTable(f"{root}/main/stg_items").version() == before


# ── selector errors ────────────────────────────────────────────────────────────────────────────

def test_an_unmatched_selector_explains_the_selector_syntax(p):
    with pytest.raises(DbtProjectError, match="matched no model"):
        p.compiled("no_such_model")


def test_an_ambiguous_selector_lists_the_candidates(p):
    """show() needs exactly one node. Rather than pick one, say which ones matched — picking would
    be a second node-selection implementation, and a silently wrong answer.

    `+mart_items` is dbt's "this node and its parents", so it selects stg_items too."""
    with pytest.raises(DbtProjectError, match="matched 2 nodes: mart_items, stg_items"):
        p.compiled("+mart_items")


# ── the connection ─────────────────────────────────────────────────────────────────────────────

def test_the_session_reuses_dbts_environment(p):
    """Not an optimization: a second DuckDB instance pins its own large memory_limit, and it is
    also how the session would start to drift from the adapter."""
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    from dbt.adapters.duckrun.environment import DuckrunEnvironment, DuckrunDebugCursor

    p.show("mart_items")
    assert isinstance(p._env, DuckrunEnvironment)
    assert p._env is DuckDBConnectionManager._ENV
    assert isinstance(p._cursor, DuckrunDebugCursor)


def test_importing_duckrun_does_not_import_dbts_cli():
    """`import duckrun` must not pull in dbt's CLI layer.

    duckrun already imports `dbt.adapters` eagerly (session.py needs delta_dml, engine, …), so this
    is NOT a claim that dbt stays out. It is the narrower, true one: `dbt.cli` — dbtRunner and the
    click machinery behind it — is a further ~0.9s that only a debug session needs, and importing it
    from duckrun/__init__ would additionally run dbt.adapters.duckrun.impl -> duckrun._runtime while
    the `duckrun` package is still half-initialized. Hence the function-level imports in dbt_debug.

    A subprocess, because dbt.cli is long since imported inside this test session."""
    code = textwrap.dedent("""
        import sys
        import duckrun
        assert callable(duckrun.dbt_project)
        print("CLI:" + str(any(m == "dbt.cli" or m.startswith("dbt.cli.") for m in sys.modules)))
    """)
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert "CLI:False" in out.stdout, out.stdout
