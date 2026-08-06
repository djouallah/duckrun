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

# The comment sits BETWEEN two CTEs, at paren depth 0, and contains a comma — the shape that a
# depth-only splitter gets wrong, and that dbt models are full of.
CTE_MODEL = """{{ config(materialized='table') }}
with base as (
    select id, name, amount from {{ ref('stg_items') }}
),
-- allocate the amount, then halve it
allocated as (select id, name, amount / 2 as share from base),
final as (
    select id, sum(share) as total from allocated group by 1
)
select * from final
"""

EPHEMERAL = """{{ config(materialized='ephemeral') }}
select id, name, amount from {{ ref('stg_items') }} where amount > 15
"""
USES_EPHEMERAL = """{{ config(materialized='table') }}
with picked as (select * from {{ ref('eph_filtered') }})
select * from picked
"""
# BOTH at once: branches on is_incremental() AND has an ephemeral parent. That intersection is the
# only shape where the branch probe's second compile can damage the first one's SQL, and a model
# that is one or the other never reaches it — see
# test_an_incremental_over_an_ephemeral_stays_whole_in_both_orders.
INCR_OVER_EPH = """{{ config(materialized='incremental', unique_key='id') }}
select * from {{ ref('eph_filtered') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
"""

# A macro that emits an entire CTE — name, body and its own comment.
MACRO = """{% macro scaled_cte(name, src, factor) %}
{{ name }} as (
    -- emitted by a macro, with a comma
    select id, amount * {{ factor }} as scaled from {{ src }}
)
{% endmacro %}
"""
MACRO_MODEL = """{{ config(materialized='table') }}
with base as (select id, amount from {{ ref('stg_items') }}),
{{ scaled_cte('macro_made', 'base', 2) }}
select * from macro_made
"""

# Generic tests on a model, because every real project has them and dbt hands them back
# alongside the model they test (indirect selection). Without this the fixture was quietly
# easier than any project this runs against.
SCHEMA_YML = """version: 2

models:
  - name: mart_items
    columns:
      - name: id
        data_tests:
          - not_null
          - unique
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
    (proj / "macros").mkdir()
    (proj / "models" / "stg_items.sql").write_text(STG, encoding="utf-8")
    (proj / "models" / "mart_items.sql").write_text(MART, encoding="utf-8")
    (proj / "models" / "incr_items.sql").write_text(INCR, encoding="utf-8")
    (proj / "models" / "cte_model.sql").write_text(CTE_MODEL, encoding="utf-8")
    (proj / "models" / "eph_filtered.sql").write_text(EPHEMERAL, encoding="utf-8")
    (proj / "models" / "uses_ephemeral.sql").write_text(USES_EPHEMERAL, encoding="utf-8")
    (proj / "models" / "incr_over_eph.sql").write_text(INCR_OVER_EPH, encoding="utf-8")
    (proj / "models" / "macro_model.sql").write_text(MACRO_MODEL, encoding="utf-8")
    (proj / "macros" / "helpers.sql").write_text(MACRO, encoding="utf-8")
    (proj / "models" / "schema.yml").write_text(SCHEMA_YML, encoding="utf-8")
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


def test_sql_can_be_called_more_than_once(p):
    """`dbt compile --inline` PARSES a node called `inline_query` into the manifest it was handed.
    Ours is warm and reused, so the second call found the first one still sitting there and dbt
    refused the duplicate name — i.e. sql() worked exactly once per parse. A notebook does nothing
    but call it repeatedly."""
    assert p.sql("select count(*) as n from {{ ref('stg_items') }}").fetchall() == [(2,)]
    assert p.sql("select 111 as a").fetchall() == [(111,)]
    assert p.sql("select 222 as b").fetchall() == [(222,)]


def test_ddl_over_an_ephemeral_ref_explains_itself(p):
    """dbt prepends `with __dbt__cte__x as (...)` to an inline query that refs an ephemeral model,
    and that only parses before a SELECT. Before this, DuckDB reported `syntax error at or near
    "create"` against a line number in SQL the caller never wrote — which reads like a typo in
    their own statement."""
    with pytest.raises(DbtProjectError, match="ephemeral model"):
        p.sql("create temp table k as select * from {{ ref('eph_filtered') }}")

    # The route that does work, and that the message points at.
    rel = p.sql("select * from {{ ref('eph_filtered') }}")
    rel.create("k")
    assert p.sql("select count(*) as n from k").fetchall() == [(1,)]

    # DDL without an ephemeral ref is untouched: no injected CTEs, nothing to collide with.
    p.sql("create temp table plain as select 1 as one")
    assert p.sql("select one from plain").fetchall() == [(1,)]


def test_a_failed_sql_does_not_poison_the_session(p):
    """dbt leaves the node it parsed for --inline behind when the compile FAILS, and it is named
    `inline_query`, so the next sql() hit "dbt found two sql_operations with the name
    inline_query" — an error about our leftovers, not about anything the caller did. One typo
    then cost a kernel restart. Only reachable because the manifest is kept warm; dbt's own CLI
    parses a fresh one every time and never sees it."""
    with pytest.raises(DbtProjectError):
        p.sql("select * from {{ ref('does_not_exist') }}")

    assert p.sql("select 1 as one").fetchall() == [(1,)]

    # And the second failure must still read like the first — same diagnosis, not a duplicate-name
    # complaint that sends you looking in the wrong place.
    with pytest.raises(DbtProjectError) as second:
        p.sql("select * from {{ ref('does_not_exist') }}")
    assert "two sql_operations" not in str(second.value)


def test_sql_and_show_do_not_disturb_each_other(p):
    """The inline node must be gone before anything else compiles against the same manifest."""
    p.sql("select 1 as one")
    assert "stg_items" in p.compiled("mart_items")
    p.sql("select 2 as two")
    assert p.ctes("cte_model")


def test_ctes_can_leave_out_the_injected_ephemeral_models(p):
    """On a project with a staging layer the injected CTEs outnumber the model's own — eighteen of
    thirty on the project this came from — so the steps you came to read get lost among them."""
    everything = p.ctes("uses_ephemeral")
    own = p.ctes("uses_ephemeral", ephemeral=False)

    assert "__dbt__cte__eph_filtered" in everything
    assert "picked" in everything
    assert own == [n for n in everything if not n.startswith("__dbt__cte__")]
    assert "picked" in own

    # Hidden from the listing only: it is really in the SQL, and slicing at it still works.
    assert p.cte("uses_ephemeral", "__dbt__cte__eph_filtered").fetchall()


def test_compiled_returns_the_sql_text(p):
    sql = p.compiled("mart_items")
    assert "stg_items" in sql and "{{" not in sql
    assert p.last_compile.model == "mart_items"
    assert p.last_compile.full_refresh is False


def test_a_path_selector_works(p, project):
    """Selectors are handed to dbt untouched, so dbt's full syntax comes along for free."""
    assert p.compiled("path:models/mart_items.sql") == p.compiled("mart_items")


def test_a_model_carrying_generic_tests_is_not_ambiguous(p):
    """dbt's `eager` indirect selection returns a model together with the tests on it, so a plain
    model name arrives here as three nodes. That is not an ambiguous selector and must not be
    reported as one — every real project puts not_null/unique on its models, which made this the
    first thing a real project hit."""
    assert "stg_items" in p.compiled("mart_items")
    assert p.last_compile.node_id.startswith("model.")


def test_a_test_can_still_be_selected_by_name(p):
    """Dropping the INDIRECT pull-in must not cost the direct one: a failing test read back as a
    relation — with real types, and filterable — is one of the better uses of this."""
    assert p.compiled("not_null_mart_items_id")
    assert p.last_compile.node_id.startswith("test.")


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


def test_dbts_own_user_yml_does_not_look_like_an_edit(p, project, capsys):
    """dbt writes `.user.yml` into the PROFILES directory — which for a duckrun project, and in a
    Fabric notebook, is the project directory. dbt's first parse creates it, so a naive mtime walk
    reports "the project changed" on the very next call and re-parses for nothing."""
    p.compiled("mart_items")
    assert (project / ".user.yml").is_file(), "precondition: dbt should have written it"
    capsys.readouterr()

    p.compiled("mart_items")
    assert "parsed" not in capsys.readouterr().out


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


def test_the_compiled_branch_is_reported(p, capsys):
    """djouallah's second condition: which branch was compiled must be visible, not silent. The
    target table exists here, so the incremental branch is what you get — and the SQL alone would
    never tell you, because rendering erases the `{% if %}`."""
    p.show("incr_items")

    # Whitespace-normalised: the assertion is about what the hint SAYS, not where it wraps.
    out = " ".join(capsys.readouterr().out.split())
    assert "incr_items: is_incremental() = True" in out
    assert "not the table's contents" in out
    assert p.last_compile.incremental is True


def test_a_model_that_does_not_branch_says_nothing(p, capsys):
    """The hint has to stay rare to stay meaningful. A model with no `is_incremental()` in its
    source has only one possible compilation, so there is nothing to disambiguate — and it must not
    pay for the second compile either."""
    p.compiled("mart_items")
    capsys.readouterr()

    p.show("mart_items")

    assert "is_incremental" not in capsys.readouterr().out
    assert p.last_compile.incremental is None


def test_the_branch_is_reported_every_time_not_once(p, capsys):
    """A hint you scrolled past three cells ago is the same as no hint."""
    p.show("incr_items")
    capsys.readouterr()

    p.show("incr_items")

    assert "is_incremental() = True" in capsys.readouterr().out


def test_asking_for_the_full_refresh_branch_is_not_lectured_at(p, capsys):
    """You typed `incremental=False`; being told the branch is the one you asked for is noise. The
    field is still set, so it can be read back."""
    p.compiled("incr_items", incremental=False)

    assert "is_incremental()" not in capsys.readouterr().out
    assert p.last_compile.incremental is False


def test_the_branch_answer_comes_from_dbt_not_from_our_own_rule(p):
    """The exact method: compile the same model with --full-refresh, which forces the branch false,
    and compare. Different text means the default compile took the incremental branch. Re-deriving
    dbt's rule (does the relation exist? was --full-refresh passed?) would be a copy that drifts."""
    default = p.compiled("incr_items")
    forced = p.compiled("incr_items", incremental=False)

    assert default != forced
    assert p.compiled("incr_items") == default        # and asking again is stable
    p.show("incr_items")
    assert p.last_compile.incremental is True


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


# ── shadowing a model with a session view ──────────────────────────────────────────────────────

def test_a_view_shadowing_a_model_does_not_survive_the_next_call(p):
    """Why there is no shadow warning and no reset().

    A `create or replace view` CAN take over the name a model is bound under, and a direct cursor
    read then answers from the override. But every method on DbtProject compiles through dbt first,
    and dbt's compile re-registers the delta_scan views — so the override is gone before it can
    mislead anyone reading through the session. A warning saying "reads no longer see the Delta
    table" would be false by the very next call.

    Pinned rather than merely deleted: if dbt ever stops re-binding here, the hazard becomes real
    and this test is what says so."""
    from deltalake import DeltaTable
    root = os.environ["DBG2_PATH"]
    before = DeltaTable(f"{root}/main/stg_items").version()

    p.show("stg_items")
    cursor = p._cursor
    cursor.sql('create or replace view "memory"."main"."stg_items" as select 99 as id')

    # Straight at the cursor, with no compile in between: the override is live.
    assert cursor.sql('select count(*) as n from "memory"."main"."stg_items"').fetchall() == [(1,)]

    p.sql("select 1 as x")            # any session call at all — it compiles, which re-binds

    assert cursor.sql('select count(*) as n from "memory"."main"."stg_items"').fetchall() == [(2,)]
    # And through all of it the lakehouse never moved.
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


# ── CTE slicing ────────────────────────────────────────────────────────────────────────────────

def test_ctes_lists_the_names_in_order(p):
    assert p.ctes("cte_model") == ["base", "allocated", "final"]


def test_cte_runs_only_up_to_that_cte(p):
    """The move a wrong-numbers model always needs: look at the intermediate step."""
    rel = p.cte("cte_model", "allocated")
    assert rel.columns == ["id", "name", "share"]
    assert sorted(rel.fetchall()) == [(1, "a", 5.25), (2, "b", 10.25)]

    final = p.show("cte_model")
    assert final.columns == ["id", "total"]        # the model itself aggregates them away


def test_cte_keeps_the_compiled_text_verbatim(p):
    """No re-parse and no re-generation, so what runs is character-for-character dbt's output —
    comments included. You debug the query, not a rendering of it."""
    p.cte("cte_model", "allocated")
    ran = p.last_compile.sql

    assert "-- allocate the amount, then halve it" in ran   # a comment BETWEEN two CTEs
    assert "select id, name, amount / 2 as share from base" in ran
    assert "final" not in ran                               # the later CTE is dropped
    assert p.last_compile.cte == "allocated"


def test_cte_is_lazy_like_show(p):
    narrowed = p.cte("cte_model", "base").filter("id = 2")
    assert type(narrowed).__name__ == "DuckDBPyRelation"
    assert narrowed.fetchall() == [(2, "b", 20.5)]


def test_an_unknown_cte_lists_the_real_ones(p):
    with pytest.raises(DbtProjectError, match="no CTE 'nope'.*base, allocated, final"):
        p.cte("cte_model", "nope")


def test_a_model_without_ctes_says_so(p):
    with pytest.raises(DbtProjectError, match="has no CTEs.*Use show"):
        p.cte("stg_items", "anything")


def test_ephemeral_models_show_up_as_ctes(p):
    """dbt has no standalone compiled form for an ephemeral model — it injects it as a CTE. So the
    thing the issue lists as an open caveat is answered by the CTE path: the ephemeral model is
    right there in ctes(), and cte() runs it."""
    names = p.ctes("uses_ephemeral")
    injected = [n for n in names if n.startswith("__dbt__cte__")]
    assert injected == ["__dbt__cte__eph_filtered"]

    rel = p.cte("uses_ephemeral", injected[0])
    assert rel.columns == ["id", "name", "amount"]
    assert sorted(rel.fetchall()) == [(2, "b", 20.5)]


def test_compiling_twice_does_not_lose_the_ephemeral_cte(p):
    """The reason `_compile` caches per manifest generation.

    dbt sets `extra_ctes_injected` on a node once it has spliced an ephemeral parent in. A second
    compile against the same manifest object rebuilds `compiled_code` from `raw_code` and skips
    re-injection — leaving SQL that still REFERENCES `__dbt__cte__<parent>` while no longer
    defining it. Silently broken, from the second call onward, for any project with ephemeral
    models. Before the cache this failed on call two."""
    first = p.compiled("uses_ephemeral")
    assert "__dbt__cte__eph_filtered as" in first

    for _ in range(3):
        assert p.compiled("uses_ephemeral") == first

    p.reload()                                     # a fresh manifest must still be correct
    assert "__dbt__cte__eph_filtered as" in p.compiled("uses_ephemeral")


def test_an_incremental_over_an_ephemeral_stays_whole_in_both_orders(p, project):
    """A model can be BOTH — and then the is_incremental() probe compiles it a second time.

    The probe answers "which branch was this?" by compiling the same model again with
    --full-refresh and comparing the text. That second compile is exactly the one dbt hands back
    without re-injecting the ephemeral parent, so without `_reset_injected_ctes` one of the two
    texts references `__dbt__cte__eph_filtered` while no longer defining it — and which one depends
    only on the order you asked in. It also made the comparison meaningless: the texts then differ
    for every such model, so the hint said "is_incremental() = True" regardless of the branch, which
    is the misreading it exists to prevent.

    Both orders, and both must agree."""
    def whole(sql):
        return "__dbt__cte__eph_filtered as" in sql and "__dbt__cte__eph_filtered" in sql

    default_first = p.compiled("incr_over_eph")
    forced_second = p.compiled("incr_over_eph", incremental=False)

    # A fresh session = a fresh manifest, so the other order gets the same clean start.
    q = duckrun.dbt_project(project)
    forced_first = q.compiled("incr_over_eph", incremental=False)
    default_second = q.compiled("incr_over_eph")

    assert whole(default_first) and whole(forced_second)
    assert whole(forced_first) and whole(default_second)
    assert default_first == default_second
    assert forced_first == forced_second
    assert default_first != forced_first          # they really are two different branches


def test_the_branch_report_is_right_for_a_model_over_an_ephemeral(p, capsys):
    """The half that fails silently: the hint itself. The target table exists (the fixture built
    it), so the incremental branch is the honest answer — and the SQL has to actually run, which is
    what proves the injected CTE survived the probe rather than merely reading right."""
    rel = p.show("incr_over_eph")

    assert rel.fetchall() == []        # `id > max(id)` over its own table: nothing new to add
    assert p.last_compile.incremental is True
    out = " ".join(capsys.readouterr().out.split())
    assert "incr_over_eph: is_incremental() = True" in out


def test_ephemeral_models_also_work_through_sql(p):
    """The other route to the same place, for when you do not want to name the generated CTE."""
    rel = p.sql("select * from {{ ref('eph_filtered') }}")
    assert sorted(rel.fetchall()) == [(2, "b", 20.5)]


def test_macro_generated_ctes_are_sliced_like_any_other(p):
    """A CTE can be emitted wholesale by a macro — name, body, comments. By the time we see it,
    macros are expanded and it is ordinary text."""
    assert "macro_made" in p.ctes("macro_model")
    rel = p.cte("macro_model", "macro_made")
    assert rel.columns == ["id", "scaled"]
    assert "-- emitted by a macro, with a comma" in p.last_compile.sql


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
