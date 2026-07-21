"""dbt-side disk-discovery behavior, through REAL `dbt run` / `docs generate`.

Two things only the adapter's discovery path (impl.list_relations_without_caching /
_with_delta_stats) provides, and that the connection-API suites cannot cover:

  * Drop-tombstone hiding. dbt itself has no DROP — but the notebook API does
    (`conn.sql("drop table x")` overwrites the table to a one-column marker, deleting nothing),
    and both surfaces share one lakehouse. If discovery surfaced the tombstone, a model with the
    dropped table's name would resolve is_incremental() == true on its next run and merge into
    the marker instead of rebuilding from scratch. Hiding it makes drop-then-dbt behave like
    CREATE-after-DROP.

  * The docs Stats panel. `dbt docs generate` enriches the catalog with stats:* columns read
    from the Delta log (_with_delta_stats); a shape regression only shows up as a silently
    empty panel, so pin it here.
"""
import json
import os
import threading
from pathlib import Path

import duckrun
import pytest

from dbt.cli.main import dbtRunner


MODEL_SQL = """{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, 'a' as name
{% if is_incremental() %}
union all select 2 as id, 'b' as name
{% endif %}
"""


@pytest.fixture
def project(tmp_path):
    """A one-model incremental project on a fresh local lakehouse root. The model emits row 1 on a
    first (full) build and rows 1+2 on an incremental build — so which branch ran is readable
    straight off the table contents."""
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        "name: disco\nversion: '1.0'\nconfig-version: 2\nprofile: disco\n"
        "model-paths: [models]\n", encoding="utf-8")
    (proj / "profiles.yml").write_text(
        "disco:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckrun\n      root_path: \"{{ env_var('DISCO_PATH') }}\"\n",
        encoding="utf-8")
    (proj / "models" / "events.sql").write_text(MODEL_SQL, encoding="utf-8")
    root = (tmp_path / "wh").as_posix()
    os.environ["DISCO_PATH"] = root
    return proj, root


def _dbt(proj: Path, *args: str):
    return dbtRunner().invoke([*args, "--project-dir", str(proj), "--profiles-dir", str(proj)])


def _rows(root):
    con = duckrun.connect(root, schema="main", read_only=True)
    return dict(con.sql("select id, name from events order by id").fetchall())


def test_dropped_table_rebuilds_instead_of_merging(project):
    proj, root = project
    # Run 1 (full build) then run 2 (incremental): discovery found the table, so the merge branch
    # ran and added row 2 — the baseline the tombstone must reset.
    assert _dbt(proj, "run").success
    assert _rows(root) == {1: "a"}
    assert _dbt(proj, "run").success
    assert _rows(root) == {1: "a", 2: "b"}

    # Drop via the notebook surface: a one-column tombstone marker, files untouched on disk.
    duckrun.connect(root, schema="main", read_only=False).sql("drop table events")
    assert (Path(root) / "main" / "events" / "_delta_log").is_dir()  # nothing was deleted

    # Run 3: discovery must HIDE the tombstone, so this is a first-run full build again — row 1
    # only, real schema. If the tombstone leaked through, is_incremental() would be true and the
    # run would merge {1,2} into the marker (failure or a marker column in the result).
    assert _dbt(proj, "run").success
    assert _rows(root) == {1: "a"}
    cols = {r[0] for r in duckrun.connect(root, schema="main")
            .sql("describe events").fetchall()}
    assert cols == {"id", "name"}  # no __duckrun_deleted__ marker resurrected


def test_show_discovery_opens_concurrently_and_creates_schema_once(project, monkeypatch):
    """Issue #16: `dbt show` of a trivial model took ~30s because discovery replayed each
    table's Delta log serially (one delta-rs open per relation) and re-created the schema once
    per relation. Pin the fix through the real command: the per-relation opens run off the main
    thread (the concurrent pool), the schema is created once per schema, and discovery still
    hides tombstones and registers views (a multi-table schema with a drop in it works)."""
    proj, root = project
    assert _dbt(proj, "run").success  # materialize `events` so discovery has it too

    # More tables via the notebook surface, plus one tombstone among them.
    con = duckrun.connect(root, schema="main", read_only=False)
    for i in range(4):
        con.sql(f"create table t{i} as select {i} as id")
    con.sql("drop table t3")

    from dbt.adapters.duckrun import engine as dr_engine
    from dbt.adapters.duckrun.impl import DuckrunAdapter

    main_id = threading.get_ident()
    opens, schema_creates = [], []
    real_open = dr_engine._delta_table
    real_cs = DuckrunAdapter.create_schema
    monkeypatch.setattr(dr_engine, "_delta_table", lambda path, so: (
        opens.append((Path(str(path)).name, threading.get_ident())), real_open(path, so))[1])
    monkeypatch.setattr(DuckrunAdapter, "create_schema", lambda self, relation: (
        schema_creates.append(str(relation)), real_cs(self, relation))[1])

    assert _dbt(proj, "show", "-s", "events").success

    # Every discovered table's log open happened off the main thread — the concurrent pool.
    off_main = {name for name, tid in opens if tid != main_id}
    assert off_main >= {"events", "t0", "t1", "t2", "t3"}
    # One schema in the project → exactly one create_schema, not one per relation.
    assert len(schema_creates) == 1


def test_show_discovery_batches_across_schemas_and_binds_views_concurrently(project, monkeypatch):
    """Issue #16, round 2: after the per-schema delta-rs opens went concurrent, a multi-schema
    project still paid the tax schema-by-schema (dbt lists every manifest schema serially on
    duckrun's single thread), and the delta_scan view registrations — whose CREATE binds by
    replaying the Delta log a second time — stayed serial. Pin the batching fix through the real
    command: ONE cross-schema open pool for the whole cache population, and every view bind off
    the main thread (the concurrent registration pool)."""
    proj, root = project
    # A second model in a custom schema so the manifest spans two physical schemas.
    (proj / "models" / "other.sql").write_text(
        "{{ config(materialized='table', schema='x2') }}\nselect 42 as answer",
        encoding="utf-8")
    assert _dbt(proj, "run").success

    # More tables in the default schema so the view-registration batch is a real pool (>1).
    con = duckrun.connect(root, schema="main", read_only=False)
    for i in range(2):
        con.sql(f"create table t{i} as select {i} as id")

    from dbt.adapters.duckrun import engine as dr_engine
    from dbt.adapters.duckrun.impl import DuckrunAdapter

    main_id = threading.get_ident()
    open_batches, view_regs = [], []
    real_open = dr_engine.open_delta_tables
    real_reg = DuckrunAdapter._register_delta_view
    monkeypatch.setattr(dr_engine, "open_delta_tables", lambda targets: (
        open_batches.append([loc for loc, _ in targets]), real_open(targets))[1])
    monkeypatch.setattr(DuckrunAdapter, "_register_delta_view", lambda self, relation, dt=None,
                        cursor=None: (view_regs.append((str(relation), threading.get_ident())),
                                      real_reg(self, relation, dt=dt, cursor=cursor))[1])

    assert _dbt(proj, "show", "-s", "events").success

    # The whole cache population opened its Delta logs in ONE cross-schema batch that spans
    # both physical schemas — not one pool per schema.
    non_empty = [b for b in open_batches if b]
    assert len(non_empty) == 1
    schemas_in_batch = {Path(loc).parent.name for loc in non_empty[0]}
    assert schemas_in_batch == {"main", "main_x2"}
    # Every discovered table's view got registered, and every bind ran off the main thread —
    # serial binds on the shared cursor were the residual per-schema cost.
    assert {name for name, _ in view_regs} >= {'"memory"."main"."events"',
                                               '"memory"."main"."t0"', '"memory"."main"."t1"',
                                               '"memory"."main_x2"."other"'}
    assert all(tid != main_id for _, tid in view_regs)


def test_docs_stats_panel_reads_the_delta_log(project):
    proj, root = project
    assert _dbt(proj, "run").success
    assert _dbt(proj, "run").success          # table now holds 2 rows
    assert _dbt(proj, "docs", "generate").success

    catalog = json.loads((proj / "target" / "catalog.json").read_text(encoding="utf-8"))
    node = catalog["nodes"]["model.disco.events"]
    stats = node["stats"]
    # The three Delta-log-sourced stats are present and included, with the real row count.
    assert stats["num_rows"]["include"] is True
    assert int(stats["num_rows"]["value"]) == 2
    assert stats["bytes"]["include"] is True and int(stats["bytes"]["value"]) > 0
    assert stats["last_modified"]["include"] is True
    # And the relation itself is reported as a table (the delta_scan view classification).
    assert node["metadata"]["type"] == "BASE TABLE"
