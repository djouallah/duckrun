"""Regression tests for the DuckDB-computed incremental rewrites (review #1/#2/#3).

Two strategies compute their result in DuckDB and hand delta_rs one plain write:
``delete+insert`` (``_store_delete_insert`` — anti-join + fenced whole-table overwrite) and
``insert`` (``_store_insert`` — anti-join + fenced plain APPEND, no target file rewritten).

Both are driven against a REAL local Delta table + a DuckDB cursor (the same objects store() hands
them), so the anti-join, the by-name projection, the column-mismatch guard, and the fence are all
exercised for real — not mocked.
"""
import types

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.delta_plugin import Plugin

try:
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


def _plugin():
    p = object.__new__(Plugin)
    return p


def _read(path):
    """Rows of the Delta table at ``path`` as a set of tuples (order-independent)."""
    return DeltaTable(path).to_pyarrow_table()


@pytest.fixture
def cur():
    return duckdb.connect()


def test_delete_insert_reordered_select_keeps_values_per_column(cur, tmp_path):
    # Target table: id, a, b (all VARCHAR so a positional UNION would NOT error — it would silently
    # shift values, the exact corruption #1 fixes).
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({
        "id": ["1", "2", "3"],
        "a": ["a1", "a2", "a3"],
        "b": ["b1", "b2", "b3"],
    }))
    # Incoming batch for keys 2 and 4, with the SELECT columns DELIBERATELY REORDERED (b, id, a).
    cur.execute(
        "create or replace temp view batch as "
        "select * from (values ('B2','2','A2'), ('B4','4','A4')) v(b, id, a)"
    )
    _plugin()._store_delete_insert(path, cur, "batch", "id", None,
                                   read_version=DeltaTable(path).version())
    got = {r["id"]: (r["a"], r["b"]) for r in _read(path).to_pylist()}
    # Key 2 replaced with its new values in the RIGHT columns (not shifted); 4 inserted; 1,3 kept.
    assert got == {
        "1": ("a1", "b1"),
        "2": ("A2", "B2"),
        "3": ("a3", "b3"),
        "4": ("A4", "B4"),
    }


def test_delete_insert_column_mismatch_raises(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select '2' as id, 'y' as a, 'z' as extra")
    with pytest.raises(CompilationError) as exc:
        _plugin()._store_delete_insert(path, cur, "batch", "id", None,
                                       read_version=DeltaTable(path).version())
    assert "extra" in str(exc.value)


def test_delete_insert_large_composite_batch_scales(cur, tmp_path):
    # A big batch keyed on (k1, k2): the anti-join must NOT materialize the key set into a giant SQL
    # string (review #2). 200k keys would be a multi-MB IN-list under the old code; here it's one
    # bounded statement. Assert it runs and the result is correct.
    n = 200_000
    path = (tmp_path / "t").as_posix()
    # Seed target with the even keys; batch re-emits ALL keys with a new value + adds none.
    # Pass the DuckDB relation straight to write_deltalake (its Arrow C-stream) — no pyarrow .arrow().
    seed = cur.sql(
        "select (i*2) as k1, 'p' as k2, 'old' as val from range(%d) t(i)" % (n // 2)
    )
    write_deltalake(path, seed)
    cur.execute(
        "create or replace temp view batch as "
        "select i as k1, 'p' as k2, 'new' as val from range(%d) t(i)" % n
    )
    _plugin()._store_delete_insert(path, cur, "batch", ["k1", "k2"], None,
                                   read_version=DeltaTable(path).version())
    # Every target row whose key is in the batch was deleted; the whole batch was inserted → exactly
    # n rows, all 'new'. (The even keys existed and were replaced; odd keys are new.)
    tbl = _read(path)
    assert tbl.num_rows == n
    vals = set(tbl.column("val").to_pylist())
    assert vals == {"new"}


def test_delete_insert_empty_batch_is_noop(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1", "2"], "a": ["x", "y"]}))
    v0 = DeltaTable(path).version()
    cur.execute("create or replace temp view batch as select * from (values ('1','x')) v(id, a) where 1=0")
    _plugin()._store_delete_insert(path, cur, "batch", "id", None, read_version=v0)
    # No write happened at all (version unchanged), and rows are intact.
    assert DeltaTable(path).version() == v0
    assert _read(path).num_rows == 2


# --------------------------------------------------------------------------- insert (insert-only)
#
# `incremental_strategy='insert'` is the one incremental shape that never removes a row, so it is a
# pure append: DuckDB anti-joins the batch against the target's key columns and delta_rs commits
# `add` actions only. These tests pin that it is byte-for-byte the same table delta_rs's
# `when_not_matched_insert_all` produces, and that no target file is ever rewritten.


def _files(path):
    """The Delta table's current data-file paths."""
    return set(pa.table(DeltaTable(path).get_add_actions(flatten=True))["path"].to_pylist())


def _seed_partitioned(cur, path):
    """A month-partitioned target with a NULL key, and a batch of one existing key, one new key and
    one NULL key — the shape the aemo fact tables use."""
    write_deltalake(path, pa.table({
        "id": ["1", "2", "3", None],
        "month_key": [202601, 202601, 202602, 202602],
        "a": ["a1", "a2", "a3", "a4"],
    }), partition_by=["month_key"])
    cur.execute(
        "create or replace temp view batch as select * from (values "
        "('A2','2',202601), ('A4','4',202602), ('A5',NULL,202601)) v(a, id, month_key)"
    )


def _rows(path):
    return sorted(DeltaTable(path).to_pyarrow_table().to_pylist(),
                  key=lambda r: (str(r["id"]), r["a"]))


def test_insert_matches_delta_rs_insert_only_merge_row_for_row(cur, tmp_path):
    """THE equivalence proof: the same batch through the DuckDB anti-join and through delta_rs's
    insert-only merge must produce identical tables — including the NULL-key rule (a NULL key never
    matches, so the source row inserts and the target row survives)."""
    duck, drs = (tmp_path / "duck").as_posix(), (tmp_path / "drs").as_posix()
    _seed_partitioned(cur, duck)
    _seed_partitioned(cur, drs)
    data = cur.sql("select * from batch")
    preds = ["target.month_key = source.month_key"]

    _plugin()._store_insert(duck, cur, "batch", data, ["id"], None, read_version=0,
                            partition_by=["month_key"], incremental_predicates=preds)
    engine.merge_delta(drs, data, ["id"], insert_only=True, read_version=0, predicates=preds)

    assert _rows(duck) == _rows(drs)
    # Existing key 2 kept its ORIGINAL value (insert-only never updates); key 4 is new.
    got = {(r["id"], r["a"]) for r in _rows(duck)}
    assert ("2", "a2") in got and ("4", "A4") in got


def test_insert_rewrites_no_existing_file(cur, tmp_path):
    """The cost model, asserted: an insert-only append adds files and removes none."""
    path = (tmp_path / "t").as_posix()
    _seed_partitioned(cur, path)
    before = _files(path)
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), ["id"], None,
                            read_version=0, partition_by=["month_key"])
    assert before <= _files(path)


def test_insert_reordered_select_keeps_values_per_column(cur, tmp_path):
    # All VARCHAR so a positional append would NOT error — it would silently shift values.
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["a1"], "b": ["b1"]}))
    cur.execute("create or replace temp view batch as "
                "select * from (values ('B2','2','A2')) v(b, id, a)")
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=DeltaTable(path).version())
    got = {r["id"]: (r["a"], r["b"]) for r in _read(path).to_pylist()}
    assert got == {"1": ("a1", "b1"), "2": ("A2", "B2")}


def test_insert_is_idempotent_and_writes_no_commit(cur, tmp_path):
    """A re-run of an already-loaded batch inserts nothing AND does not move the Delta version —
    where a delta_rs merge would commit a no-op version."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1", "2"], "a": ["x", "y"]}))
    cur.execute("create or replace temp view batch as select * from (values ('2','Y'),('3','z')) v(id, a)")
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=DeltaTable(path).version())
    v1 = DeltaTable(path).version()
    assert _read(path).num_rows == 3

    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=v1)
    assert DeltaTable(path).version() == v1
    assert _read(path).num_rows == 3


def test_insert_empty_batch_is_noop(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    v0 = DeltaTable(path).version()
    cur.execute("create or replace temp view batch as select * from (values ('1','x')) v(id, a) where 1=0")
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=v0)
    assert DeltaTable(path).version() == v0


def test_insert_duplicate_source_keys_raise(cur, tmp_path):
    """The keyed-merge cardinality rule, shared with every delta_rs merge path."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select * from (values ('2','p'),('2','q')) v(id, a)")
    with pytest.raises(ValueError) as exc:
        _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                                read_version=DeltaTable(path).version())
    assert "not unique on the join key" in str(exc.value)


def test_insert_column_mismatch_raises(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select '2' as id, 'y' as a, 'z' as extra")
    with pytest.raises(CompilationError) as exc:
        _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                                read_version=DeltaTable(path).version())
    assert "extra" in str(exc.value)


def test_insert_evolves_schema_when_on_schema_change_allows(cur, tmp_path):
    """merge_schema=True (what on_schema_change resolves to) lets a new column through; existing
    rows read NULL for it."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select '2' as id, 'y' as a, 'z' as extra")
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=DeltaTable(path).version(), merge_schema=True)
    got = {r["id"]: r.get("extra") for r in _read(path).to_pylist()}
    assert got == {"1": None, "2": "z"}


def test_insert_is_fenced_against_a_concurrent_commit(cur, tmp_path):
    """The anti-join READS the target, so the append must be pinned to the version it read: a writer
    that commits in between makes the anti-join stale and would let a duplicate through."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    stale = DeltaTable(path).version()
    write_deltalake(path, pa.table({"id": ["9"], "a": ["z"]}), mode="append")  # the racing writer
    cur.execute("create or replace temp view batch as select '2' as id, 'y' as a")
    with pytest.raises(CommitFailedError):
        _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                                read_version=stale)


def test_insert_honors_merge_insert_condition(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select * from (values ('2','keep'),('3','drop')) v(id, a)")
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                            read_version=DeltaTable(path).version(),
                            insert_condition="DBT_INTERNAL_SOURCE.a = 'keep'")
    assert {r["id"] for r in _read(path).to_pylist()} == {"1", "2"}


def test_insert_rejects_insert_condition_referencing_the_target(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    cur.execute("create or replace temp view batch as select '2' as id, 'y' as a")
    with pytest.raises(CompilationError) as exc:
        _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), "id", None,
                                read_version=DeltaTable(path).version(),
                                insert_condition="DBT_INTERNAL_DEST.a = 'x'")
    assert "no target to read" in str(exc.value)


def test_insert_composite_key_and_incremental_predicate(cur, tmp_path):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({
        "k1": ["a", "a"], "k2": [1, 2], "month_key": [202601, 202602], "v": ["old1", "old2"],
    }), partition_by=["month_key"])
    cur.execute(
        "create or replace temp view batch as select * from (values "
        "('a',1,202601,'new1'), ('a',3,202601,'new3')) v(k1, k2, month_key, v)"
    )
    _plugin()._store_insert(path, cur, "batch", cur.sql("select * from batch"), ["k1", "k2"], None,
                            read_version=DeltaTable(path).version(), partition_by=["month_key"],
                            incremental_predicates=["target.month_key = source.month_key"])
    got = {(r["k1"], r["k2"]): r["v"] for r in _read(path).to_pylist()}
    assert got == {("a", 1): "old1", ("a", 2): "old2", ("a", 3): "new3"}


# --- the constant probe filters (what makes the target probe skip files) --------------------------
#
# Every filter here is derived ONLY from a declared equality, and is result-neutral for that reason:
# the EXISTS body requires `t.k = s.k`, so a target row outside the source's value set / min-max range
# (or NULL) could never have matched.


@pytest.mark.parametrize("part_type, values, expected", [
    ("INTEGER", "202601, 202602", '"month_key" IN (202601, 202602)'),
    ("VARCHAR", "'nsw', 'vic'", "\"month_key\" IN ('nsw', 'vic')"),
    ("DATE", "DATE '2026-01-01'", "\"month_key\" IN ('2026-01-01')"),
])
def test_probe_filters_render_partition_value_sets(cur, part_type, values, expected):
    """A PARTITION column joined by equality gets its exact value set — an IN list beats a range, and
    a bimodal source (an old backfill unioned with the current feed) would smear a min/max bound
    across every partition in between."""
    cur.execute(f"create or replace temp view batch as select unnest([{values}])::{part_type} as month_key")
    assert Plugin._probe_filters(cur, "batch", ["month_key"], ["month_key"]) == [expected]


def test_probe_filters_range_bound_a_non_partition_key(cur):
    """A high-cardinality key column gets the source's min/max instead — the same early filter
    delta_rs derives from source statistics, so the probe skips files by their Delta stats rather than
    reading the whole key column."""
    cur.execute("create or replace temp view batch as select i as id from range(100, 200) t(i)")
    assert Plugin._probe_filters(cur, "batch", None, ["id"]) == ['"id" >= 100 AND "id" <= 199']


def test_probe_filters_collapse_a_single_valued_range_to_equality(cur):
    cur.execute("create or replace temp view batch as select 7 as id from range(3)")
    assert Plugin._probe_filters(cur, "batch", None, ["id"]) == ['"id" = 7']


def test_probe_filters_require_the_column_to_be_join_keyed(cur):
    """Result-neutrality depends on the equality being declared: a partition column that is NOT
    equality-joined contributes nothing, because pruning on it could drop a real match."""
    cur.execute("create or replace temp view batch as select 202601 as month_key, 'x' as id")
    got = Plugin._probe_filters(cur, "batch", ["month_key"], ["id"])
    assert got == ['"id" = \'x\'']            # only the joined key, never month_key
    assert Plugin._probe_filters(cur, "batch", ["month_key"], []) == []


def test_probe_filters_skip_an_all_null_key(cur):
    cur.execute("create or replace temp view batch as select NULL::INTEGER as id")
    assert Plugin._probe_filters(cur, "batch", None, ["id"]) == []


# --- routing: which strategies reach the DuckDB append, and what stays on delta_rs ---------------


def _store_target_config(path, relation_name, cfg):
    return types.SimpleNamespace(
        location=types.SimpleNamespace(path=path),
        config=cfg,
        relation=types.SimpleNamespace(render=lambda: relation_name),
    )


def _store_plugin(con):
    p = Plugin.__new__(Plugin)
    p._storage_options = None
    p._catalogs = {}
    p._default_database = None
    p._conn = con
    p._cursor_handle = con
    p._baseline_memory_limit = None
    p._microbatch_seen = set()
    return p


@pytest.mark.parametrize("cfg_extra, expect_duckdb", [
    ({}, True),
    # merge_clauses routes to delta_rs's ordered clause list, which has no anti-join form. Spelling
    # insert-only as an explicit clause list is therefore the documented way back to the old path.
    ({"merge_clauses": {"when_not_matched": [{"action": "insert"}]}}, False),
])
def test_insert_strategy_routes_to_the_duckdb_append(tmp_path, monkeypatch, cfg_extra, expect_duckdb):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")

    seen = []
    monkeypatch.setattr(Plugin, "_store_insert",
                        lambda self, *a, **k: seen.append("duckdb"))
    monkeypatch.setattr(Plugin, "_store_merge",
                        lambda self, *a, **k: seen.append("delta_rs"))

    cfg = {"incremental": True, "full_refresh": False, "dbt_believes_exists": True,
           "incremental_strategy": "insert", "unique_key": "id",
           "read_version": DeltaTable(path).version()}
    cfg.update(cfg_extra)
    _store_plugin(con).store(_store_target_config(path, "increment", cfg))
    assert seen == (["duckdb"] if expect_duckdb else ["delta_rs"])


def test_insert_strategy_forwards_the_merge_overrides(tmp_path, monkeypatch):
    """incremental_strategy='insert' forwards merge_max_spill_size / merge_max_temp_directory_size /
    merge_streamed_exec to engine.merge_delta. streamed_exec matters most: it is the documented way
    back to a real delta_rs merge for this spelling (the engine diverts to the anti-join only when
    it is False), and it used to be silently dropped on this path; the spill caps bound the pool if
    the engine falls through to delta_rs (AntiJoinUnsupported)."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")

    seen = {}
    monkeypatch.setattr(engine, "merge_delta", lambda *a, **k: seen.update(k))
    _store_plugin(con).store(_store_target_config(path, "increment", {
        "incremental": True, "full_refresh": False, "dbt_believes_exists": True,
        "incremental_strategy": "insert", "unique_key": "id",
        "merge_max_spill_size": 123, "merge_max_temp_directory_size": 456,
        "merge_streamed_exec": True,
        "read_version": DeltaTable(path).version(),
    }))
    assert seen.get("max_spill_size") == 123
    assert seen.get("max_temp_directory_size") == 456
    assert seen.get("streamed_exec") is True


def test_model_timestamp_ntz_reaches_the_write_seam(tmp_path, monkeypatch):
    """issue #42: `+timestamp_ntz: true` travels macro dict → store() → the engine write kwargs,
    and the handle store() already opened rides along as existing_dt, so the target-aware
    naive-timestamp skip never pays a second log open on the dbt surface."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")

    seen = {}
    monkeypatch.setattr(engine, "write_delta", lambda *a, **k: seen.update(k))
    _store_plugin(con).store(_store_target_config(path, "increment", {
        "incremental": True, "full_refresh": False, "dbt_believes_exists": True,
        "incremental_strategy": "append", "timestamp_ntz": True,
    }))
    assert seen.get("timestamp_ntz") is True
    assert seen.get("existing_dt") is not None  # the store-time handle, reused


def test_merge_strategy_still_routes_to_delta_rs(tmp_path, monkeypatch):
    """Scope guard: only insert-only moved. A true upsert must remove old row versions, which can
    never be a plain append, so `merge` is untouched."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")
    seen = []
    monkeypatch.setattr(Plugin, "_store_insert", lambda self, *a, **k: seen.append("duckdb"))
    monkeypatch.setattr(Plugin, "_store_merge", lambda self, *a, **k: seen.append("delta_rs"))
    _store_plugin(con).store(_store_target_config(path, "increment", {
        "incremental": True, "full_refresh": False, "dbt_believes_exists": True,
        "incremental_strategy": "merge", "unique_key": "id",
        "read_version": DeltaTable(path).version(),
    }))
    assert seen == ["delta_rs"]


def test_clause_merge_forwards_partition_by_and_sort_by(tmp_path, monkeypatch):
    """A merge_clauses list that is insert-only (dbt-duckdb's `when_matched: do_nothing`, #20) is
    routed to the anti-join + plain append at the engine seam, and that append needs the model's
    `partition_by` for the exact partition IN probe filter and `sort_by` for the write order. The
    merge path used to drop both (a merge writes into whatever partitioning exists), which made the
    portable spelling quietly prune worse than `incremental_strategy='insert'`. They are inert on the
    delta_rs merge branch, which never reads them."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}), partition_by=["id"])
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")

    seen = {}
    monkeypatch.setattr(engine, "merge_delta_clauses", lambda *a, **k: seen.update(k))
    _store_plugin(con).store(_store_target_config(path, "increment", {
        "incremental": True, "full_refresh": False, "dbt_believes_exists": True,
        "incremental_strategy": "merge", "unique_key": "id",
        "partition_by": ["id"], "sort_by": ["a"],
        "merge_clauses": {"when_matched": [{"action": "do_nothing"}]},
        "read_version": DeltaTable(path).version(),
    }))
    assert seen.get("partition_by") == ["id"] and seen.get("sort_by") == ["a"]


# `sort_by='auto'` PROFILES the staged relation to pick a key — the expensive part of the whole
# feature. Three branches then discard the answer (see the test below), so with a project-wide
# `+sort_by: auto` every incremental run of every merge model used to pay that profile for nothing.
# Each row: (cfg extras, does the branch this resolves to actually honor sort_by?).
@pytest.mark.parametrize("cfg_extra, profiles", [
    # Inert — the branch reads the staged relation NAME, or writes into the target's existing layout.
    ({"incremental_strategy": "merge", "unique_key": "id"}, False),
    ({"incremental_strategy": None, "unique_key": "id"}, False),          # None => merge
    ({"incremental_strategy": "delete+insert", "unique_key": "id"}, False),
    ({"incremental_strategy": "microbatch", "unique_key": "id"}, False),
    # Honored — these lay out the rows they write, so the key has to be picked.
    ({"incremental_strategy": "insert", "unique_key": "id"}, True),
    ({"incremental_strategy": "append"}, True),
    # A custom clause list may route to the insert-only anti-join + append, which DOES sort. The
    # gate is deliberately conservative and keeps profiling rather than reasoning about the clauses.
    ({"incremental_strategy": "merge", "unique_key": "id",
      "merge_clauses": {"when_matched": [{"action": "do_nothing"}]}}, True),
])
def test_sort_by_auto_only_profiles_when_the_write_path_sorts(tmp_path, monkeypatch, cfg_extra,
                                                              profiles):
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")

    called = []
    monkeypatch.setattr(engine, "auto_sort_cols",
                        lambda *a, **k: called.append(a[1]) or ([], []))
    # Stop at the dispatch: this asserts what the ROUTING decided, not what each branch writes.
    for meth in ("_store_merge", "_store_insert", "_store_delete_insert", "_store_microbatch",
                 "_store_append"):
        monkeypatch.setattr(Plugin, meth, lambda self, *a, **k: None)

    cfg = {"incremental": True, "full_refresh": False, "dbt_believes_exists": True,
           "sort_by": "auto", "read_version": DeltaTable(path).version(), **cfg_extra}
    _store_plugin(con).store(_store_target_config(path, "increment", cfg))
    assert bool(called) is profiles, f"auto_sort_cols called={called!r}, expected profiles={profiles}"


def test_sort_by_auto_in_a_list_still_raises_on_an_inert_path(tmp_path):
    """Skipping the PROFILE must not skip the VALIDATION: `sort_by: ['auto']` is a typo on every
    path, and a merge model happening not to sort is no reason to let it through quietly."""
    path = (tmp_path / "t").as_posix()
    write_deltalake(path, pa.table({"id": ["1"], "a": ["x"]}))
    con = duckdb.connect()
    con.execute("create view increment as select '2' as id, 'y' as a")
    with pytest.raises(ValueError, match="must be the scalar value"):
        _store_plugin(con).store(_store_target_config(path, "increment", {
            "incremental": True, "full_refresh": False, "dbt_believes_exists": True,
            "incremental_strategy": "merge", "unique_key": "id", "sort_by": ["auto"],
            "read_version": DeltaTable(path).version(),
        }))


def test_probe_filters_fall_back_to_a_range_over_the_value_cap(cur):
    """Past the IN-list cap the exact set stops helping, but a min/max bound still does — so the
    column degrades to a range rather than contributing nothing."""
    n = engine._PART_PRUNE_MAX + 1
    cur.execute(
        "create or replace temp view batch as select i as month_key from range(%d) t(i)" % n
    )
    assert Plugin._probe_filters(cur, "batch", ["month_key"], ["month_key"]) == [
        '"month_key" >= 0 AND "month_key" <= %d' % (n - 1)
    ]
