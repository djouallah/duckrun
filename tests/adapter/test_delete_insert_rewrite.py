"""Regression tests for the delete+insert rewrite (review #1/#2/#3).

Drives ``Plugin._store_delete_insert`` against a REAL local Delta table + a DuckDB cursor (the same
objects store() hands it), so the anti-join delete, the by-name projection, the column-mismatch
guard, and the fenced overwrite are all exercised for real — not mocked.
"""
import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.delta_plugin import Plugin

try:
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


def _plugin():
    p = object.__new__(Plugin)
    p._compaction_threshold = 100
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
    seed = cur.sql(
        "select (i*2) as k1, 'p' as k2, 'old' as val from range(%d) t(i)" % (n // 2)
    ).arrow()
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
