"""Parquet layout contract: what each duckrun write path must put in the files it writes.

Green CI once shipped releases whose Direct Lake first loads were ~3x slower (#84: parquet-rs
listed the dictionary page after the data pages in ``encoding_stats``). Direct Lake itself needs
Fabric, so it is not a release gate; this is. For every write path it checks the footer and the
Delta-log properties that layout rests on, on the deltalake / duckdb the job installs (cores runs
it on the newest releases and on the pyproject floors):

* every column chunk lists its dictionary page FIRST in ``encoding_stats`` (#84);
* every data page is dictionary-encoded (the full-dictionary layout), SNAPPY, no bloom filter,
  row groups within the 4M-row ceiling;
* every added file has Delta stats: ``numRecords`` equal to its parquet row count, and min/max for
  each column;
* a rewrite (UPDATE / DELETE / MERGE) keeps the rows it carries over in the table's sort order.

The delta-rs fallback paths (a change-data-feed table) write with delta-rs defaults on purpose, so
they are held to the #84 / SNAPPY / stats part only."""
import os

import deltalake
import duckdb
import pytest

import duckrun
from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.policy import ROW_GROUP_DEFAULT_ROWS

N = 200_000
COLS = ("id", "k", "s", "d", "ts", "amt", "x")
SELECT = ("select i as id, (i % 50)::int as k, 'v' || (i % 217) as s, "
          "date '2024-01-01' + (i % 365)::int as d, "
          "timestamptz '2024-01-01 00:00:00+00' + to_minutes(i % 1440) as ts, "
          "((i % 1000) / 100)::decimal(18, 2) as amt, ((i % 97) / 7)::double as x "
          "from range({lo}, {hi}) r(i)")
DICT_DATA_ENCODINGS = ("RLE_DICTIONARY", "PLAIN_DICTIONARY")


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _files(path):
    return {u.replace("file://", "") for u in deltalake.DeltaTable(path).file_uris()}


@pytest.fixture
def table(tmp_path):
    """``(conn, path)`` of table ``t``: N rows written by CTAS, sorted by (k, id)."""
    conn = duckrun.connect(str(tmp_path / "wh"), read_only=False)
    conn.sql(f"create table t sorted by (k, id) as {SELECT.format(lo=0, hi=N)}")
    return conn, f"{conn.root_path}/dbo/t"


def _footer_violations(files, *, full: bool):
    lit = ", ".join(_lit(f) for f in sorted(files))
    data_not_dict = ("len(list_filter(coalesce(encoding_stats, []), lambda e: e.page_type LIKE 'DATA_PAGE%' "
                     f"AND e.encoding NOT IN {DICT_DATA_ENCODINGS})) > 0")
    rules = [
        ("dictionary page not first in encoding_stats (#84)",
         "coalesce(encoding_stats[1].page_type, 'missing') <> 'DICTIONARY_PAGE'"),
        ("not SNAPPY", "compression <> 'SNAPPY'"),
    ]
    if full:
        rules += [
            ("a data page is not dictionary-encoded", data_not_dict),
            ("bloom filter written", "bloom_filter_offset IS NOT NULL"),
            (f"row group over {ROW_GROUP_DEFAULT_ROWS:,} rows",
             f"row_group_num_rows > {ROW_GROUP_DEFAULT_ROWS}"),
        ]
    out = []
    for what, cond in rules:
        rows = duckdb.sql(
            f"SELECT DISTINCT path_in_schema, encoding_stats::VARCHAR, compression "
            f"FROM parquet_metadata([{lit}]) WHERE {cond}").fetchall()
        out += [f"{what}: {r}" for r in rows]
    return out


def _stats_violations(path, files):
    acts = deltalake.DeltaTable(path).get_add_actions(flatten=True)  # noqa: F841 - read by DuckDB
    rel = duckdb.sql("SELECT * FROM acts")
    by_name = {}
    for row in rel.fetchall():
        rec = dict(zip(rel.columns, row))
        by_name[os.path.basename(str(rec["path"]))] = rec
    lit = ", ".join(_lit(f) for f in sorted(files))
    counts = dict(duckdb.sql(f"SELECT file_name, num_rows FROM parquet_file_metadata([{lit}])").fetchall())
    out = []
    for f in files:
        rec = by_name.get(os.path.basename(f))
        if rec is None:
            out.append(f"{f}: not in the Delta log")
            continue
        if rec.get("num_records") != counts.get(f):
            out.append(f"{f}: numRecords {rec.get('num_records')} != parquet rows {counts.get(f)}")
        for c in COLS:
            if rec.get(f"min.{c}") is None or rec.get(f"max.{c}") is None:
                out.append(f"{f}: no min/max stat for {c}")
    return out


def _descents(files, where: str = "true") -> int:
    """Adjacent pairs, in each file's physical order, that break the (k, id) sort."""
    lit = ", ".join(_lit(f) for f in sorted(files))
    return duckdb.sql(
        f"SELECT count(*) FROM (SELECT k, id, lag(k) OVER w AS pk, lag(id) OVER w AS pid FROM "
        f"(SELECT * FROM read_parquet([{lit}], filename = true, file_row_number = true) "
        f"WHERE {where}) WINDOW w AS (PARTITION BY filename ORDER BY file_row_number)) "
        f"WHERE (k, id) < (pk, pid)").fetchall()[0][0]


def _assert_contract(path, new, *, full=True):
    assert new, "the write added no data file"
    problems = _footer_violations(new, full=full) + _stats_violations(path, new)
    assert not problems, "\n".join(problems)


def test_create_table_as(table):
    conn, path = table
    new = _files(path)
    _assert_contract(path, new)
    assert _descents(new) == 0


def test_insert(table):
    conn, path = table
    before = _files(path)
    conn.sql(f"insert into t {SELECT.format(lo=N, hi=N + 50_000)}")
    _assert_contract(path, _files(path) - before)


def test_copy_on_write_update(table):
    conn, path = table
    before = _files(path)
    conn.sql("update t set s = 'updated', x = x + 1 where id % 100 = 0")
    new = _files(path) - before
    _assert_contract(path, new)
    assert _descents(new) == 0


def test_copy_on_write_delete(table):
    conn, path = table
    before = _files(path)
    conn.sql("delete from t where id % 7 = 0")
    new = _files(path) - before
    _assert_contract(path, new)
    assert _descents(new) == 0


def test_copy_on_write_merge(table):
    conn, path = table
    before = _files(path)
    conn.sql(f"merge into t using ({SELECT.format(lo=N - 1000, hi=N + 1000)}) src on t.id = src.id "
             "when matched then update set s = 'merged' when not matched then insert *")
    new = _files(path) - before
    _assert_contract(path, new)
    assert _descents(new, where=f"id < {N}") == 0     # inserted rows go last; the old rows stay sorted


def test_compaction(table):
    conn, path = table
    for lo in range(N, N + 4 * 1000, 1000):
        conn.sql(f"insert into t {SELECT.format(lo=lo, hi=lo + 1000)}")
    before = _files(path)
    engine.optimize(path)
    _assert_contract(path, _files(path) - before)


@pytest.mark.parametrize("statement", [
    "update t set s = 'cdf' where k = 3",
    "delete from t where k = 4",
    f"merge into t using ({SELECT.format(lo=0, hi=100)}) src on t.id = src.id "
    "when matched then update set s = 'cdf'",
])
def test_delta_rs_fallback(table, statement):
    conn, path = table
    deltalake.DeltaTable(path).alter.set_table_properties({"delta.enableChangeDataFeed": "true"})
    before = _files(path)
    conn.sql(statement)
    _assert_contract(path, _files(path) - before, full=False)
