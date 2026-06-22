"""``DeltaTable.convertToDelta`` — convert an existing parquet directory to Delta in place
(the delta-spark ``DeltaTable.convertToDelta`` surface).

Zero-copy: a ``_delta_log`` is written over the parquet, the data files are not rewritten. Covers
both identifier forms (``"parquet.`<path>`"`` and a bare path), the round-trip back through the
catalog, the read-only gate, and the already-converted error. All local and network-free.
"""
from pathlib import Path

import pytest

import duckrun
from duckrun import DeltaTable
from duckrun.delta_table import _parse_parquet_identifier


@pytest.fixture
def conn(tmp_path):
    return duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)


def _write_parquet(conn, dirpath: Path):
    """Stage a 2-row parquet file under ``dirpath`` (out-of-band, via the raw DuckDB connection)."""
    dirpath.mkdir(parents=True, exist_ok=True)
    target = (dirpath / "data.parquet").as_posix()
    conn._connection.execute(
        f"COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)) "
        f"TO '{target}' (FORMAT parquet)"
    )


# ---- identifier parsing (no IO) --------------------------------------------------------------
def test_parse_parquet_identifier_spark_form():
    assert _parse_parquet_identifier("parquet.`/a/b`") == "/a/b"
    assert _parse_parquet_identifier("PARQUET . `/a/b` ") == "/a/b"


def test_parse_parquet_identifier_bare_path():
    assert _parse_parquet_identifier("/a/b") == "/a/b"


def test_parse_parquet_identifier_rejects_empty():
    with pytest.raises(ValueError, match="non-empty path"):
        _parse_parquet_identifier("   ")


# ---- round-trip --------------------------------------------------------------------------------
def test_convert_spark_identifier_round_trip(conn, tmp_path):
    table_dir = tmp_path / "wh" / "dbo" / "seeded"
    _write_parquet(conn, table_dir)
    DeltaTable.convertToDelta(conn, f"parquet.`{table_dir.as_posix()}`")
    conn.refresh()
    assert conn.table("seeded").relation.order("id").fetchall() == [(1, "a"), (2, "b")]


def test_convert_bare_path_round_trip(conn, tmp_path):
    table_dir = tmp_path / "wh" / "dbo" / "bare"
    _write_parquet(conn, table_dir)
    DeltaTable.convertToDelta(conn, table_dir.as_posix())
    conn.refresh()
    assert conn.table("bare").count() == 2


def test_convert_is_zero_copy(conn, tmp_path):
    """The original parquet file survives — convert only adds a _delta_log, it does not rewrite."""
    table_dir = tmp_path / "wh" / "dbo" / "zc"
    _write_parquet(conn, table_dir)
    DeltaTable.convertToDelta(conn, table_dir.as_posix())
    assert (table_dir / "data.parquet").exists()
    assert (table_dir / "_delta_log").is_dir()


# ---- guards ------------------------------------------------------------------------------------
def test_convert_read_only_raises(tmp_path):
    ro = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=True)
    table_dir = tmp_path / "wh" / "dbo" / "blocked"
    _write_parquet(ro, table_dir)
    with pytest.raises(PermissionError):
        DeltaTable.convertToDelta(ro, table_dir.as_posix())


def test_convert_already_delta_raises(conn, tmp_path):
    table_dir = tmp_path / "wh" / "dbo" / "twice"
    _write_parquet(conn, table_dir)
    DeltaTable.convertToDelta(conn, table_dir.as_posix())
    with pytest.raises(Exception):  # delta-rs mode='error' on an already-converted dir
        DeltaTable.convertToDelta(conn, table_dir.as_posix())
