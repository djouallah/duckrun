"""``DuckSession.createDataFrame`` — the in-memory DataFrame constructor (createDataFrame API).

Covers every supported input/schema form (tuples ± names ± DDL, scalars, pandas ± schema,
pyarrow, empty + DDL) plus a Delta round-trip proving the data lands on disk. All local and
network-free, mirroring the rest of the connection_api suite. The constructor runs on duckrun's
own DuckDB connection — a guard test asserts it pulls in no ``duckdb.experimental.spark``.
"""
from pathlib import Path

import pytest

import duckrun
import duckrun.session as session_mod


@pytest.fixture
def conn(tmp_path):
    """A writable local-fs session (createDataFrame itself never writes; saveAsTable needs it)."""
    return duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)


def _types(df):
    return [str(t) for t in df.relation.types]


# ---- tuples ----------------------------------------------------------------------------------
def test_tuples_no_schema_autonames(conn):
    df = conn.createDataFrame([(1, "a"), (2, "b")])
    assert df.columns == ["_1", "_2"]
    assert df.collect() == [(1, "a"), (2, "b")]


def test_tuples_with_names(conn):
    df = conn.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert df.columns == ["id", "name"]
    assert df.count() == 2


def test_tuples_with_ddl_casts_types(conn):
    df = conn.createDataFrame([(1, "a")], "id int, name string")
    assert df.columns == ["id", "name"]
    assert _types(df) == ["INTEGER", "VARCHAR"]


def test_ddl_colon_spelling(conn):
    df = conn.createDataFrame([(1, "a")], "id: int, name: string")
    assert df.columns == ["id", "name"]
    assert _types(df) == ["INTEGER", "VARCHAR"]


def test_ddl_decimal_with_comma_survives(conn):
    df = conn.createDataFrame([(1, "1.50")], "id long, amount decimal(10,2)")
    assert _types(df) == ["BIGINT", "DECIMAL(10,2)"]
    assert df.collect() == [(1, pytest.approx(1.50))]


def test_list_of_scalars_single_column(conn):
    df = conn.createDataFrame([1, 2, 3], "value int")
    assert df.columns == ["value"]
    assert [r[0] for r in df.collect()] == [1, 2, 3]


def test_ragged_rows_error(conn):
    with pytest.raises(ValueError, match="same number of columns"):
        conn.createDataFrame([(1, "a"), (2,)])


# ---- pandas ----------------------------------------------------------------------------------
def test_pandas(conn):
    pd = pytest.importorskip("pandas")
    df = conn.createDataFrame(pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    assert df.columns == ["id", "name"]
    assert df.count() == 2


def test_pandas_with_schema_rename(conn):
    pd = pytest.importorskip("pandas")
    df = conn.createDataFrame(pd.DataFrame({"x": [1], "y": ["a"]}), ["id", "name"])
    assert df.columns == ["id", "name"]


# ---- pyarrow ---------------------------------------------------------------------------------
def test_pyarrow_table(conn):
    pa = pytest.importorskip("pyarrow")
    df = conn.createDataFrame(pa.table({"id": [1, 2], "name": ["a", "b"]}))
    assert df.columns == ["id", "name"]
    assert df.count() == 2


# ---- empty -----------------------------------------------------------------------------------
def test_empty_with_ddl(conn):
    df = conn.createDataFrame([], "id int, name string")
    assert df.columns == ["id", "name"]
    assert _types(df) == ["INTEGER", "VARCHAR"]
    assert df.count() == 0


def test_empty_without_schema_errors(conn):
    with pytest.raises(ValueError, match="empty dataset"):
        conn.createDataFrame([])


# ---- schema validation -----------------------------------------------------------------------
def test_name_count_mismatch_errors(conn):
    with pytest.raises(ValueError, match="columns"):
        conn.createDataFrame([(1, "a")], ["only_one"])


def test_bad_schema_type_errors(conn):
    with pytest.raises(TypeError, match="schema must be"):
        conn.createDataFrame([(1,)], schema=123)


# ---- round-trip to Delta ---------------------------------------------------------------------
def test_round_trip_to_delta(conn):
    conn.createDataFrame([(1, "a"), (2, "b")], "id int, name string") \
        .write.mode("overwrite").saveAsTable("seeded")
    fresh = duckrun.connect(conn.root_path, schema="dbo", read_only=True)
    back = fresh.table("seeded").relation.order("id").fetchall()
    assert back == [(1, "a"), (2, "b")]


# ---- no experimental dependency --------------------------------------------------------------
def test_no_experimental_spark_import():
    src = Path(session_mod.__file__).read_text(encoding="utf-8")
    assert "experimental.spark" not in src
