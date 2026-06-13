"""Local, network-free coverage for the top-level connection API (``duckrun.connect``).

Exercises the whole Spark-shaped surface against a Delta ``./wh`` on the local filesystem:
discovery + catalog, the ``DataFrameWriter`` save modes, the ``DataFrameReader``, and the
``DeltaTable.merge`` upsert builder. Storage-neutrality (s3/gcs/abfss) shares this exact code
path — only the secret/discovery backend differs — so the local run is representative.
"""
import duckdb
import pytest

import duckrun
from duckrun import DeltaTable
from dbt.adapters.duckrun import engine


def _seed(path, sql):
    """Write a Delta table at ``path`` from a one-off DuckDB relation."""
    con = duckdb.connect()
    engine.write_delta(path, con.sql(sql), mode="overwrite")
    con.close()


@pytest.fixture
def wh(tmp_path):
    root = tmp_path / "wh"
    _seed(str(root / "dbo" / "t1"), "select * from (values (1,'a'),(2,'b')) t(id, name)")
    _seed(str(root / "dbo" / "t2"), "select 42 as answer")
    return str(root)


def test_discovery_and_catalog(wh):
    conn = duckrun.connect(wh, schema="dbo")
    assert set(conn.catalog.listTables()) == {"t1", "t2"}
    assert conn.catalog.currentDatabase() == "dbo"
    # SHOW TABLES works for free (native DuckDB over the registered views).
    shown = {r[0] for r in conn.sql("SHOW TABLES").fetchall()}
    assert {"t1", "t2"} <= shown
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2


def test_discover_all_schemas(wh):
    # Add a second schema folder, then connect with no schema → discover everything.
    _seed(wh + "/sales/orders", "select 7 as n")
    conn = duckrun.connect(wh)
    assert set(conn.catalog.listDatabases()) == {"dbo", "sales"}
    assert conn.sql("select n from sales.orders").fetchone()[0] == 7


def test_write_modes_round_trip(wh):
    conn = duckrun.connect(wh, schema="dbo")

    conn.sql("select 1 as id, 'x' as v").write.mode("overwrite").saveAsTable("t3")
    assert conn.sql("select count(*) from t3").fetchone()[0] == 1

    conn.sql("select 2 as id, 'y' as v").write.mode("append").saveAsTable("t3")
    assert conn.table("t3").count() == 2

    # Spark default mode is 'error' → refuse to clobber an existing table.
    with pytest.raises(ValueError):
        conn.sql("select 3 as id, 'z' as v").write.saveAsTable("t3")

    # 'ignore' is a no-op when the table exists.
    conn.sql("select 99 as id, 'q' as v").write.mode("ignore").saveAsTable("t3")
    assert conn.table("t3").count() == 2


def test_write_partitioned_and_merge_schema(wh):
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'eu' region").write.mode("overwrite").partitionBy("region").saveAsTable("p")
    conn.sql("select 2 id, 'us' region, true flag") \
        .write.mode("append").option("mergeSchema", "true").partitionBy("region").saveAsTable("p")
    assert conn.table("p").count() == 2
    assert "flag" in [c for c in conn.sql("select * from p").columns]


def test_overwrite_schema_replaces(wh):
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'x' a, 'y' b").write.mode("overwrite").saveAsTable("os")
    assert conn.sql("select * from os").columns == ["id", "a", "b"]
    # Plain overwrite with a narrower schema fails (Delta won't drop columns silently)…
    with pytest.raises(Exception):
        conn.sql("select 2 id").write.mode("overwrite").saveAsTable("os")
    # …but overwriteSchema replaces the schema wholesale.
    conn.sql("select 2 id").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("os")
    assert conn.sql("select * from os").columns == ["id"]
    assert conn.table("os").count() == 1


def test_sql_writes_persist_to_delta(wh):
    # CREATE TABLE AS / INSERT / UPDATE / DELETE via conn.sql() must land as real Delta, visible
    # to a brand-new connection (not just DuckDB-native tables in this session).
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("CREATE TABLE evt AS SELECT 1 id, 'A' grp")
    conn.sql("INSERT INTO evt VALUES (2, 'B')")
    conn.sql("INSERT INTO evt (grp, id) SELECT 'C', 3")   # column list reordered
    conn.sql("UPDATE evt SET grp = 'Z' WHERE id = 1")
    conn.sql("DELETE FROM evt WHERE id = 2")
    assert sorted(conn.sql("select * from evt").collect()) == [(1, "Z"), (3, "C")]

    # real persistence: a fresh connection reads it off the store
    fresh = duckrun.connect(wh, schema="dbo")
    assert sorted(fresh.table("evt").collect()) == [(1, "Z"), (3, "C")]


def test_read_api(wh):
    conn = duckrun.connect(wh, schema="dbo")
    t1_path = conn.table_path("dbo", "t1")
    assert conn.read.delta(t1_path).count() == 2
    assert conn.read.format("delta").load(t1_path).count() == 2


def test_merge_upsert(wh):
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select * from (values (1,10),(2,10),(3,10)) t(id, val)") \
        .write.mode("overwrite").saveAsTable("m")

    src = conn.sql("select * from (values (2,99),(4,99)) t(id, val)")
    DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    assert conn.table("m").count() == 4                                   # 3 + 1 insert
    assert conn.sql("select val from m where id = 2").fetchone()[0] == 99  # matched updated
    assert conn.sql("select val from m where id = 4").fetchone()[0] == 99  # new inserted


def test_merge_insert_only(wh):
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select * from (values (1,10),(2,10)) t(id, val)") \
        .write.mode("overwrite").saveAsTable("io")

    src = conn.sql("select * from (values (2,99),(3,99)) t(id, val)")
    DeltaTable.forName(conn, "dbo.io").merge(src, "target.id = source.id") \
        .whenNotMatchedInsertAll().execute()

    assert conn.table("io").count() == 3                                    # only id=3 added
    assert conn.sql("select val from io where id = 2").fetchone()[0] == 10  # existing untouched


def test_update_only_merge_rejected(wh):
    conn = duckrun.connect(wh, schema="dbo")
    src = conn.sql("select 1 id, 1 val")
    builder = DeltaTable.forName(conn, "dbo.t1").merge(src, "target.id = source.id") \
        .whenMatchedUpdateAll()
    with pytest.raises(ValueError):
        builder.execute()


def test_toPandas(wh):
    # .toPandas()/.df() are the only pandas-touching bits of the API (DuckDB materializes to a
    # pandas DataFrame, like Spark's toPandas). pandas is in the [test] extra so this runs for real.
    conn = duckrun.connect(wh, schema="dbo")
    pdf = conn.sql("select name from t1 order by id").toPandas()
    assert list(pdf["name"]) == ["a", "b"]
