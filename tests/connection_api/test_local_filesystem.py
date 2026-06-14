"""Local, network-free coverage for the top-level connection API (``duckrun.connect``).

Exercises the whole Spark-shaped surface against a Delta ``./wh`` on the local filesystem:
discovery + catalog, the ``DataFrameWriter`` save modes, the ``DataFrameReader``, and the
``DeltaTable.merge`` upsert builder. Storage-neutrality (s3/gcs/abfss) shares this exact code
path — only the secret/discovery backend differs — so the local run is representative.
"""
import duckdb
import pytest
from deltalake.exceptions import CommitFailedError

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


def test_safeappend_creates_then_appends(wh):
    conn = duckrun.connect(wh, schema="dbo")
    # First run on a missing table: nothing to fence against → create via append.
    conn.sql("select 1 id, 'a' v").write.mode("safeappend").saveAsTable("sa")
    assert conn.table("sa").count() == 1
    # Unchanged table → optimistic append commits and grows the table.
    conn.sql("select 2 id, 'b' v").write.mode("safeappend").saveAsTable("sa")
    assert sorted(conn.table("sa").collect()) == [(1, "a"), (2, "b")]


def test_safeappend_refuses_on_concurrent_commit(wh, monkeypatch):
    # safeappend pins to the version it read; if a writer lands before the commit, it must fail
    # loud (CommitFailedError) instead of duplicating — identical to the dbt safeappend strategy.
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("sc")
    path = conn.table_path("dbo", "sc")
    stale = engine.table_version(path, conn.storage_options)  # the version "as read"

    # A concurrent writer commits, moving HEAD past the version safeappend will pin to.
    engine.write_delta(path, duckdb.connect().sql("select 99 id, 'x' v"), mode="append")
    monkeypatch.setattr(engine, "table_version", lambda *a, **k: stale)

    with pytest.raises(CommitFailedError):
        conn.sql("select 2 id, 'b' v").write.mode("safeappend").saveAsTable("sc")


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


def test_spark_writes_persist_to_delta(wh):
    # create/append via saveAsTable and mutate via the DeltaTable handle must land as real Delta,
    # visible to a brand-new connection (not DuckDB-native tables in this session). conn.sql() is
    # read-only — these writes go through the Spark API.
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'A' grp").write.mode("overwrite").saveAsTable("evt")
    conn.sql("select 2 id, 'B' grp").write.mode("append").saveAsTable("evt")
    conn.sql("select 3 id, 'C' grp").write.mode("append").saveAsTable("evt")
    evt = conn.delta_table("evt")
    evt.update(set={"grp": "'Z'"}, where="id = 1")
    evt.delete("id = 2")
    assert sorted(conn.sql("select * from evt").collect()) == [(1, "Z"), (3, "C")]

    # a write statement through conn.sql() is rejected (read-only) — no silent DuckDB-local table
    with pytest.raises(ValueError, match="read-only"):
        conn.sql("CREATE TABLE evt2 AS SELECT 1 id")

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


def test_dataframe_show(wh):
    # .show() is the Spark print alias over the DuckDB relation: prints to stdout, returns None.
    conn = duckrun.connect(wh, schema="dbo")
    assert conn.sql("select * from t1 order by id").show() is None


def test_raw_connection_escape_hatch(wh):
    # conn.connection exposes the underlying DuckDB connection for anything the Spark surface
    # doesn't cover — scalar queries, and reading the registered views directly.
    conn = duckrun.connect(wh, schema="dbo")
    assert conn.connection.execute("select 40 + 2").fetchone()[0] == 42
    assert conn.connection.execute("select count(*) from t1").fetchone()[0] == 2


def test_refresh_picks_up_external_writes(wh):
    # A table created on the store after connect is invisible until refresh() re-discovers it.
    conn = duckrun.connect(wh, schema="dbo")
    assert "t3" not in conn.catalog.listTables()
    _seed(wh + "/dbo/t3", "select 1 as id")
    conn.refresh(quiet=True)
    assert "t3" in conn.catalog.listTables()
    assert conn.table("t3").count() == 1


def test_reader_parquet_csv_and_table(wh, tmp_path):
    conn = duckrun.connect(wh, schema="dbo")
    pq = str(tmp_path / "t1.parquet")
    csv = str(tmp_path / "t1.csv")
    conn.connection.execute(f"COPY (select * from t1) TO '{pq}' (FORMAT parquet)")
    conn.connection.execute(f"COPY (select * from t1) TO '{csv}' (FORMAT csv, HEADER)")

    assert conn.read.parquet(pq).count() == 2
    # csv read with an explicit option (header) routed through DataFrameReader.option.
    assert conn.read.format("csv").option("header", True).load(csv).count() == 2
    assert conn.read.csv(csv).count() == 2
    # read.table is the by-name shortcut (same as conn.table).
    assert conn.read.table("t1").count() == 2


def test_writer_format(wh):
    conn = duckrun.connect(wh, schema="dbo")
    # .format('delta') is accepted (the only supported writer format)…
    conn.sql("select 1 id").write.format("delta").mode("overwrite").saveAsTable("wf")
    assert conn.table("wf").count() == 1
    # …anything else is rejected up front.
    with pytest.raises(ValueError):
        conn.sql("select 1 id").write.format("parquet")


def test_catalog_database_and_column_introspection(wh):
    # A second schema folder so setCurrentDatabase / databaseExists have something to switch to.
    _seed(wh + "/sales/orders", "select 7 as n, 'x' as label")
    conn = duckrun.connect(wh)  # no schema → discover every schema

    assert conn.catalog.databaseExists("sales") is True
    assert conn.catalog.databaseExists("nope") is False
    assert conn.catalog.tableExists("dbo.t1") is True
    assert conn.catalog.tableExists("nope") is False
    assert conn.catalog.listColumns("dbo.t1") == ["id", "name"]

    conn.catalog.setCurrentDatabase("sales")
    assert conn.catalog.currentDatabase() == "sales"
    assert conn.catalog.tableExists("orders") is True            # resolved in the current db
    assert conn.sql("select n from orders").fetchone()[0] == 7   # unqualified resolves to sales


def test_delta_table_for_path_and_version(wh):
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("ver")
    path = conn.table_path("dbo", "ver")

    by_name = DeltaTable.forName(conn, "dbo.ver")
    by_path = DeltaTable.forPath(conn, path)
    assert by_path.version() == by_name.version() == 0

    conn.sql("select 2 id, 'b' v").write.mode("append").saveAsTable("ver")
    assert DeltaTable.forPath(conn, path).version() == 1   # a new commit bumps the version


def test_replace_where(wh):
    # replaceWhere atomically swaps the rows matching the predicate for the source rows.
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select * from (values (1,'eu'),(2,'us'),(3,'eu')) t(id, region)") \
        .write.mode("overwrite").saveAsTable("rw")
    new_eu = conn.sql("select 9 id, 'eu' region")
    DeltaTable.forName(conn, "dbo.rw").replaceWhere(new_eu, "region = 'eu'")
    assert sorted(conn.table("rw").collect()) == [(2, "us"), (9, "eu")]  # eu rows replaced, us kept


def test_merge_update_columns_and_sync_delete(wh):
    # whenMatchedUpdate (column-list form) + whenNotMatchedInsertAll + whenNotMatchedBySourceDelete:
    # a full sync of the target to the source's key set.
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select * from (values (1,10,'a'),(2,10,'b'),(3,10,'c')) t(id, val, note)") \
        .write.mode("overwrite").saveAsTable("sync")

    src = conn.sql("select * from (values (2,99,'X'),(4,99,'Y')) t(id, val, note)")
    DeltaTable.forName(conn, "dbo.sync").merge(src, "target.id = source.id") \
        .whenMatchedUpdate(set={"val": "source.val"}) \
        .whenNotMatchedInsertAll() \
        .whenNotMatchedBySourceDelete() \
        .execute()

    # id=2 updates val only (note 'b' preserved), id=4 inserted, ids 1 & 3 deleted (not in source).
    assert sorted(conn.table("sync").collect()) == [(2, 99, "b"), (4, 99, "Y")]
