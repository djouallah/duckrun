"""Per-method capability matrix for the duckrun.connect() API.

Each public method/option is its own discrete test, grouped into a class per surface
(Session / Catalog / DataFrame / DataFrameReader / DataFrameWriter / DeltaTable). Runs on a
local-filesystem Delta warehouse (no network, no secrets). The `connection-card` workflow renders
the JUnit results into a scorecard (tests/tools/connection_summary.py) so every method shows up
with a ✅/❌ — the connection-API analogue of the conformance card.

Granularity is the point: keep one assertion-ish concept per test so the card reads as a method
checklist. (Broader end-to-end coverage lives in test_local_filesystem.py and coffeeshop/.)
"""
import pytest

import duckrun
from duckrun import DeltaTable


@pytest.fixture
def conn(tmp_path):
    """A connected local-fs session with a seed table `src` (dbo) and a second schema `other`."""
    c = duckrun.connect(str(tmp_path / "wh"), schema="dbo")
    c.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)") \
        .write.mode("overwrite").saveAsTable("src")
    c.sql("select 7 as n").write.mode("overwrite").saveAsTable("other.thing")
    c.refresh()
    return c


class TestSession:
    def test_connect(self, conn):
        assert isinstance(conn, duckrun.DuckSession)

    def test_sql(self, conn):
        assert conn.sql("select count(*) from src").fetchone()[0] == 3

    def test_table(self, conn):
        assert conn.table("src").count() == 3

    def test_read_property(self, conn):
        assert conn.read is not None

    def test_catalog_property(self, conn):
        assert conn.catalog is not None

    def test_refresh(self, conn):
        assert conn.refresh() is conn

    def test_connection(self, conn):
        assert conn.connection.execute("select 1").fetchone()[0] == 1

    def test_table_path(self, conn):
        assert conn.table_path("dbo", "src").endswith("dbo/src")

    def test_show_tables(self, conn):
        assert "src" in {r[0] for r in conn.sql("SHOW TABLES").fetchall()}


class TestCatalog:
    def test_listTables(self, conn):
        assert "src" in conn.catalog.listTables()

    def test_listDatabases(self, conn):
        assert {"dbo", "other"} <= set(conn.catalog.listDatabases())

    def test_currentDatabase(self, conn):
        assert conn.catalog.currentDatabase() == "dbo"

    def test_setCurrentDatabase(self, conn):
        conn.catalog.setCurrentDatabase("other")
        assert conn.catalog.currentDatabase() == "other"
        assert conn.sql("select n from thing").fetchone()[0] == 7  # resolves via search_path


class TestDataFrame:
    def test_collect(self, conn):
        assert len(conn.sql("select * from src").collect()) == 3

    def test_count(self, conn):
        assert conn.sql("select * from src").count() == 3

    def test_columns(self, conn):
        assert conn.sql("select id, name from src").columns == ["id", "name"]

    def test_show(self, conn):
        conn.sql("select * from src").show()  # smoke: must not raise

    def test_toPandas(self, conn):
        # toPandas() == relation.df() (Spark parity). DuckDB .df() materializes a pandas
        # DataFrame, so pandas+numpy are required — provided by the [test] extra.
        assert list(conn.sql("select name from src order by id").toPandas()["name"]) == ["a", "b", "c"]

    def test_relation_passthrough(self, conn):
        # unknown attrs fall through to the DuckDB relation (e.g. .fetchall())
        assert conn.sql("select 1").fetchall() == [(1,)]


class TestDataFrameReader:
    def test_format_load_delta(self, conn):
        assert conn.read.format("delta").load(conn.table_path("dbo", "src")).count() == 3

    def test_delta(self, conn):
        assert conn.read.delta(conn.table_path("dbo", "src")).count() == 3

    def test_table(self, conn):
        assert conn.read.table("src").count() == 3

    def test_parquet(self, conn, tmp_path):
        p = tmp_path / "s.parquet"
        conn.connection.execute(f"copy (select 1 a, 2 b) to '{p.as_posix()}' (format parquet)")
        assert conn.read.parquet(p.as_posix()).count() == 1

    def test_csv(self, conn, tmp_path):
        p = tmp_path / "s.csv"
        p.write_text("x,y\n1,2\n3,4\n")
        assert conn.read.option("header", True).csv(p.as_posix()).count() == 2


class TestDataFrameWriter:
    def test_saveAsTable(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.refresh()
        assert conn.table("w").count() == 1

    def test_mode_overwrite(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a").write.mode("overwrite").saveAsTable("w")
        assert conn.table("w").count() == 1

    def test_mode_append(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a").write.mode("append").saveAsTable("w")
        assert conn.table("w").count() == 2

    def test_mode_ignore(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a").write.mode("ignore").saveAsTable("w")
        assert conn.table("w").count() == 1  # no-op when it exists

    def test_mode_error(self, conn):
        with pytest.raises(ValueError):
            conn.sql("select 1 a").write.saveAsTable("src")  # default error, src exists

    def test_option_mergeSchema(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a, 3 b").write.mode("append").option("mergeSchema", "true").saveAsTable("w")
        conn.refresh()
        assert "b" in conn.sql("select * from w").columns

    def test_option_overwriteSchema(self, conn):
        conn.sql("select 1 a, 2 b").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 1 a").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("w")
        conn.refresh()
        assert conn.sql("select * from w").columns == ["a"]

    def test_partitionBy(self, conn):
        conn.sql("select * from (values (1,'eu'),(2,'us')) t(id, region)") \
            .write.mode("overwrite").partitionBy("region").saveAsTable("w")
        conn.refresh()
        assert conn.table("w").count() == 2

    def test_format(self, conn):
        with pytest.raises(ValueError):
            conn.sql("select 1 a").write.format("parquet").saveAsTable("w")  # only delta


class TestDeltaTable:
    def _seed(self, conn):
        conn.sql("select * from (values (1,10),(2,10),(3,10)) t(id, val)") \
            .write.mode("overwrite").saveAsTable("m")
        conn.refresh()

    def test_forName(self, conn):
        self._seed(conn)
        assert DeltaTable.forName(conn, "dbo.m").path.endswith("dbo/m")

    def test_forPath(self, conn):
        assert DeltaTable.forPath(conn, conn.table_path("dbo", "src")).path.endswith("dbo/src")

    def test_merge_upsert(self, conn):
        self._seed(conn)
        src = conn.sql("select * from (values (2,99),(4,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        assert conn.table("m").count() == 4
        assert conn.sql("select val from m where id = 2").fetchone()[0] == 99

    def test_merge_update_columns(self, conn):
        self._seed(conn)
        src = conn.sql("select 1 id, 555 val")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdate(set={"val": "source.val"}).whenNotMatchedInsertAll().execute()
        assert conn.sql("select val from m where id = 1").fetchone()[0] == 555

    def test_merge_insert_only(self, conn):
        self._seed(conn)
        src = conn.sql("select * from (values (2,99),(5,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenNotMatchedInsertAll().execute()
        assert conn.table("m").count() == 4  # only id=5 added
        assert conn.sql("select val from m where id = 2").fetchone()[0] == 10  # untouched

    def test_update_only_rejected(self, conn):
        self._seed(conn)
        src = conn.sql("select 1 id, 1 val")
        with pytest.raises(ValueError):
            DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
                .whenMatchedUpdateAll().execute()
