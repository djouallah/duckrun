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


def _delta_scan_version_supported() -> bool:
    """True if the installed duckdb-delta exposes `delta_scan(..., version => N)` (duckdb-delta
    #312, ships with the 1.5.4 floor). Tests that pin a read version skip on older builds."""
    import duckdb
    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta")
        con.execute("SELECT * FROM delta_scan('__nope__', version => 0)")
    except Exception as exc:  # noqa: BLE001
        return 'named parameter "version"' not in str(exc)  # binder rejects the param → unsupported
    finally:
        con.close()
    return True


needs_version_param = pytest.mark.skipif(
    not _delta_scan_version_supported(),
    reason="installed duckdb-delta lacks delta_scan(version => N) (needs the 1.5.4 floor)",
)


@pytest.fixture
def conn(tmp_path):
    """A connected local-fs session with a seed table `src` (dbo) and a second schema `other`."""
    c = duckrun.connect(str(tmp_path / "wh"), schema="dbo")
    c.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)") \
        .write.mode("overwrite").saveAsTable("src")
    c.sql("select 7 as n").write.mode("overwrite").saveAsTable("other.thing")
    return c  # saveAsTable surfaces tables itself — no manual refresh needed


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

    def test_tableExists(self, conn):
        assert conn.catalog.tableExists("src") is True
        assert conn.catalog.tableExists("nope") is False
        assert conn.catalog.tableExists("other.thing") is True  # qualified name

    def test_tableExists_is_fresh(self, conn):
        # safety: a table written out-of-band (no manual refresh) must still be found —
        # tableExists refreshes internally.
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("fresh")
        assert conn.catalog.tableExists("fresh") is True

    def test_databaseExists(self, conn):
        assert conn.catalog.databaseExists("dbo") is True
        assert conn.catalog.databaseExists("ghost") is False

    def test_listColumns(self, conn):
        assert conn.catalog.listColumns("src") == ["id", "name"]


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
        assert conn.table("w").count() == 1  # queryable immediately, no refresh

    def test_mode_overwrite(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a").write.mode("overwrite").saveAsTable("w")
        assert conn.table("w").count() == 1

    def test_mode_append(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 2 a").write.mode("append").saveAsTable("w")
        assert conn.table("w").count() == 2

    def test_mode_safeappend(self, conn):
        conn.sql("select 1 a").write.mode("safeappend").saveAsTable("w")  # missing → create
        conn.sql("select 2 a").write.mode("safeappend").saveAsTable("w")  # unchanged → append
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
        assert "b" in conn.sql("select * from w").columns

    def test_option_overwriteSchema(self, conn):
        conn.sql("select 1 a, 2 b").write.mode("overwrite").saveAsTable("w")
        conn.sql("select 1 a").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("w")
        assert conn.sql("select * from w").columns == ["a"]

    def test_partitionBy(self, conn):
        conn.sql("select * from (values (1,'eu'),(2,'us')) t(id, region)") \
            .write.mode("overwrite").partitionBy("region").saveAsTable("w")
        assert conn.table("w").count() == 2

    def test_format(self, conn):
        with pytest.raises(ValueError):
            conn.sql("select 1 a").write.format("parquet").saveAsTable("w")  # only delta


class TestDeltaTable:
    def _seed(self, conn):
        conn.sql("select * from (values (1,10),(2,10),(3,10)) t(id, val)") \
            .write.mode("overwrite").saveAsTable("m")

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

    def test_version(self, conn):
        self._seed(conn)  # one overwrite → version 0
        assert DeltaTable.forName(conn, "dbo.m").version() == 0

    def test_delete(self, conn):
        self._seed(conn)
        DeltaTable.forName(conn, "dbo.m").delete("id = 2")
        assert sorted(r[0] for r in conn.table("m").collect()) == [1, 3]

    def test_update(self, conn):
        self._seed(conn)
        DeltaTable.forName(conn, "dbo.m").update(set={"val": "val + 1"}, where="id = 1")
        assert conn.sql("select val from m where id = 1").fetchone()[0] == 11

    def test_replaceWhere(self, conn):
        self._seed(conn)  # ids 1,2,3 all val=10
        new = conn.sql("select * from (values (1,77),(2,77)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").replaceWhere(new, "id < 3")
        assert dict(conn.sql("select id, val from m").collect()) == {1: 77, 2: 77, 3: 10}

    def test_merge_by_source_delete(self, conn):
        # full sync: source carries ids {2,4}; matched updates, unmatched-by-source (1,3) deleted.
        self._seed(conn)
        src = conn.sql("select * from (values (2,99),(4,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
        assert dict(conn.sql("select id, val from m").collect()) == {2: 99, 4: 99}

    def test_merge_is_pinned_by_default(self, conn):
        # merge pins the target snapshot automatically — the caller passes nothing extra.
        self._seed(conn)
        src = conn.sql("select 1 id, 11 val")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        assert conn.sql("select val from m where id = 1").fetchone()[0] == 11


class TestSqlReadOnly:
    """conn.sql() is read-only: reads (incl. version-pinned delta_scan) pass through; a write
    statement raises with a pointer to the Spark write API."""

    def test_select_passthrough(self, conn):
        assert conn.sql("SELECT 1").fetchall() == [(1,)]

    @needs_version_param
    def test_version_pinned_read(self, conn):
        # write v0 then v1, then read v0 back via the passthrough — time travel for free.
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("tt")  # v0
        conn.sql("select 2 a").write.mode("overwrite").saveAsTable("tt")  # v1
        path = conn.table_path("dbo", "tt")
        assert conn.sql(f"select a from delta_scan('{path}', version => 0)").fetchone()[0] == 1

    @pytest.mark.parametrize("stmt", [
        "CREATE TABLE w AS SELECT 1 a",
        "INSERT INTO src VALUES (9, 'z')",
        "DELETE FROM src WHERE id = 1",
        "UPDATE src SET name = 'z'",
        "MERGE INTO src USING src s ON src.id = s.id WHEN MATCHED THEN DELETE",
    ])
    def test_write_statement_rejected(self, conn, stmt):
        with pytest.raises(ValueError, match="read-only"):
            conn.sql(stmt)
