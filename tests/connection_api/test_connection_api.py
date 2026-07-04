"""The duckrun.connect() test suite — one file, three complementary views of the same API.

1. **Per-method capability matrix** (the ``Test*`` classes) — one discrete test per public
   method/option, grouped by surface (Session / Catalog / DataFrame / DataFrameReader /
   DataFrameWriter / DeltaTable / SqlDml). The ``connection-card`` workflow renders *just these
   classes* (``tests/tools/connection_summary.py``) into the README method scorecard, so each method
   shows a ✅/❌. Granularity is the point — one concept per test.

2. **Local-filesystem contract & plumbing** (``test_*`` functions on the ``wh`` fixture) — discovery,
   catalog introspection, save-MODE contracts (error/ignore/safeappend), the connect() error
   formatting, reader round-trips, and the merge-builder contracts. Behaviour, not data equality.
   Storage-neutrality (s3/gcs/abfss) shares this exact code path — only the secret/discovery backend
   differs — so the local run is representative.

3. **Write-correctness matrix** (``test_sql_equals_dataframe`` + the Tier-2/3/4 functions) — does *our
   glue* land the right Delta data? The load-bearing oracle is **cross-API equivalence**: the same
   logical write expressed via the SQL API and the DataFrame API must land byte-identical Delta data, so
   a bug in either path shows up as a mismatch with almost no hand-maintained expected values. Every
   assertion reads back through a **fresh** ``duckrun.connect`` — which only sees real Delta on disk
   (discovery globs ``_delta_log``), subsuming the old ``is_deltatable`` boundary check.

All local, network-free, serial (duckrun's write path is single-writer). No DAT, no external engine.
"""
import os
from pathlib import Path

import duckdb
import deltalake
import pytest
from deltalake.exceptions import CommitFailedError

import duckrun
import duckrun.session as session_mod
from duckrun import DeltaTable
from duckrun.delta_table import _parse_parquet_identifier
from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.delta_dml import TOMBSTONE_COLUMN


def _delta_scan_version_supported() -> bool:
    """True if the installed duckdb-delta exposes `delta_scan(..., version => N)` (duckdb-delta
    #312, ships with the 1.5.4 floor). Tests that pin a read version skip on older builds."""
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


# ════════════════════════════════════════════════════════════════════════════════════════════════
# 1. Per-method capability matrix — rendered into the README scorecard (the Test* classes ONLY).
# ════════════════════════════════════════════════════════════════════════════════════════════════
@pytest.fixture
def conn(tmp_path):
    """A connected local-fs session with a seed table `src` (dbo) and a second schema `other`."""
    c = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)
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

    def test_createDataFrame(self, conn):
        df = conn.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
        assert df.columns == ["id", "name"]
        assert df.count() == 2

    def test_read_property(self, conn):
        assert conn.read is not None

    def test_catalog_property(self, conn):
        assert conn.catalog is not None

    def test_refresh(self, conn):
        assert conn.refresh() is conn

    def test_connection(self, conn):
        assert conn._connection.execute("select 1").fetchone()[0] == 1

    def test_stop(self, conn):
        conn.stop()  # closes the DuckDB connection (Spark's SparkSession.stop())
        with pytest.raises(Exception):
            conn.sql("select 1").collect()  # connection is closed -> unusable

    def test_table_path(self, conn):
        assert conn._table_path("dbo", "src").endswith("dbo/src")

    def test_attach(self, conn, tmp_path):
        # attach a second lakehouse as a named catalog; cross-catalog read resolves catalog.schema.table.
        other = duckrun.connect(str(tmp_path / "wh2"), schema="dbo", read_only=False)
        other.sql("select 99 as n").write.mode("overwrite").saveAsTable("only_there")
        other.stop()
        assert conn.attach(str(tmp_path / "wh2"), name="sales") is conn  # chains
        assert "sales" in conn.catalog.listCatalogs()
        assert conn.sql("select n from sales.dbo.only_there").fetchone()[0] == 99

    def test_show_tables(self, conn):
        assert "src" in {r[0] for r in conn.sql("SHOW TABLES").fetchall()}

    def test_copy(self, conn, tmp_path):
        # upload preserves the tree and honours the extension filter (COPY … FORMAT BLOB, no obstore).
        src = tmp_path / "src"
        (src / "sub").mkdir(parents=True)
        (src / "a.csv").write_bytes(b"hello")
        (src / "sub" / "b.parquet").write_bytes(b"world")
        (src / "skip.txt").write_bytes(b"no")
        # a .gz-named file must be copied byte-verbatim, NOT re-compressed by the extension (the bug
        # that double-gzipped AEMO's CSV.gz and broke fct_price). COMPRESSION 'none' guards it.
        import gzip
        gz = gzip.compress(b"col1,col2\n1,2\n" * 50)
        (src / "d.csv.gz").write_bytes(gz)
        conn.copy(str(src), "uploaded", file_extensions=[".csv", "parquet", ".gz"])
        base = Path(conn.root_path) / "uploaded"
        assert (base / "a.csv").read_bytes() == b"hello"
        assert (base / "sub" / "b.parquet").read_bytes() == b"world"
        assert (base / "d.csv.gz").read_bytes() == gz  # byte-verbatim, not double-compressed
        assert not (base / "skip.txt").exists()  # filtered out

    def test_download(self, conn, tmp_path):
        # download mirrors copy; overwrite=False (default) skips files already present locally.
        remote = Path(conn.root_path) / "remote"
        remote.mkdir()
        (remote / "x.bin").write_bytes(b"payload")
        dst = tmp_path / "dl"
        conn.download("remote", str(dst))
        assert (dst / "x.bin").read_bytes() == b"payload"
        (dst / "x.bin").write_bytes(b"changed")
        conn.download("remote", str(dst))  # skips existing
        assert (dst / "x.bin").read_bytes() == b"changed"

    def test_list_files(self, conn, tmp_path):
        # list the files copy() lands, as relative paths; the extension filter is honoured.
        src = tmp_path / "src"
        (src / "sub").mkdir(parents=True)
        (src / "a.csv").write_bytes(b"x")
        (src / "sub" / "b.parquet").write_bytes(b"y")
        conn.copy(str(src), "listed")
        assert set(conn.list_files("listed")) == {"a.csv", "sub/b.parquet"}
        assert conn.list_files("listed", file_extensions=[".csv"]) == ["a.csv"]

    def test_get_stats(self, conn):
        st = conn.get_stats("src")
        d = dict(zip(st.columns, st.collect()[0]))
        assert d["table"] == "src" and d["total_rows"] == 3 and d["num_files"] >= 1
        assert d["num_row_groups"] >= 1 and d["compression"]  # a real parquet footer was read
        # source=None → every table in the current schema (dbo has src)
        allrows = conn.get_stats()
        assert "src" in {r[allrows.columns.index("table")] for r in allrows.collect()}

    def test_get_stats_detailed(self, conn):
        st = conn.get_stats("src", detailed=True)  # one row per parquet row group
        assert st.count() >= 1 and "table" in st.columns


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

    def test_refreshTable(self, conn):
        # a table materialized out-of-band isn't visible until refreshed; refreshTable surfaces
        # just that one (the per-table peer of conn.refresh()).
        path = _stage_parquet(conn, "dbo/rt")
        DeltaTable.convertToDelta(conn, f"parquet.`{path}`")  # writes _delta_log, registers no view
        conn.catalog.refreshTable("rt")
        assert conn.sql("select * from rt").fetchall() == [(1, "a")]

    def test_createTable_ddl(self, conn):
        df = conn.catalog.createTable("ct", "id int, name string")
        assert df.count() == 0 and df.columns == ["id", "name"]
        assert conn.catalog.tableExists("ct")          # managed Delta, queryable immediately
        conn.sql("insert into ct values (1, 'x')")
        assert conn.table("ct").collect() == [(1, "x")]

    def test_createTable_from_struct(self, conn):
        # schema can be a StructType lifted from another frame
        conn.catalog.createTable("ct2", conn.sql("select 1::bigint k, 2.0::double v").schema)
        assert conn.catalog.getTable("ct2").tableType == "MANAGED"
        assert conn.sql("select * from ct2").schema.simpleString() == "struct<k:BIGINT,v:DOUBLE>"

    def test_createTable_bad_schema(self, conn):
        with pytest.raises(ValueError):
            conn.catalog.createTable("ct3", 123)

    def test_getTable(self, conn):
        t = conn.catalog.getTable("src")
        assert (t.name, t.database, t.catalog) == ("src", "dbo", "wh")
        assert t.tableType == "MANAGED" and t.isTemporary is False
        assert conn.catalog.getTable("other.thing").database == "other"  # qualified name
        with pytest.raises(ValueError):
            conn.catalog.getTable("nope")

    def test_getDatabase(self, conn):
        d = conn.catalog.getDatabase("dbo")
        assert (d.name, d.catalog) == ("dbo", "wh")
        assert d.locationUri.endswith("/dbo")
        with pytest.raises(ValueError):
            conn.catalog.getDatabase("ghost")

    def test_dropTempView(self, conn):
        conn.sql("select * from src").createOrReplaceTempView("tv")
        assert conn.sql("select count(*) from tv").fetchone()[0] == 3
        assert conn.catalog.dropTempView("tv") is True   # existed → dropped
        assert conn.catalog.dropTempView("tv") is False  # already gone → no-op

    def test_listCatalogs(self, conn):
        # single-catalog session: the primary, named from the lakehouse folder ("wh") — no name= given.
        assert conn.catalog.listCatalogs() == ["wh"]

    def test_currentCatalog(self, conn):
        assert conn.catalog.currentCatalog() == "wh"

    def test_setCurrentCatalog(self, conn):
        conn.catalog.setCurrentCatalog("wh")  # the only catalog; no-op switch must hold
        assert conn.catalog.currentCatalog() == "wh"
        with pytest.raises(ValueError):
            conn.catalog.setCurrentCatalog("ghost")  # unknown catalog → fail loud


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
        # toPandas() == relation.df() (DataFrame-API parity). DuckDB .df() materializes a pandas
        # DataFrame, so pandas+numpy are required — provided by the [test] extra.
        assert list(conn.sql("select name from src order by id").toPandas()["name"]) == ["a", "b", "c"]

    def test_toArrow(self, conn):
        # toArrow() returns a streaming pyarrow.RecordBatchReader (not a materialized Table).
        import pyarrow as pa
        reader = conn.sql("select name from src order by id").toArrow()
        assert isinstance(reader, pa.RecordBatchReader)
        assert reader.read_all().column("name").to_pylist() == ["a", "b", "c"]

    def test_first(self, conn):
        assert conn.sql("select id from src order by id").first() == (1,)
        assert conn.sql("select id from src where id > 99").first() is None

    def test_head(self, conn):
        assert conn.sql("select id from src order by id").head() == (1,)
        assert conn.sql("select id from src order by id").head(2) == [(1,), (2,)]

    def test_take(self, conn):
        assert conn.sql("select id from src order by id").take(2) == [(1,), (2,)]

    def test_isEmpty(self, conn):
        assert conn.sql("select * from src").isEmpty() is False
        assert conn.sql("select * from src where id > 99").isEmpty() is True

    def test_schema(self, conn):
        s = conn.sql("select id, name from src").schema
        assert s.names == ["id", "name"]
        assert (s.fields[0].name, s.fields[0].nullable) == ("id", True)
        assert s.simpleString().startswith("struct<id:")

    def test_printSchema(self, conn, capsys):
        conn.sql("select id from src").printSchema()
        out = capsys.readouterr().out
        assert out.startswith("root\n")
        assert "|-- id:" in out and "(nullable = true)" in out

    def test_relation_passthrough(self, conn):
        # unknown attrs fall through to the DuckDB relation (e.g. .fetchall())
        assert conn.sql("select 1").fetchall() == [(1,)]


class TestDataFrameReader:
    def test_format_load_delta(self, conn):
        assert conn.read.format("delta").load(conn._table_path("dbo", "src")).count() == 3

    def test_table(self, conn):
        assert conn.read.table("src").count() == 3

    def test_parquet(self, conn, tmp_path):
        p = tmp_path / "s.parquet"
        conn._connection.execute(f"copy (select 1 a, 2 b) to '{p.as_posix()}' (format parquet)")
        assert conn.read.parquet(p.as_posix()).count() == 1

    def test_csv(self, conn, tmp_path):
        p = tmp_path / "s.csv"
        p.write_text("x,y\n1,2\n3,4\n")
        assert conn.read.option("header", True).csv(p.as_posix()).count() == 2

    def test_json(self, conn, tmp_path):
        p = tmp_path / "s.json"
        p.write_text('{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n')
        assert conn.read.json(p.as_posix()).count() == 2

    def test_schema_csv_ddl(self, conn, tmp_path):
        # explicit schema names + types the columns and skips the header (Spark override).
        p = tmp_path / "s.csv"
        p.write_text("a,b\n1,2\n3,4\n")
        df = conn.read.schema("x int, y bigint").option("header", True).csv(p.as_posix())
        assert df.schema.simpleString() == "struct<x:INTEGER,y:BIGINT>"
        assert df.collect() == [(1, 2), (3, 4)]

    def test_schema_ddl_with_comma_type(self, conn, tmp_path):
        # DECIMAL(10,2) would break naive comma-splitting; DuckDB parses it for us.
        p = tmp_path / "d.csv"
        p.write_text("1,2.50\n")
        df = conn.read.schema("id int, price decimal(10,2)").csv(p.as_posix())
        assert df.schema.simpleString() == "struct<id:INTEGER,price:DECIMAL(10,2)>"

    def test_schema_json_struct(self, conn, tmp_path):
        # schema may be a StructType lifted from another frame
        st = conn.sql("select 1::int as id, 2.5::double as amt").schema
        p = tmp_path / "s.json"
        p.write_text('{"id": 7, "amt": 1.5}\n')
        assert conn.read.schema(st).json(p.as_posix()).collect() == [(7, 1.5)]

    def test_schema_rejected_for_delta(self, conn):
        with pytest.raises(ValueError):
            conn.read.schema("x int").format("delta").load(conn._table_path("dbo", "src"))

    @needs_version_param
    def test_versionAsOf(self, conn):
        # spark.read.format("delta").option("versionAsOf", N).load(path) — time travel.
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("tt")   # v0
        conn.sql("select 2 a").write.mode("append").saveAsTable("tt")      # v1
        path = conn._table_path("dbo", "tt")
        assert conn.read.format("delta").option("versionAsOf", 0).load(path).count() == 1
        assert conn.read.format("delta").option("versionAsOf", 1).load(path).count() == 2

    def test_timestampAsOf_rejected(self, conn):
        with pytest.raises(ValueError):
            conn.read.format("delta").option("timestampAsOf", "2024-01-01") \
                .load(conn._table_path("dbo", "src"))


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

    def test_option_replaceWhere(self, conn):
        # df.write.option("replaceWhere", pred).mode("overwrite") — atomic slice swap.
        conn.sql("select * from (values (1,10),(2,10),(3,10)) t(id, val)") \
            .write.mode("overwrite").saveAsTable("rw")
        conn.sql("select * from (values (1,77),(2,77)) t(id, val)") \
            .write.option("replaceWhere", "id < 3").mode("overwrite").saveAsTable("rw")
        assert dict(conn.sql("select id, val from rw").collect()) == {1: 77, 2: 77, 3: 10}

    def test_option_replaceWhere_requires_overwrite(self, conn):
        conn.sql("select 1 id, 10 val").write.mode("overwrite").saveAsTable("rw2")
        with pytest.raises(ValueError):
            conn.sql("select 1 id, 77 val").write.option("replaceWhere", "id = 1") \
                .mode("append").saveAsTable("rw2")

    def test_insertInto(self, conn):
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("ii")
        conn.sql("select 2 a").write.insertInto("ii")               # append into existing
        assert sorted(r[0] for r in conn.table("ii").collect()) == [1, 2]
        conn.sql("select 9 a").write.insertInto("ii", overwrite=True)  # replace all
        assert conn.table("ii").collect() == [(9,)]

    def test_insertInto_requires_existing(self, conn):
        with pytest.raises(ValueError):
            conn.sql("select 1 a").write.insertInto("nope")

    def test_partitionBy(self, conn):
        conn.sql("select * from (values (1,'eu'),(2,'us')) t(id, region)") \
            .write.mode("overwrite").partitionBy("region").saveAsTable("w")
        assert conn.table("w").count() == 2

    def test_sort(self, conn):
        df = conn.sql("select * from (values (3),(1),(2)) t(n)").sort("n")
        assert isinstance(df, duckrun.session.DataFrame) and hasattr(df, "write")
        assert [r[0] for r in df.collect()] == [1, 2, 3]

    def test_orderBy_alias_and_desc(self, conn):
        # orderBy is Spark's alias of sort; ascending=False sorts descending.
        assert [r[0] for r in conn.sql("select * from (values (1),(3),(2)) t(n)")
                .orderBy("n", ascending=False).collect()] == [3, 2, 1]

    def test_sort_then_partition_write(self, conn):
        # sort() returns a writable DataFrame that composes with partitionBy: delta-rs does the
        # partitioning, sort only sets row order. Round-trips all rows across partitions.
        conn.sql("select (i % 2) as region, (9 - i % 5) as k from range(40) t(i)") \
            .sort("region", "k").write.mode("overwrite").partitionBy("region").saveAsTable("sp")
        assert conn.table("sp").count() == 40
        assert sorted(r[0] for r in conn.sql("select distinct region from sp").collect()) == [0, 1]

    def test_format(self, conn):
        with pytest.raises(ValueError):
            conn.sql("select 1 a").write.format("parquet").saveAsTable("w")  # only delta

    def test_save_by_path(self, conn, tmp_path):
        p = (tmp_path / "by_path").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)  # no catalog name
        assert conn.read.format("delta").load(p).count() == 1  # read back BY PATH, not as a table

    def test_save_modes(self, conn, tmp_path):
        p = (tmp_path / "modes").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)
        conn.sql("select 2 a").write.mode("append").save(p)
        assert conn.read.format("delta").load(p).count() == 2

    def test_save_mode_error_when_exists(self, conn, tmp_path):
        p = (tmp_path / "err").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)
        with pytest.raises(ValueError):
            conn.sql("select 2 a").write.save(p)  # default error, path exists

    def test_write_optimize_user_keys(self, conn):
        # .write.optimize("region").mode("overwrite") lands the write sorted by region in the tuned
        # read layout in ONE pass. Data preserved; the active file is clustered by region.
        conn.sql("select (i * 7 % 5) as region, (i % 1000) * 1.5 as amount, i as id from range(20000) t(i)") \
            .write.optimize("region").mode("overwrite").saveAsTable("wo")
        assert conn.table("wo").count() == 20000
        assert conn.get_stats("wo").df()["compression"].tolist() == ["SNAPPY"]
        f = engine._delta_table(conn.root_path + "/dbo/wo", None).file_uris()[0].replace("file://", "")
        regs = [r[0] for r in conn.sql("select region from parquet_scan('%s')" % f).fetchall()]
        assert regs == sorted(regs)

    def test_write_optimize_auto_keys(self, conn):
        # .write.optimize() with no args profiles the data being written to pick the sort key, then
        # writes in the tuned layout — the write-time twin of conn.table(name).optimize().
        conn.sql("select (i * 7 % 5) as region, (i % 1000) * 1.5 as amount, i as id from range(20000) t(i)") \
            .write.optimize().mode("overwrite").saveAsTable("wo_auto")
        assert conn.table("wo_auto").count() == 20000
        assert conn.get_stats("wo_auto").df()["compression"].tolist() == ["SNAPPY"]
        f = engine._delta_table(conn.root_path + "/dbo/wo_auto", None).file_uris()[0].replace("file://", "")
        regs = [r[0] for r in conn.sql("select region from parquet_scan('%s')" % f).fetchall()]
        assert regs == sorted(regs)  # region is the low-cardinality key the profiler leads with

    def test_write_optimize_partitioned(self, conn):
        # optimize composes with partitionBy: partitions are preserved and lead the physical order.
        conn.sql("select (i % 3) as region, (9 - i % 5) as k, i as id from range(300) t(i)") \
            .write.optimize("k").partitionBy("region").mode("overwrite").saveAsTable("wo_part")
        assert conn.table("wo_part").count() == 300
        assert sorted(r[0] for r in conn.sql("select distinct region from wo_part").collect()) == [0, 1, 2]

    def test_write_optimize_requires_overwrite(self, conn):
        conn.sql("select 1 region, 1 id").write.mode("overwrite").saveAsTable("wo_bad")
        with pytest.raises(ValueError):
            conn.sql("select 2 region, 2 id").write.optimize("region").mode("append").saveAsTable("wo_bad")


class TestDeltaTable:
    def _seed(self, conn):
        conn.sql("select * from (values (1,10),(2,10),(3,10)) t(id, val)") \
            .write.mode("overwrite").saveAsTable("m")

    def test_forName(self, conn):
        self._seed(conn)
        assert DeltaTable.forName(conn, "dbo.m").path.endswith("dbo/m")

    def test_forPath(self, conn):
        assert DeltaTable.forPath(conn, conn._table_path("dbo", "src")).path.endswith("dbo/src")

    def test_convertToDelta(self, conn):
        path = _stage_parquet(conn, "dbo/conv")            # out-of-band parquet under the catalog root
        DeltaTable.convertToDelta(conn, f"parquet.`{path}`")  # zero-copy: writes a _delta_log over it
        conn.refresh()
        assert conn.table("conv").fetchall() == [(1, "a")]

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

    def test_merge_rejects_duplicate_source_keys(self, conn):
        # Parity with the dbt merge strategy: a keyed upsert whose SOURCE has two rows for one key
        # must FAIL LOUD (delta_rs would otherwise silently produce duplicate target rows), exactly
        # like the dbt incremental merge. Both land in engine.merge_delta_clauses -> same behaviour.
        self._seed(conn)
        src = conn.sql("select * from (values (2,99),(2,98)) t(id, val)")  # duplicate id=2
        with pytest.raises(Exception, match="(?i)unique|duplicate"):
            DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        # and the table is untouched (the guard runs before any write)
        assert conn.table("m").count() == 3
        assert conn.sql("select val from m where id = 2").fetchone()[0] == 10

    def test_merge_insert_only(self, conn):
        self._seed(conn)
        src = conn.sql("select * from (values (2,99),(5,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenNotMatchedInsertAll().execute()
        assert conn.table("m").count() == 4  # only id=5 added
        assert conn.sql("select val from m where id = 2").fetchone()[0] == 10  # untouched

    def test_merge_update_only(self, conn):
        # update-only merge (matched update, no insert) is supported — matched rows change, none added.
        self._seed(conn)
        src = conn.sql("select * from (values (1,99),(5,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().execute()
        assert conn.sql("select val from m where id = 1").fetchone()[0] == 99  # updated
        assert conn.table("m").count() == 3                                    # id=5 NOT inserted

    def test_merge_matched_delete(self, conn):
        # WHEN MATCHED THEN DELETE — matched rows removed (CDC-style).
        self._seed(conn)
        src = conn.sql("select * from (values (2,0),(3,0)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedDelete().execute()
        assert sorted(r[0] for r in conn.table("m").collect()) == [1]

    def test_merge_update_and_delete(self, conn):
        # two WHEN MATCHED clauses in one merge: delete flagged rows, update the rest.
        self._seed(conn)
        src = conn.sql("select * from (values (1,99,false),(2,99,true)) t(id, val, gone)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedDelete("source.gone").whenMatchedUpdate(set={"val": "source.val"}).execute()
        rows = dict(conn.sql("select id, val from m").collect())
        assert 2 not in rows           # deleted (gone = true)
        assert rows[1] == 99           # updated
        assert rows[3] == 10           # untouched

    def test_merge_insert_values(self, conn):
        # whenNotMatchedInsert with explicit value expressions.
        self._seed(conn)
        src = conn.sql("select * from (values (9,'5')) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenNotMatchedInsert(values={"id": "source.id", "val": "cast(source.val as int) * 10"}) \
            .execute()
        assert conn.sql("select val from m where id = 9").fetchone()[0] == 50

    def test_merge_by_source_update(self, conn):
        # whenNotMatchedBySourceUpdate — touch rows the source doesn't carry.
        self._seed(conn)
        src = conn.sql("select * from (values (1,99)) t(id, val)")
        DeltaTable.forName(conn, "dbo.m").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().whenNotMatchedBySourceUpdate(set={"val": "-1"}).execute()
        rows = dict(conn.sql("select id, val from m").collect())
        assert rows[1] == 99    # matched → updated from source
        assert rows[2] == -1    # not in source → set to -1
        assert rows[3] == -1

    def test_version(self, conn):
        self._seed(conn)  # one overwrite → version 0
        assert DeltaTable.forName(conn, "dbo.m").version() == 0

    def test_history(self, conn):
        self._seed(conn)  # v0
        conn.sql("select 9 id, 9 val").write.mode("append").saveAsTable("m")  # v1
        hist = DeltaTable.forName(conn, "dbo.m").history()
        assert [h["version"] for h in hist] == [1, 0]          # newest-first
        assert DeltaTable.forName(conn, "dbo.m").history(1)[0]["version"] == 1   # limit

    def test_delete(self, conn):
        self._seed(conn)
        DeltaTable.forName(conn, "dbo.m").delete("id = 2")
        assert sorted(r[0] for r in conn.table("m").collect()) == [1, 3]

    def test_update(self, conn):
        self._seed(conn)
        DeltaTable.forName(conn, "dbo.m").update(condition="id = 1", set={"val": "val + 1"})
        assert conn.sql("select val from m where id = 1").fetchone()[0] == 11

    def test_optimize(self, conn):
        # optimize operates on a TABLE (DeltaTable.forName), not on the session: bin-packing compaction,
        # or a z-order with zorder_by. write two commits → two files, then compact/z-order; data
        # unchanged, metrics returned. (The experimental sort rewrite is conn.table(name).optimize().)
        self._seed(conn)
        conn.sql("select 4 id, 10 val").write.mode("append").saveAsTable("m")
        metrics = DeltaTable.forName(conn, "dbo.m").optimize()          # bin-packing compaction
        assert metrics["numFilesAdded"] >= 1
        assert conn.table("m").count() == 4
        assert isinstance(DeltaTable.forName(conn, "dbo.m").optimize(zorder_by=["id"]), dict)  # z-order

    def test_table_optimize_auto_keys(self, conn):
        # conn.table(name).optimize() — the experimental sort rewrite: profiles the table (_get_rle),
        # picks the sort key automatically, and rewrites every file physically ordered by it. Data is
        # preserved and the active file comes out clustered by the key.
        conn.sql("select (i * 7 % 5) as region, (i % 1000) * 1.5 as amount, i as id from range(20000) t(i)") \
            .write.mode("overwrite").saveAsTable("to_auto")
        m = conn.table("dbo.to_auto").optimize()
        assert m["operation"] == "sortRewrite" and "region" in m["sortedBy"]
        # reports REAL measured on-disk bytes (Delta-log size_bytes), never an estimate
        assert m["sizeBytesBefore"] > 0 and m["sizeBytesAfter"] > 0
        assert m["savedPct"] == round(100.0 * (m["sizeBytesBefore"] - m["sizeBytesAfter"]) / m["sizeBytesBefore"], 1)
        assert conn.table("to_auto").count() == 20000
        # active file is clustered: reading it in file order, region is non-decreasing.
        f = engine._delta_table(conn.root_path + "/dbo/to_auto", None).file_uris()[0].replace("file://", "")
        regs = [r[0] for r in conn.sql("select region from parquet_scan('%s')" % f).fetchall()]
        assert regs == sorted(regs)

    def test_table_optimize_user_keys(self, conn):
        # conn.table(name).optimize("region") — full rewrite sorted by exactly the given columns, no
        # profiler. The active file comes out ordered by region regardless of what the profiler'd pick.
        conn.sql("select (i * 7 % 5) as region, (i % 1000) * 1.5 as amount, i as id from range(20000) t(i)") \
            .write.mode("overwrite").saveAsTable("to_keys")
        m = conn.table("dbo.to_keys").optimize("region")
        assert m["sortedBy"] == ["region"]
        assert conn.table("to_keys").count() == 20000
        f = engine._delta_table(conn.root_path + "/dbo/to_keys", None).file_uris()[0].replace("file://", "")
        regs = [r[0] for r in conn.sql("select region from parquet_scan('%s')" % f).fetchall()]
        assert regs == sorted(regs)

    def test_table_optimize_where_scopes_partitions(self, conn):
        # conn.table(name).optimize(where=...) rewrites ONLY the matching partition; the untouched
        # partitions keep their data and total count is preserved.
        conn.sql("select (i % 3) as region, i as id from range(300) t(i)") \
            .write.mode("overwrite").partitionBy("region").saveAsTable("to_where")
        v_before = engine.table_version(conn.root_path + "/dbo/to_where", None)
        m = conn.table("dbo.to_where").optimize("id", where="region = 1")
        assert m["operation"] == "sortRewrite"
        # a replaceWhere commit landed (new version) and every row survived across all partitions
        assert engine.table_version(conn.root_path + "/dbo/to_where", None) > v_before
        assert conn.table("to_where").count() == 300
        assert sorted(r[0] for r in conn.sql("select distinct region from to_where").collect()) == [0, 1, 2]
        # the rewritten partition is sorted by id; other partitions still hold their rows
        assert conn.sql("select count(*) from to_where where region = 0").fetchone()[0] == 100

    def test_table_optimize_rejects_query_frame(self, conn):
        # optimize() is only meaningful on a real table handle; a derived/query DataFrame refuses.
        with pytest.raises(ValueError):
            conn.sql("select 1 x").optimize()

    def test_get_rle_scan_count_is_constant(self, conn):
        # Regression: the auto profiler must NOT re-scan the (possibly remote) table once per column.
        # Those O(columns) full-table reads are what made optimize take 20 min on a 142M-row OneLake
        # table. _get_rle now materializes ONE reservoir sample and profiles that, so the number of
        # delta_scan reads it issues is a small constant independent of table width.
        class _Spy:   # session.con is a plain attribute; _get_rle reads it via self.con
            def __init__(self, real, log):
                self._real, self._log = real, log

            def __getattr__(self, name):
                attr = getattr(self._real, name)
                if name in ("sql", "execute") and callable(attr):
                    def wrapped(q, *a, **k):
                        if "delta_scan(" in str(q):
                            self._log.append(str(q))
                        return attr(q, *a, **k)
                    return wrapped
                return attr

        def _is_profiling(q):
            # Ignore catalog view-registration reads (CREATE VIEW … + its backing SELECT *) — those are
            # catalog-state churn, not per-column profiling. What we're guarding is the profiling reads:
            # the DESCRIBE and the single sample materialize (CREATE … TEMP TABLE _rle_src AS …). A
            # regression that re-scanned per column would show as extra aggregate reads kept here.
            s = q.strip().lower()
            return not (s.startswith("create or replace view") or s.startswith("select * from delta_scan"))

        def _scan_count(name, ncols):
            proj = ", ".join(f"(i % {j + 2}) as c{j}" for j in range(ncols))
            conn.sql(f"select {proj}, i as id from range(3000) t(i)") \
                .write.mode("overwrite").saveAsTable(name)
            seen, real = [], conn.con
            conn.con = _Spy(real, seen)   # _get_rle uses no add_actions replacement scan, so proxy is safe
            try:
                conn._get_rle(f"dbo.{name}")
            finally:
                conn.con = real
            return [q for q in seen if _is_profiling(q)]

        narrow = _scan_count("rle_narrow", 4)
        wide = _scan_count("rle_wide", 16)
        # DESCRIBE + one sample materialize, independent of column count — NOT one scan per column.
        assert len(narrow) == len(wide) == 2

    def test_vacuum(self, conn):
        # dry_run lists removable files without deleting; never errors on a healthy table.
        self._seed(conn)
        assert isinstance(DeltaTable.forName(conn, "dbo.m").vacuum(dry_run=True), list)

    def test_restoreToVersion(self, conn):
        self._seed(conn)                                                    # v0: ids 1,2,3
        conn.sql("select 9 id, 9 val").write.mode("append").saveAsTable("m")  # v1: + id 9
        assert conn.table("m").count() == 4
        DeltaTable.forName(conn, "dbo.m").restoreToVersion(0)
        assert sorted(r[0] for r in conn.table("m").collect()) == [1, 2, 3]

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


class TestSqlDml:
    """conn.sql(): reads (incl. version-pinned delta_scan) pass through, and Delta DML is applied
    via delta_rs — create-as / insert-select / insert-values / update / delete / alter-add, drop
    (a tombstone: no data deleted), and merge (upsert; same boundary as the DeltaTable.merge
    builder, ON/WHEN may use your own aliases or the table/relation names)."""

    def test_select_passthrough(self, conn):
        assert conn.sql("SELECT 1").fetchall() == [(1,)]

    @needs_version_param
    def test_version_pinned_read(self, conn):
        # write v0 then v1, then read v0 back via the passthrough — time travel for free.
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("tt")  # v0
        conn.sql("select 2 a").write.mode("overwrite").saveAsTable("tt")  # v1
        path = conn._table_path("dbo", "tt")
        assert conn.sql(f"select a from delta_scan('{path}', version => 0)").fetchone()[0] == 1

    def test_sql_create_table_as(self, conn):
        conn.sql("create table cta as select * from (values (1),(2)) t(x)")
        assert conn.table("cta").count() == 2

    def test_sql_insert_select(self, conn):
        conn.sql("insert into src select * from (values (9,'z')) t(id, name)")
        assert conn.table("src").count() == 4

    def test_sql_insert_values(self, conn):
        conn.sql("insert into src values (9, 'z')")
        assert conn.table("src").count() == 4
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_insert_values_named_subset(self, conn):
        # explicit column list → unsupplied target columns are filled with NULL (schema-fill, not
        # positional luck).
        conn.sql("insert into src (id) values (9)")
        assert conn.sql("select name from src where id = 9").fetchone()[0] is None

    def test_sql_update(self, conn):
        conn.sql("update src set name = 'Z' where id = 1")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "Z"

    def test_sql_delete(self, conn):
        conn.sql("delete from src where id = 1")
        assert conn.table("src").count() == 2

    def test_sql_update_where_inside_set_literal(self, conn):
        # A literal containing the word `where` in the SET list must not mis-split the statement
        # (review #5): only the trailing WHERE is the predicate.
        conn.sql("update src set name = 'a where b' where id = 1")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "a where b"
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "b"

    def test_sql_update_subquery_predicate(self, conn):
        # UPDATE with a subquery predicate: delta_rs's update() would panic, so duckrun evaluates it
        # in DuckDB and commits a fenced overwrite (review #9). Rows matching the subquery update.
        conn.sql("select * from (values (1),(3)) t(k)").write.mode("overwrite").saveAsTable("keys")
        conn.sql("update src set name = 'Q' where id in (select k from keys)")
        got = {r[0]: r[1] for r in conn.sql("select id, name from src").fetchall()}
        assert got == {1: "Q", 2: "b", 3: "Q"}

    def test_sql_delete_subquery_predicate(self, conn):
        # DELETE with a subquery predicate takes the same DuckDB-evaluated fenced fallback (#8).
        conn.sql("select * from (values (2)) t(k)").write.mode("overwrite").saveAsTable("dk")
        conn.sql("delete from src where id in (select k from dk)")
        assert {r[0] for r in conn.sql("select id from src").fetchall()} == {1, 3}

    def test_sql_alter_add_column(self, conn):
        conn.sql("alter table src add column qty integer")
        assert "qty" in conn.sql("select * from src").columns

    def test_sql_alter_add_column_not_null(self, conn):
        # the trailing NOT NULL clause must be stripped WITHOUT gluing `not` onto the type; the
        # column lands as an all-null INTEGER (delta_rs widens the schema, existing rows get NULL).
        conn.sql("alter table src add column qty integer not null")
        df = conn.sql("select * from src")
        qty_type = dict(zip(df.columns, df.types))["qty"]
        assert str(qty_type).upper().startswith("INT")
        assert conn.sql("select count(*) from src where qty is not null").fetchone()[0] == 0

    def test_sql_drop_tombstone(self, conn):
        # drop is a tombstone (no data deleted); the table leaves the catalog.
        conn.sql("drop table src")
        assert "src" not in conn.catalog.listTables()

    def test_sql_merge_upsert(self, conn):
        conn.sql("MERGE INTO src USING (values (2,'B'),(9,'z')) t(id, name) "
                 "ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.table("src").count() == 4
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "B"
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_update_columns(self, conn):
        conn.sql("MERGE INTO src USING (values (1,'X')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"

    def test_sql_merge_insert_only(self, conn):
        conn.sql("MERGE INTO src USING (values (2,'B'),(5,'e')) t(id, name) "
                 "ON target.id = source.id WHEN NOT MATCHED THEN INSERT *")
        assert conn.table("src").count() == 4                                   # only id=5 added
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "b"  # untouched

    def test_sql_merge_by_source_delete(self, conn):
        # full sync: source carries ids {2,3}; matched updates, unmatched-by-source (1) deleted.
        conn.sql("MERGE INTO src USING (values (2,'B'),(3,'C')) t(id, name) "
                 "ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
                 "WHEN NOT MATCHED BY SOURCE THEN DELETE")
        assert dict(conn.sql("select id, name from src").collect()) == {2: "B", 3: "C"}

    def test_sql_merge_subquery_source(self, conn):
        conn.sql("MERGE INTO src USING (select 9 as id, 'z' as name) AS source "
                 "ON target.id = source.id WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_user_aliases(self, conn):
        # user-chosen aliases (s/t) on both sides resolve to the engine's target/source.
        conn.sql("MERGE INTO src s USING (values (1,'X'),(9,'z')) t(id, name) ON s.id = t.id "
                 "WHEN MATCHED THEN UPDATE SET name = t.name WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_qualify_target_by_table_name(self, conn):
        # no target alias: qualify the target by its table name, the source by its alias.
        conn.sql("MERGE INTO src USING (values (1,'X')) u(id, name) ON src.id = u.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"

    def test_sql_merge_qualify_source_by_relation_name(self, conn):
        # a bare-relation source can be qualified by its own name (no alias needed).
        conn.sql("create table donor as select * from (values (1,'X'),(9,'z')) t(id, name)")
        conn.sql("MERGE INTO src USING donor ON src.id = donor.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_literal_aliases_still_work(self, conn):
        # backward compatible: the literal target/source aliases keep working.
        conn.sql("MERGE INTO src USING (values (1,'X')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"

    def test_sql_merge_literal_with_then_in_predicate(self, conn):
        # a string literal containing the word "then" (with spaces) must NOT be mistaken for the
        # clause's THEN — the clause is split on its top-level THEN only.
        conn.sql("MERGE INTO src USING (values (1,'X'),(9,'z')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED AND source.name <> 'do then redo' THEN UPDATE SET * "
                 "WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"  # pred true → updated
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"  # inserted

    def test_sql_merge_inline_line_comment(self, conn):
        # an interior `--` comment (here carrying the word "when") must not inject a false clause
        # boundary — comments are stripped before structural parsing.
        conn.sql("MERGE INTO src USING (values (1,'X')) t(id, name) ON target.id = source.id\n"
                 "WHEN MATCHED THEN UPDATE SET *\n"
                 "-- recompute when stale\n"
                 "WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"

    def test_sql_merge_inline_block_comment(self, conn):
        # a `/* */` comment (with using/on/when inside) between the target and USING is ignored, not
        # mistaken for the structural USING/ON/WHEN keywords.
        conn.sql("MERGE INTO src /* using on when */ USING (values (1,'X'),(9,'z')) t(id, name) "
                 "ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_bad_alias_rejected(self, conn):
        # an alias matching neither side (not the table/source names or their aliases) is rejected.
        with pytest.raises(ValueError, match="target.*source"):
            conn.sql("MERGE INTO src s USING (values (1,'x')) t(id, name) ON foo.id = bar.id "
                     "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

    def test_sql_merge_matched_delete(self, conn):
        # WHEN MATCHED THEN DELETE — matched rows removed (full delta-rs surface).
        conn.sql("MERGE INTO src USING (values (2,'x'),(3,'x')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN DELETE")
        assert sorted(r[0] for r in conn.table("src").collect()) == [1]

    def test_sql_merge_matched_delete_and_update(self, conn):
        # two WHEN MATCHED clauses, applied in order: delete flagged rows, update the rest.
        conn.sql("MERGE INTO src USING (values (1,'A',false),(2,'B',true)) t(id, name, gone) "
                 "ON target.id = source.id "
                 "WHEN MATCHED AND source.gone THEN DELETE "
                 "WHEN MATCHED THEN UPDATE SET name = source.name")
        rows = dict(conn.sql("select id, name from src").collect())
        assert 2 not in rows          # deleted (gone = true)
        assert rows[1] == "A"         # updated
        assert rows[3] == "c"         # untouched (not in source)

    def test_sql_merge_insert_values(self, conn):
        # WHEN NOT MATCHED THEN INSERT (cols) VALUES (<exprs>) — arbitrary value expressions.
        conn.sql("MERGE INTO src USING (values (9,'z')) t(id, name) ON target.id = source.id "
                 "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, upper(source.name))")
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "Z"

    def test_sql_merge_update_expression(self, conn):
        # UPDATE SET col = <arbitrary expr> (not just plain source.col copies).
        conn.sql("MERGE INTO src USING (values (1,'x')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET name = upper(source.name) || '!' "
                 "WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X!"

    def test_sql_merge_case_expression(self, conn):
        # a CASE WHEN … THEN … END inside a clause action carries its OWN when/then keywords; the
        # clause splitter is CASE-aware so they aren't mistaken for the structural MERGE WHEN/THEN.
        conn.sql("MERGE INTO src USING (values (1,'x'),(9,'z')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET "
                 "  name = case when source.id = 1 then 'one' else source.name end "
                 "WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "one"   # CASE true branch
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"     # inserted

    def test_sql_merge_by_source_update(self, conn):
        # WHEN NOT MATCHED BY SOURCE THEN UPDATE — touch rows the source doesn't carry.
        conn.sql("MERGE INTO src USING (values (1,'A')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * "
                 "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET name = 'gone'")
        rows = dict(conn.sql("select id, name from src").collect())
        assert rows[1] == "A"          # matched → updated from source
        assert rows[2] == "gone"       # not in source → updated
        assert rows[3] == "gone"

    def test_sql_merge_update_only(self, conn):
        # update-only merge (matched update, no insert clause) is now supported.
        conn.sql("MERGE INTO src USING (values (1,'A'),(9,'z')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "A"  # updated
        assert conn.table("src").count() == 3                                      # id=9 NOT inserted


# ════════════════════════════════════════════════════════════════════════════════════════════════
# 2. Local-filesystem contract & plumbing — discovery, save-MODE contracts, connect() errors, etc.
# ════════════════════════════════════════════════════════════════════════════════════════════════
def _write_table(path, sql):
    """Write a Delta table at ``path`` from a one-off DuckDB relation (out-of-band seeding)."""
    con = duckdb.connect()
    engine.write_delta(path, con.sql(sql), mode="overwrite")
    con.close()


@pytest.fixture
def wh(tmp_path):
    root = tmp_path / "wh"
    _write_table(str(root / "dbo" / "t1"), "select * from (values (1,'a'),(2,'b')) t(id, name)")
    _write_table(str(root / "dbo" / "t2"), "select 42 as answer")
    return str(root)


def test_unreadable_table_error_drops_generated_sql(wh):
    # A folder discovered as a table whose delta_scan fails (here a _delta_log with a non-commit
    # json -> "No files in log segment", the same failure mode as the OneLake delta-kernel bug)
    # must surface the REAL engine error and the table location — but NOT echo the internal
    # `CREATE OR REPLACE VIEW ... delta_scan(...)` statement duckrun generated. Regression for
    # the confusing connect() traceback (duckdb-delta#307).
    import os
    broken_log = os.path.join(wh, "dbo", "broken", "_delta_log")
    os.makedirs(broken_log, exist_ok=True)
    with open(os.path.join(broken_log, "stray.json"), "w") as fh:
        fh.write("{}\n")  # matches the discovery glob but isn't a valid commit

    with pytest.raises(RuntimeError) as ei:
        duckrun.connect(wh, schema="dbo")
    msg = str(ei.value)
    assert "dbo.broken" in msg                       # location named
    assert ("log segment" in msg or "IO Error" in msg)  # real engine signal kept
    assert "CREATE OR REPLACE VIEW" not in msg       # generated-SQL echo gone


def test_onelake_guid_hint():
    from duckrun.session import _onelake_guid_hint

    friendly = "abfss://tpch@onelake.dfs.fabric.microsoft.com/duckrun.Lakehouse/Tables"
    hint = _onelake_guid_hint(friendly)
    assert hint is not None
    assert "duckdb-delta#307" in hint and "GUID" in hint

    guid = ("abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/"
            "22222222-2222-2222-2222-222222222222/Tables")
    assert _onelake_guid_hint(guid) is None           # already GUIDs -> no nag
    assert _onelake_guid_hint("./wh") is None          # non-abfss -> no hint


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
    _write_table(wh + "/sales/orders", "select 7 as n")
    conn = duckrun.connect(wh)
    assert set(conn.catalog.listDatabases()) == {"dbo", "sales"}
    assert conn.sql("select n from sales.orders").fetchone()[0] == 7


def test_write_modes_round_trip(wh):
    conn = duckrun.connect(wh, schema="dbo", read_only=False)

    conn.sql("select 1 as id, 'x' as v").write.mode("overwrite").saveAsTable("t3")
    assert conn.sql("select count(*) from t3").fetchone()[0] == 1

    conn.sql("select 2 as id, 'y' as v").write.mode("append").saveAsTable("t3")
    assert conn.table("t3").count() == 2

    # the default mode is 'error' → refuse to clobber an existing table.
    with pytest.raises(ValueError):
        conn.sql("select 3 as id, 'z' as v").write.saveAsTable("t3")

    # 'ignore' is a no-op when the table exists.
    conn.sql("select 99 as id, 'q' as v").write.mode("ignore").saveAsTable("t3")
    assert conn.table("t3").count() == 2


def test_append_if_unchanged_creates_then_appends(wh):
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    # First run on a missing table: nothing to fence against → create via append.
    conn.sql("select 1 id, 'a' v").write.mode("append_if_unchanged").saveAsTable("sa")
    assert conn.table("sa").count() == 1
    # Unchanged table → optimistic append commits and grows the table.
    conn.sql("select 2 id, 'b' v").write.mode("append_if_unchanged").saveAsTable("sa")
    assert sorted(conn.table("sa").collect()) == [(1, "a"), (2, "b")]
    # "safeappend" stays as a deprecated alias for append_if_unchanged.
    conn.sql("select 3 id, 'c' v").write.mode("safeappend").saveAsTable("sa")
    assert conn.table("sa").count() == 3


def test_append_if_unchanged_refuses_on_concurrent_commit(wh, monkeypatch):
    # append_if_unchanged pins to the version it read; if a writer lands before the commit, it must
    # fail loud (CommitFailedError) instead of duplicating — identical to the dbt strategy.
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("sc")
    path = conn._table_path("dbo", "sc")
    stale = engine.table_version(path, conn.storage_options)  # the version "as read"

    # A concurrent writer commits, moving HEAD past the version append_if_unchanged will pin to.
    engine.write_delta(path, duckdb.connect().sql("select 99 id, 'x' v"), mode="append")
    monkeypatch.setattr(engine, "table_version", lambda *a, **k: stale)

    with pytest.raises(CommitFailedError):
        conn.sql("select 2 id, 'b' v").write.mode("append_if_unchanged").saveAsTable("sc")


def test_overwrite_if_unchanged_commits_when_unchanged_and_creates(wh):
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,'a'),(2,'b')) t(id,v)").write.mode("overwrite").saveAsTable("ou")
    # unchanged → fenced full overwrite replaces all rows
    conn.sql("select 5 id, 'z' v").write.mode("overwrite_if_unchanged").saveAsTable("ou")
    assert conn.table("ou").collect() == [(5, "z")]
    # missing table → nothing to fence → create via plain overwrite
    conn.sql("select 7 id, 'q' v").write.mode("overwrite_if_unchanged").saveAsTable("ou_new")
    assert conn.table("ou_new").collect() == [(7, "q")]


def test_overwrite_if_unchanged_refuses_on_concurrent_commit(wh, monkeypatch):
    # overwrite_if_unchanged pins to the version it read; a foreign commit makes it fail loud
    # instead of clobbering the concurrent change — the overwrite sibling of append_if_unchanged.
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("oc")
    path = conn._table_path("dbo", "oc")
    stale = engine.table_version(path, conn.storage_options)

    engine.write_delta(path, duckdb.connect().sql("select 99 id, 'x' v"), mode="append")
    monkeypatch.setattr(engine, "table_version", lambda *a, **k: stale)

    with pytest.raises(CommitFailedError):
        conn.sql("select 2 id, 'b' v").write.mode("overwrite_if_unchanged").saveAsTable("oc")


def test_handle_pinned_mutations_refuse_after_foreign_write(wh):
    # The DeltaTable handle pins the version captured when it was taken (forName). A CONFLICTING
    # write that lands on the table AFTER the handle was taken makes delete/update/merge through
    # that handle fail loud (CommitFailedError) — poor-man's SNAPSHOT isolation, same as merge
    # (delta-rs native OCC over (V, HEAD]: a conflict aborts).
    conn = duckrun.connect(wh, schema="dbo", read_only=False)

    def handle_then_foreign_write():
        conn.sql("select * from (values (1,'a'),(2,'b')) t(id,name)") \
            .write.mode("overwrite").saveAsTable("h")
        dt = DeltaTable.forName(conn, "h")                                   # pinned at V
        # a conflicting write (full overwrite) lands after the handle was taken -> V+1
        conn.sql("select 9 as id, 'z' as name").write.mode("overwrite").saveAsTable("h")
        return dt

    with pytest.raises(CommitFailedError):
        handle_then_foreign_write().delete("id = 1")
    with pytest.raises(CommitFailedError):
        handle_then_foreign_write().update(condition="id = 1", set={"name": "'X'"})
    with pytest.raises(CommitFailedError):
        handle_then_foreign_write().merge(conn.sql("select 1 as id, 'M' as name"),
                                          "target.id = source.id").whenMatchedUpdateAll().execute()


def test_overwrite_append_stay_unsafe(wh):
    # overwrite/append are NOT fenced (Spark SaveMode parity): a write landing on the table does
    # not make a later overwrite/append fail — last-writer-wins / additive, by design.
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("us")
    pending = conn.sql("select 2 id, 'b' v")
    conn.sql("select 9 id, 'z' v").write.mode("append").saveAsTable("us")   # a write lands
    pending.write.mode("append").saveAsTable("us")                          # unfenced -> commits
    conn.sql("select 5 id, 'q' v").write.mode("overwrite").saveAsTable("us")  # unfenced -> commits
    assert conn.table("us").collect() == [(5, "q")]                         # overwrite replaced all


def test_dataframe_writes_persist_to_delta(wh):
    # create/append via saveAsTable and mutate via the DeltaTable handle must land as real Delta,
    # visible to a brand-new connection (not DuckDB-native tables in this session). conn.sql() is
    # read-only for Delta writes — these go through the DataFrame API.
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select 1 id, 'A' grp").write.mode("overwrite").saveAsTable("evt")
    conn.sql("select 2 id, 'B' grp").write.mode("append").saveAsTable("evt")
    conn.sql("select 3 id, 'C' grp").write.mode("append").saveAsTable("evt")
    evt = DeltaTable.forName(conn, "evt")
    evt.update(condition="id = 1", set={"grp": "'Z'"})
    evt.delete("id = 2")
    assert sorted(conn.sql("select * from evt").collect()) == [(1, "Z"), (3, "C")]

    # real persistence: a fresh connection reads it off the store
    fresh = duckrun.connect(wh, schema="dbo")
    assert sorted(fresh.table("evt").collect()) == [(1, "Z"), (3, "C")]


def test_read_only_is_default_and_blocks_writes(wh):
    # connect() is read-only by default: every Delta-write entry point raises PermissionError,
    # reads and native scratch still work. read_only=False opts back in.
    ro = duckrun.connect(wh, schema="dbo")
    assert ro.sql("select count(*) from t1").fetchone()[0] == 2          # reads fine
    ro.sql("create temp table scratch as select 1 x")                    # native scratch fine
    with pytest.raises(PermissionError):
        ro.sql("select 1 id").write.mode("overwrite").saveAsTable("nope")  # writer blocked
    with pytest.raises(PermissionError):
        ro.sql("insert into t1 values (3, 'c')")                          # write-DML blocked
    with pytest.raises(PermissionError):
        DeltaTable.forName(ro, "t1").delete("id = 1")                      # DeltaTable mutator blocked

    rw = duckrun.connect(wh, schema="dbo", read_only=False)
    rw.sql("select 9 id, 'z' v").write.mode("overwrite").saveAsTable("ok")  # opt-in writes
    assert rw.table("ok").count() == 1


def test_read_api(wh):
    conn = duckrun.connect(wh, schema="dbo")
    t1_path = conn._table_path("dbo", "t1")
    assert conn.read.format("delta").load(t1_path).count() == 2
    assert conn.read.format("delta").load(t1_path).count() == 2


def test_merge_insert_only(wh):
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,10),(2,10)) t(id, val)") \
        .write.mode("overwrite").saveAsTable("io")

    src = conn.sql("select * from (values (2,99),(3,99)) t(id, val)")
    DeltaTable.forName(conn, "dbo.io").merge(src, "target.id = source.id") \
        .whenNotMatchedInsertAll().execute()

    assert conn.table("io").count() == 3                                    # only id=3 added
    assert conn.sql("select val from io where id = 2").fetchone()[0] == 10  # existing untouched


def test_update_only_merge(wh):
    # update-only merge (matched update, no insert) is supported — only existing rows change.
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    src = conn.sql("select * from (values (1,'A'),(9,'z')) t(id, name)")
    DeltaTable.forName(conn, "dbo.t1").merge(src, "target.id = source.id") \
        .whenMatchedUpdateAll().execute()
    assert conn.sql("select name from t1 where id = 1").fetchone()[0] == "A"  # updated
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2             # id=9 NOT inserted


def test_toPandas(wh):
    # .toPandas()/.df() are the only pandas-touching bits of the API (DuckDB materializes to a
    # pandas DataFrame, like the DataFrame API's toPandas). pandas is in the [test] extra so this runs for real.
    conn = duckrun.connect(wh, schema="dbo")
    pdf = conn.sql("select name from t1 order by id").toPandas()
    assert list(pdf["name"]) == ["a", "b"]


def test_dataframe_show(wh):
    # .show() is the print alias over the DuckDB relation: prints to stdout, returns None.
    conn = duckrun.connect(wh, schema="dbo")
    assert conn.sql("select * from t1 order by id").show() is None


def test_raw_connection_escape_hatch(wh):
    # conn._connection exposes the underlying DuckDB connection for anything the DataFrame surface
    # doesn't cover — scalar queries, and reading the registered views directly.
    conn = duckrun.connect(wh, schema="dbo")
    assert conn._connection.execute("select 40 + 2").fetchone()[0] == 42
    assert conn._connection.execute("select count(*) from t1").fetchone()[0] == 2


def test_refresh_picks_up_external_writes(wh):
    # A table created on the store after connect is invisible until refresh() re-discovers it.
    conn = duckrun.connect(wh, schema="dbo")
    assert "t3" not in conn.catalog.listTables()
    _write_table(wh + "/dbo/t3", "select 1 as id")
    conn.refresh(quiet=True)
    assert "t3" in conn.catalog.listTables()
    assert conn.table("t3").count() == 1


def test_reader_parquet_csv_and_table(wh, tmp_path):
    conn = duckrun.connect(wh, schema="dbo")
    pq = str(tmp_path / "t1.parquet")
    csv = str(tmp_path / "t1.csv")
    conn._connection.execute(f"COPY (select * from t1) TO '{pq}' (FORMAT parquet)")
    conn._connection.execute(f"COPY (select * from t1) TO '{csv}' (FORMAT csv, HEADER)")

    assert conn.read.parquet(pq).count() == 2
    # csv read with an explicit option (header) routed through DataFrameReader.option.
    assert conn.read.format("csv").option("header", True).load(csv).count() == 2
    assert conn.read.csv(csv).count() == 2
    # read.table is the by-name shortcut (same as conn.table).
    assert conn.read.table("t1").count() == 2


def test_writer_format(wh):
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    # .format('delta') is accepted (the only supported writer format)…
    conn.sql("select 1 id").write.format("delta").mode("overwrite").saveAsTable("wf")
    assert conn.table("wf").count() == 1
    # …anything else is rejected up front.
    with pytest.raises(ValueError):
        conn.sql("select 1 id").write.format("parquet")


def test_catalog_database_and_column_introspection(wh):
    # A second schema folder so setCurrentDatabase / databaseExists have something to switch to.
    _write_table(wh + "/sales/orders", "select 7 as n, 'x' as label")
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
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("ver")
    path = conn._table_path("dbo", "ver")

    by_name = DeltaTable.forName(conn, "dbo.ver")
    by_path = DeltaTable.forPath(conn, path)
    assert by_path.version() == by_name.version() == 0

    conn.sql("select 2 id, 'b' v").write.mode("append").saveAsTable("ver")
    assert DeltaTable.forPath(conn, path).version() == 1   # a new commit bumps the version


# ── createDataFrame: every input/schema form ──────────────────────────────────────────────────────
# The scorecard (TestSession.test_createDataFrame) has the one-liner; these are the edge cases, merged
# in from the former test_create_dataframe.py so CI (which runs THIS file by path) actually runs them.
def _cdf_types(df):
    return [str(t) for t in df.relation.types]


def test_createDataFrame_tuples_no_schema_autonames(conn):
    df = conn.createDataFrame([(1, "a"), (2, "b")])
    assert df.columns == ["_1", "_2"]
    assert df.collect() == [(1, "a"), (2, "b")]


def test_createDataFrame_tuples_with_names(conn):
    df = conn.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert df.columns == ["id", "name"]
    assert df.count() == 2


def test_createDataFrame_ddl_casts_types(conn):
    df = conn.createDataFrame([(1, "a")], "id int, name string")
    assert df.columns == ["id", "name"]
    assert _cdf_types(df) == ["INTEGER", "VARCHAR"]


def test_createDataFrame_ddl_colon_spelling(conn):
    df = conn.createDataFrame([(1, "a")], "id: int, name: string")
    assert df.columns == ["id", "name"]
    assert _cdf_types(df) == ["INTEGER", "VARCHAR"]


def test_createDataFrame_ddl_decimal_with_comma_survives(conn):
    df = conn.createDataFrame([(1, "1.50")], "id long, amount decimal(10,2)")
    assert _cdf_types(df) == ["BIGINT", "DECIMAL(10,2)"]
    assert df.collect() == [(1, pytest.approx(1.50))]


def test_createDataFrame_list_of_scalars_single_column(conn):
    df = conn.createDataFrame([1, 2, 3], "value int")
    assert df.columns == ["value"]
    assert [r[0] for r in df.collect()] == [1, 2, 3]


def test_createDataFrame_ragged_rows_error(conn):
    with pytest.raises(ValueError, match="same number of columns"):
        conn.createDataFrame([(1, "a"), (2,)])


def test_createDataFrame_pandas(conn):
    pd = pytest.importorskip("pandas")
    df = conn.createDataFrame(pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    assert df.columns == ["id", "name"]
    assert df.count() == 2


def test_createDataFrame_pandas_with_schema_rename(conn):
    pd = pytest.importorskip("pandas")
    df = conn.createDataFrame(pd.DataFrame({"x": [1], "y": ["a"]}), ["id", "name"])
    assert df.columns == ["id", "name"]


def test_createDataFrame_pyarrow_table(conn):
    pa = pytest.importorskip("pyarrow")
    df = conn.createDataFrame(pa.table({"id": [1, 2], "name": ["a", "b"]}))
    assert df.columns == ["id", "name"]
    assert df.count() == 2


def test_createDataFrame_empty_with_ddl(conn):
    df = conn.createDataFrame([], "id int, name string")
    assert df.columns == ["id", "name"]
    assert _cdf_types(df) == ["INTEGER", "VARCHAR"]
    assert df.count() == 0


def test_createDataFrame_empty_without_schema_errors(conn):
    with pytest.raises(ValueError, match="empty dataset"):
        conn.createDataFrame([])


def test_createDataFrame_name_count_mismatch_errors(conn):
    with pytest.raises(ValueError, match="columns"):
        conn.createDataFrame([(1, "a")], ["only_one"])


def test_createDataFrame_bad_schema_type_errors(conn):
    with pytest.raises(TypeError, match="schema must be"):
        conn.createDataFrame([(1,)], schema=123)


def test_createDataFrame_column_name_with_embedded_quote(conn):
    # Regression: _project_rename must escape identifiers via _qid. A column name containing a
    # double-quote previously built broken SQL (`"_1" AS "id"x"`); now it round-trips.
    df = conn.createDataFrame([(1, "a"), (2, "b")], ['id"x', "name"])
    assert df.columns == ['id"x', "name"]
    assert df.count() == 2


def test_createDataFrame_round_trip_to_delta(conn):
    conn.createDataFrame([(1, "a"), (2, "b")], "id int, name string") \
        .write.mode("overwrite").saveAsTable("seeded")
    fresh = duckrun.connect(conn.root_path, schema="dbo", read_only=True)
    assert fresh.table("seeded").relation.order("id").fetchall() == [(1, "a"), (2, "b")]


def test_createDataFrame_no_experimental_spark_import():
    src = Path(session_mod.__file__).read_text(encoding="utf-8")
    assert "experimental.spark" not in src


# ── convertToDelta: zero-copy parquet→Delta in place (delta-spark DeltaTable.convertToDelta) ─────────
# The scorecard (TestDeltaTable.test_convertToDelta) has the one-liner; these are the identifier /
# zero-copy / guard cases, merged in from the former test_convert_to_delta.py.
def _stage_parquet(conn, rel_dir, sql="select 1 AS id, 'a' AS nm"):
    """Stage a parquet file at <root>/<rel_dir>/data.parquet via the raw DuckDB connection
    (out-of-band — a native parquet write, not a Delta write, so the read-only gate lets it through)."""
    path = conn.root_path + "/" + rel_dir
    os.makedirs(path, exist_ok=True)
    conn._connection.execute(f"COPY ({sql}) TO '{path}/data.parquet' (FORMAT parquet)")
    return path


def test_convertToDelta_parse_identifier_spark_form():
    assert _parse_parquet_identifier("parquet.`/a/b`") == "/a/b"
    assert _parse_parquet_identifier("PARQUET . `/a/b` ") == "/a/b"


def test_convertToDelta_parse_identifier_bare_path():
    assert _parse_parquet_identifier("/a/b") == "/a/b"


def test_convertToDelta_parse_identifier_rejects_empty():
    with pytest.raises(ValueError, match="non-empty path"):
        _parse_parquet_identifier("   ")


def test_convertToDelta_bare_path_round_trip(conn):
    path = _stage_parquet(conn, "dbo/bare")
    DeltaTable.convertToDelta(conn, path)            # a bare path works, not just parquet.`…`
    conn.refresh()
    assert conn.table("bare").count() == 1


def test_convertToDelta_is_zero_copy(conn):
    path = _stage_parquet(conn, "dbo/zc")
    DeltaTable.convertToDelta(conn, path)
    assert os.path.exists(path + "/data.parquet")    # original parquet survives
    assert os.path.isdir(path + "/_delta_log")       # only a _delta_log was added


def test_convertToDelta_read_only_raises(tmp_path):
    ro = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=True)
    path = _stage_parquet(ro, "dbo/blocked")
    with pytest.raises(PermissionError):
        DeltaTable.convertToDelta(ro, path)


def test_convertToDelta_already_delta_raises(conn):
    path = _stage_parquet(conn, "dbo/twice")
    DeltaTable.convertToDelta(conn, path)
    with pytest.raises(Exception):                   # delta-rs mode='error' on an already-converted dir
        DeltaTable.convertToDelta(conn, path)


# ════════════════════════════════════════════════════════════════════════════════════════════════
# 3. Write-correctness matrix — cross-API equivalence is the oracle (SQL ≡ DataFrame → byte-identical).
# ════════════════════════════════════════════════════════════════════════════════════════════════
def _k(row):
    """Order-insensitive, None-safe sort key for a row of mixed scalars."""
    return tuple(str(c) for c in row)


def _seed(wh):
    """Seed a fresh warehouse at ``wh`` with the canonical tables and return the connection:
    ``items(id, name)`` = (1,a),(2,b),(3,c) and ``wide(id, name, qty)`` = (1,a,10)."""
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)") \
        .write.mode("overwrite").saveAsTable("items")
    conn.sql("select * from (values (1,'a',10)) t(id, name, qty)") \
        .write.mode("overwrite").saveAsTable("wide")
    return conn


def _dump(wh, name):
    """Read ``name`` back through a FRESH connection → ``(columns, rows-sorted)``."""
    rel = duckrun.connect(wh, schema="dbo").sql(f"select * from {name}")
    return list(rel.columns), sorted(rel.fetchall(), key=_k)


def _select(wh, sql):
    """Run an explicit projection on a fresh connection → rows-sorted (column-order-stable)."""
    return sorted(duckrun.connect(wh, schema="dbo").sql(sql).fetchall(), key=_k)


def _dtypes(wh, name):
    rel = duckrun.connect(wh, schema="dbo").sql(f"select * from {name}")
    return {c: str(t) for c, t in zip(rel.columns, rel.types)}


# Tier 1 — cross-API equivalence (the core oracle). Each pair expresses the SAME logical write via
# the SQL API and the DataFrame API against the same seed; `expected` (in items column order) anchors it.
EQUIV = [
    dict(id="overwrite", table="items",
         sql=["create or replace table items as select * from (values (9,'z'),(8,'y')) t(id, name)"],
         dataframe=lambda c: c.sql("select * from (values (9,'z'),(8,'y')) t(id, name)")
                          .write.mode("overwrite").saveAsTable("items"),
         expected=[(9, "z"), (8, "y")]),
    dict(id="append_select", table="items",
         sql=["insert into items select * from (values (4,'d')) t(id, name)"],
         dataframe=lambda c: c.sql("select * from (values (4,'d')) t(id, name)")
                          .write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="append_values", table="items",
         sql=["insert into items values (5, 'e')"],
         dataframe=lambda c: c.sql("select 5 id, 'e' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (5, "e")]),
    dict(id="append_collist_reordered", table="items",
         # SQL maps by name from a reordered column list; the DataFrame API appends an in-order df — both land id=4,name='d'.
         sql=["insert into items (name, id) select 'd', 4"],
         dataframe=lambda c: c.sql("select 4 id, 'd' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="with_prefixed_insert", table="items",
         sql=["with s as (select 8 id, 'h' as name) insert into items select * from s"],
         dataframe=lambda c: c.sql("select 8 id, 'h' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (8, "h")]),
    dict(id="update_predicate", table="items",
         sql=["update items set name = 'Z' where id = 1"],
         dataframe=lambda c: DeltaTable.forName(c, "items").update(condition="id = 1", set={"name": "'Z'"}),
         expected=[(1, "Z"), (2, "b"), (3, "c")]),
    dict(id="delete_predicate", table="items",
         sql=["delete from items where id = 2"],
         dataframe=lambda c: DeltaTable.forName(c, "items").delete("id = 2"),
         expected=[(1, "a"), (3, "c")]),
    dict(id="upsert", table="items",
         # SQL upsert = delete literal keys + insert (delta-rs DELETE takes literals, not IN (SELECT)).
         sql=["delete from items where id = 2 or id = 4", "insert into items values (2, 'B'), (4, 'D')"],
         dataframe=lambda c: DeltaTable.forName(c, "items")
             .merge(c.sql("select * from (values (2,'B'),(4,'D')) t(id, name)"), "target.id = source.id")
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute(),
         expected=[(1, "a"), (2, "B"), (3, "c"), (4, "D")]),
]


@pytest.mark.parametrize("case", EQUIV, ids=[c["id"] for c in EQUIV])
def test_sql_equals_dataframe(tmp_path, case):
    a, b = str(tmp_path / "A"), str(tmp_path / "B")
    ca = _seed(a)
    for stmt in case["sql"]:
        ca.sql(stmt)
    case["dataframe"](_seed(b))

    dump_a, dump_b = _dump(a, case["table"]), _dump(b, case["table"])
    assert dump_a == dump_b, f"SQL≠DataFrame for {case['id']}: {dump_a} vs {dump_b}"
    assert dump_a[1] == sorted(case["expected"], key=_k)   # anchor: agreeing-but-wrong can't pass


# Tier 1b — SQL-routing forms with no DataFrame-API equivalent; the assertion is the persisted data.
@pytest.mark.parametrize("stmt,table,cols,rows", [
    ("-- build it\ncreate table cm as select 1 id", "cm", ["id"], [(1,)]),
    ("/* note */ insert into items values (4, 'd')", "items", ["id", "name"],
     [(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    ("create table p as (select 5 id)", "p", ["id"], [(5,)]),
    ("create table c as with s as (select * from (values (1),(2),(3)) t(id)) select id from s",
     "c", ["id"], [(1,), (2,), (3,)]),
    ("create table if not exists items as select 99 id, 'x' name", "items", ["id", "name"],
     [(1, "a"), (2, "b"), (3, "c")]),   # table exists → no-op, seed untouched
    # Regression: a `select` keyword inside a string LITERAL in the VALUES payload (e.g. Elementary's
    # run-results upload, whose `compiled_code` column value contains `\n    select * from (…)`) must
    # NOT make duckrun mis-read INSERT…VALUES as INSERT…SELECT and fall through to the read-only
    # delta_scan view ("… is not a table"). The classification is by the first top-level keyword.
    ("insert into items values (4, '\n    select * from (foo)')", "items", ["id", "name"],
     [(1, "a"), (2, "b"), (3, "c"), (4, "\n    select * from (foo)")]),
], ids=["leading_line_comment", "leading_block_comment", "create_parenthesised",
        "create_as_cte", "create_if_not_exists_noop", "insert_values_select_in_literal"])
def test_sql_routing_lands_correct_delta(tmp_path, stmt, table, cols, rows):
    wh = str(tmp_path / "wh")
    _seed(wh).sql(stmt)
    got_cols, got_rows = _dump(wh, table)
    assert got_cols == cols
    assert got_rows == sorted(rows, key=_k)


def test_split_top_level_skips_dollar_quoted_bodies():
    # Regression: dbt persist_docs emits `COMMENT ON ... IS $tag$...$tag$`, and the body can carry
    # embedded quotes and ';'. The multi-statement splitter must treat a dollar-quoted run as opaque,
    # or it fragments the COMMENT into pieces with an unterminated $-quote (broke conformance
    # test_persist_docs::test_has_comments_pglike — DuckDB "unterminated dollar-quoted string").
    from dbt.adapters.duckrun.delta_dml import _split_top_level
    batch = ('comment on view "d"."s"."v" is $tag$desc "q" and \'q\'; x$tag$;'
             ' comment on column "d"."s"."v"."id" is $tag$the; id$tag$')
    assert len(_split_top_level(batch)) == 2          # NOT 4 — ';' inside $tag$...$tag$ is literal
    # a genuine multi-statement batch still splits
    assert len(_split_top_level("delete from t where id=1; insert into t values (1)")) == 2


def test_partial_insert_null_fills_and_keeps_type(tmp_path):
    # INSERT with a column list shorter than the table null-fills the rest — and the omitted column
    # keeps its declared type (no drift to a nullable string).
    wh = str(tmp_path / "wh")
    _seed(wh).sql("insert into wide (id, name) values (7, 'g')")
    assert _select(wh, "select qty from wide where id = 7") == [(None,)]
    assert _dtypes(wh, "wide")["qty"] in ("INTEGER", "BIGINT", "HUGEINT")


# The lossy-numeric guard: INSERT fails loud when a numeric value would be SILENTLY changed by the
# cast onto its target column (e.g. 3.9 → INTEGER lands 4). The intentional alignment — timestamp ntz,
# int widening, whole-number decimals — is untouched. (wide.qty is INTEGER.)
@pytest.mark.parametrize("stmt", [
    "insert into wide (id, name, qty) values (7, 'g', 3.9)",            # fractional → INTEGER
    "insert into wide (id, name, qty) values (7, 'g', 9999999999999)",  # out of INTEGER range
    "insert into wide select 7, 'g', 3.9",                             # same loss via a SELECT body
], ids=["values_fractional", "values_out_of_range", "select_fractional"])
def test_insert_rejects_lossy_numeric_narrowing(tmp_path, stmt):
    with pytest.raises(ValueError, match="silently narrow"):
        _seed(str(tmp_path / "wh")).sql(stmt)


def test_insert_allows_whole_number_decimal(tmp_path):
    # 4.0 → INTEGER loses nothing (round-trips), so it is allowed and lands 4.
    wh = str(tmp_path / "wh")
    _seed(wh).sql("insert into wide (id, name, qty) values (7, 'g', 4.0)")
    assert _select(wh, "select qty from wide where id = 7") == [(4,)]


def test_insert_allows_widening_numeric(tmp_path):
    # An int literal into a BIGINT column is a lossless widening — not flagged.
    wh = str(tmp_path / "wh")
    c = duckrun.connect(wh, schema="dbo", read_only=False)
    c.sql("select cast(1 as bigint) as id").write.mode("overwrite").saveAsTable("big")
    c.sql("insert into big values (2)")
    assert _dump(wh, "big")[1] == sorted([(1,), (2,)], key=_k)


# Tier 2 — single-API ops; inline golden expected, read back via a fresh connection.
def test_replace_where_dataframe_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select * from (values (1,'eu'),(2,'us'),(3,'eu')) t(id, region)") \
        .write.mode("overwrite").saveAsTable("rw")
    c.sql("select 9 id, 'eu' region").write.option("replaceWhere", "region = 'eu'") \
        .mode("overwrite").saveAsTable("rw")
    assert _dump(wh, "rw")[1] == sorted([(2, "us"), (9, "eu")], key=_k)


def test_merge_sync_delete_dataframe_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select * from (values (1,10,'a'),(2,10,'b'),(3,10,'c')) t(id, val, note)") \
        .write.mode("overwrite").saveAsTable("sync")
    src = c.sql("select * from (values (2,99,'X'),(4,99,'Y')) t(id, val, note)")
    DeltaTable.forName(c, "sync").merge(src, "target.id = source.id") \
        .whenMatchedUpdate(set={"val": "source.val"}) \
        .whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    # id=2 val-only update (note 'b' kept), id=4 inserted, ids 1 & 3 deleted (absent from source).
    assert _dump(wh, "sync")[1] == sorted([(2, 99, "b"), (4, 99, "Y")], key=_k)


def test_partition_and_merge_schema_dataframe_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select 1 id, 'eu' region").write.mode("overwrite").partitionBy("region").saveAsTable("p")
    c.sql("select 2 id, 'us' region, true flag") \
        .write.mode("append").option("mergeSchema", "true").partitionBy("region").saveAsTable("p")
    assert "flag" in _dump(wh, "p")[0]
    assert _select(wh, "select id, region, flag from p") == \
        sorted([(1, "eu", None), (2, "us", True)], key=_k)


def test_overwrite_schema_dataframe_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select 1 id, 'x' a, 'y' b").write.mode("overwrite").saveAsTable("os")
    assert _dump(wh, "os")[0] == ["id", "a", "b"]
    with pytest.raises(Exception):   # plain overwrite can't silently drop columns
        c.sql("select 2 id").write.mode("overwrite").saveAsTable("os")
    c.sql("select 2 id").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("os")
    assert _dump(wh, "os") == (["id"], [(2,)])


def test_alter_add_column_sql_only(tmp_path):
    wh = str(tmp_path / "wh")
    _seed(wh).sql("alter table items add column qty integer")
    assert "qty" in _dump(wh, "items")[0]
    assert _select(wh, "select count(*) from items where qty is null") == [(3,)]


def test_create_coldefs_empty_and_logs_create_table(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("create table e (id integer, name varchar)")
    assert _dump(wh, "e") == (["id", "name"], [])
    c.sql("create table money (id integer, amount decimal(10, 2))")   # nested coldefs parse
    assert _dump(wh, "money") == (["id", "amount"], [])
    # A bare CREATE TABLE records a CREATE TABLE op, NOT a WRITE/Overwrite (that's CREATE OR REPLACE).
    c.sql("create table fresh (i integer)")
    dt = deltalake.DeltaTable(c._table_path("dbo", "fresh"))
    assert dt.history(1000)[-1]["operation"] == "CREATE TABLE"   # history newest-first; oldest = create


def test_drop_tombstones_without_deleting_data(tmp_path):
    # `drop table` marks the table dropped (one-column tombstone via delta_rs) WITHOUT deleting data:
    # gone from the catalog, files stay, and a later create-as revives it with the real schema.
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    path = c._table_path("dbo", "items")
    c.sql("drop table items")
    assert "items" not in c.catalog.listTables()
    with pytest.raises(Exception):
        c.table("items")
    assert deltalake.DeltaTable.is_deltatable(path)                               # NOT deleted
    assert [f.name for f in deltalake.DeltaTable(path).schema().fields] == [TOMBSTONE_COLUMN]
    assert "items" not in duckrun.connect(wh, schema="dbo").catalog.listTables()  # fresh conn hides it
    c.sql("create table items as select * from (values (10,'x')) t(id, name)")
    assert _dump(wh, "items") == (["id", "name"], [(10, "x")])


# Tier 3 — native passthrough: queryable in-session, but NOT a Delta table.
@pytest.mark.parametrize("stmt,name", [
    ("create temp table scratch as select 1 x", "scratch"),
    ("create view v as select 1 x", "v"),
], ids=["temp_table", "view"])
def test_native_passthrough_not_delta(tmp_path, stmt, name):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql(stmt)
    assert c.sql(f"select count(*) from {name}").fetchone()[0] is not None   # queryable now
    assert not deltalake.DeltaTable.is_deltatable(c._table_path("dbo", name))  # but not Delta
    assert name not in duckrun.connect(wh, schema="dbo").catalog.listTables()  # and not persisted


# Tier 4 — rejection contract: conn.sql() refuses what it can't route to delta_rs.
# (A raw MERGE *is* routed — see TestSqlDml — but its ON aliases must resolve to a side.)
@pytest.mark.parametrize("stmt,msg", [
    ("merge into items t using items s on foo.id = bar.id when matched then update set * "
     "when not matched then insert *",
     "target.*source"),
    ("update items set name = o.name from wide o where items.id = o.id", "UPDATE . FROM"),
    ("delete from items using wide o where items.id = o.id", "DELETE . USING"),
    ("insert into items values (4,'d'); insert into items values (5,'e')", "one statement"),
    ("create table items as select 1 id, 'a' name", "already exists"),
    ("create table items (id integer)", "already exists"),
], ids=["merge_bad_alias", "update_from", "delete_using", "multi_statement",
        "create_as_exists", "create_coldefs_exists"])
def test_rejected(tmp_path, stmt, msg):
    with pytest.raises(ValueError, match=msg):
        _seed(str(tmp_path / "wh")).sql(stmt)


@pytest.mark.xfail(strict=True, reason="WITH … UPDATE can't be expressed through a delta_rs predicate")
def test_with_prefixed_update_parked(tmp_path):
    # Leading-CTE UPDATE is parked: the CTE can't be threaded into a delta_rs predicate, so it falls
    # through and raises rather than applying — pinned strict-xfail so the gap stays visible.
    c = _seed(str(tmp_path / "wh"))
    c.sql("with bump as (select 1 id) update items set name = 'Z' where id in (select id from bump)")
    assert c.sql("select name from items where id = 1").fetchone()[0] == "Z"


# ════════════════════════════════════════════════════════════════════════════════════════════════
# Multi-catalog — attach a second+ lakehouse root as a named DuckDB catalog (catalog.schema.table).
# Each catalog is its own lakehouse root; storage-neutral, so the local-fs run is representative of
# OneLake (only the secret/discovery backend differs). The genuine mixed local+abfss case is the
# WAREHOUSE_PATH-gated test at the bottom.
# ════════════════════════════════════════════════════════════════════════════════════════════════
def _two_lakehouses(tmp_path):
    """connect(A) + attach(B, name='other'); A has dbo.t1, B has dbo.t2 + sales.s. Returns the conn."""
    a, b = str(tmp_path / "lhA"), str(tmp_path / "lhB")
    _write_table(a + "/dbo/t1", "select * from (values (1,'a'),(2,'b')) t(id, name)")
    _write_table(b + "/dbo/t2", "select 7 as n")
    _write_table(b + "/sales/s", "select 'x' as label")
    conn = duckrun.connect(a, read_only=False)
    conn.attach(b, name="other")
    return conn


def test_multi_catalog_cross_query(tmp_path):
    conn = _two_lakehouses(tmp_path)
    assert conn.catalog.listCatalogs() == ["lhA", "other"]   # primary derives its folder name "lhA"
    assert conn.catalog.currentCatalog() == "lhA"
    # cross-catalog read resolves catalog.schema.table across the two lakehouse roots.
    assert conn.sql("select n from other.dbo.t2").fetchone()[0] == 7
    assert conn.sql("select label from other.sales.s").fetchone()[0] == "x"
    # 2-part / unqualified still resolve in the CURRENT catalog (lhA), not the attached one.
    assert conn.sql("select count(*) from dbo.t1").fetchone()[0] == 2
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2  # via USE lhA.dbo


def test_multi_catalog_set_current(tmp_path):
    conn = _two_lakehouses(tmp_path)
    conn.catalog.setCurrentCatalog("other")
    assert conn.catalog.currentCatalog() == "other"
    assert conn.catalog.currentDatabase() == "dbo"          # picks dbo when present
    assert conn.sql("select n from t2").fetchone()[0] == 7   # unqualified now resolves in 'other'
    assert set(conn.catalog.listDatabases()) == {"dbo", "sales"}  # other's schemas, not lhA's
    assert conn.catalog.listTables() == ["t2"]              # current catalog (other) + db (dbo)


def test_multi_catalog_write_lands_in_right_root(tmp_path):
    conn = _two_lakehouses(tmp_path)
    # a cross-catalog DataFrame write must land under the ATTACHED root (lhB), not the primary (lhA).
    conn.sql("select 1 as id, 'z' as v").write.mode("overwrite").saveAsTable("other.dbo.created")
    assert deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhB" / "dbo" / "created"))
    assert not deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhA" / "dbo" / "created"))
    assert conn.sql("select v from other.dbo.created").fetchone()[0] == "z"
    # and the DeltaTable handle resolves the attached catalog's path + storage_options.
    dt = DeltaTable.forName(conn, "other.dbo.created")
    assert dt.path.replace("\\", "/").endswith("lhB/dbo/created")


def test_multi_catalog_bijective_guards(tmp_path):
    conn = _two_lakehouses(tmp_path)
    b = str(tmp_path / "lhB")
    with pytest.raises(ValueError, match="already attached"):       # name taken
        conn.attach(b, name="other")
    with pytest.raises(ValueError, match="already attached as"):    # same URL, any name
        conn.attach(b, name="different")
    guid = ("abfss://ws@onelake.dfs.fabric.microsoft.com/"
            "22222222-2222-2222-2222-222222222222.Lakehouse/Tables/dbo")
    with pytest.raises(ValueError, match="could not derive a catalog name"):  # GUID, no name=
        conn.attach(guid)


def test_primary_name_derive_explicit_and_fallback(tmp_path):
    # No name= → derive from the path's last segment (the folder name).
    a = str(tmp_path / "wh")
    _write_table(a + "/dbo/t", "select 1 as id")
    assert duckrun.connect(a).catalog.currentCatalog() == "wh"
    # Explicit name= wins.
    assert duckrun.connect(a, name="picked").catalog.currentCatalog() == "picked"
    # Nothing derivable (a GUID-shaped segment → _derive_catalog_name returns None) → "data" fallback,
    # which is non-reserved so it's usable bare in 3-part SQL.
    g = str(tmp_path / "11111111-1111-1111-1111-111111111111")
    _write_table(g + "/dbo/t", "select 1 as id")
    conn = duckrun.connect(g)
    assert conn.catalog.currentCatalog() == "data"
    assert conn.sql("select count(*) from data.dbo.t").fetchone()[0] == 1   # bare, no quoting needed


def test_name_url_bijection_with_derived_names(tmp_path, tmp_path_factory):
    # one name <-> one url, even when names are AUTO-DERIVED: two different roots whose folders derive
    # the SAME name must not silently collide.
    a = str(tmp_path / "wh")
    b = str(tmp_path_factory.mktemp("other") / "wh")   # different URL, same derived name "wh"
    _write_table(a + "/dbo/t", "select 1 as id")
    _write_table(b + "/dbo/t", "select 2 as id")
    conn = duckrun.connect(a, read_only=False)         # primary derives "wh"
    with pytest.raises(ValueError, match="already attached|another name"):
        conn.attach(b)                                 # derived name "wh" clashes with the primary
    conn.attach(b, name="wh2")                         # an explicit name resolves it
    assert set(conn.catalog.listCatalogs()) == {"wh", "wh2"}
    assert conn.sql("select id from wh2.dbo.t").fetchone()[0] == 2


def test_attach_schema_filter_skips_discovery(tmp_path):
    # schema= on attach restricts discovery to that one schema (no full glob) — other schemas absent.
    a, b = str(tmp_path / "lhA"), str(tmp_path / "lhB")
    _write_table(a + "/dbo/t1", "select 1 as id")
    _write_table(b + "/dbo/keep", "select 1 as id")
    _write_table(b + "/skipme/hidden", "select 1 as id")
    conn = duckrun.connect(a, read_only=False)
    conn.attach(b, name="other", schema="dbo")
    conn.catalog.setCurrentCatalog("other")
    assert conn.catalog.listDatabases() == ["dbo"]          # skipme not discovered
    assert conn.catalog.listTables() == ["keep"]


def test_cross_catalog_raw_dml_routes_to_target_root(tmp_path):
    # raw DML through conn.sql() routes by the 3-part target's catalog: a target naming ANOTHER
    # catalog writes to THAT catalog's root (delta_rs gets the target's root + storage_options),
    # not the current one. The current catalog stays untouched.
    conn = _two_lakehouses(tmp_path)              # current = lhA; 'other' = lhB (dbo.t2 has 1 row)

    # INSERT ... VALUES into the attached catalog lands under lhB, leaving lhA's dbo.t2 absent.
    conn.sql("insert into other.dbo.t2 values (8)")
    assert conn.sql("select count(*) from other.dbo.t2").fetchone()[0] == 2
    assert deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhB" / "dbo" / "t2"))
    assert not deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhA" / "dbo" / "t2"))

    # INSERT ... SELECT reading from the CURRENT catalog (lhA.dbo.t1) into the attached one (the
    # user's literal shape, cross-root): both rows of t1 append to other.dbo.t2.
    conn.sql("insert into other.dbo.t2 select id from lhA.dbo.t1")
    assert conn.sql("select count(*) from other.dbo.t2").fetchone()[0] == 4

    # a non-INSERT verb routes the same way: UPDATE/DELETE hit the target catalog's root.
    conn.sql("delete from other.dbo.t2 where n = 8")
    assert conn.sql("select count(*) from other.dbo.t2").fetchone()[0] == 3

    # a 3-part target naming the CURRENT catalog ("lhA") still works (routes to the current root).
    conn.sql("insert into lhA.dbo.t1 values (3, 'c')")
    assert conn.sql("select count(*) from t1").fetchone()[0] == 3

    # an unknown catalog name is rejected with the attach pointer (not silently written anywhere).
    with pytest.raises(ValueError, match="unknown catalog 'nope'"):
        conn.sql("insert into nope.dbo.t2 values (1)")


def test_attach_read_only_catalog_fences_writes(tmp_path):
    # A read-only attached catalog fences writes independently of the (writable) session/primary —
    # so a reference store (e.g. a Fabric Warehouse) can sit next to a writable lakehouse.
    a, b = str(tmp_path / "lhA"), str(tmp_path / "ref")
    _write_table(a + "/dbo/t1", "select 1 as id")
    _write_table(b + "/dbo/lookup", "select 1 as id, 'one' as label")
    conn = duckrun.connect(a, read_only=False)              # primary writable
    conn.attach(b, name="ref", read_only=True)              # attached read-only

    # the read-only catalog still reads (cross-catalog) ...
    assert conn.sql("select label from ref.dbo.lookup").fetchone()[0] == "one"
    # ... but every write entry point into it fails loud, even though the session is writable.
    with pytest.raises(PermissionError):
        conn.sql("select 2 as id, 'two' as label").write.mode("append").saveAsTable("ref.dbo.lookup")
    with pytest.raises(PermissionError):
        DeltaTable.forName(conn, "ref.dbo.lookup").delete("id = 1")
    # cross-catalog raw DML fences off the TARGET catalog's read-only flag, not the current one.
    with pytest.raises(PermissionError):
        conn.sql("insert into ref.dbo.lookup values (2, 'two')")
    # writes to the writable primary still work.
    conn.sql("select 2 as id").write.mode("append").saveAsTable("t1")
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2


def test_stop_closes_connection(tmp_path):
    conn = _two_lakehouses(tmp_path)
    conn.stop()
    with pytest.raises(Exception):
        conn.sql("select 1").collect()  # underlying DuckDB connection is closed


def test_context_manager_closes_connection(tmp_path):
    # `with duckrun.connect(...) as conn:` closes the connection on exit.
    with duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False) as conn:
        conn.sql("select 1 as x").write.mode("overwrite").saveAsTable("t")
        assert conn.sql("select count(*) from t").collect()[0][0] == 1
    with pytest.raises(Exception):
        conn.sql("select 1").collect()  # closed on `with` exit


def test_primary_secret_mint_failure_raises_and_closes_connection(tmp_path, monkeypatch):
    # A primary catalog whose secret can't be minted must fail loud at connect() — not warn and
    # resurface as a cryptic 403 later — AND must not leak the DuckDB connection it opened.
    from duckrun import session as S

    created = []
    real_connect = S.duckdb.connect

    def tracking_connect(*a, **k):
        c = real_connect(*a, **k)
        created.append(c)
        return c

    def boom(*a, **k):
        raise RuntimeError("mint exploded")

    monkeypatch.setattr(S.duckdb, "connect", tracking_connect)
    monkeypatch.setattr(S.secret, "ensure_azure_secret", boom)

    with pytest.raises(RuntimeError, match="could not mint OneLake secret"):
        duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)

    assert created, "expected connect() to have opened a DuckDB connection"
    with pytest.raises(Exception):
        created[-1].execute("select 1")  # closed by the failed __init__, not leaked


# Weird-but-valid catalog names: duckrun quotes every identifier (_qid), so a name that's a SQL
# reserved word, or has spaces / dashes / unicode / mixed case / a leading digit, works fine — you
# just quote it in your own SQL. (A dot is the one thing a name can't contain: it's the
# catalog.schema.table separator.)
_WEIRD_NAMES = ["select", "my lake", "cat-2024", "café", "MixedCase", "default", "123start"]


@pytest.mark.parametrize("weird", _WEIRD_NAMES)
def test_weird_attached_catalog_names(tmp_path, tmp_path_factory, weird):
    a = str(tmp_path / "wh")
    b = str(tmp_path_factory.mktemp("store") / "store")
    _write_table(a + "/dbo/t", "select 1 as id")
    _write_table(b + "/dbo/t2", "select 9 as n")
    conn = duckrun.connect(a, read_only=False)
    conn.attach(b, name=weird)
    assert weird in conn.catalog.listCatalogs()
    qn = '"' + weird.replace('"', '""') + '"'   # the caller quotes the weird name in their own SQL
    # cross-catalog read through the quoted 3-part name
    assert conn.sql(f"select n from {qn}.dbo.t2").fetchone()[0] == 9
    # cross-catalog write via the DataFrame API (catalog resolved from the quoted 3-part name)
    conn.sql("select 7 as v").write.mode("overwrite").saveAsTable(f"{qn}.dbo.created")
    assert conn.sql(f"select v from {qn}.dbo.created").fetchone()[0] == 7
    # switch to it and introspect under the weird name
    conn.catalog.setCurrentCatalog(weird)
    assert conn.catalog.currentCatalog() == weird
    assert {"t2", "created"} <= set(conn.catalog.listTables("dbo"))


@pytest.mark.parametrize("weird", ["select", "my lake", "café", "default"])
def test_weird_primary_catalog_name(tmp_path, weird):
    # an explicit name= on connect() may be weird too — bare names still resolve in the current
    # catalog, and the explicit 3-part form works when the caller quotes the name.
    a = str(tmp_path / "wh")
    _write_table(a + "/dbo/t", "select 5 as id")
    conn = duckrun.connect(a, name=weird, read_only=False)
    assert conn.catalog.currentCatalog() == weird
    assert conn.sql("select id from t").fetchone()[0] == 5            # bare, current catalog
    qn = '"' + weird.replace('"', '""') + '"'
    assert conn.sql(f"select id from {qn}.dbo.t").fetchone()[0] == 5  # explicit 3-part, quoted


# ── _get_rle: PRIVATE / experimental (parked) — deliberately OUT of the TestSession scorecard ──
def test_get_rle_hidden(conn):
    # Byte model on a fact-shaped table: a constant (ndv 1) sorts nothing; a low-card dimension enters
    # the key; its FD-derived twin does NOT (sorting the dimension already clusters it for free — a key
    # slot on it is meaningless, R5); a unique column can't be compressed by sorting. `_get_rle` is private.
    conn.sql("select 1 as const, (i%4) as region, (i%4)*10 as rderived, i as uid "
             "from range(1000) t(i)").write.mode("overwrite").saveAsTable("facttbl")
    df = conn._get_rle("facttbl")
    assert df.columns == ["table", "in_sort_key", "sort_position", "column", "data_type", "encoding",
                          "ndv", "skew_pct", "current_runs", "is_unique", "est_kb_current",
                          "est_kb_sorted", "saved_pct"]
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.collect())}
    assert not recs["const"]["in_sort_key"]        # ndv 1 → nothing to sort
    assert recs["region"]["in_sort_key"]           # low-card dimension compresses
    assert not recs["rderived"]["in_sort_key"]     # FD on region → already clustered, not a key slot
    assert not recs["uid"]["in_sort_key"]          # unique → sorting can't help
    assert sorted(r["sort_position"] for r in recs.values() if r["in_sort_key"]) == [1]
    # is_unique drives the PLAIN (no-dictionary) decision on the experimental optimize write path:
    # only the unique column is flagged, low-card / constant columns keep their (useful) dictionary.
    assert recs["uid"]["is_unique"]
    assert not recs["region"]["is_unique"] and not recs["const"]["is_unique"]


def test_get_rle_hidden_key_organized(conn):
    # A (near-)unique key with no compressible structure ⇒ key-organized (a dimension, or a table at
    # its grain): recommend ORDER BY the key itself, not a marginal compression sort. The unique key
    # is never told to "cut cardinality".
    conn.sql("select i as pk, i * 2 as a, i * 3 as b from range(500) t(i)") \
        .write.mode("overwrite").saveAsTable("dimtbl")
    df = conn._get_rle("dimtbl")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.collect())}
    assert recs["pk"]["in_sort_key"] and recs["pk"]["sort_position"] == 1
    assert not recs["a"]["in_sort_key"] and not recs["b"]["in_sort_key"]


def test_get_rle_hidden_single_table_only(conn):
    with pytest.raises(ValueError):
        conn._get_rle("dbo")   # a schema, not a table
    with pytest.raises(ValueError):
        conn._get_rle(None)    # None → not single-table


def test_get_rle_hidden_date_leads(conn):
    # R6: a moderate-NDV date/temporal column leads the key even though a lower-cardinality flag
    # exists (ndv 30 date ahead of the ndv-3 flag) — natural clustering wins the lead slot.
    # i%31 and i%3 are coprime → flag is independent of the date (not a functional dependency of it).
    conn.sql("select (date '2024-01-01' + (i % 31)::int) as d, (i % 3) as flag, i as uid "
             "from range(3000) t(i)").write.mode("overwrite").saveAsTable("dateleads")
    df = conn._get_rle("dateleads")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.collect())}
    assert recs["d"]["in_sort_key"] and recs["d"]["sort_position"] == 1     # date leads despite higher ndv
    assert recs["flag"]["in_sort_key"] and recs["flag"]["sort_position"] == 2
    assert not recs["uid"]["in_sort_key"]                                    # near-unique → out


def test_get_rle_hidden_partition_leads(conn, capsys):
    # R8: partition columns lead the printed ORDER BY (write-locality) but take no compression slot.
    conn.sql("select (i % 4) as region, (i % 3) as cat, (i % 3) * 7 as catlike "
             "from range(60000) t(i)").write.mode("overwrite").partitionBy("region").saveAsTable("parttbl")
    df = conn._get_rle("parttbl")
    out = capsys.readouterr().out
    assert "ORDER BY region," in out                    # partition col leads the ORDER BY
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.collect())}
    assert not recs["region"]["in_sort_key"]            # ...but holds no compression-key slot
    assert recs["cat"]["in_sort_key"]                   # the real low-card dimension is the key


def test_get_rle_hidden_null_heavy_excluded(conn, capsys):
    # S1: a mostly-null column is dropped from sort-key candidacy. `sparse` is 70% null with 3 non-null
    # values → by its ndv (3) it would otherwise out-rank and lead ahead of `region` (ndv 4); but its
    # nulls already collapse to one run under any order, so a key slot on it clusters little and crowds
    # out the real dimension. The null share is read from the Delta LOG (get_add_actions), not the sample.
    conn.sql("select (i % 4) as region, "
             "case when (i % 10) < 7 then null else (i % 3) end as sparse, i as uid "
             "from range(20000) t(i)").write.mode("overwrite").saveAsTable("nullheavy")
    df = conn._get_rle("nullheavy")
    out = capsys.readouterr().out
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.collect())}
    assert recs["region"]["in_sort_key"] and recs["region"]["sort_position"] == 1   # real dimension keys
    assert not recs["sparse"]["in_sort_key"]                     # 70% null → excluded from candidacy
    assert "null-heavy" in out and "sparse" in out               # and the exclusion is reported


def test_writer_keeps_dictionary_encoding(conn):
    # Regression guard for the tuned delta-rs writer properties: a low-cardinality column must stay
    # dictionary-encoded (dictionary page present + RLE_DICTIONARY), never fall back to PLAIN. Reads
    # the delta-rs-written footer via DuckDB's parquet_metadata (delta-rs writes; DuckDB only reads).
    # Per-page encodings aren't exposed by any parquet reader, so this catches a full PLAIN fallback /
    # dropped writer_properties — the real regression; genuine high-NDV columns are excluded upstream.
    import glob
    import os
    conn.sql("select (i % 8) as lowcard, ('v' || (i % 5)) as lowstr from range(50000) t(i)") \
        .write.mode("overwrite").saveAsTable("dicttbl")
    files = [f.replace(os.sep, "/")
             for f in glob.glob(os.path.join(conn.root_path, "dbo", "dicttbl", "**", "*.parquet"),
                                recursive=True)]
    assert files, "no parquet files written"
    md = {r[0]: (r[1], r[2]) for r in conn.sql(
        f"select path_in_schema, encodings, dictionary_page_offset "
        f"from parquet_metadata({files!r})").collect()}
    for col in ("lowcard", "lowstr"):
        encodings, dict_off = md[col]
        assert dict_off is not None, f"{col} lost its dictionary page (PLAIN fallback)"
        assert "RLE_DICTIONARY" in encodings, f"{col} not dictionary-encoded: {encodings}"


# ════════════════════════════════════════════════════════════════════════════════════════════════
# optimize() end-to-end — INGEST raw data from a file, land it through every optimize combination,
# and assert the one invariant that matters: OPTIMIZE CHANGES THE LAYOUT, NEVER THE DATA. Each variant
# must hold the exact same row multiset as the raw baseline, and the optimized layout must be physically
# clustered by its sort key. Codec is SNAPPY throughout — the one read-layout profile every file write
# uses. All local-fs, network-free.
# ════════════════════════════════════════════════════════════════════════════════════════════════

# A realistic sales fact with no unique column (max ndv 5000 << 60000 rows), so the profiler picks the
# low-cardinality dimensions by ascending cardinality — region (5) leads, then category (23), day (90) —
# and treats `amount` as a measure (never a sort key). Deterministic: derived from range(), no RNG.
_SALES_SQL = """
    select (i % 5)                             as region,
           (i % 23)                            as category,
           (i % 90)                            as day,
           (i % 5000)                          as customer_id,
           ((i % 997) + 1) * 1.25              as amount,
           (i % 7) + 1                         as qty
    from range(60000) t(i)
"""
_SALES_COLS = "region, category, day, customer_id, amount, qty"


@pytest.fixture
def raw_sales(tmp_path):
    """A writable local warehouse plus a STAGED RAW PARQUET (the 'raw data' we ingest). Returns
    ``(conn, parquet_path, expected_rows)`` — expected_rows is the raw row multiset in ``_SALES_COLS``
    order, the oracle every optimized write must reproduce exactly."""
    conn = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)
    praw = (tmp_path / "raw_sales.parquet").as_posix()
    conn._connection.execute(f"COPY ({_SALES_SQL}) TO '{praw}' (FORMAT PARQUET)")
    expected = sorted(
        conn.sql(f"select {_SALES_COLS} from parquet_scan('{praw}')").fetchall(), key=_k)
    return conn, praw, expected


def _rows(conn, name):
    """The table's row multiset projected in a stable column order (partition-layout-independent)."""
    return sorted(conn.sql(f"select {_SALES_COLS} from {name}").fetchall(), key=_k)


def _codecs(conn, name):
    """The set of parquet codecs across the table's active files (e.g. {'SNAPPY'} or {'ZSTD'})."""
    return set(conn.get_stats(name).df()["compression"].tolist())


def _active_files(conn, name):
    return [f.replace("file://", "")
            for f in engine._delta_table(conn.root_path + f"/dbo/{name}", None).file_uris()]


def _clustered_by(conn, name, col):
    """True iff EVERY active file is internally non-decreasing on ``col`` (the sort-key signature)."""
    for f in _active_files(conn, name):
        vals = [r[0] for r in conn.sql(f"select {col} from parquet_scan('{f}')").fetchall()]
        if vals != sorted(vals):
            return False
    return True


def test_optimize_e2e_ingest_then_write_variants(raw_sales):
    # Ingest the raw parquet once, then land it four ways: raw baseline, write.optimize() auto,
    # write.optimize(explicit), and write.optimize()+partitionBy. Every table must hold the SAME rows.
    conn, praw, expected = raw_sales
    read = lambda: conn.read.format("parquet").load(praw)  # a fresh source relation per write

    read().write.mode("overwrite").saveAsTable("s_raw")                       # baseline: SNAPPY, unsorted
    read().write.optimize().mode("overwrite").saveAsTable("s_auto")           # DL layout, auto key
    read().write.optimize("category").mode("overwrite").saveAsTable("s_cat")  # DL layout, explicit key
    read().write.optimize("day").partitionBy("region").mode("overwrite").saveAsTable("s_part")

    # 1) DATA INVARIANCE — the whole point. Layout differs, the row multiset is byte-identical.
    for name in ("s_raw", "s_auto", "s_cat", "s_part"):
        assert _rows(conn, name) == expected, f"{name} changed the data"

    # 2) CODECS — SNAPPY throughout; one read-layout profile for normal write and optimize alike.
    for name in ("s_raw", "s_auto", "s_cat", "s_part"):
        assert _codecs(conn, name) == {"SNAPPY"}, f"{name} is not SNAPPY"

    # 3) CLUSTERING — the file is physically ordered by its sort key.
    assert _clustered_by(conn, "s_auto", "region")  # region is the leading low-card dimension
    assert _clustered_by(conn, "s_cat", "category")  # explicit key honoured verbatim
    assert _clustered_by(conn, "s_part", "day")      # secondary sort within each region partition
    assert sorted({r[0] for r in conn.sql("select distinct region from s_part").collect()}) == [0, 1, 2, 3, 4]


def test_optimize_e2e_write_raw_then_table_optimize(raw_sales):
    # The other entry point: land raw, THEN rewrite in place via conn.table(name).optimize(). Auto and
    # explicit keys both preserve the data; the codec is SNAPPY (the one read-layout profile).
    conn, praw, expected = raw_sales
    conn.read.format("parquet").load(praw).write.mode("overwrite").saveAsTable("t1")
    assert _codecs(conn, "t1") == {"SNAPPY"}

    m = conn.table("t1").optimize()                       # auto-key rewrite
    assert m["operation"] == "sortRewrite" and m["sortedBy"]
    assert _rows(conn, "t1") == expected
    assert _codecs(conn, "t1") == {"SNAPPY"}
    assert _clustered_by(conn, "t1", m["sortedBy"][0])    # clustered by whatever the profiler led with

    # Re-optimizing with an explicit key re-clusters the same data (idempotent on rows).
    conn.table("t1").optimize("category")
    assert _rows(conn, "t1") == expected
    assert _clustered_by(conn, "t1", "category")


def test_optimize_e2e_chain_write_optimize_then_rekey(raw_sales):
    # Chain the two surfaces: write in the DL layout keyed by category, then re-key to day via the table
    # handle. Data survives every hop; the clustering follows the most recent key.
    conn, praw, expected = raw_sales
    conn.read.format("parquet").load(praw).write.optimize("category").mode("overwrite").saveAsTable("c1")
    assert _clustered_by(conn, "c1", "category") and _codecs(conn, "c1") == {"SNAPPY"}

    conn.table("c1").optimize("day")
    assert _rows(conn, "c1") == expected
    assert _clustered_by(conn, "c1", "day")


def test_optimize_e2e_where_scopes_one_partition(raw_sales):
    # Partition by region, then optimize(where="region = 2") — a scoped rewrite. Only region 2's files
    # move; all rows across all partitions survive, and region 2 comes out clustered by customer_id.
    conn, praw, expected = raw_sales
    conn.read.format("parquet").load(praw) \
        .write.partitionBy("region").mode("overwrite").saveAsTable("p1")
    v0 = engine.table_version(conn.root_path + "/dbo/p1", None)

    m = conn.table("p1").optimize("customer_id", where="region = 2")
    assert m["operation"] == "sortRewrite"
    assert engine.table_version(conn.root_path + "/dbo/p1", None) > v0     # a replaceWhere commit landed
    assert _rows(conn, "p1") == expected                                   # nothing lost, nothing dup'd
    # per-region counts are unchanged (the scope didn't touch other partitions' data)
    counts = dict(conn.sql("select region, count(*) from p1 group by region").collect())
    assert counts == {r: 12000 for r in range(5)}
    # the rewritten partition's file is now sorted by the requested key
    f2 = [f for f in _active_files(conn, "p1") if "region=2" in f.replace(os.sep, "/")]
    assert f2, "region=2 partition file not found"
    vals = [r[0] for r in conn.sql(f"select customer_id from parquet_scan('{f2[0]}')").fetchall()]
    assert vals == sorted(vals)
