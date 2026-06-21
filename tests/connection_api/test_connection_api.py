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

3. **Write-correctness matrix** (``test_sql_equals_spark`` + the Tier-2/3/4 functions) — does *our
   glue* land the right Delta data? The load-bearing oracle is **cross-API equivalence**: the same
   logical write expressed via the SQL API and the Spark API must land byte-identical Delta data, so
   a bug in either path shows up as a mismatch with almost no hand-maintained expected values. Every
   assertion reads back through a **fresh** ``duckrun.connect`` — which only sees real Delta on disk
   (discovery globs ``_delta_log``), subsuming the old ``is_deltatable`` boundary check.

All local, network-free, serial (duckrun's write path is single-writer). No DAT, no Spark engine.
"""
import duckdb
import deltalake
import pytest
from deltalake.exceptions import CommitFailedError

import duckrun
from duckrun import DeltaTable
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

    def test_save_by_path(self, conn, tmp_path):
        p = (tmp_path / "by_path").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)  # no catalog name
        assert conn.read.delta(p).count() == 1  # read back BY PATH, not as a table

    def test_save_modes(self, conn, tmp_path):
        p = (tmp_path / "modes").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)
        conn.sql("select 2 a").write.mode("append").save(p)
        assert conn.read.delta(p).count() == 2

    def test_save_mode_error_when_exists(self, conn, tmp_path):
        p = (tmp_path / "err").as_posix()
        conn.sql("select 1 a").write.mode("overwrite").save(p)
        with pytest.raises(ValueError):
            conn.sql("select 2 a").write.save(p)  # default error, path exists


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


class TestSqlDml:
    """conn.sql(): reads (incl. version-pinned delta_scan) pass through, and Delta DML is applied
    via delta_rs — create-as / insert-select / insert-values / update / delete / alter-add, and drop
    (a tombstone: no data deleted). Only a SQL merge isn't expressible as delta_rs DML and is
    directed to the Spark write API."""

    def test_select_passthrough(self, conn):
        assert conn.sql("SELECT 1").fetchall() == [(1,)]

    @needs_version_param
    def test_version_pinned_read(self, conn):
        # write v0 then v1, then read v0 back via the passthrough — time travel for free.
        conn.sql("select 1 a").write.mode("overwrite").saveAsTable("tt")  # v0
        conn.sql("select 2 a").write.mode("overwrite").saveAsTable("tt")  # v1
        path = conn.table_path("dbo", "tt")
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

    def test_sql_alter_add_column(self, conn):
        conn.sql("alter table src add column qty integer")
        assert "qty" in conn.sql("select * from src").columns

    def test_sql_drop_tombstone(self, conn):
        # drop is a tombstone (no data deleted); the table leaves the catalog.
        conn.sql("drop table src")
        assert "src" not in conn.catalog.listTables()

    def test_sql_merge_rejected(self, conn):
        # a SQL merge can't be expressed as delta_rs DML → directed to the .merge() builder.
        with pytest.raises(ValueError):
            conn.sql("MERGE INTO src USING src s ON src.id = s.id WHEN MATCHED THEN DELETE")


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


def test_spark_writes_persist_to_delta(wh):
    # create/append via saveAsTable and mutate via the DeltaTable handle must land as real Delta,
    # visible to a brand-new connection (not DuckDB-native tables in this session). conn.sql() is
    # read-only for Delta writes — these go through the Spark API.
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'A' grp").write.mode("overwrite").saveAsTable("evt")
    conn.sql("select 2 id, 'B' grp").write.mode("append").saveAsTable("evt")
    conn.sql("select 3 id, 'C' grp").write.mode("append").saveAsTable("evt")
    evt = conn.delta_table("evt")
    evt.update(set={"grp": "'Z'"}, where="id = 1")
    evt.delete("id = 2")
    assert sorted(conn.sql("select * from evt").collect()) == [(1, "Z"), (3, "C")]

    # real persistence: a fresh connection reads it off the store
    fresh = duckrun.connect(wh, schema="dbo")
    assert sorted(fresh.table("evt").collect()) == [(1, "Z"), (3, "C")]


def test_read_api(wh):
    conn = duckrun.connect(wh, schema="dbo")
    t1_path = conn.table_path("dbo", "t1")
    assert conn.read.delta(t1_path).count() == 2
    assert conn.read.format("delta").load(t1_path).count() == 2


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
    _write_table(wh + "/dbo/t3", "select 1 as id")
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
    conn = duckrun.connect(wh, schema="dbo")
    conn.sql("select 1 id, 'a' v").write.mode("overwrite").saveAsTable("ver")
    path = conn.table_path("dbo", "ver")

    by_name = DeltaTable.forName(conn, "dbo.ver")
    by_path = DeltaTable.forPath(conn, path)
    assert by_path.version() == by_name.version() == 0

    conn.sql("select 2 id, 'b' v").write.mode("append").saveAsTable("ver")
    assert DeltaTable.forPath(conn, path).version() == 1   # a new commit bumps the version


# ════════════════════════════════════════════════════════════════════════════════════════════════
# 3. Write-correctness matrix — cross-API equivalence is the oracle (SQL ≡ Spark → byte-identical).
# ════════════════════════════════════════════════════════════════════════════════════════════════
def _k(row):
    """Order-insensitive, None-safe sort key for a row of mixed scalars."""
    return tuple(str(c) for c in row)


def _seed(wh):
    """Seed a fresh warehouse at ``wh`` with the canonical tables and return the connection:
    ``items(id, name)`` = (1,a),(2,b),(3,c) and ``wide(id, name, qty)`` = (1,a,10)."""
    conn = duckrun.connect(wh, schema="dbo")
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
# the SQL API and the Spark API against the same seed; `expected` (in items column order) anchors it.
EQUIV = [
    dict(id="overwrite", table="items",
         sql=["create or replace table items as select * from (values (9,'z'),(8,'y')) t(id, name)"],
         spark=lambda c: c.sql("select * from (values (9,'z'),(8,'y')) t(id, name)")
                          .write.mode("overwrite").saveAsTable("items"),
         expected=[(9, "z"), (8, "y")]),
    dict(id="append_select", table="items",
         sql=["insert into items select * from (values (4,'d')) t(id, name)"],
         spark=lambda c: c.sql("select * from (values (4,'d')) t(id, name)")
                          .write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="append_values", table="items",
         sql=["insert into items values (5, 'e')"],
         spark=lambda c: c.sql("select 5 id, 'e' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (5, "e")]),
    dict(id="append_collist_reordered", table="items",
         # SQL maps by name from a reordered column list; Spark appends an in-order df — both land id=4,name='d'.
         sql=["insert into items (name, id) select 'd', 4"],
         spark=lambda c: c.sql("select 4 id, 'd' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="with_prefixed_insert", table="items",
         sql=["with s as (select 8 id, 'h' as name) insert into items select * from s"],
         spark=lambda c: c.sql("select 8 id, 'h' as name").write.mode("append").saveAsTable("items"),
         expected=[(1, "a"), (2, "b"), (3, "c"), (8, "h")]),
    dict(id="update_predicate", table="items",
         sql=["update items set name = 'Z' where id = 1"],
         spark=lambda c: c.delta_table("items").update(set={"name": "'Z'"}, where="id = 1"),
         expected=[(1, "Z"), (2, "b"), (3, "c")]),
    dict(id="delete_predicate", table="items",
         sql=["delete from items where id = 2"],
         spark=lambda c: c.delta_table("items").delete("id = 2"),
         expected=[(1, "a"), (3, "c")]),
    dict(id="upsert", table="items",
         # SQL upsert = delete literal keys + insert (delta-rs DELETE takes literals, not IN (SELECT)).
         sql=["delete from items where id = 2 or id = 4", "insert into items values (2, 'B'), (4, 'D')"],
         spark=lambda c: DeltaTable.forName(c, "items")
             .merge(c.sql("select * from (values (2,'B'),(4,'D')) t(id, name)"), "target.id = source.id")
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute(),
         expected=[(1, "a"), (2, "B"), (3, "c"), (4, "D")]),
]


@pytest.mark.parametrize("case", EQUIV, ids=[c["id"] for c in EQUIV])
def test_sql_equals_spark(tmp_path, case):
    a, b = str(tmp_path / "A"), str(tmp_path / "B")
    ca = _seed(a)
    for stmt in case["sql"]:
        ca.sql(stmt)
    case["spark"](_seed(b))

    dump_a, dump_b = _dump(a, case["table"]), _dump(b, case["table"])
    assert dump_a == dump_b, f"SQL≠Spark for {case['id']}: {dump_a} vs {dump_b}"
    assert dump_a[1] == sorted(case["expected"], key=_k)   # anchor: agreeing-but-wrong can't pass


# Tier 1b — SQL-routing forms with no Spark equivalent; the assertion is the persisted data.
@pytest.mark.parametrize("stmt,table,cols,rows", [
    ("-- build it\ncreate table cm as select 1 id", "cm", ["id"], [(1,)]),
    ("/* note */ insert into items values (4, 'd')", "items", ["id", "name"],
     [(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    ("create table p as (select 5 id)", "p", ["id"], [(5,)]),
    ("create table c as with s as (select * from (values (1),(2),(3)) t(id)) select id from s",
     "c", ["id"], [(1,), (2,), (3,)]),
    ("create table if not exists items as select 99 id, 'x' name", "items", ["id", "name"],
     [(1, "a"), (2, "b"), (3, "c")]),   # table exists → no-op, seed untouched
], ids=["leading_line_comment", "leading_block_comment", "create_parenthesised",
        "create_as_cte", "create_if_not_exists_noop"])
def test_sql_routing_lands_correct_delta(tmp_path, stmt, table, cols, rows):
    wh = str(tmp_path / "wh")
    _seed(wh).sql(stmt)
    got_cols, got_rows = _dump(wh, table)
    assert got_cols == cols
    assert got_rows == sorted(rows, key=_k)


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
    c = duckrun.connect(wh, schema="dbo")
    c.sql("select cast(1 as bigint) as id").write.mode("overwrite").saveAsTable("big")
    c.sql("insert into big values (2)")
    assert _dump(wh, "big")[1] == sorted([(1,), (2,)], key=_k)


# Tier 2 — single-API ops; inline golden expected, read back via a fresh connection.
def test_replace_where_spark_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select * from (values (1,'eu'),(2,'us'),(3,'eu')) t(id, region)") \
        .write.mode("overwrite").saveAsTable("rw")
    DeltaTable.forName(c, "rw").replaceWhere(c.sql("select 9 id, 'eu' region"), "region = 'eu'")
    assert _dump(wh, "rw")[1] == sorted([(2, "us"), (9, "eu")], key=_k)


def test_merge_sync_delete_spark_only(tmp_path):
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


def test_partition_and_merge_schema_spark_only(tmp_path):
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    c.sql("select 1 id, 'eu' region").write.mode("overwrite").partitionBy("region").saveAsTable("p")
    c.sql("select 2 id, 'us' region, true flag") \
        .write.mode("append").option("mergeSchema", "true").partitionBy("region").saveAsTable("p")
    assert "flag" in _dump(wh, "p")[0]
    assert _select(wh, "select id, region, flag from p") == \
        sorted([(1, "eu", None), (2, "us", True)], key=_k)


def test_overwrite_schema_spark_only(tmp_path):
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
    dt = deltalake.DeltaTable(c.table_path("dbo", "fresh"))
    assert dt.history(1000)[-1]["operation"] == "CREATE TABLE"   # history newest-first; oldest = create


def test_drop_tombstones_without_deleting_data(tmp_path):
    # `drop table` marks the table dropped (one-column tombstone via delta_rs) WITHOUT deleting data:
    # gone from the catalog, files stay, and a later create-as revives it with the real schema.
    wh = str(tmp_path / "wh")
    c = _seed(wh)
    path = c.table_path("dbo", "items")
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
    assert not deltalake.DeltaTable.is_deltatable(c.table_path("dbo", name))  # but not Delta
    assert name not in duckrun.connect(wh, schema="dbo").catalog.listTables()  # and not persisted


# Tier 4 — rejection contract: conn.sql() refuses what it can't route to delta_rs.
@pytest.mark.parametrize("stmt,msg", [
    ("merge into items t using items s on t.id = s.id when matched then update set name = 'x'", "MERGE"),
    ("update items set name = o.name from wide o where items.id = o.id", "UPDATE . FROM"),
    ("delete from items using wide o where items.id = o.id", "DELETE . USING"),
    ("insert into items values (4,'d'); insert into items values (5,'e')", "one statement"),
    ("create table items as select 1 id, 'a' name", "already exists"),
    ("create table items (id integer)", "already exists"),
], ids=["merge", "update_from", "delete_using", "multi_statement",
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
