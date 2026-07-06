"""The duckrun.connect() SQL-only test suite.

``conn.sql()`` returns DuckDB's native ``DuckDBPyRelation``; every write is SQL (CREATE TABLE AS /
INSERT / UPDATE / DELETE / MERGE, routed to delta_rs). The suite has three parts:

1. **Session plumbing** (``TestSession``) — connect / sql / register / attach / copy / download /
   list_files / get_stats / convert_to_delta / close, plus ``TestSqlDml`` (the raw-DML matrix:
   create-as / insert / update / delete / alter-add / drop-tombstone / merge with every clause and
   the adversarial parsing cases).

2. **Local-filesystem contract & plumbing** (``test_*`` on the ``wh`` fixture) — discovery, catalog
   introspection, read-only gate, multi-catalog attach/routing, and the connect() error formatting.
   Storage-neutrality (s3/gcs/abfss) shares this exact code path, so the local run is representative.

3. **Write-correctness matrix** (``test_sql_write_lands_expected`` + siblings) — does *our glue* land
   the right Delta data? Each case applies a logical write via SQL and reads it back through a
   **fresh** ``duckrun.connect`` (real Delta on disk), asserting the golden expected multiset.

Sort / partition on write use DuckDB's `CREATE TABLE … SORTED BY (…) PARTITIONED BY (…) AS …`
(`SORTED BY AUTO` is a duckrun extension) — see `test_sql_only.py`. Write-options still without a SQL
surface (optimize / replaceWhere / the append_if_unchanged verb) are gaps; the engine capabilities
stay covered by tests/adapter and tests/correctness.
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
    c.sql("CREATE OR REPLACE TABLE src AS select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)")
    c.sql("CREATE OR REPLACE TABLE other.thing AS select 7 as n")
    return c  # CREATE TABLE AS surfaces the table itself — no manual refresh needed


class TestSession:
    def test_connect(self, conn):
        assert isinstance(conn, duckrun.DuckSession)

    def test_sql(self, conn):
        assert conn.sql("select count(*) from src").fetchone()[0] == 3

    def test_refresh(self, conn):
        assert conn.refresh() is conn

    def test_connection(self, conn):
        assert conn._connection.execute("select 1").fetchone()[0] == 1

    def test_close(self, conn):
        conn.close()  # closes the DuckDB connection
        with pytest.raises(Exception):
            conn.sql("select 1").fetchall()  # connection is closed -> unusable

    def test_table_path(self, conn):
        assert conn._table_path("dbo", "src").endswith("dbo/src")

    def test_attach(self, conn, tmp_path):
        # attach a second lakehouse as a named catalog; cross-catalog read resolves catalog.schema.table.
        other = duckrun.connect(str(tmp_path / "wh2"), schema="dbo", read_only=False)
        other.sql("CREATE OR REPLACE TABLE only_there AS select 99 as n")
        other.close()
        assert conn.attach(str(tmp_path / "wh2"), name="sales") is conn  # chains
        assert "sales" in conn._catalogs
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
        d = dict(zip(st.columns, st.fetchall()[0]))
        assert d["table"] == "src" and d["total_rows"] == 3 and d["num_files"] >= 1
        assert d["num_row_groups"] >= 1 and d["compression"]  # a real parquet footer was read
        # source=None → every table in the current schema (dbo has src)
        allrows = conn.get_stats()
        assert "src" in {r[allrows.columns.index("table")] for r in allrows.fetchall()}

    def test_get_stats_detailed(self, conn):
        st = conn.get_stats("src", detailed=True)  # one row per parquet row group
        assert len(st.fetchall()) >= 1 and "table" in st.columns

    def test_get_stats_glob(self, conn):
        # wildcard patterns match table names across schemas in the current catalog.
        conn.sql("CREATE OR REPLACE TABLE fct_a AS select 1 a")
        conn.sql("CREATE OR REPLACE TABLE fct_b AS select 1 a")
        conn.sql("CREATE OR REPLACE TABLE dim_x AS select 1 a")
        names = lambda st: {r[st.columns.index("table")] for r in st.fetchall()}
        assert names(conn.get_stats("fct_*")) == {"fct_a", "fct_b"}       # bare pattern
        assert names(conn.get_stats("dbo.fct_*")) == {"fct_a", "fct_b"}   # schema.table pattern
        assert names(conn.get_stats("*")) >= {"fct_a", "fct_b", "dim_x", "src"}
        with pytest.raises(ValueError):  # a pattern that matches nothing is a reported miss
            conn.get_stats("nope_*")

    def test_get_stats_vorder_flag(self, conn):
        # The Fabric write flag is the table property delta.parquet.vorder.enabled; get_stats reads it
        # off the reconstructed Delta metadata (delta-rs does not surface the per-file add.tags).
        import json, glob
        conn.sql("CREATE OR REPLACE TABLE plain_t AS select 1 a")
        conn.sql("CREATE OR REPLACE TABLE vo_t AS select 1 a")
        # stamp the property into vo_t's log the way Spark/Fabric does (delta-rs refuses to write it).
        for lf in glob.glob(str(Path(conn.root_path) / "**" / "vo_t" / "_delta_log" / "*.json"),
                            recursive=True):
            out = []
            for ln in Path(lf).read_text().splitlines():
                o = json.loads(ln)
                if "metaData" in o:
                    o["metaData"].setdefault("configuration", {})["delta.parquet.vorder.enabled"] = "true"
                out.append(json.dumps(o))
            Path(lf).write_text("\n".join(out) + "\n")
        vorder_of = lambda n: dict(zip(conn.get_stats(n).columns, conn.get_stats(n).fetchall()[0]))["vorder"]
        assert vorder_of("plain_t") is False
        assert vorder_of("vo_t") is True

    def test_convert_to_delta(self, conn):
        # session-level convert (no DeltaTable needed): parquet dir → Delta in place, returns the path.
        path = _stage_parquet(conn, "dbo/sconv")
        assert conn.convert_to_delta(f"parquet.`{path}`") == path
        conn.refresh()
        assert conn.sql("select * from sconv").fetchall() == [(1, "a")]


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
        conn.sql("CREATE OR REPLACE TABLE tt AS select 1 a")  # v0
        conn.sql("CREATE OR REPLACE TABLE tt AS select 2 a")  # v1
        path = conn._table_path("dbo", "tt")
        assert conn.sql(f"select a from delta_scan('{path}', version => 0)").fetchone()[0] == 1

    def test_sql_create_table_as(self, conn):
        conn.sql("create table cta as select * from (values (1),(2)) t(x)")
        assert conn.sql("select count(*) from cta").fetchone()[0] == 2

    def test_sql_insert_select(self, conn):
        conn.sql("insert into src select * from (values (9,'z')) t(id, name)")
        assert conn.sql("select count(*) from src").fetchone()[0] == 4

    def test_sql_insert_values(self, conn):
        conn.sql("insert into src values (9, 'z')")
        assert conn.sql("select count(*) from src").fetchone()[0] == 4
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
        assert conn.sql("select count(*) from src").fetchone()[0] == 2

    def test_sql_update_where_inside_set_literal(self, conn):
        # A literal containing the word `where` in the SET list must not mis-split the statement
        # (review #5): only the trailing WHERE is the predicate.
        conn.sql("update src set name = 'a where b' where id = 1")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "a where b"
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "b"

    def test_sql_update_subquery_predicate(self, conn):
        # UPDATE with a subquery predicate: delta_rs's update() would panic, so duckrun evaluates it
        # in DuckDB and commits a fenced overwrite (review #9). Rows matching the subquery update.
        conn.sql("CREATE OR REPLACE TABLE keys AS select * from (values (1),(3)) t(k)")
        conn.sql("update src set name = 'Q' where id in (select k from keys)")
        got = {r[0]: r[1] for r in conn.sql("select id, name from src").fetchall()}
        assert got == {1: "Q", 2: "b", 3: "Q"}

    def test_sql_delete_subquery_predicate(self, conn):
        # DELETE with a subquery predicate takes the same DuckDB-evaluated fenced fallback (#8).
        conn.sql("CREATE OR REPLACE TABLE dk AS select * from (values (2)) t(k)")
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
        assert "src" not in conn._cat_tables()

    def test_sql_merge_upsert(self, conn):
        conn.sql("MERGE INTO src USING (values (2,'B'),(9,'z')) t(id, name) "
                 "ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select count(*) from src").fetchone()[0] == 4
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "B"
        assert conn.sql("select name from src where id = 9").fetchone()[0] == "z"

    def test_sql_merge_update_columns(self, conn):
        conn.sql("MERGE INTO src USING (values (1,'X')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "X"

    def test_sql_merge_insert_only(self, conn):
        conn.sql("MERGE INTO src USING (values (2,'B'),(5,'e')) t(id, name) "
                 "ON target.id = source.id WHEN NOT MATCHED THEN INSERT *")
        assert conn.sql("select count(*) from src").fetchone()[0] == 4          # only id=5 added
        assert conn.sql("select name from src where id = 2").fetchone()[0] == "b"  # untouched

    def test_sql_merge_by_source_delete(self, conn):
        # full sync: source carries ids {2,3}; matched updates, unmatched-by-source (1) deleted.
        conn.sql("MERGE INTO src USING (values (2,'B'),(3,'C')) t(id, name) "
                 "ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
                 "WHEN NOT MATCHED BY SOURCE THEN DELETE")
        assert dict(conn.sql("select id, name from src").fetchall()) == {2: "B", 3: "C"}

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
        assert sorted(r[0] for r in conn.sql("select * from src").fetchall()) == [1]

    def test_sql_merge_matched_delete_and_update(self, conn):
        # two WHEN MATCHED clauses, applied in order: delete flagged rows, update the rest.
        conn.sql("MERGE INTO src USING (values (1,'A',false),(2,'B',true)) t(id, name, gone) "
                 "ON target.id = source.id "
                 "WHEN MATCHED AND source.gone THEN DELETE "
                 "WHEN MATCHED THEN UPDATE SET name = source.name")
        rows = dict(conn.sql("select id, name from src").fetchall())
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
        rows = dict(conn.sql("select id, name from src").fetchall())
        assert rows[1] == "A"          # matched → updated from source
        assert rows[2] == "gone"       # not in source → updated
        assert rows[3] == "gone"

    def test_sql_merge_update_only(self, conn):
        # update-only merge (matched update, no insert clause) is now supported.
        conn.sql("MERGE INTO src USING (values (1,'A'),(9,'z')) t(id, name) ON target.id = source.id "
                 "WHEN MATCHED THEN UPDATE SET *")
        assert conn.sql("select name from src where id = 1").fetchone()[0] == "A"  # updated
        assert conn.sql("select count(*) from src").fetchone()[0] == 3             # id=9 NOT inserted


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
    assert set(conn._cat_tables()) == {"t1", "t2"}
    assert conn._current_database == "dbo"
    # SHOW TABLES works for free (native DuckDB over the registered views).
    shown = {r[0] for r in conn.sql("SHOW TABLES").fetchall()}
    assert {"t1", "t2"} <= shown
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2


def test_discover_all_schemas(wh):
    # Add a second schema folder, then connect with no schema → discover everything.
    _write_table(wh + "/sales/orders", "select 7 as n")
    conn = duckrun.connect(wh)
    assert set(conn._cat_databases()) == {"dbo", "sales"}
    assert conn.sql("select n from sales.orders").fetchone()[0] == 7


def test_write_modes_round_trip(wh):
    # The SQL write modes with a DuckDB home: overwrite (CREATE OR REPLACE), append (INSERT),
    # ignore (CREATE TABLE IF NOT EXISTS → no-op when the table exists).
    conn = duckrun.connect(wh, schema="dbo", read_only=False)

    conn.sql("CREATE OR REPLACE TABLE t3 AS select 1 as id, 'x' as v")
    assert conn.sql("select count(*) from t3").fetchone()[0] == 1

    conn.sql("insert into t3 select 2 as id, 'y' as v")
    assert conn.sql("select count(*) from t3").fetchone()[0] == 2

    conn.sql("create table if not exists t3 as select 99 as id, 'q' as v")  # no-op, table exists
    assert conn.sql("select count(*) from t3").fetchone()[0] == 2


def test_writes_persist_to_delta(wh):
    # CTAS/INSERT and conn.sql() UPDATE/DELETE land as real Delta, visible to a brand-new
    # connection (not DuckDB-native tables in this session).
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    conn.sql("CREATE OR REPLACE TABLE evt AS select 1 id, 'A' grp")
    conn.sql("insert into evt select 2 id, 'B' grp")
    conn.sql("insert into evt select 3 id, 'C' grp")
    conn.sql("update evt set grp = 'Z' where id = 1")
    conn.sql("delete from evt where id = 2")
    assert sorted(conn.sql("select * from evt").fetchall()) == [(1, "Z"), (3, "C")]

    # real persistence: a fresh connection reads it off the store
    fresh = duckrun.connect(wh, schema="dbo")
    assert sorted(fresh.sql("select * from evt").fetchall()) == [(1, "Z"), (3, "C")]


def test_read_only_is_default_and_blocks_writes(wh):
    # connect() is read-only by default: every Delta-write raises PermissionError, reads and native
    # scratch still work. read_only=False opts back in.
    ro = duckrun.connect(wh, schema="dbo")
    assert ro.sql("select count(*) from t1").fetchone()[0] == 2          # reads fine
    ro.sql("create temp table scratch as select 1 x")                    # native scratch fine
    with pytest.raises(PermissionError):
        ro.sql("create or replace table nope as select 1 id")             # CTAS blocked
    with pytest.raises(PermissionError):
        ro.sql("insert into t1 values (3, 'c')")                          # write-DML blocked
    with pytest.raises(PermissionError):
        ro.sql("delete from t1 where id = 1")                             # delete blocked

    rw = duckrun.connect(wh, schema="dbo", read_only=False)
    rw.sql("create or replace table ok as select 9 id, 'z' v")            # opt-in writes
    assert rw.sql("select count(*) from ok").fetchone()[0] == 1


def test_raw_connection_escape_hatch(wh):
    # conn._connection exposes the underlying DuckDB connection for anything the DataFrame surface
    # doesn't cover — scalar queries, and reading the registered views directly.
    conn = duckrun.connect(wh, schema="dbo")
    assert conn._connection.execute("select 40 + 2").fetchone()[0] == 42
    assert conn._connection.execute("select count(*) from t1").fetchone()[0] == 2


def test_refresh_picks_up_external_writes(wh):
    # A table created on the store after connect is invisible until refresh() re-discovers it.
    conn = duckrun.connect(wh, schema="dbo")
    assert "t3" not in conn._cat_tables()
    _write_table(wh + "/dbo/t3", "select 1 as id")
    conn.refresh(quiet=True)
    assert "t3" in conn._cat_tables()
    assert conn.sql("select count(*) from t3").fetchone()[0] == 1


def test_read_files_via_sql(wh, tmp_path):
    # Reading parquet/csv is native DuckDB — no reader wrapper needed.
    conn = duckrun.connect(wh, schema="dbo")
    pq = str(tmp_path / "t1.parquet")
    csv = str(tmp_path / "t1.csv")
    conn._connection.execute(f"COPY (select * from t1) TO '{pq}' (FORMAT parquet)")
    conn._connection.execute(f"COPY (select * from t1) TO '{csv}' (FORMAT csv, HEADER)")

    assert conn.sql(f"select count(*) from read_parquet('{pq}')").fetchone()[0] == 2
    assert conn.sql(f"select count(*) from read_csv_auto('{csv}')").fetchone()[0] == 2


# ── convert_to_delta: zero-copy parquet→Delta in place (session-level convert) ───────────────────────
# The identifier / zero-copy / guard cases, merged in from the former test_convert_to_delta.py.
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
    conn.convert_to_delta(path)                      # a bare path works, not just parquet.`…`
    conn.refresh()
    assert conn.sql("select count(*) from bare").fetchone()[0] == 1


def test_convertToDelta_is_zero_copy(conn):
    path = _stage_parquet(conn, "dbo/zc")
    conn.convert_to_delta(path)
    assert os.path.exists(path + "/data.parquet")    # original parquet survives
    assert os.path.isdir(path + "/_delta_log")       # only a _delta_log was added


def test_convertToDelta_read_only_raises(tmp_path):
    ro = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=True)
    path = _stage_parquet(ro, "dbo/blocked")
    with pytest.raises(PermissionError):
        ro.convert_to_delta(path)


def test_convertToDelta_already_delta_raises(conn):
    path = _stage_parquet(conn, "dbo/twice")
    conn.convert_to_delta(path)
    with pytest.raises(Exception):                   # delta-rs mode='error' on an already-converted dir
        conn.convert_to_delta(path)


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
    conn.sql("CREATE OR REPLACE TABLE items AS select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)")
    conn.sql("CREATE OR REPLACE TABLE wide AS select * from (values (1,'a',10)) t(id, name, qty)")
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


# Write-correctness — each case applies a logical write through the SQL surface against the seed;
# `expected` (in items column order) is the oracle, read back through a FRESH connection (real Delta
# on disk). Covers overwrite / append (select, values, reordered col list, CTE) / update / delete /
# upsert (delete literals + insert, delta-rs DELETE takes literals not IN (SELECT)).
EQUIV = [
    dict(id="overwrite", table="items",
         sql=["create or replace table items as select * from (values (9,'z'),(8,'y')) t(id, name)"],
         expected=[(9, "z"), (8, "y")]),
    dict(id="append_select", table="items",
         sql=["insert into items select * from (values (4,'d')) t(id, name)"],
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="append_values", table="items",
         sql=["insert into items values (5, 'e')"],
         expected=[(1, "a"), (2, "b"), (3, "c"), (5, "e")]),
    dict(id="append_collist_reordered", table="items",
         # a reordered column list maps by name: 'd' → name, 4 → id.
         sql=["insert into items (name, id) select 'd', 4"],
         expected=[(1, "a"), (2, "b"), (3, "c"), (4, "d")]),
    dict(id="with_prefixed_insert", table="items",
         sql=["with s as (select 8 id, 'h' as name) insert into items select * from s"],
         expected=[(1, "a"), (2, "b"), (3, "c"), (8, "h")]),
    dict(id="update_predicate", table="items",
         sql=["update items set name = 'Z' where id = 1"],
         expected=[(1, "Z"), (2, "b"), (3, "c")]),
    dict(id="delete_predicate", table="items",
         sql=["delete from items where id = 2"],
         expected=[(1, "a"), (3, "c")]),
    dict(id="upsert", table="items",
         sql=["delete from items where id = 2 or id = 4", "insert into items values (2, 'B'), (4, 'D')"],
         expected=[(1, "a"), (2, "B"), (3, "c"), (4, "D")]),
]


@pytest.mark.parametrize("case", EQUIV, ids=[c["id"] for c in EQUIV])
def test_sql_write_lands_expected(tmp_path, case):
    wh = str(tmp_path / "A")
    c = _seed(wh)
    for stmt in case["sql"]:
        c.sql(stmt)
    cols, rows = _dump(wh, case["table"])
    assert rows == sorted(case["expected"], key=_k)


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


def test_dotted_name_sites_are_quote_aware():
    # Regression: the name-splitting sites (session._resolve, session._glob_stats_targets,
    # delta_dml.dml_target_catalog, delta_dml._refresh_view) all route through one quote-aware
    # splitter, so a dot INSIDE a quoted identifier ("a.b", one legal name) is never mistaken for a
    # schema/catalog separator. Previously these split on "." first and stripped quotes after.
    from dbt.adapters.duckrun.delta_dml import _split_dotted, dml_target_catalog
    assert _split_dotted('"a.b"') == ["a.b"]                    # one quoted identifier, not schema.table
    assert _split_dotted("cat.sch.tbl") == ["cat", "sch", "tbl"]
    assert _split_dotted('cat.sch."a.b"') == ["cat", "sch", "a.b"]
    # dml_target_catalog only reports a catalog for a genuinely 3-part target
    assert dml_target_catalog('insert into "a.b" select 1') is None       # 1 part → no catalog
    assert dml_target_catalog("insert into cat.sch.tbl select 1") == "cat"
    assert dml_target_catalog('insert into cat.sch."a.b" select 1') == "cat"


def test_malformed_create_layout_fails_loud_and_writes_nothing(tmp_path):
    # Regression: a mis-spelled layout clause (SORT BY AUTO — the correct spelling is SORTED BY) is
    # NOT peeled off the CREATE target, so it used to leak into the table name and SILENTLY create a
    # spaces-in-name Delta table (data committed, then the view step failed — a partial write). That
    # garbage table later broke get_stats on abfss. It must now fail loud BEFORE any write and leave
    # nothing behind. A name with spaces is refused even when quoted (spaces in the path trip abfss
    # globbing); a quoted name WITHOUT spaces (e.g. a dotted "a.b") still works.
    from dbt.adapters.duckrun.delta_dml import _is_clean_relation, _create_target_error
    assert _is_clean_relation("c.s.t") and _is_clean_relation('"a.b"')
    assert not _is_clean_relation("t SORT BY AUTO")             # stray tokens (structural)
    assert _create_target_error("dbo.t") is None
    assert _create_target_error('dbo."my report"') is not None  # space in the table name
    assert _create_target_error('"my schema".t') is not None    # space in the schema name
    assert _create_target_error('"my lake".dbo.t') is None      # space only in the CATALOG alias → OK

    wh = str(tmp_path / "wh")
    conn = duckrun.connect(wh, schema="dbo", read_only=False)
    for stmt in ["CREATE OR REPLACE TABLE dbo.fct SORT BY AUTO AS SELECT 1 AS a",  # stray tokens, CTAS
                 "CREATE OR REPLACE TABLE dbo.fct SORT BY AUTO (a int)",           # stray tokens, coldefs
                 'CREATE OR REPLACE TABLE dbo."my report" AS SELECT 1 AS a',       # space in name, CTAS
                 'CREATE OR REPLACE TABLE dbo."my report" (a int)']:               # space in name, coldefs
        with pytest.raises(ValueError, match="can't contain"):
            conn.sql(stmt)
    dbo = tmp_path / "wh" / "dbo"
    assert not dbo.exists() or list(dbo.iterdir()) == []   # nothing written for any rejected statement
    # the correct clause and a quoted dotted name (no spaces) both still write
    conn.sql("CREATE OR REPLACE TABLE dbo.good SORTED BY AUTO AS SELECT 1 AS a")
    conn.sql('CREATE OR REPLACE TABLE dbo."a.b" AS SELECT 1 AS a')
    assert sorted(p.name for p in dbo.iterdir()) == ["a.b", "good"]


def _fs_case_sensitive() -> bool:
    """True if the filesystem distinguishes ``A`` from ``a`` (Linux) — False on Windows/macOS, which
    fold case on disk so two case-variant directories can't coexist."""
    import tempfile
    d = tempfile.mkdtemp()
    probe = os.path.join(d, "CaseProbe")
    open(probe, "w").close()
    sensitive = not os.path.exists(os.path.join(d, "caseprobe"))
    os.remove(probe)
    os.rmdir(d)
    return sensitive


def test_case_collision_detector():
    """The pure fold-collision detector behind fail-loud discovery (runs everywhere)."""
    assert session_mod._case_collision(["a", "b", "c"]) is None
    assert session_mod._case_collision(["Foo", "bar", "foo"]) == ("Foo", "foo")
    assert session_mod._case_collision(["t", "T"]) == ("t", "T")
    assert session_mod._case_collision([]) is None
    assert session_mod._case_collision(["dbo", "DBO"]) == ("dbo", "DBO")


@pytest.mark.skipif(not _fs_case_sensitive(),
                    reason="needs a case-sensitive filesystem (Linux CI); Windows/macOS fold Foo/foo on disk")
def test_case_variant_tables_fail_loud_on_discovery(tmp_path):
    """The 'case split-brain' fix, in the scenario that actually matters: two case-variant Delta tables
    that an EXTERNAL engine (Spark/Fabric) already wrote to the lakehouse — duckrun only discovers what
    is on disk. Written here straight to disk with write_deltalake to mimic that, NOT via duckrun.

    Reproducible ONLY on a case-sensitive filesystem — which is why it can't be seen on Windows (there
    ``Foo/`` and ``foo/`` ARE the same directory, so the two tables can't coexist). On Linux they are
    two real tables, but DuckDB's catalog folds ``Foo == foo`` and could expose only one, silently
    hiding the other. duckrun can't fix the store, so discovery FAILS LOUD instead of shadowing.
    """
    import pyarrow as pa
    # mimic Spark/Fabric: two case-variant Delta tables written straight to the store, not via duckrun
    deltalake.write_deltalake(str(tmp_path / "dbo" / "Foo"), pa.table({"n": [1]}))
    deltalake.write_deltalake(str(tmp_path / "dbo" / "foo"), pa.table({"n": [2]}))
    assert (tmp_path / "dbo" / "Foo").is_dir() and (tmp_path / "dbo" / "foo").is_dir()
    with pytest.raises(RuntimeError, match="differ only by case"):
        duckrun.connect(str(tmp_path), schema="dbo")


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
    c.sql("CREATE OR REPLACE TABLE big AS select cast(1 as bigint) as id")
    c.sql("insert into big values (2)")
    assert _dump(wh, "big")[1] == sorted([(1,), (2,)], key=_k)


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
    assert "items" not in c._cat_tables()
    with pytest.raises(Exception):
        c.sql("select * from items").fetchall()
    assert deltalake.DeltaTable.is_deltatable(path)                               # NOT deleted
    assert [f.name for f in deltalake.DeltaTable(path).schema().fields] == [TOMBSTONE_COLUMN]
    assert "items" not in duckrun.connect(wh, schema="dbo")._cat_tables()  # fresh conn hides it
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
    assert name not in duckrun.connect(wh, schema="dbo")._cat_tables()  # and not persisted


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
    assert list(conn._catalogs) == ["lhA", "other"]   # primary derives its folder name "lhA"
    assert conn._current_catalog == "lhA"
    # cross-catalog read resolves catalog.schema.table across the two lakehouse roots.
    assert conn.sql("select n from other.dbo.t2").fetchone()[0] == 7
    assert conn.sql("select label from other.sales.s").fetchone()[0] == "x"
    # 2-part / unqualified still resolve in the CURRENT catalog (lhA), not the attached one.
    assert conn.sql("select count(*) from dbo.t1").fetchone()[0] == 2
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2  # via USE lhA.dbo


def _set_current_catalog(conn, name):
    """Switch the current catalog and pick its default database (dbo when present, else the first) —
    pure SQL, exactly as a user would: USE is the single switch (duckrun derives 'current' from
    DuckDB's current_database())."""
    q = '"' + name.replace('"', '""') + '"'
    conn.sql(f"USE {q}")                           # make it current so its schemas are visible
    dbs = [r[0] for r in conn.sql(
        "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = current_database() "
        "AND schema_name NOT IN ('information_schema', 'pg_catalog', 'main')").fetchall()]
    db = "dbo" if "dbo" in dbs else (dbs[0] if dbs else "dbo")
    conn.sql(f'USE {q}."{db}"')


def test_multi_catalog_set_current(tmp_path):
    conn = _two_lakehouses(tmp_path)
    _set_current_catalog(conn, "other")
    assert conn._current_catalog == "other"
    assert conn._current_database == "dbo"                  # picks dbo when present
    assert conn.sql("select n from t2").fetchone()[0] == 7   # unqualified now resolves in 'other'
    assert set(conn._cat_databases()) == {"dbo", "sales"}   # other's schemas, not lhA's
    assert conn._cat_tables() == ["t2"]                     # current catalog (other) + db (dbo)


def test_multi_catalog_write_lands_in_right_root(tmp_path):
    conn = _two_lakehouses(tmp_path)
    # a cross-catalog write must land under the ATTACHED root (lhB), not the primary (lhA).
    conn.sql("CREATE OR REPLACE TABLE other.dbo.created AS select 1 as id, 'z' as v")
    assert deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhB" / "dbo" / "created"))
    assert not deltalake.DeltaTable.is_deltatable(str(tmp_path / "lhA" / "dbo" / "created"))
    assert conn.sql("select v from other.dbo.created").fetchone()[0] == "z"
    # and the resolved path lands under the attached catalog's root.
    path = conn._table_path("dbo", "created", catalog="other")
    assert path.replace("\\", "/").endswith("lhB/dbo/created")


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
    assert duckrun.connect(a)._current_catalog == "wh"
    # Explicit name= wins.
    assert duckrun.connect(a, name="picked")._current_catalog == "picked"
    # Nothing derivable (a GUID-shaped segment → _derive_catalog_name returns None) → "data" fallback,
    # which is non-reserved so it's usable bare in 3-part SQL.
    g = str(tmp_path / "11111111-1111-1111-1111-111111111111")
    _write_table(g + "/dbo/t", "select 1 as id")
    conn = duckrun.connect(g)
    assert conn._current_catalog == "data"
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
    assert set(conn._catalogs) == {"wh", "wh2"}
    assert conn.sql("select id from wh2.dbo.t").fetchone()[0] == 2


def test_attach_schema_filter_skips_discovery(tmp_path):
    # schema= on attach restricts discovery to that one schema (no full glob) — other schemas absent.
    a, b = str(tmp_path / "lhA"), str(tmp_path / "lhB")
    _write_table(a + "/dbo/t1", "select 1 as id")
    _write_table(b + "/dbo/keep", "select 1 as id")
    _write_table(b + "/skipme/hidden", "select 1 as id")
    conn = duckrun.connect(a, read_only=False)
    conn.attach(b, name="other", schema="dbo")
    _set_current_catalog(conn, "other")
    assert conn._cat_databases() == ["dbo"]                 # skipme not discovered
    assert conn._cat_tables() == ["keep"]


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
    # ... but a write into it fails loud, even though the session is writable — the fence is on the
    # TARGET catalog's read-only flag, not the current one.
    with pytest.raises(PermissionError):
        conn.sql("create or replace table ref.dbo.created as select 2 as id")
    with pytest.raises(PermissionError):
        conn.sql("insert into ref.dbo.lookup values (2, 'two')")
    # writes to the writable primary still work.
    conn.sql("insert into t1 select 2 as id")
    assert conn.sql("select count(*) from t1").fetchone()[0] == 2


def test_close_closes_connection(tmp_path):
    conn = _two_lakehouses(tmp_path)
    conn.close()
    with pytest.raises(Exception):
        conn.sql("select 1").fetchall()  # underlying DuckDB connection is closed


def test_context_manager_closes_connection(tmp_path):
    # `with duckrun.connect(...) as conn:` closes the connection on exit.
    with duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False) as conn:
        conn.sql("CREATE OR REPLACE TABLE t AS select 1 as x")
        assert conn.sql("select count(*) from t").fetchall()[0][0] == 1
    with pytest.raises(Exception):
        conn.sql("select 1").fetchall()  # closed on `with` exit


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
    assert weird in conn._catalogs
    qn = '"' + weird.replace('"', '""') + '"'   # the caller quotes the weird name in their own SQL
    # cross-catalog read through the quoted 3-part name
    assert conn.sql(f"select n from {qn}.dbo.t2").fetchone()[0] == 9
    # cross-catalog write (catalog resolved from the quoted 3-part name)
    conn.sql(f"CREATE OR REPLACE TABLE {qn}.dbo.created AS select 7 as v")
    assert conn.sql(f"select v from {qn}.dbo.created").fetchone()[0] == 7
    # switch to it and introspect under the weird name
    _set_current_catalog(conn, weird)
    assert conn._current_catalog == weird
    assert {"t2", "created"} <= set(conn._cat_tables("dbo"))


@pytest.mark.parametrize("weird", ["select", "my lake", "café", "default"])
def test_weird_primary_catalog_name(tmp_path, weird):
    # an explicit name= on connect() may be weird too — bare names still resolve in the current
    # catalog, and the explicit 3-part form works when the caller quotes the name.
    a = str(tmp_path / "wh")
    _write_table(a + "/dbo/t", "select 5 as id")
    conn = duckrun.connect(a, name=weird, read_only=False)
    assert conn._current_catalog == weird
    assert conn.sql("select id from t").fetchone()[0] == 5            # bare, current catalog
    qn = '"' + weird.replace('"', '""') + '"'
    assert conn.sql(f"select id from {qn}.dbo.t").fetchone()[0] == 5  # explicit 3-part, quoted


# ── _get_rle: PRIVATE / experimental (parked) — deliberately OUT of the TestSession scorecard ──
def test_get_rle_hidden(conn):
    # Byte model on a fact-shaped table: a constant (ndv 1) sorts nothing; a low-card dimension enters
    # the key; its FD-derived twin does NOT (sorting the dimension already clusters it for free — a key
    # slot on it is meaningless, R5); a unique column can't be compressed by sorting. `_get_rle` is private.
    conn.sql("CREATE OR REPLACE TABLE facttbl AS select 1 as const, (i%4) as region, (i%4)*10 as rderived, i as uid "
             "from range(1000) t(i)")
    df = conn._get_rle("facttbl")
    assert df.columns == ["table", "in_sort_key", "sort_position", "column", "data_type", "encoding",
                          "ndv", "skew_pct", "current_runs", "is_unique", "est_kb_current",
                          "est_kb_sorted", "saved_pct"]
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
    assert not recs["const"]["in_sort_key"]        # ndv 1 → nothing to sort
    assert recs["region"]["in_sort_key"]           # low-card dimension compresses
    assert not recs["rderived"]["in_sort_key"]     # FD on region → already clustered, not a key slot
    assert not recs["uid"]["in_sort_key"]          # unique → sorting can't help
    assert sorted(r["sort_position"] for r in recs.values() if r["in_sort_key"]) == [1]
    # is_unique drives the PLAIN (no-dictionary) decision on the experimental optimize write path:
    # only the unique column is flagged, low-card / constant columns keep their (useful) dictionary.
    assert recs["uid"]["is_unique"]
    assert not recs["region"]["is_unique"] and not recs["const"]["is_unique"]


def test_get_rle_hidden_near_fd_dropped(conn):
    # Threshold-FD: `b` equals `a` except for one perturbed group, so it's ~99% determined by `a` — a
    # NEAR (not exact) functional dependency. The old exact-equality test kept such a column (distinct
    # grew, if only by one), wasting a scarce key slot; the HLL threshold (fd_band) now drops it — it
    # clusters for free under `a`. `a` is the one independent dimension. No exact COUNT(DISTINCT) runs.
    conn.sql("CREATE OR REPLACE TABLE nearfd AS select (i % 100) as a, "
             "case when (i % 1000) = 7 then 999 else (i % 100) end as b "
             "from range(40000) t(i)")
    df = conn._get_rle("nearfd")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
    assert recs["a"]["in_sort_key"] and recs["a"]["sort_position"] == 1   # the independent dimension
    assert not recs["b"]["in_sort_key"]                                   # 99%-determined by a → no slot


def test_get_rle_hidden_key_organized(conn):
    # A (near-)unique key with no compressible structure ⇒ key-organized (a dimension, or a table at
    # its grain): recommend ORDER BY the key itself, not a marginal compression sort. The unique key
    # is never told to "cut cardinality".
    conn.sql("CREATE OR REPLACE TABLE dimtbl AS select i as pk, i * 2 as a, i * 3 as b from range(500) t(i)")
    df = conn._get_rle("dimtbl")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
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
    conn.sql("CREATE OR REPLACE TABLE dateleads AS select (date '2024-01-01' + (i % 31)::int) as d, (i % 3) as flag, i as uid "
             "from range(3000) t(i)")
    df = conn._get_rle("dateleads")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
    assert recs["d"]["in_sort_key"] and recs["d"]["sort_position"] == 1     # date leads despite higher ndv
    assert recs["flag"]["in_sort_key"] and recs["flag"]["sort_position"] == 2
    assert not recs["uid"]["in_sort_key"]                                    # near-unique → out


def test_get_rle_hidden_date_plus_lowcard_dims(conn):
    # A fact table with TWO independent dates + low-card dimensions. Only ONE date leads; the low-card
    # dims queries actually filter on (status, flag) must make the key too — they must not be crowded out
    # by the second high-card date, and the near-unique id stays out. Regression guard: before the ranking
    # fix the temporal thumb promoted every date, and the byte-gain gate starved the low-card dims, so the
    # key collapsed to a single date. Periods 53/7/11/59 are pairwise coprime → all four are independent
    # (no column is a functional dependency of another).
    conn.sql("CREATE OR REPLACE TABLE factdims AS select (date '2024-01-01' + (i % 53)::int) as d1, "
             "(date '2024-01-01' + (i % 59)::int) as d2, "
             "(i % 7) as status, (i % 11) as flag, i as id "
             "from range(60000) t(i)")
    df = conn._get_rle("factdims")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
    key = sorted((c for c, r in recs.items() if r["in_sort_key"]), key=lambda c: recs[c]["sort_position"])
    assert recs[key[0]]["data_type"] == "DATE"                          # a date leads
    assert recs["status"]["in_sort_key"] and recs["flag"]["in_sort_key"]  # low-card dims make the key
    assert not recs["id"]["in_sort_key"]                               # near-unique id does not
    assert sum(recs[c]["data_type"] == "DATE" for c in key) == 1       # ONE date, not both crowding out dims


def test_get_rle_hidden_near_unique_timestamp_not_lead(conn):
    # Regression (found on real NYC-taxi data, not synthetic): when the only temporal is a ~unique
    # microsecond timestamp (tpep_pickup_datetime is ndv ~= 0.7*n), it must NOT be promoted to lead —
    # doing so grain-stops the very first pick and leaves an EMPTY key ("no key pays off"). The low-card
    # dimensions are the real key; the too-fine timestamp stays out (it can't form runs).
    conn.sql("CREATE OR REPLACE TABLE nutime AS select (timestamp '2024-01-01' + (i * interval '1 second')) as ts, "
             "(i % 2) as flag, (i % 5) as kind, (i % 3) as vendor "
             "from range(40000) t(i)")
    df = conn._get_rle("nutime")
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
    assert any(r["in_sort_key"] for r in recs.values())   # NOT empty — the bug produced an empty key
    assert not recs["ts"]["in_sort_key"]                  # ~unique timestamp is too fine to be the key
    assert recs["flag"]["in_sort_key"]                    # the low-card dimensions form the key instead


def test_get_rle_hidden_null_heavy_excluded(conn, capsys):
    # S1: a mostly-null column is dropped from sort-key candidacy. `sparse` is 70% null with 3 non-null
    # values → by its ndv (3) it would otherwise out-rank and lead ahead of `region` (ndv 4); but its
    # nulls already collapse to one run under any order, so a key slot on it clusters little and crowds
    # out the real dimension. The null share is read from the Delta LOG (get_add_actions), not the sample.
    conn.sql("CREATE OR REPLACE TABLE nullheavy AS select (i % 4) as region, "
             "case when (i % 10) < 7 then null else (i % 3) end as sparse, i as uid "
             "from range(20000) t(i)")
    df = conn._get_rle("nullheavy")
    out = capsys.readouterr().out
    recs = {r["column"]: r for r in (dict(zip(df.columns, row)) for row in df.fetchall())}
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
    conn.sql("CREATE OR REPLACE TABLE dicttbl AS select (i % 8) as lowcard, ('v' || (i % 5)) as lowstr from range(50000) t(i)")
    files = [f.replace(os.sep, "/")
             for f in glob.glob(os.path.join(conn.root_path, "dbo", "dicttbl", "**", "*.parquet"),
                                recursive=True)]
    assert files, "no parquet files written"
    md = {r[0]: (r[1], r[2]) for r in conn.sql(
        f"select path_in_schema, encodings, dictionary_page_offset "
        f"from parquet_metadata({files!r})").fetchall()}
    for col in ("lowcard", "lowstr"):
        encodings, dict_off = md[col]
        assert dict_off is not None, f"{col} lost its dictionary page (PLAIN fallback)"
        assert "RLE_DICTIONARY" in encodings, f"{col} not dictionary-encoded: {encodings}"


# NOTE: sort / partition on write are covered by test_sql_only.py (CREATE TABLE … SORTED BY /
# PARTITIONED BY / SORTED BY AUTO). optimize / replaceWhere / the append_if_unchanged verb still have
# no DuckDB-SQL surface and are gaps for now — their engine capabilities remain covered by
# tests/adapter and tests/correctness.
