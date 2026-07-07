"""Phase-0 spec tests for the SQL-only refactor — the contract, written before the code.

These pin the target behaviour of the DataFrame-API removal:

  * ``conn.sql()`` returns DuckDB's **native** ``DuckDBPyRelation`` (no duckrun wrapper).
  * ``conn.register(name, obj)`` is the ``createDataFrame`` replacement (DuckDB's replacement scan
    can't see a caller-local through the ``session.sql`` wrapper frame — see the module note).
  * the router's statement **classifier** (``delta_dml.classify``) maps every statement form to a
    route — passthrough / delta-rs DML / reject — including the adversarial parsing cases.
  * notebook CTAS and routed MERGE go through the **same** ``engine`` seam the dbt adapter uses, and
    MERGE is fenced (a ``read_version`` reaches delta-rs).

Some of these are RED until Phase 1 lands (native return, ``conn.register``, ``delta_dml.classify``);
that is the point — the spec leads the implementation. The spy tests (engine path / fencing) are
GREEN today and guard the shared-seam invariant during the refactor.
"""
import os

import duckdb
import pytest

import duckrun
import duckrun.session as session_mod
from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun import delta_dml


@pytest.fixture
def w(tmp_path):
    """A writable local-fs session (schema ``dbo``), no seed tables — each test seeds via SQL."""
    return duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)


# ─────────────────────────────────────────────────────────────────── native relation return

def test_sql_returns_native_duckdb_relation(w):
    """conn.sql() hands back DuckDB's own relation, unwrapped — exact type, not isinstance-of-ours."""
    rel = w.sql("select 1 as x")
    assert type(rel) is duckdb.DuckDBPyRelation
    # the native surface users rely on is all present (maintained upstream by DuckDB)
    for m in ("show", "df", "arrow", "pl", "fetchall", "fetchone", "filter", "aggregate"):
        assert hasattr(rel, m), m
    assert rel.fetchall() == [(1,)]


# ─────────────────────────────────────────────────────────── conn.register (createDataFrame)

def test_register_makes_local_object_queryable(w):
    """The createDataFrame replacement: register a local object under a name, then FROM it in SQL.
    (A bare `FROM local_df` can't work — DuckDB's replacement scan only sees the immediate calling
    frame, which is session.sql, not the user's — so registration is explicit.)"""
    pd = pytest.importorskip("pandas")
    local_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    w.register("local_df", local_df)
    assert w.sql("select sum(id) from local_df").fetchone()[0] == 6
    # and it can seed a real Delta table through the normal write path
    w.sql("CREATE OR REPLACE TABLE seeded AS SELECT * FROM local_df")
    assert w.sql("select count(*) from seeded").fetchone()[0] == 3


# ─────────────────────────────────────────────────────── router classification matrix (RED → P1)

# (statement, expected route). Route is form-level: "delta" = a DML form routed to delta-rs when the
# target is a Delta table; "passthrough" = native DuckDB; "reject" = a form delta-rs can't express.
_CLASSIFY = [
    # passthrough — reads and native scratch DDL
    ("select 1", "passthrough"),
    ("SELECT * FROM t", "passthrough"),
    ("-- a comment\nselect 1", "passthrough"),
    ("/* c */ select 1", "passthrough"),
    ("select 'merge into x' as s", "passthrough"),          # 'merge' only in a string literal
    ("with c as (select 1) select * from c", "passthrough"),  # CTE-wrapped SELECT
    ("create view v as select 1", "passthrough"),
    ("create temp table tmp as select 1", "passthrough"),
    ("CREATE TEMPORARY TABLE tmp2 AS SELECT 1", "passthrough"),
    ("show tables", "passthrough"),
    ("describe t", "passthrough"),
    # delta — DML forms routed to delta-rs
    ("create table t as select 1", "delta"),
    ("CREATE OR REPLACE TABLE t AS SELECT 1", "delta"),
    ("create table if not exists t as select 1", "delta"),
    ("create table t sorted by (id) as select 1 id", "delta"),          # DuckDB layout clause
    ("create table t partitioned by (region) as select 1 id, 'x' region", "delta"),
    ("create or replace table t sorted by auto as select 1 id", "delta"),  # duckrun extension
    ("insert into t select 1", "delta"),
    ("insert into t values (1)", "delta"),
    ("update t set x = 1 where id = 2", "delta"),
    ("delete from t where id = 1", "delta"),
    ("merge into t using s on target.id = source.id when matched then update set *", "delta"),
    ("/* c */\nMERGE INTO t a USING s b ON a.id = b.id WHEN MATCHED THEN DELETE", "delta"),
    ("with c as (select 1 id) insert into t select id from c", "delta"),  # CTE-wrapped DML
    ("alter table t add column x int", "delta"),
    ("alter table t drop column x", "delta"),
    ("alter table t rename column a to b", "delta"),
    ("insert with schema evolution into t select 1 id, 2 extra", "delta"),
    ("restore table t to version as of 3", "delta"),
    ("drop table t", "delta"),
    ("drop table if exists t", "delta"),
    ("vacuum t", "delta"),                                   # DuckDB verb → Delta compact + vacuum
    ("VACUUM ANALYZE t", "delta"),
    ("vacuum", "passthrough"),                              # bare VACUUM (no operand) → native no-op
    ("insert into t replace where region = 'eu' select 1 id, 'eu' region", "delta"),  # replaceWhere
    # reject — single-statement forms delta-rs cannot express
    ("update t set x = s.x from s where t.id = s.id", "reject"),
    ("delete from t using s where t.id = s.id", "reject"),
]


@pytest.mark.parametrize("sql, expected", _CLASSIFY, ids=[c[0][:40] for c in _CLASSIFY])
def test_classify_matrix(sql, expected):
    assert delta_dml.classify(sql) == expected


def test_multi_statement_dml_is_rejected_by_session():
    """The connection API runs one statement per call — a multi-statement DML batch is refused up
    front (session policy), not partially executed."""
    assert session_mod._unsupported_dml("insert into t values (1); delete from t") is not None
    assert session_mod._unsupported_dml("select 1; select 2") is None  # non-DML batch is not our concern


# ─────────────────────────────────────────────── CREATE TABLE layout: SORTED BY / PARTITIONED BY

def _first_file(w, name):
    return engine._delta_table(w._table_path("dbo", name), None).file_uris()[0].replace("file://", "")


def test_create_sorted_by_explicit(w):
    """CREATE TABLE … SORTED BY (cols) AS … (native DuckDB syntax) clusters the write by the key."""
    w.sql("CREATE OR REPLACE TABLE s SORTED BY (id) AS SELECT * FROM (VALUES (3),(1),(2)) t(id)")
    assert [r[0] for r in w.sql(f"select id from parquet_scan('{_first_file(w, 's')}')").fetchall()] == [1, 2, 3]


def test_create_partitioned_by(w):
    """CREATE TABLE … PARTITIONED BY (cols) AS … (native DuckDB syntax) writes Hive-partitioned Delta."""
    import glob
    w.sql("CREATE OR REPLACE TABLE p PARTITIONED BY (region) AS "
          "SELECT * FROM (VALUES (1,'eu'),(2,'us'),(3,'eu')) t(id, region)")
    dirs = {os.path.basename(os.path.dirname(f))
            for f in glob.glob(os.path.join(w._table_path("dbo", "p"), "**", "*.parquet"), recursive=True)}
    assert dirs == {"region=eu", "region=us"}
    assert w.sql("select count(*) from p").fetchone()[0] == 3


def test_create_sorted_and_partitioned(w):
    """SORTED BY and PARTITIONED BY compose on one CREATE TABLE."""
    import glob
    w.sql("CREATE OR REPLACE TABLE sp SORTED BY (id) PARTITIONED BY (region) AS "
          "SELECT (i % 2) region, (9 - i % 5) id FROM range(40) t(i)")
    dirs = {os.path.basename(os.path.dirname(f))
            for f in glob.glob(os.path.join(w._table_path("dbo", "sp"), "**", "*.parquet"), recursive=True)}
    assert dirs == {"region=0", "region=1"}
    assert w.sql("select count(*) from sp").fetchone()[0] == 40


def test_create_sorted_by_auto(w):
    """CREATE TABLE … SORTED BY AUTO AS … (duckrun extension) profiles the query and clusters by the
    auto-picked low-cardinality key; all rows preserved."""
    w.sql("CREATE OR REPLACE TABLE a SORTED BY AUTO AS SELECT (i%5) as region, i as id FROM range(20000) t(i)")
    regs = [r[0] for r in w.sql(f"select region from parquet_scan('{_first_file(w, 'a')}')").fetchall()]
    assert regs == sorted(regs)                                   # clustered by the auto-picked key
    assert w.sql("select count(*) from a").fetchone()[0] == 20000


def test_sorted_by_auto_single_table_profiles_exactly(w, monkeypatch):
    """SORTED BY AUTO over a bare `SELECT * FROM <delta table>` (re-cluster this table) profiles the
    table EXACTLY from the Delta log; a filtered/projected body samples the result relation instead."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT (i%5) region, i id FROM range(100) t(i)")
    calls = []
    monkeypatch.setattr(type(w), "_auto_sort_cols_from_table",
                        lambda self, name, **k: calls.append(("exact", name)) or [])
    monkeypatch.setattr(type(w), "_auto_sort_cols",
                        lambda self, rel, **k: calls.append(("sample", None)) or [])
    w.sql("CREATE OR REPLACE TABLE t SORTED BY AUTO AS SELECT * FROM t")
    assert calls == [("exact", "t")]                             # single bare Delta table → exact
    calls.clear()
    w.sql("CREATE OR REPLACE TABLE t2 SORTED BY AUTO AS SELECT * FROM t WHERE id > 10")
    assert calls == [("sample", None)]                           # filtered body → sampler
    calls.clear()
    w.sql("CREATE OR REPLACE TABLE t3 SORTED BY AUTO AS SELECT region, id FROM t")
    assert calls == [("sample", None)]                           # projection (no `*`) → sampler


# ─────────────────────────────────────────────────────── shared engine seam + fencing (GREEN)

def test_ctas_goes_through_engine_write_delta(w, monkeypatch):
    """Notebook CREATE TABLE AS materializes via engine.write_delta — the same seam dbt uses — not a
    duplicated write path."""
    calls = []
    real = engine.write_delta

    def spy(path, data, mode="overwrite", **kw):
        calls.append((path, mode))
        return real(path, data, mode, **kw)

    monkeypatch.setattr(engine, "write_delta", spy)
    w.sql("CREATE OR REPLACE TABLE t2 AS SELECT 1 AS id")
    assert calls and calls[-1][1] == "overwrite"
    assert w.sql("select id from t2").fetchone()[0] == 1


def test_routed_merge_is_fenced(w, monkeypatch):
    """A routed MERGE reaches delta-rs through engine.merge_delta_clauses with a read_version — the
    fence is applied at the router, so a stale snapshot fails loud (OCC proven in tests/correctness)."""
    w.sql("CREATE OR REPLACE TABLE tgt AS SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id, name)")
    w.sql("CREATE OR REPLACE TABLE src AS SELECT * FROM (VALUES (2,'B'),(3,'c')) v(id, name)")

    seen = {}
    real = engine.merge_delta_clauses

    def spy(path, data, predicate, clauses, **kw):
        seen.update(kw)
        return real(path, data, predicate, clauses, **kw)

    monkeypatch.setattr(engine, "merge_delta_clauses", spy)
    w.sql("MERGE INTO tgt USING src ON target.id = source.id "
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    assert "read_version" in seen and seen["read_version"] is not None


# ───────────────────────────────────────────────────────── VACUUM: compact + vacuum via a DuckDB verb

def test_vacuum_compacts_small_files(w):
    """`VACUUM <table>` repurposes DuckDB's VACUUM verb for Delta maintenance — it compacts the small
    files that many appends leave behind (and vacuums tombstones) via the same engine.optimize the auto-
    maintenance uses. Active file count drops; no rows are lost."""
    w.sql("CREATE OR REPLACE TABLE m AS SELECT 1 AS id")
    for i in range(2, 8):
        w.sql(f"INSERT INTO m VALUES ({i})")

    def nfiles():
        return len(engine._delta_table(w._table_path("dbo", "m"), None).file_uris())

    before = nfiles()
    assert before >= 6                                     # one file per append, uncompacted
    status = w.sql("VACUUM m")
    assert status.fetchone()[0] == "ok"                    # native status relation, like other DML
    assert nfiles() < before                               # compacted into fewer files
    assert w.sql("select count(*) from m").fetchone()[0] == 7   # every row preserved


# ───────────────────────────────────────────── INSERT … REPLACE WHERE: atomic slice overwrite

def test_insert_replace_where_overwrites_only_the_slice(w):
    """INSERT INTO t REPLACE WHERE <pred> SELECT … atomically replaces only the matching slice; rows
    outside the predicate are untouched (delta_rs replaceWhere, one commit)."""
    w.sql("CREATE OR REPLACE TABLE s PARTITIONED BY (region) AS "
          "SELECT * FROM (VALUES (1,'eu'),(2,'eu'),(3,'us')) t(id, region)")
    w.sql("INSERT INTO s REPLACE WHERE region = 'eu' "
          "SELECT * FROM (VALUES (9,'eu'),(8,'eu')) t(id, region)")
    counts = dict(w.sql("select region, count(*) from s group by region").fetchall())
    assert counts == {"eu": 2, "us": 1}                    # eu slice replaced (2 new), us untouched
    assert sorted(r[0] for r in w.sql("select id from s where region='eu'").fetchall()) == [8, 9]
    assert w.sql("select id from s where region='us'").fetchone()[0] == 3


def test_routed_replace_where_is_fenced(w, monkeypatch):
    """A routed REPLACE WHERE reaches delta-rs through engine.replace_where WITH a read_version and the
    parsed predicate — so it's a fenced (CAS) read-modify-write, not a blind-HEAD overwrite."""
    w.sql("CREATE OR REPLACE TABLE rw AS SELECT * FROM (VALUES (1,'a'),(2,'b')) v(id, name)")
    seen = {}
    real = engine.replace_where

    def spy(path, data, predicate, **kw):
        seen.update(kw)
        seen["predicate"] = predicate
        return real(path, data, predicate, **kw)

    monkeypatch.setattr(engine, "replace_where", spy)
    w.sql("INSERT INTO rw REPLACE WHERE id = 1 SELECT * FROM (VALUES (1,'A')) v(id, name)")
    assert seen.get("read_version") is not None
    assert seen["predicate"].strip() == "id = 1"           # predicate split off the body cleanly


def test_replace_where_accepts_positional_columns_like_insert(w):
    """REPLACE WHERE aligns a positional body onto the target schema exactly like plain INSERT — a
    `SELECT 501, 2, 'x'` body (whose columns are named 501/2/x) must land, not raise delta_rs
    SchemaMismatchError. Regression for the append/replace-where projection parity gap."""
    w.sql("CREATE OR REPLACE TABLE p AS "
          "SELECT * FROM (VALUES (1,1,'a'),(2,2,'b'),(3,3,'c')) t(id, grp, val)")
    w.sql("INSERT INTO p SELECT 500, 1, 'x'")              # plain positional INSERT — the oracle
    w.sql("INSERT INTO p REPLACE WHERE grp = 2 SELECT 501, 2, 'x'")   # same body shape, must succeed
    rows = {(r[0], r[1], r[2]) for r in w.sql("select id, grp, val from p").fetchall()}
    assert (501, 2, "x") in rows                            # the replaced grp=2 slice landed positionally
    assert (500, 1, "x") in rows                            # the earlier positional insert survived
    assert (2, 2, "b") not in rows                          # old grp=2 row fully replaced
    assert (3, 3, "c") in rows                              # untouched slice intact


def test_self_reading_append_is_fenced_blind_append_is_not(w, monkeypatch):
    """`insert into t select … from t` fences to the version read (a read_version reaches write_delta,
    so a concurrent commit fails it loud); a plain append over other tables / VALUES stays unfenced
    (last-writer-wins). Proves the router derives the fence only for a read-modify-append."""
    w.sql("CREATE OR REPLACE TABLE acc AS SELECT * FROM (VALUES (1,10),(2,20)) v(id, val)")
    w.sql("CREATE OR REPLACE TABLE src AS SELECT * FROM (VALUES (3,30)) v(id, val)")
    seen = []
    real = engine.write_delta

    def spy(path, data, mode, **kw):
        seen.append((mode, kw.get("read_version")))
        return real(path, data, mode, **kw)

    monkeypatch.setattr(engine, "write_delta", spy)

    w.sql("INSERT INTO acc SELECT * FROM acc WHERE id = 1")     # self-reading -> fenced
    assert seen[-1][0] == "append" and seen[-1][1] is not None
    w.sql("INSERT INTO acc SELECT * FROM src")                  # reads another table -> blind
    assert seen[-1][0] == "append" and seen[-1][1] is None
    w.sql("INSERT INTO acc VALUES (9, 90)")                     # VALUES -> blind
    assert seen[-1][0] == "append" and seen[-1][1] is None


def test_use_switches_write_routing(tmp_path):
    """conn.sql("USE <cat>.<schema>") is native DuckDB, but duckrun resyncs its write routing from
    current_database()/current_schema() so an unqualified write after USE lands in the catalog the USE
    selected — reads and writes agree on one current catalog."""
    import glob
    prim, sec = str(tmp_path / "prim"), str(tmp_path / "sec")
    c = duckrun.connect(prim, schema="dbo", read_only=False)
    c.sql("CREATE OR REPLACE TABLE seed AS SELECT 1 x")
    c.attach(sec, name="sec")
    c.sql("CREATE OR REPLACE TABLE sec.dbo.s0 AS SELECT 1 x")   # give sec a dbo schema to USE into
    c.sql("USE sec.dbo")
    assert c.sql("SELECT current_database()").fetchone()[0] == "sec"
    assert c._current_catalog == "sec"                          # routing followed the USE, not just reads
    c.sql("CREATE OR REPLACE TABLE t AS SELECT 42 x")           # unqualified write
    assert glob.glob(os.path.join(sec, "**", "t"), recursive=True)      # landed in sec
    assert not glob.glob(os.path.join(prim, "**", "t"), recursive=True)  # NOT the primary
    c.close()


# ───────────────────────────────────────── schema evolution: INSERT … / DROP / RENAME COLUMN

def test_insert_with_schema_evolution_adds_columns(w):
    """INSERT WITH SCHEMA EVOLUTION INTO t … (Spark/Delta spelling) widens the table with the source's
    new columns (existing rows get NULL); a plain INSERT with an extra column is rejected loud instead
    (see test_insert_select_extra_columns_fails_loud)."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS id, 'a' AS name")
    w.sql("INSERT WITH SCHEMA EVOLUTION INTO t SELECT 2 AS id, 'b' AS name, 99 AS extra")
    assert w.sql("select * from t").columns == ["id", "name", "extra"]
    assert w.sql("select id, name, extra from t order by id").fetchall() == [(1, "a", None), (2, "b", 99)]


# ─────────────────────────────── adversarial INSERT / UPDATE / DROP forms (conformance-SLT regressions)

def test_insert_values_casts_mixed_literal_types_to_target(w):
    """INSERT … VALUES whose literals share no common type (a VARCHAR 'inf' next to a DECIMAL 0.0) still
    casts each row to the TARGET column type, the way a native INSERT does — instead of failing with
    DuckDB's "Cannot combine types VARCHAR and DECIMAL" when the bare VALUES list is typed on its own."""
    w.sql("CREATE OR REPLACE TABLE fl (f DOUBLE)")
    w.sql("INSERT INTO fl VALUES ('inf'), ('-inf'), ('nan'), (0.0), (-0.0), (1.5)")
    assert w.sql("select count(*) filter (where isinf(f)), count(*) filter (where isnan(f)), "
                 "count(*) filter (where f = 0.0) from fl").fetchone() == (2, 1, 2)


def test_insert_into_with_cte_after_target_shadowing_its_name(w):
    """INSERT INTO t WITH cte AS (…) SELECT … — a CTE placed AFTER the target (even one that SHADOWS the
    target's name) is routed, not swallowed into the relation: the CTE's row is appended."""
    w.sql("CREATE OR REPLACE TABLE sales AS SELECT 1 AS id, 'a' AS note")
    w.sql("INSERT INTO sales WITH sales AS (SELECT 999 AS id, 'cte' AS note) SELECT * FROM sales")
    assert w.sql("select count(*), count(*) filter (where id = 999) from sales").fetchone() == (2, 1)


def test_insert_by_name_aligns_source_columns_to_target(w):
    """INSERT INTO t BY NAME SELECT … aligns the source's columns to the target BY NAME (not by
    position); a target column the source omits is written NULL."""
    w.sql("CREATE OR REPLACE TABLE colorder (a INTEGER, b INTEGER, c INTEGER)")
    w.sql("INSERT INTO colorder BY NAME SELECT 30 AS c, 10 AS a, 20 AS b")
    assert w.sql("select a, b, c from colorder").fetchall() == [(10, 20, 30)]
    w.sql("INSERT INTO colorder BY NAME SELECT 7 AS b")          # a, c omitted → NULL
    assert w.sql("select a, b, c from colorder where b = 7").fetchall() == [(None, 7, None)]


def test_predicate_less_update_touches_every_row_over_many_files(w):
    """UPDATE t SET c = <expr> with NO WHERE updates EVERY row even when the table is many small files.
    delta_rs 1.5.0's predicate-less update() silently updates only some rows of a multi-file table (a
    predicate referencing no column mis-prunes to a broken fast path), so duckrun routes a full-table
    update through a DuckDB-evaluated fenced overwrite."""
    w.sql("CREATE OR REPLACE TABLE batch (i INTEGER)")
    for k in range(60):
        w.sql(f"INSERT INTO batch VALUES ({k})")                 # 60 single-row commits → many files
    w.sql("UPDATE batch SET i = i + 1")
    assert w.sql("select count(*), sum(i), min(i), max(i) from batch").fetchone() == (60, 1830, 1, 60)


def test_plain_drop_of_already_dropped_table_errors(w):
    """DROP TABLE <t> a second time (after it's already dropped) raises like SQL requires; only DROP
    TABLE IF EXISTS is a silent no-op. A dropped duckrun table leaves a tombstone at its path — the drop
    must not read that as 'still exists' and succeed."""
    w.sql("CREATE OR REPLACE TABLE gone (id INTEGER)")
    w.sql("DROP TABLE gone")
    w.sql("DROP TABLE IF EXISTS gone")                           # already gone → silent no-op
    with pytest.raises(Exception):
        w.sql("DROP TABLE gone")                                 # already gone, no IF EXISTS → error


def test_alter_drop_column(w):
    """ALTER TABLE t DROP COLUMN c — a fenced overwrite rewrite (delta_rs has no in-place drop)."""
    w.sql("CREATE OR REPLACE TABLE d AS SELECT 1 AS a, 2 AS b, 3 AS cc")
    w.sql("ALTER TABLE d DROP COLUMN b")
    assert w.sql("select * from d").columns == ["a", "cc"]
    assert w.sql("select * from d").fetchall() == [(1, 3)]


def test_alter_rename_column(w):
    """ALTER TABLE t RENAME COLUMN old TO new — a fenced overwrite rewrite, data preserved."""
    w.sql("CREATE OR REPLACE TABLE r AS SELECT 1 AS a, 2 AS b")
    w.sql("ALTER TABLE r RENAME COLUMN b TO bb")
    assert w.sql("select * from r").columns == ["a", "bb"]
    assert w.sql("select * from r").fetchall() == [(1, 2)]


# ─────────────────────────────────────── DESCRIBE DETAIL / DESCRIBE HISTORY (Delta introspection)

def test_describe_detail(w):
    """DESCRIBE DETAIL <t> (Spark/Delta verb) returns the table's location/partitionColumns/numFiles/
    version from the Delta log — the public way to get a table's storage path (plain DESCRIBE stays
    DuckDB's column view)."""
    w.sql("CREATE OR REPLACE TABLE t PARTITIONED BY (r) AS SELECT (i % 2) r, i id FROM range(20) t(i)")
    d = w.sql("DESCRIBE DETAIL t")
    assert d.columns == ["format", "id", "name", "location", "partitionColumns",
                         "numFiles", "sizeInBytes", "version"]
    row = dict(zip(d.columns, d.fetchone()))
    assert row["format"] == "delta"
    assert row["location"].replace("\\", "/").endswith("dbo/t")
    assert row["partitionColumns"] == ["r"] and row["numFiles"] >= 1 and row["version"] == 0
    # plain DESCRIBE still passes through to DuckDB (column info)
    assert set(r[0] for r in w.sql("DESCRIBE t").fetchall()) == {"r", "id"}


def test_describe_history_and_time_travel(w):
    """DESCRIBE HISTORY <t> returns one row per commit (newest first) — retiring the version/history
    gap — and the location from DESCRIBE DETAIL powers pure-SQL time travel."""
    w.sql("CREATE OR REPLACE TABLE h AS SELECT 1 AS id")
    w.sql("INSERT INTO h SELECT 2 AS id")
    hist = w.sql("DESCRIBE HISTORY h")
    assert hist.columns == ["version", "timestamp", "operation", "operationMetrics"]
    assert [r[0] for r in hist.fetchall()] == [1, 0]        # newest first
    d = w.sql("DESCRIBE DETAIL h")
    loc = dict(zip(d.columns, d.fetchone()))["location"]
    assert w.sql(f"SELECT count(*) FROM delta_scan('{loc}', version => 0)").fetchone()[0] == 1


def test_restore_to_version(w):
    """RESTORE TABLE t TO VERSION AS OF n (Spark/Delta verb) rolls the table back via delta_rs restore
    — a new commit on top of history, so the restore is itself revertible."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS id")   # v0
    w.sql("INSERT INTO t SELECT 2 AS id")                   # v1
    assert sorted(r[0] for r in w.sql("select id from t").fetchall()) == [1, 2]
    w.sql("RESTORE TABLE t TO VERSION AS OF 0")
    assert [r[0] for r in w.sql("select id from t").fetchall()] == [1]
    assert [r[0] for r in w.sql("DESCRIBE HISTORY t").fetchall()] == [2, 1, 0]   # restore = new commit


def test_restore_refused_on_read_only(tmp_path):
    """RESTORE commits a new version, so a read-only session refuses it."""
    wdir = str(tmp_path / "wh")
    w = duckrun.connect(wdir, schema="dbo", read_only=False)
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS id")
    w.sql("INSERT INTO t SELECT 2 AS id")
    ro = duckrun.connect(wdir, schema="dbo")
    with pytest.raises(PermissionError):
        ro.sql("RESTORE TABLE t TO VERSION AS OF 0")


def test_vacuum_refused_on_read_only(tmp_path):
    """VACUUM writes (compacted files + tombstone GC), so a read-only session refuses it loudly rather
    than silently mutating the store."""
    wdir = str(tmp_path / "wh")
    w = duckrun.connect(wdir, schema="dbo", read_only=False)
    w.sql("CREATE OR REPLACE TABLE r AS SELECT 1 AS id")
    ro = duckrun.connect(wdir, schema="dbo")               # read_only is the default
    with pytest.raises(PermissionError):
        ro.sql("VACUUM r")


# ─────────────────────── fail-loud + hostile-identifier guards (external black-box suite regressions)
# Each pins a real defect the external suite surfaced; they exist so CI catches a re-break, since that
# suite isn't run here. Every "no write happened" claim is checked against the log, not just the raise.

def test_update_unknown_set_column_fails_loud_no_commit(w):
    """UPDATE SET on a column that doesn't exist must raise BEFORE any commit — delta_rs's update()
    silently accepts an unknown column and writes a no-op commit that still advances the log."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS id, 'x' AS s")
    before = [r[0] for r in w.sql("DESCRIBE HISTORY t").fetchall()]
    with pytest.raises(Exception):
        w.sql("UPDATE t SET nope = 1")
    assert [r[0] for r in w.sql("DESCRIBE HISTORY t").fetchall()] == before   # no new commit
    assert w.sql("select s from t").fetchone()[0] == "x"                      # data untouched


def test_update_scalar_subquery_in_set_does_not_panic(w):
    """UPDATE t SET c = (SELECT …) — exactly the rewrite the UPDATE…FROM rejection recommends — is
    evaluated via the DuckDB fallback, never handed to delta_rs's datafusion 'not implemented' panic."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT range AS id, 0 AS g FROM range(4)")
    w.sql("CREATE OR REPLACE TABLE ref AS SELECT 7 AS id")
    w.sql("UPDATE t SET g = (SELECT max(id) FROM ref) WHERE id = 1")
    got = dict(w.sql("select id, g from t").fetchall())
    assert got[1] == 7 and got[0] == 0 and got[2] == 0        # only the matched row changed


def test_quoted_identifier_with_dot_is_one_table(w):
    """CREATE TABLE "a.b" is ONE table named a.b (a legal quoted identifier) — the relation splitter is
    quote-aware, so the inner dot is part of the name, not a schema/table separator."""
    w.sql('CREATE OR REPLACE TABLE "a.b" AS SELECT 1 AS id')
    assert os.path.isdir(w._table_path("dbo", "a.b"))         # landed at <root>/dbo/a.b …
    assert not os.path.isdir(w._table_path("a", "b"))         # … NOT dot-split into schema a / table b
    assert w.sql('SELECT id FROM "a.b"').fetchone()[0] == 1


@pytest.mark.parametrize("evil", ['"../escape"', '"x/../../pwned"', '".."'])
def test_path_traversal_table_name_is_rejected(w, evil):
    """A quoted identifier carrying a path separator or a `..` component must fail loud and create
    nothing above the lakehouse root — a quoted name is otherwise opaque to the splitter."""
    w.sql("CREATE OR REPLACE TABLE seed AS SELECT 1 AS id")   # materialize the root dir to watch it
    before = set(os.listdir(w.root_path))
    with pytest.raises(Exception):
        w.sql(f"CREATE TABLE {evil} AS SELECT 1 AS id")
    assert set(os.listdir(w.root_path)) == before             # nothing written outside <root>/dbo


def test_insert_select_extra_columns_fails_loud(w):
    """INSERT INTO t SELECT with MORE columns than the target must raise — a wider SELECT would
    otherwise be silently truncated by the positional projection (data loss); no commit is written."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS a, 'x' AS b")
    before = [r[0] for r in w.sql("DESCRIBE HISTORY t").fetchall()]
    with pytest.raises(Exception):
        w.sql("INSERT INTO t SELECT 1, 'y', 99")              # 3 cols into a 2-col table
    assert [r[0] for r in w.sql("DESCRIBE HISTORY t").fetchall()] == before
    assert w.sql("select count(*) from t").fetchone()[0] == 1


def test_delta_write_inside_explicit_transaction_is_rejected(w):
    """A Delta write auto-commits (delta_rs), so it can't join an open explicit transaction — the
    session rejects it rather than let a following ROLLBACK report success while the row persisted."""
    w.sql("CREATE OR REPLACE TABLE t AS SELECT 1 AS id")
    w.sql("BEGIN")
    with pytest.raises(Exception):
        w.sql("INSERT INTO t VALUES (2)")
    w.sql("ROLLBACK")
    assert [r[0] for r in w.sql("select id from t").fetchall()] == [1]   # the write never landed
