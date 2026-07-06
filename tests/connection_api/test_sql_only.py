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


def test_vacuum_refused_on_read_only(tmp_path):
    """VACUUM writes (compacted files + tombstone GC), so a read-only session refuses it loudly rather
    than silently mutating the store."""
    wdir = str(tmp_path / "wh")
    w = duckrun.connect(wdir, schema="dbo", read_only=False)
    w.sql("CREATE OR REPLACE TABLE r AS SELECT 1 AS id")
    ro = duckrun.connect(wdir, schema="dbo")               # read_only is the default
    with pytest.raises(PermissionError):
        ro.sql("VACUUM r")
