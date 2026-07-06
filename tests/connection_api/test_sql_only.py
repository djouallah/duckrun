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
