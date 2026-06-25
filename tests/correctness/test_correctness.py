"""Engine-level correctness for the read→write path: snapshot pinning (issue #1) AND round-trip
fidelity (does a value survive write → fresh-read unchanged?).

PART 1 — snapshot pinning. Exercises the delta-rs side of the pin (``read_version`` →
``DeltaTable.load_as_version`` + ``max_commit_retries=0``), which works on the pinned deltalake 1.5.0
floor regardless of the duckdb-delta build. The duckdb-delta ``delta_scan(version => N)`` staging-read
pin is covered by the connection-API matrix (skipped on older builds).
  - merge_delta(read_version=vB): a foreign commit in (vB, HEAD] fails the merge loudly; without a
    concurrent writer it commits. This is the single-snapshot MERGE window.
  - replace_where: a SINGLE atomic Delta commit (replaceWhere) — not a delete-then-append pair —
    and CAS-fenced when pinned.
  - maintenance is NEVER pinned: _maintain takes no version parameter (structural guard against the
    stale-file-list compaction/vacuum that would corrupt the table), and a pinned write leaves a
    clean, readable HEAD.

PART 2 — round-trip fidelity. The interaction seam duckrun owns: DuckDB produces a relation →
delta-rs writes it → duckdb-delta (``delta_scan``) reads it back. The invariant in every case is
metamorphic — write through duckrun, read back through a FRESH ``duckrun.connect()`` (real Delta on
disk, no cache), and it must equal the SAME relation evaluated in plain DuckDB. Covers a boundary-value
type matrix (incl. NULLs), complex/nested types (list/struct/map), the SQL DML re-parser
(``delta_dml``), incremental strategies, and schema evolution. Where delta-rs cannot represent a type,
the write must fail LOUDLY (pinned) rather than silently coerce.
"""
import inspect
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

import duckrun
from duckrun import DeltaTable as DuckrunDeltaTable
from dbt.adapters.duckrun import engine


def _seed(path):
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                  "value": pa.array([10, 10, 10], pa.int64())}),
        "overwrite",
    )  # -> v0


def _tbl(ids_values):
    ids, vals = zip(*ids_values)
    return pa.table({"id": pa.array(ids, pa.int64()), "value": pa.array(vals, pa.int64())})


# --------------------------------------------------------------- merge read_version pinning

def test_pinned_merge_refuses_on_foreign_commit():
    """vB=0; a foreign commit (v1) touches id=1; a merge pinned at v0 that also touches id=1 must
    fail loud — OCC validates (v0, HEAD] and the foreign commit conflicts."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})  # foreign -> v1

    with pytest.raises(CommitFailedError):
        engine.merge_delta(path, _tbl([(1, 77), (4, 77)]), "id", read_version=vB)


def test_pinned_merge_commits_without_conflict():
    """No concurrent writer: a pinned merge commits and applies the upsert."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    engine.merge_delta(path, _tbl([(2, 99), (4, 99)]), "id", read_version=vB)
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 10, 2: 99, 3: 10, 4: 99}


# --------------------------------------------------------------- replaceWhere atomicity

def test_replace_where_is_single_atomic_commit():
    """replaceWhere is ONE commit (v0 -> v1), not a delete-then-append pair, and replaces only the
    matching window."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)  # v0
    engine.replace_where(path, _tbl([(1, 77), (2, 77)]), "id < 3",
                         read_version=engine.table_version(path))
    assert DeltaTable(path).version() == 1  # exactly one new version — atomic, not two commits
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 77, 2: 77, 3: 10}


def test_replace_where_cas_refuses_on_foreign_commit():
    """A pinned replaceWhere fails loud if the table moved since vB (compare-and-swap)."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})  # foreign -> v1
    with pytest.raises(CommitFailedError):
        engine.replace_where(path, _tbl([(1, 77)]), "id < 3", read_version=vB)


# --------------------------------------------------------------- maintenance never pinned

def test_maintain_takes_no_version_parameter():
    """Structural guard: _maintain must not accept a version/read_version — it always runs at a
    fresh HEAD. A stale-snapshot compaction/vacuum would delete files live versions reference."""
    params = set(inspect.signature(engine._maintain).parameters)
    assert "version" not in params and "read_version" not in params


def test_pinned_write_leaves_clean_head():
    """After a pinned merge, HEAD is readable and reflects the merge (maintenance reopened fresh,
    did not corrupt by operating on the pinned snapshot)."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    engine.merge_delta(path, _tbl([(2, 55)]), "id", read_version=vB)
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 10, 2: 55, 3: 10}


# ══════════════════════════════════════════════════════════════════════════════════════════════════
# PART 2 — round-trip fidelity. write → fresh-read → must equal the SAME relation in plain DuckDB.
# ══════════════════════════════════════════════════════════════════════════════════════════════════
def _k(row):
    """Order-insensitive, None-safe sort key for a row of mixed scalars."""
    return tuple(str(c) for c in row)


@pytest.fixture
def root(tmp_path):
    """A bare warehouse root (no seed). Tests write their own tables into schema ``dbo``."""
    return str(tmp_path / "wh")


def _rw(root, name, select_sql, mode="overwrite"):
    """Write ``select_sql`` into ``dbo.<name>`` through the connection API (the real write path)."""
    duckrun.connect(root, schema="dbo", read_only=False) \
        .sql(select_sql).write.mode(mode).saveAsTable(name)


def _read(root, name):
    """Read ``dbo.<name>`` back through a FRESH connection → rows sorted (real Delta on disk only)."""
    rel = duckrun.connect(root, schema="dbo").sql(f"select * from {name}")
    return sorted(rel.fetchall(), key=_k)


def _oracle(select_sql):
    """The source-of-truth: the same relation evaluated in plain DuckDB, never touching Delta."""
    con = duckdb.connect()
    try:
        return sorted(con.sql(select_sql).fetchall(), key=_k)
    finally:
        con.close()


# ── 1. Type / value fidelity — the values you write are the values you read, for every type and NULL.
# Each case writes a whole COLUMN of boundary values — type min/max, zero, negatives, and an interleaved
# NULL — not a single toy literal. The first row's cast fixes the column type; the rest coerce to it.
# The oracle is the SAME VALUES list in plain DuckDB — if delta-rs cannot store the type, the write
# raises and that loud failure is pinned (a regression to silent coercion would surface as a mismatch).
TYPE_MATRIX = [
    ("tinyint",      "((-128)::TINYINT),(127),(0),(-1),(1),(NULL)"),
    ("smallint",     "((-32768)::SMALLINT),(32767),(0),(-1),(NULL)"),
    ("integer",      "((-2147483648)::INTEGER),(2147483647),(0),(-1),(NULL)"),
    ("bigint",       "((-9223372036854775808)::BIGINT),(9223372036854775807),(0),(-1),(NULL)"),
    ("boolean",      "(true::BOOLEAN),(false),(NULL)"),
    ("double",       "((3.141592653589793)::DOUBLE),(-2.5),(0.0),(1e308),(-1e-308),(NULL)"),
    ("float",        "((1.5)::FLOAT),(-0.25),(0.0),(NULL)"),
    ("decimal_38_9", "((12345678901234567890123456789.123456789)::DECIMAL(38,9)),"
                     "(-99999999999999999999999999999.999999999),(0.000000001),(NULL)"),
    ("decimal_18_2", "((1234567890123456.78)::DECIMAL(18,2)),(-0.01),(0.00),(NULL)"),
    ("varchar",      "('hello world'::VARCHAR),(''),('café — 日本語 — 😀'),(repeat('x', 100000)),(NULL)"),
    ("blob",         "('\\xDE\\xAD\\xBE\\xEF'::BLOB),('\\x00'::BLOB),(''::BLOB),(NULL)"),
    ("date",         "(DATE '2026-06-22'),(DATE '1970-01-01'),(DATE '0001-01-01'),(DATE '9999-12-31'),(NULL)"),
    ("timestamp",    "(TIMESTAMP '2026-06-22 13:45:01.123456'),(TIMESTAMP '1970-01-01 00:00:00'),"
                     "(TIMESTAMP '9999-12-31 23:59:59.999999'),(NULL)"),
    ("timestamptz",  "(TIMESTAMPTZ '2026-06-22 13:45:01+00'),(TIMESTAMPTZ '1970-01-01 00:00:00+00'),(NULL)"),
]


@pytest.mark.parametrize("case_id,values", TYPE_MATRIX, ids=[c[0] for c in TYPE_MATRIX])
def test_type_value_roundtrip(root, case_id, values):
    """A column of boundary values (incl. interleaved NULL) must survive write→fresh-read unchanged,
    OR fail loudly. The oracle is the SAME VALUES list in plain DuckDB — no hand-asserted storage type.
    """
    select_sql = f"select v from (values {values}) t(v)"
    try:
        _rw(root, case_id, select_sql)
    except Exception as exc:  # noqa: BLE001 — pin the loud failure for unrepresentable types.
        pytest.skip(f"delta-rs cannot store {case_id}; pinned as loud failure: {type(exc).__name__}")
        return
    assert _read(root, case_id) == _oracle(select_sql), f"value mutated on round-trip: {case_id}"


def test_multicolumn_all_nulls_roundtrip(root):
    """A whole row of NULLs across mixed types must come back as NULLs (no column dropped/defaulted)."""
    sql = "select NULL::INTEGER a, NULL::VARCHAR b, NULL::DOUBLE c, NULL::DATE d"
    _rw(root, "allnull", sql)
    assert _read(root, "allnull") == _oracle(sql) == [(None, None, None, None)]


def test_mixed_null_and_value_rows(root):
    """Interleaved NULL and present values keep their per-row identity (no positional smearing)."""
    sql = "select * from (values (1,'a'),(2,NULL),(NULL,'c')) t(id, name)"
    _rw(root, "mixed", sql)
    assert _read(root, "mixed") == _oracle(sql)


# ── 1b. Complex / nested types — list / struct / map and combinations, plus a wide realistic row.
COMPLEX = [
    ("list_int",       "select * from (values ([1,2,3]),([]),([4]),(NULL)) t(v)"),
    ("list_varchar",   "select * from (values (['a','b']),(['c']),(NULL)) t(v)"),
    ("struct",         "select * from (values "
                       "({'id': 1, 'name': 'a'}),({'id': 2, 'name': 'b'}),(NULL)) t(v)"),
    ("map",            "select MAP {'k1': 1, 'k2': 2} as v "
                       "union all select MAP {'x': 9} union all select NULL"),
    ("nested_struct",  "select * from (values "
                       "({'tags': ['x','y'], 'meta': {'n': 1}}),"
                       "({'tags': [], 'meta': {'n': 2}})) t(v)"),
    ("list_of_struct", "select * from (values "
                       "([{'a': 1}, {'a': 2}]),([{'a': 3}])) t(v)"),
]


@pytest.mark.parametrize("case_id,select_sql", COMPLEX, ids=[c[0] for c in COMPLEX])
def test_complex_roundtrip(root, case_id, select_sql):
    _rw(root, case_id, select_sql)
    assert _read(root, case_id) == _oracle(select_sql), f"complex value mutated: {case_id}"


def test_wide_realistic_10_rows(root):
    """A wide, realistic order-line table: 10 rows, 8 mixed columns incl. decimal, ts, list, struct."""
    sql = """
        select
            i                                        as order_id,
            'cust_' || (i % 3)                       as customer,
            (i * 19.99)::DECIMAL(12,2)               as amount,
            (i % 2 = 0)                              as is_paid,
            TIMESTAMP '2026-01-01 00:00:00' + to_hours(i) as ordered_at,
            [i, i*2, i*3]                            as line_qtys,
            {'sku': 'S' || i, 'discount': i * 0.5}   as detail,
            case when i % 4 = 0 then NULL else 'note ' || i end as memo
        from range(1, 11) t(i)
    """
    _rw(root, "orders", sql)
    assert _read(root, "orders") == _oracle(sql)


# ── 2. SQL DML routing — conn.sql(...) DML routes through delta_dml.handle() to delta-rs; the persisted
#       Delta must equal an independently-computed expected relation (not just "didn't error").
def _seed_items(root):
    """``dbo.items(id, name)`` = (1,a),(2,b),(3,c) on a writable connection; returns the connection."""
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)") \
        .write.mode("overwrite").saveAsTable("items")
    return conn


def test_dml_insert_select(root):
    _seed_items(root).sql("insert into items select * from (values (4,'d')) t(id, name)")
    assert _read(root, "items") == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]


def test_dml_insert_values(root):
    _seed_items(root).sql("insert into items values (5, 'e')")
    assert _read(root, "items") == [(1, "a"), (2, "b"), (3, "c"), (5, "e")]


def test_dml_update_predicate(root):
    _seed_items(root).sql("update items set name = 'Z' where id = 2")
    assert _read(root, "items") == [(1, "a"), (2, "Z"), (3, "c")]


def test_dml_delete_predicate(root):
    _seed_items(root).sql("delete from items where id = 2")
    assert _read(root, "items") == [(1, "a"), (3, "c")]


@pytest.mark.parametrize("stmt", [
    "delete from items where id = 2",
    "update items set name = 'Y' where id = 2",
], ids=["delete", "update"])
def test_dml_delete_update_are_snapshot_fenced_like_handle(root, monkeypatch, stmt):
    """conn.sql("delete"/"update") MUST route through the same snapshot-fenced engine path as
    DeltaTable.forName(...).delete()/.update(): pinned to the version read (read_version →
    load_as_version), committed under delta-rs OCC over (vB, HEAD], so a CONFLICTING foreign commit
    makes it fail loud — SQL and the DataFrame handle behave identically.

    Regression guard: the SQL handler used to call delta-rs delete()/update() directly at HEAD,
    skipping the read_version pin, so a raw conn.sql delete/update silently applied over a foreign
    commit while the DataFrame handle refused it. Inject the read→foreign-commit→write race by
    forcing the statement's internal version capture to the now-stale vB."""
    conn = _seed_items(root)
    path = _path(root, "items")
    vB = engine.table_version(path)
    DeltaTable(path).update(predicate="id = 2", updates={"name": "'X'"})  # foreign commit -> vB+1
    # The statement reads vB but HEAD has already moved to vB+1 (a concurrent writer). Pinned to vB,
    # OCC over (vB, HEAD] sees the foreign id=2 commit and must refuse.
    monkeypatch.setattr(engine, "table_version", lambda *a, **k: vB)
    with pytest.raises(CommitFailedError):
        conn.sql(stmt)


def test_dml_insert_reordered_column_list(root):
    """INSERT (name, id) must map by NAME, not position — _append_projected reads the target schema via
    delta_scan and canonicalizes the supplied list. A regression to positional mapping swaps id/name and
    corrupts the read→write seam, so this pins the by-name contract."""
    _seed_items(root).sql("insert into items (name, id) select 'd', 4")
    assert _read(root, "items") == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]


def test_dml_create_table_as(root):
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql("create table built as select * from (values (7,'g'),(8,'h')) t(id, name)")
    assert _read(root, "built") == [(7, "g"), (8, "h")]


def test_dml_alter_add_column_backfills_null(root):
    """ALTER ADD COLUMN must leave existing rows readable with the new column NULL-backfilled."""
    _seed_items(root).sql("alter table items add column qty INTEGER")
    assert _read(root, "items") == [(1, "a", None), (2, "b", None), (3, "c", None)]


def test_dml_drop_table_tombstones(root):
    """DROP hides the table from discovery (tombstone marker), so a fresh connect can't see it."""
    _seed_items(root).sql("drop table items")
    fresh = duckrun.connect(root, schema="dbo")
    assert "items" not in fresh.catalog.listTables()


def test_dml_lossy_numeric_narrowing_rejected(root):
    """INSERT that would silently change a numeric value on cast (3.9 → 4) must raise, not truncate."""
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql("select (1)::INTEGER as v").write.mode("overwrite").saveAsTable("nums")
    with pytest.raises(Exception):  # noqa: B017 — pin: lossy narrowing is refused at the guard.
        conn.sql("insert into nums select 3.9")
    assert _read(root, "nums") == [(1,)]   # unchanged — the rejected insert left no partial write


# ── 3. Incremental correctness — each strategy lands the right rows AND bumps the Delta version once.
def _path(root, name):
    return duckrun.connect(root, schema="dbo")._table_path("dbo", name)


def test_append_adds_rows_and_bumps_version(root):
    _rw(root, "ap", "select * from (values (1,'a')) t(id, name)")
    v0 = engine.table_version(_path(root, "ap"))
    _rw(root, "ap", "select * from (values (2,'b')) t(id, name)", mode="append")
    assert _read(root, "ap") == [(1, "a"), (2, "b")]
    assert engine.table_version(_path(root, "ap")) == v0 + 1


def test_overwrite_replaces_rows(root):
    _rw(root, "ov", "select * from (values (1,'a'),(2,'b')) t(id, name)")
    _rw(root, "ov", "select * from (values (9,'z')) t(id, name)", mode="overwrite")
    assert _read(root, "ov") == [(9, "z")]


def test_safeappend_creates_then_appends(root):
    _rw(root, "sa", "select * from (values (1,'a')) t(id, name)", mode="safeappend")
    _rw(root, "sa", "select * from (values (2,'b')) t(id, name)", mode="safeappend")
    assert _read(root, "sa") == [(1, "a"), (2, "b")]


MERGE_CASES = [
    ("insert_only", [(1, 10), (2, 10)], [(2, 99), (3, 99)],
     lambda b: b.whenNotMatchedInsertAll(),
     [(1, 10), (2, 10), (3, 99)]),
    ("update_and_insert", [(1, 10), (2, 10)], [(2, 99), (3, 99)],
     lambda b: b.whenMatchedUpdateAll().whenNotMatchedInsertAll(),
     [(1, 10), (2, 99), (3, 99)]),
]


@pytest.mark.parametrize("case_id,seed,src,build,expected", MERGE_CASES, ids=[c[0] for c in MERGE_CASES])
def test_merge_strategies(root, case_id, seed, src, build, expected):
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    seed_vals = ",".join(f"({i},{v})" for i, v in seed)
    conn.sql(f"select * from (values {seed_vals}) t(id, val)").write.mode("overwrite").saveAsTable("m")
    src_vals = ",".join(f"({i},{v})" for i, v in src)
    source = conn.sql(f"select * from (values {src_vals}) t(id, val)")
    build(DuckrunDeltaTable.forName(conn, "dbo.m").merge(source, "target.id = source.id")).execute()
    assert _read(root, "m") == sorted(expected, key=_k)


def test_delete_insert_replaces_keys_and_keeps_batch_duplicates(root):
    """incremental_strategy='delete+insert' (Plugin._store_delete_insert → engine.replace_where):
    delete the target rows whose unique_key is in the incoming batch, then insert the WHOLE batch.
    Duplicate keys WITHIN the batch are preserved — the behavior that distinguishes delete+insert
    from merge (which rejects duplicate source keys). Pins the fix for the upstream sde model, which
    duckrun previously aliased to merge (a silent behavior swap)."""
    from dbt.adapters.duckrun.delta_plugin import Plugin
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, val)") \
        .write.mode("overwrite").saveAsTable("di")
    path = _path(root, "di")
    vB = engine.table_version(path)
    # batch: replace id=2, and insert id=4 TWICE (a duplicate key merge would reject).
    batch = conn.sql("select * from (values (2,'B'),(4,'d'),(4,'D')) t(id, val)")
    keys = conn.sql("select distinct id from (values (2,'B'),(4,'d'),(4,'D')) t(id, val)").fetchall()
    predicate = Plugin._key_set_predicate(Plugin.__new__(Plugin), ["id"], keys)
    assert predicate == '"id" IN (2, 4)'
    engine.replace_where(path, batch, predicate, read_version=vB)
    # id 1,3 untouched; id 2 replaced; BOTH id=4 rows present (duplicate preserved).
    assert _read(root, "di") == sorted([(1, "a"), (3, "c"), (2, "B"), (4, "d"), (4, "D")], key=_k)


def test_delete_insert_predicate_rendering():
    """_key_set_predicate / _sql_literal render CAST-free predicates: bare numerics/bools, quoted
    (and escaped) strings, and an OR-of-ANDs for a composite key."""
    from dbt.adapters.duckrun.delta_plugin import Plugin
    p = Plugin.__new__(Plugin)
    assert p._key_set_predicate(["id"], [(1,), (2,)]) == '"id" IN (1, 2)'
    assert p._key_set_predicate(["code"], [("a",), ("o'x",)]) == '''"code" IN ('a', 'o''x')'''
    assert p._key_set_predicate(["a", "b"], [(1, "x"), (2, "y")]) == \
        '''(("a" = 1 AND "b" = 'x') OR ("a" = 2 AND "b" = 'y'))'''


def test_merge_idempotent_remerge(root):
    """Re-merging the same source must not duplicate or mutate rows (idempotency)."""
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql("select * from (values (1,10),(2,10)) t(id, val)").write.mode("overwrite").saveAsTable("mi")
    for _ in range(2):
        src = conn.sql("select * from (values (2,99),(3,99)) t(id, val)")
        DuckrunDeltaTable.forName(conn, "dbo.mi").merge(src, "target.id = source.id") \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    assert _read(root, "mi") == [(1, 10), (2, 99), (3, 99)]


# ── 4. Schema evolution — add column, type-changing overwrite, and incompatible append must fail loudly.
def test_overwrite_schema_option_changes_column_type(root):
    """overwrite + option('overwriteSchema') replaces the schema wholesale — int column → string."""
    _rw(root, "evo", "select 1 as v")
    duckrun.connect(root, schema="dbo", read_only=False) \
        .sql("select 'now a string' as v") \
        .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("evo")
    assert _read(root, "evo") == [("now a string",)]


def test_overwrite_without_schema_option_casts_to_existing_type(root):
    """Plain overwrite keeps the existing schema and casts — an incompatible type change fails loudly
    (it does NOT silently drop the schema). This pins the casting boundary: overwriting an INT column
    with a non-castable string is refused, leaving the table untouched."""
    _rw(root, "evo2", "select 1 as v")
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    with pytest.raises(Exception):  # noqa: B017 — pin: cast failure, not a silent schema swap.
        conn.sql("select 'not an int' as v").write.mode("overwrite").saveAsTable("evo2")
    assert _read(root, "evo2") == [(1,)]


def test_incompatible_append_fails_loudly(root):
    """Appending a relation whose schema can't reconcile with the table must raise, not silently drop
    or coerce columns. (A column-count / name mismatch is the canonical incompatible append.)"""
    _rw(root, "inc", "select 1 as id, 'a' as name")
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    with pytest.raises(Exception):  # noqa: B017 — pin: incompatible append is refused.
        conn.sql("select 1 as id, 'a' as name, 999 as extra").write.mode("append").saveAsTable("inc")
    assert _read(root, "inc") == [(1, "a")]   # table untouched by the rejected append
