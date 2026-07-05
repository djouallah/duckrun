"""Property tests for the sort-key recommender (``duckrun/sortkey.py``).

Pure-local and network-free: each test plants a small DuckDB table with a KNOWN structure, calls
``sortkey.recommend_sort_key`` / ``sortkey.plan_sample`` directly, and asserts the R-rule outcome.
No Delta table is needed — ``stats`` (null-heavy) and ``partition_cols`` are passed as literals.

The recommender is a deterministic function of its sample, so an ``exact`` profile (the whole table
handed in, ``exact=True``) is fully reproducible with no sampling at all — that is what most tests
use. Two tests exercise the seeded reservoir sample and the session plumbing.

Every outcome asserted here is the CURRENT R-rule behaviour (the fixtures are tuned so today's code
passes) EXCEPT the two the work order fixes: INTERVAL eligibility (test 12) and module purity
(test 15) fail before their task and pass after.
"""
import duckdb
import pytest

import duckrun
from duckrun import sortkey


_COLS = ["table", "in_sort_key", "sort_position", "column", "data_type", "encoding", "ndv",
         "skew_pct", "current_runs", "is_unique", "est_kb_current", "est_kb_sorted", "saved_pct"]


def _con():
    return duckdb.connect()


def _profile(con, select_sql, *, table="t", partition_cols=(), stats=None, exact=True, **kw):
    """Materialize ``select_sql`` as ``table``, profile it, and return ``(recs, lines)`` where
    ``recs`` is ``{column: {field: value}}`` over ``_COLS``. ``exact`` defaults True (whole table)."""
    con.execute(f"CREATE OR REPLACE TABLE {table} AS {select_sql}")
    desc = con.sql(f"DESCRIBE {table}").fetchall()
    cols = [r[0] for r in desc]
    types = {r[0]: r[1] for r in desc}
    n = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
    rows, _schema, lines = sortkey.recommend_sort_key(
        con, "sch", "tbl", table, cols, types, list(partition_cols),
        stats=stats, sample_rows=n, exact=exact, **kw)
    recs = {r[3]: dict(zip(_COLS, r)) for r in rows}
    return recs, lines


def _key_order(recs):
    """Columns in the sort key, ordered by sort_position."""
    return [c for c in sorted((c for c, r in recs.items() if r["in_sort_key"]),
                              key=lambda c: recs[c]["sort_position"])]


# ── 1. FD collapse (R5): country/city, city → country. Never both; country wins. ────────────────
def test_fd_collapse_country_city():
    con = duckdb.connect()
    # 200 cities, each in exactly one of 20 countries (city → country). Ascending cardinality leads
    # with country (ndv 20); city (ndv 200) is at the table's grain (200 ≈ n) so it grain-stops —
    # the key never carries both, and country leads. (city determines country, not the reverse, so
    # this converges via the grain stop rather than an FD-drop.)
    recs, _ = _profile(con, "select (i % 200) as city, (i % 200) // 10 as country "
                             "from range(300) t(i)")
    assert recs["country"]["in_sort_key"] and recs["country"]["sort_position"] == 1
    assert not recs["city"]["in_sort_key"]              # never both; country alone wins


# ── 2. Near-FD band (R5): 1% of cities span two countries. Same outcome. ─────────────────────────
def test_near_fd_band_country_city():
    con = duckdb.connect()
    recs, _ = _profile(con, "select (i % 200) as city, "
                             "case when (i % 100) = 7 then 19 else (i % 200) // 10 end as country "
                             "from range(300) t(i)")
    assert recs["country"]["in_sort_key"] and recs["country"]["sort_position"] == 1
    assert not recs["city"]["in_sort_key"]


# ── 3. Ascending cardinality (R4): ndv 5 / 50 / 500 → key ordered by cardinality. ───────────────
def test_ascending_cardinality():
    con = duckdb.connect()
    # nested moduli keep the combined grain small (distinct(a5,a50,a500) = 500), so all three fit
    # under the grain stop and enter the key strictly in ascending-cardinality order.
    recs, _ = _profile(con, "select (i % 5) as a5, (i % 50) as a50, (i % 500) as a500, i as noise "
                             "from range(4000) t(i)")
    assert _key_order(recs) == ["a5", "a50", "a500"]
    assert not recs["noise"]["in_sort_key"]             # near-unique → grain-stopped


# ── 4. Temporal lead (R6): a date (ndv 365) leads a lower-card dim (ndv 10). ─────────────────────
def test_temporal_leads_despite_higher_ndv():
    con = duckdb.connect()
    recs, _ = _profile(con, "select (date '2020-01-01' + (i % 365)::int) as d, "
                             "(i % 10) as dim, i as uid from range(8000) t(i)")
    assert recs["d"]["sort_position"] == 1              # date leads despite ndv 365 > 10
    assert recs["dim"]["in_sort_key"] and recs["dim"]["sort_position"] == 2
    assert not recs["uid"]["in_sort_key"]


# ── 5. Raw-timestamp demotion (R6): ~unique microsecond ts stays OUT; low-card dims key. ─────────
def test_raw_timestamp_demoted():
    con = duckdb.connect()
    # ts ndv ≈ 0.7·n (like real tpep_pickup_datetime) — past the grain, so not lead-eligible and it
    # grain-stops as a plain candidate. The low-card dims are the real key.
    recs, _ = _profile(con, "select (timestamp '2020-01-01' + ((i % 7000) * interval '1 microsecond')) as ts, "
                             "(i % 2) as flag, (i % 3) as vendor, (i % 5) as kind "
                             "from range(10000) t(i)")
    assert not recs["ts"]["in_sort_key"]                # too fine to form runs → out
    assert recs["flag"]["in_sort_key"] and recs["vendor"]["in_sort_key"] and recs["kind"]["in_sort_key"]


# ── 6. Grain stop: the key stops once the prefix reaches grain_frac·n; nothing finer admitted. ──
def test_grain_stop():
    con = duckdb.connect()
    # a(10)·b(11) = 110 < 0.5·1000; adding c(13) pushes the combined grain past 500 → stop at [a,b].
    recs, _ = _profile(con, "select (i % 10) as a, (i % 11) as b, (i % 13) as c, (i % 17) as d "
                             "from range(1000) t(i)")
    assert _key_order(recs) == ["a", "b"]
    assert not recs["c"]["in_sort_key"] and not recs["d"]["in_sort_key"]


# ── 7. Measures never keys; a heavy one is flagged. ─────────────────────────────────────────────
def test_measures_never_keys():
    con = duckdb.connect()
    recs, lines = _profile(con, "select (i % 4) as region, "
                                 "(i * 1.7)::double as price, ((i % 4) * 1.5)::double as amount "
                                 "from range(4000) t(i)")
    assert not recs["price"]["in_sort_key"] and not recs["amount"]["in_sort_key"]
    assert recs["region"]["in_sort_key"]
    assert any("price" in ln and "measures" in ln for ln in lines)   # heavy measure flagged


# ── 8. Key-organized branch: exact profile, unique id, incompressible others → key == [id]. ─────
def test_key_organized_exact():
    con = duckdb.connect()
    recs, lines = _profile(con, "select i as pk, i * 2 as a, i * 3 as b from range(500) t(i)")
    assert _key_order(recs) == ["pk"]
    assert not recs["a"]["in_sort_key"] and not recs["b"]["in_sort_key"]
    assert recs["pk"]["is_unique"]
    assert any("key-organized" in ln for ln in lines)


# ── 9. Sampled uniqueness refusal: same table, exact=False → no unique flags, no key-organized. ─
def test_sampled_uniqueness_refused():
    con = duckdb.connect()
    recs, lines = _profile(con, "select i as pk, i * 2 as a, i * 3 as b from range(500) t(i)",
                           exact=False)
    assert not any(r["is_unique"] for r in recs.values())       # a sample cannot claim uniqueness
    assert not any("key-organized" in ln for ln in lines)


# ── 10. Null-heavy exclusion (S1): a 80%-null column (from the log) is dropped and named. ────────
def test_null_heavy_excluded():
    con = duckdb.connect()
    stats = {"sparse": {"null_frac": 0.8, "constancy": 0.0, "ndv_cap": None}}
    recs, lines = _profile(con, "select (i % 4) as region, (i % 3) as sparse, i as uid "
                                 "from range(2000) t(i)", stats=stats)
    assert not recs["sparse"]["in_sort_key"]            # null_frac 0.8 > null_excl 0.5 → excluded
    assert recs["region"]["in_sort_key"]
    assert any("null-heavy" in ln and "sparse" in ln for ln in lines)


# ── 11. Partition columns (R8): a partition col leads ORDER BY, holds no slot, no regression. ────
def test_partition_leads_order_by():
    con = duckdb.connect()
    recs, lines = _profile(con, "select (i % 4) as region, (i % 5) as cat from range(4000) t(i)",
                           partition_cols=["region"])
    assert any(ln.strip().startswith("ORDER BY region,") for ln in lines)   # partition col leads
    assert not recs["region"]["in_sort_key"] and recs["region"]["sort_position"] == 0
    assert recs["region"]["saved_pct"] >= 0.0           # already partitioned → no size regression
    assert recs["cat"]["in_sort_key"]                   # the real low-card dimension is the key


# ── 12. INTERVAL ineligible (fails before Task 5, passes after). ─────────────────────────────────
def test_interval_ineligible():
    con = duckdb.connect()
    # dur has the LOWEST cardinality (ndv 3) so it would lead the key if it were value-eligible; an
    # interval is a duration, not a filterable dimension, so it must be excluded — region leads.
    recs, _ = _profile(con, "select (i % 4) as region, ((i % 3) * interval '1 hour') as dur "
                             "from range(4000) t(i)")
    assert not recs["dur"]["in_sort_key"]
    assert recs["region"]["in_sort_key"] and recs["region"]["sort_position"] == 1


# ── 13. Determinism: a seeded reservoir sample gives byte-identical recommendation rows. ────────
def test_seeded_sample_is_deterministic():
    con = duckdb.connect()
    con.execute("CREATE TABLE big AS SELECT (i % 50) a, (i % 7) b, i c FROM range(5000) t(i)")
    con.execute("CREATE TABLE s1 AS SELECT * FROM big USING SAMPLE reservoir(1000 ROWS) REPEATABLE (42)")
    con.execute("CREATE TABLE s2 AS SELECT * FROM big USING SAMPLE reservoir(1000 ROWS) REPEATABLE (42)")
    cols, types = ["a", "b", "c"], {"a": "BIGINT", "b": "BIGINT", "c": "BIGINT"}
    r1, _, _ = sortkey.recommend_sort_key(con, "s", "s1", "s1", cols, types, [],
                                          sample_rows=1000, exact=False)
    r2, _, _ = sortkey.recommend_sort_key(con, "s", "s2", "s2", cols, types, [],
                                          sample_rows=1000, exact=False)
    # only the column-1 label ("s1"/"s2") differs by construction; strip it before comparing.
    assert [r[1:] for r in r1] == [r[1:] for r in r2]


# ── 14. plan_sample math. ───────────────────────────────────────────────────────────────────────
def test_plan_sample_math():
    budget = 256 * 1024 * 1024
    assert sortkey.plan_sample(10_000, 100) == (10_000, True)          # small → exact, no sampling
    assert sortkey.plan_sample(1_000_000_000, 40) == (budget // 40, False)      # 6,710,886, in range
    assert sortkey.plan_sample(1_000_000_000, 4096) == (100_000, False)         # budget/4096 < min → min
    assert sortkey.plan_sample(None, 100) == (budget // 100, False)             # unknown total → sample
    # width clamps the high end: a very narrow row cannot pull more than max_rows.
    assert sortkey.plan_sample(1_000_000_000, 1) == (8_000_000, False)


# ── 15a. Purity: recommend_sort_key itself prints nothing (fails before Task 4). ────────────────
def test_recommend_sort_key_is_silent(capsys):
    con = duckdb.connect()
    _profile(con, "select (i % 4) as region, (i % 5) as cat from range(1000) t(i)")
    assert capsys.readouterr().out == ""


# ── 15b. …but the public advisory still prints the ORDER BY line. ───────────────────────────────
@pytest.fixture
def session(tmp_path):
    return duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)


def test_optimize_analyze_prints_order_by(session, capsys):
    session.sql("select (i % 4) as region, (i % 5) as cat from range(4000) t(i)") \
        .write.mode("overwrite").saveAsTable("an")
    session.table("an").optimize(analyze=True)
    assert "ORDER BY" in capsys.readouterr().out


def test_optimize_analyze_seed_is_deterministic(session, capsys):
    # Seeded advisory over a table is reproducible run-to-run (small table → exact, but the seed
    # path must at least run and stay stable).
    session.sql("select (i % 50) a, (i % 7) b, i c from range(4000) t(i)") \
        .write.mode("overwrite").saveAsTable("seeded")
    r1 = [tuple(row) for row in session.table("seeded").optimize(analyze=True, seed=7).collect()]
    capsys.readouterr()
    r2 = [tuple(row) for row in session.table("seeded").optimize(analyze=True, seed=7).collect()]
    assert r1 == r2
