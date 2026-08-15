"""Property tests for the sort-key recommender (``dbt/adapters/duckrun/sortkey.py``).

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
from dbt.adapters.duckrun import engine, sortkey


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


# ── 5b. Watermark TIMESTAMP (S2 + R6): a near-constant audit ts never leads or joins the key. ───
def test_watermark_timestamp_never_leads_or_joins_key():
    con = duckdb.connect()
    # cutoff: 3 values, 99% of mass on one → effective cardinality ≈ 1, a constant for sorting.
    # It is the lowest-ndv temporal, so pre-fix it stole the tier-0 lead from the ndv≈300 business
    # date (unpatched this profiles to ``ORDER BY cutoff, time`` with date shredded out — the exact
    # production failure): S2's near-constant guard keeps it out, R6 tiers the DATE ahead of it.
    recs, _ = _profile(con,
        "select (date '2018-01-01' + (i % 300)::int)::date as date, (i % 288) * 5 as time, "
        "'DUID_' || (i % 470) as duid, (random() * 1000)::double as mw, "
        "case when i % 1000 < 990 then timestamp '2026-07-05 08:05:00' "
        "     when i % 1000 < 995 then timestamp '2026-07-05 18:00:00' "
        "     else                    timestamp '2026-07-05 19:30:00' end as cutoff "
        "from range(6000) t(i)")
    assert not recs["cutoff"]["in_sort_key"]            # near-constant watermark stays out
    assert recs["date"]["sort_position"] == 1           # DATE leads, not the low-ndv TIMESTAMP


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
    session.sql("CREATE OR REPLACE TABLE an AS "
                "select (i % 4) as region, (i % 5) as cat from range(4000) t(i)")
    session._get_rle("an")                       # the sort-key profiler prints the advisory
    assert "ORDER BY" in capsys.readouterr().out


def test_optimize_analyze_seed_is_deterministic(session, capsys):
    # Seeded advisory over a table is reproducible run-to-run (small table → exact, but the seed
    # path must at least run and stay stable).
    session.sql("CREATE OR REPLACE TABLE seeded AS "
                "select (i % 50) a, (i % 7) b, i c from range(4000) t(i)")
    r1 = [tuple(row) for row in session._get_rle("seeded", seed=7).fetchall()]
    capsys.readouterr()
    r2 = [tuple(row) for row in session._get_rle("seeded", seed=7).fetchall()]
    assert r1 == r2


# ── 16. The DEFAULT (no seed passed) sample is seeded too. ──────────────────────────────────────
# Regression: every production caller reaches the sampler WITHOUT a seed — dbt's `sort_by='auto'`
# (delta_plugin) and the connection API's `SORTED BY AUTO`, over a relation (engine.auto_sort_cols)
# or over a table (session._get_rle). That used to mean an UNSEEDED reservoir, so one config could
# pick a different key on every build: measured at 8.8% (700 MB) of a 592M-row table when a
# different measure won the last tail slot. There are exactly two sampling seams and the tests below
# cover one each — the default is the only thing those callers actually exercise.
def test_unseeded_sampler_emits_repeatable():
    """The SQL the sampler builds carries REPEATABLE even when no seed is passed."""
    con = duckdb.connect()
    con.execute("CREATE TABLE wide AS SELECT (i % 50) a, (i % 7) b, i c FROM range(40000) t(i)")
    captured = []

    class Spy:                      # DuckDBPyConnection attributes are read-only, so proxy it
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            captured.append(sql)
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    engine.auto_sort_cols(Spy(con), "wide")       # no seed — the production call shape
    sample_sql = [s for s in captured if "USING SAMPLE" in s]
    assert sample_sql, "profiler did not sample"
    assert "REPEATABLE" in sample_sql[0], sample_sql[0]


def test_unseeded_sample_draws_the_same_rows_twice():
    """The rows the reservoir actually draws are identical across two unseeded profiles.

    Stronger than comparing the chosen key: a key can survive a different sample by luck, so this
    checksums `_rle_src` itself, which is what the recommender reads. The table must be WIDE for
    the reservoir to bite at all — `plan_sample` budgets 256 MiB over the estimated row width, so
    4 narrow columns would plan 8M rows and simply take the whole table (no sampling, trivially
    stable). 112 VARCHAR columns => 2,688 B/row => a 100,000-row plan against 150,000 rows."""
    ncols, nrows = 112, 150_000
    assert sortkey.plan_sample(None, sortkey.estimate_row_bytes(
        {f"c{i}": "VARCHAR" for i in range(ncols)})) == (100_000, False)

    con = duckdb.connect()
    # c0..c2 are real candidates; the rest are constants the picker drops (ndv == 1). All VARCHAR
    # so estimate_row_bytes sees the 24 B/column width the plan above assumes.
    sel = ["(i % 97)::VARCHAR c0", "(i % 13)::VARCHAR c1", "(i % 3)::VARCHAR c2"]
    sel += [f"'x' c{i}" for i in range(3, ncols)]
    con.execute(f"CREATE TABLE wide AS SELECT {', '.join(sel)} FROM range({nrows}) t(i)")

    sums = []

    class Spy:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            out = self._inner.execute(sql, *a, **k)
            if "USING SAMPLE" in sql:      # _rle_src now holds the draw — fingerprint it
                sums.append(self._inner.execute(          # bit_xor: order-free and cannot overflow
                    "SELECT count(*), bit_xor(hash(c0 || '|' || c1 || '|' || c2)) FROM _rle_src"
                ).fetchone())
            return out

        def __getattr__(self, name):
            return getattr(self._inner, name)

    engine.auto_sort_cols(Spy(con), "wide")       # no seed, twice — the production call shape
    engine.auto_sort_cols(Spy(con), "wide")
    assert len(sums) == 2 and sums[0][0] == 100_000, sums
    assert sums[0] == sums[1], f"reservoir drew different rows: {sums}"


def test_get_rle_sample_is_seeded_by_default(session, capsys, monkeypatch):
    """The OTHER sampling seam — `session._get_rle`, behind `SORTED BY AUTO` over a table.

    `engine.auto_sort_cols` (above) and this are the only two places that build a `USING SAMPLE`,
    and they are separate code. Without this, reverting half the fix would keep the suite green.

    `plan_sample` floors at `min_rows=100_000`, so any table small enough to build in a unit test
    profiles EXACTLY and never reaches the sampling branch — hence the monkeypatch, which is the
    cheap way to force `exact=False` rather than materialising 100k+ rows of Delta just to get
    there."""
    session.sql("CREATE OR REPLACE TABLE rle_src AS "
                "select (i % 50) a, (i % 7) b, i c from range(4000) t(i)")
    monkeypatch.setattr(sortkey, "plan_sample", lambda *a, **k: (1000, False))

    r1 = [tuple(row) for row in session._get_rle("rle_src").fetchall()]
    capsys.readouterr()
    r2 = [tuple(row) for row in session._get_rle("rle_src").fetchall()]
    capsys.readouterr()
    # ndv / skew / run estimates come straight off the draw, so two different 1000-of-4000
    # reservoirs would show up here even when the chosen key happens to survive.
    assert r1 == r2, "unseeded _get_rle sample: two profiles of one table disagree"


# ── 17. Measure tail (step 6): a dim-correlated zero-heavy measure gets a TAIL slot, below the ──
#        dim key — the nyc fare/tip/tolls shape (probe: tests/parquet_layout/nyc). ───────────────
def test_measure_tail_zero_heavy():
    con = duckdb.connect()
    # price is 0 for three of region's four values and takes 7 small values under region=3 —
    # ordering rows by price INSIDE each region group collapses it into a handful of runs.
    recs, lines = _profile(
        con, "select (i % 4) as region, "
             "(case when i % 4 = 3 then ((i // 4) % 7) * 1.5 else 0 end)::double as price "
             "from range(4000) t(i)")
    assert recs["region"]["in_sort_key"] and recs["region"]["sort_position"] == 1
    assert recs["price"]["in_sort_key"]                                  # earned a tail slot …
    assert recs["price"]["sort_position"] > recs["region"]["sort_position"]  # … below every dim
    assert any("measure tail" in ln and "price" in ln for ln in lines)
    assert any(ln.strip().startswith("ORDER BY region, price") for ln in lines)


# ── 17b. Tail FD band: a measure determined by the dims clusters for free — no slot; an ─────────
#         uncorrelated continuous measure has no in-group runs to buy — no slot either. ──────────
def test_measure_tail_fd_and_uncorrelated_refused():
    con = duckdb.connect()
    recs, lines = _profile(
        con, "select (i % 4) as region, ((i % 4) * 1.5)::double as fd_amount, "
             "(i * 1.7)::double as noise_price from range(4000) t(i)")
    assert recs["region"]["in_sort_key"]
    assert not recs["fd_amount"]["in_sort_key"]      # FD of region → free clustering, no slot
    assert not recs["noise_price"]["in_sort_key"]    # in-group runs ~iid → no modeled saving
    assert not any("measure tail" in ln for ln in lines)


# ── 17c. Key-organized tables grow no tail: a unique key leaves no groups to refine. ────────────
def test_measure_tail_skipped_when_key_organized():
    con = duckdb.connect()
    recs, lines = _profile(
        con, "select i as pk, (case when i % 4 = 3 then ((i // 4) % 7) * 1.5 else 0 end)::double "
             "as price from range(500) t(i)")
    assert _key_order(recs) == ["pk"]
    assert not recs["price"]["in_sort_key"]
    assert not any("measure tail" in ln for ln in lines)


# ── 18. decimal_narrow_target: a wide DECIMAL (p>18 → FLBA, no arrow-rs dictionary) narrows to ──
#        DECIMAL(18,s) iff its EXACT max fits; scale is preserved. Pure — no DB. ──────────────────
def test_decimal_narrow_target():
    from decimal import Decimal
    f = sortkey.decimal_narrow_target
    # p > 18 and the true max fits DECIMAL(18,s) → narrow, scale unchanged.
    assert f("DECIMAL(38,6)", Decimal("9412.50")) == "DECIMAL(18,6)"
    assert f("DECIMAL(38,6)", 9412.5) == "DECIMAL(18,6)"
    assert f("DECIMAL(38,4)", 100) == "DECIMAL(18,4)"          # scale preserved, only precision cut
    assert f("DECIMAL(38,2)", None) == "DECIMAL(18,2)"          # all-NULL column trivially fits
    # p <= 18 already fits INT64 — nothing to narrow.
    assert f("DECIMAL(18,2)", 5) is None
    assert f("DECIMAL(10,2)", 5) is None
    # true max does NOT fit DECIMAL(18,0) (>= 10**18) → keep FLBA (exact-fit rule, no headroom guess).
    assert f("DECIMAL(38,0)", 10 ** 18) is None                # boundary: 10**18 does not fit
    assert f("DECIMAL(38,0)", 5 * 10 ** 18) is None
    # scale leaves no integer digit (s > 17) → cannot narrow to DECIMAL(18,s).
    assert f("DECIMAL(38,18)", 0) is None
    # non-decimal / unparseable → None.
    assert f("BIGINT", 5) is None
    assert f("VARCHAR", None) is None
    # NUMERIC alias and scale-less DECIMAL(p) (scale 0) are the same type in other spellings —
    # they narrow identically instead of silently never matching.
    assert f("NUMERIC(38,4)", 100) == "DECIMAL(18,4)"
    assert f("DECIMAL(38)", 100) == "DECIMAL(18,0)"
    assert f("NUMERIC(38)", 10 ** 18) is None                  # same exact-fit rule applies
    assert f("NUMERIC(18,2)", 5) is None                       # p <= 18 still nothing to narrow
