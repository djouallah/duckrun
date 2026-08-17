"""Property tests for the sort-key recommender (``dbt/adapters/duckrun/sortkey.py``).

Pure-local and network-free: each test plants a small DuckDB table with a KNOWN structure, calls
``sortkey.recommend_sort_key`` directly, and asserts the R-rule outcome. No Delta table is needed —
``stats`` (null-heavy) and ``partition_cols`` are passed as literals.

The recommender profiles every row it is handed — there is no sampling and nothing seeded — so it is
a deterministic function of the data alone. The reproducibility tests below therefore assert the
strong form: two profiles of one table agree with no seed in the picture, including over a source
whose row order is not stable.

Every outcome asserted here is the CURRENT R-rule behaviour (the fixtures are tuned so today's code
passes).
"""
import duckdb
import pytest

import duckrun
from dbt.adapters.duckrun import engine, sortkey


_COLS = ["table", "in_sort_key", "sort_position", "column", "data_type", "encoding", "ndv",
         "skew_pct", "current_runs", "is_unique", "est_kb_current", "est_kb_sorted", "saved_pct"]


def _con():
    return duckdb.connect()


def _profile(con, select_sql, *, table="t", partition_cols=(), stats=None, **kw):
    """Materialize ``select_sql`` as ``table``, profile it, and return ``(recs, lines)`` where
    ``recs`` is ``{column: {field: value}}`` over ``_COLS``. Mirrors production: the profiler is
    always handed a materialized relation holding every row."""
    con.execute(f"CREATE OR REPLACE TABLE {table} AS {select_sql}")
    desc = con.sql(f"DESCRIBE {table}").fetchall()
    cols = [r[0] for r in desc]
    types = {r[0]: r[1] for r in desc}
    rows, _schema, lines = sortkey.recommend_sort_key(
        con, "sch", "tbl", table, cols, types, list(partition_cols),
        stats=stats, **kw)
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


# ── 2b. R5 at REAL dimension cardinality: a strict FD still earns no slot. ──────────────────────
# Tests 1 and 2 above profile 300-row tables at ndv 200, where `approx_count_distinct` is
# near-exact — which is precisely why they never caught this. At the cardinality a real date column
# lives at (10³–10⁵) the sketch is wrong by ±10–25%, so the ratio R5 tests against `fd_band` (12%)
# was dominated by its own noise: measured on duckdb 1.5.5 for a TRUE ratio of exactly 1.0, the
# sketched ratio ranged 0.88–1.48. Observed in production — aemo's `year`, joined from a calendar
# dimension on `date` and therefore determined by it, took the second key slot on a real 142M-row
# build (`ORDER BY date, year, time`). HLL is deterministic, so that was systematic: every build of
# that table burned the same slot. `year` here is `year(date)` — an FD by construction, as the real
# one is.
@pytest.mark.parametrize("n_dates", [500, 2_000, 5_000, 20_000, 50_000])
def test_strict_fd_takes_no_slot_at_real_cardinality(n_dates):
    con = duckdb.connect()
    recs, _ = _profile(con, f"""
        select date '2000-01-01' + ((i % {n_dates})::INTEGER) as date,
               year(date '2000-01-01' + ((i % {n_dates})::INTEGER)) as year,
               (i % 24) as hour
        from range(200000) t(i)""")
    key = _key_order(recs)
    assert "year" not in key, f"year is determined by date and must earn no slot; key={key}"
    assert key[:1] == ["date"], f"the date should still lead; key={key}"


def test_genuine_refiner_still_earns_its_slot():
    """Control for the test above: the exact confirm must only DROP columns that are really FD.

    A drop is permanent, so an over-eager R5 silently costs a real clustering dimension — the same
    failure in the other direction. `hour` refines `date` genuinely (24 distinct values inside every
    date, grain stays well under grain_frac·n), so it must survive."""
    con = duckdb.connect()
    recs, _ = _profile(con, """
        select date '2000-01-01' + ((i % 2000)::INTEGER) as date,
               year(date '2000-01-01' + ((i % 2000)::INTEGER)) as year,
               (i % 24) as hour
        from range(200000) t(i)""")
    key = _key_order(recs)
    assert key == ["date", "hour"], f"expected date then hour, got {key}"


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


# ── 9. Uniqueness is claimed on EVERY profile (model v5). ───────────────────────────────────────
# It used to be suppressed whenever the profile was a sample, because a column whose true ndv exceeds
# the sample size saturates to ndv≈n and looks unique. With the whole table profiled there is no such
# failure mode, so the key-organized branch is reachable on a table of any size — where before, with
# a 256 MiB sample budget, it could only ever fire on a small one.
def test_uniqueness_is_claimed_on_every_profile():
    con = duckdb.connect()
    recs, lines = _profile(con, "select i as pk, i * 2 as a, i * 3 as b from range(500) t(i)")
    assert recs["pk"]["is_unique"]
    assert any("key-organized" in ln for ln in lines)


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


# ── 13. Determinism: two profiles of one table are byte-identical, with nothing seeded. ─────────
def test_two_profiles_of_one_table_agree():
    con = duckdb.connect()
    con.execute("CREATE TABLE big AS SELECT (i % 50) a, (i % 7) b, i c FROM range(5000) t(i)")
    cols, types = ["a", "b", "c"], {"a": "BIGINT", "b": "BIGINT", "c": "BIGINT"}
    r1, _, _ = sortkey.recommend_sort_key(con, "s", "big", "big", cols, types, [])
    r2, _, _ = sortkey.recommend_sort_key(con, "s", "big", "big", cols, types, [])
    assert r1 == r2


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


def test_optimize_analyze_is_deterministic(session, capsys):
    session.sql("CREATE OR REPLACE TABLE seeded AS "
                "select (i % 50) a, (i % 7) b, i c from range(4000) t(i)")
    r1 = [tuple(row) for row in session._get_rle("seeded").fetchall()]
    capsys.readouterr()
    r2 = [tuple(row) for row in session._get_rle("seeded").fetchall()]
    assert r1 == r2


# ── 16. Nothing is sampled, anywhere, and reproducibility no longer rests on a seed. ────────────
# These replace the #48 seed-regression trio. That fix made the reservoir `REPEATABLE` because an
# unseeded draw could pick a different key on every build (measured at 8.8% / 700 MB of a 592M-row
# table when a different measure won the last tail slot). Sampling is now gone entirely, so the
# invariant is stronger and simpler: the profilers emit no `USING SAMPLE` at all, and two profiles of
# one table agree because they read the same rows — not because a constant made them.
class _Spy:                         # DuckDBPyConnection attributes are read-only, so proxy it
    def __init__(self, inner):
        self._inner, self.sql_seen = inner, []

    def execute(self, sql, *a, **k):
        self.sql_seen.append(sql)
        return self._inner.execute(sql, *a, **k)

    def sql(self, sql, *a, **k):
        self.sql_seen.append(sql)
        return self._inner.sql(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_profiler_never_samples():
    """The relation seam (`engine.auto_sort_cols`, behind dbt's sort_by='auto')."""
    con = duckdb.connect()
    con.execute("CREATE TABLE wide AS SELECT (i % 50) a, (i % 7) b, i c FROM range(40000) t(i)")
    spy = _Spy(con)
    engine.auto_sort_cols(spy, "wide")
    assert not [s for s in spy.sql_seen if "USING SAMPLE" in s.upper()]


def test_no_profiling_seam_builds_a_sample():
    """The OTHER seam — `session._get_rle`, behind `SORTED BY AUTO` over a table — plus the model
    itself. It and `auto_sort_cols` are separate code, so ripping sampling out of one must not leave
    it in the other, and #48 showed exactly that failure mode (one seam fixed, one missed).

    Asserted on the source rather than by spying the cursor: `_get_rle` hands its connection to
    `engine.delta_column_stats`, which resolves `add_actions` through DuckDB's replacement scan of
    the CALLING frame — a proxy object in between silently breaks that lookup."""
    import inspect
    from duckrun import session as session_mod
    for mod in (session_mod, engine, sortkey):
        src = inspect.getsource(mod)
        offenders = [ln.strip() for ln in src.splitlines()
                     if "USING SAMPLE" in ln.upper() and not ln.strip().startswith(("#", "*"))
                     and "``" not in ln]
        assert not offenders, f"{mod.__name__} still samples: {offenders}"


def test_profile_is_stable_over_an_unordered_source():
    """Two profiles agree even when the source's row ORDER is not stable between reads.

    This is the case the seed could never cover: DuckDB only guarantees `REPEATABLE` at
    `threads=1`, and over a view containing a `GROUP BY` — i.e. any aggregating dbt model — parallel
    hash aggregation emits groups in a different order each time, so a seeded reservoir drew
    different rows anyway. Reading every row is order-insensitive by construction."""
    con = duckdb.connect()
    con.execute("SET threads=8")
    con.execute("SET preserve_insertion_order=false")
    con.execute("CREATE TABLE base AS SELECT (i % 400) g, (i % 7) b, i * 1.5 amt "
                "FROM range(200000) t(i)")
    con.execute("CREATE VIEW agg AS SELECT g, b, sum(amt) amt FROM base GROUP BY g, b")
    k1, _, _ = engine.auto_sort_cols(con, "agg")
    k2, _, _ = engine.auto_sort_cols(con, "agg")
    assert k1 == k2, f"profile of an aggregating source disagreed: {k1} vs {k2}"


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


# ── 17d. The aemo tail, measured directly off every row. ────────────────────────────────────────
# `price` is a regional price — exactly 5 values per (date, time) interval — while `mw` is per-DUID
# (450 per interval). On the real 142M-row mart `date, time, price` measures 596 MB against
# `date, time`'s 779 MB, a 23% saving.
#
# The picker could not see it while it profiled a SAMPLE: 2.6M groups against an 8M-row draw is 3.1
# rows per group, so every measure read as ~unique per group and no tail was ever awarded. That is
# what the group-stratified read of the source existed to recover, and it is why these tests used to
# build a starved sample on purpose. Profiling every row answers the question directly —
# distinct(date, time, price) IS price's run count under the sorted layout — so the slice, its K and
# the run-fraction arithmetic are gone and these tests assert the outcome on the table itself.
def _aemo_shaped(con, days=20, times=288, duids=450):
    """One row per (date, time, DUID); price regional (5 per interval), mw per-DUID."""
    con.execute(f"""create or replace table full_t as
        select date '2024-01-01' + ((d)::INTEGER) as date, (t*5) as time, ('DUID'||u) as DUID,
               ((u*7+d*3+t) % 9973 * 0.11)::DECIMAL(18,4) as mw,
               (((d*288+t)*31 + (u%5)*977) % 4001 * 0.05)::DECIMAL(18,4) as price
        from range({days}) tt(d), range({times}) t2(t), range({duids}) u2(u)""")


def _key_of(con, table, **kw):
    desc = con.sql(f"describe {table}").fetchall()
    rows, _, _ = sortkey.recommend_sort_key(
        con, "m", "t", table, [r[0] for r in desc], {r[0]: str(r[1]) for r in desc}, [], **kw)
    return [r[3] for r in sorted((x for x in rows if x[1]), key=lambda x: x[2])]


def test_measure_tail_awarded_on_a_real_fact_shape():
    """price (5 per interval) earns the tail slot; mw (450 per interval) does not."""
    con = duckdb.connect()
    _aemo_shaped(con)
    assert _key_of(con, "full_t") == ["date", "time", "price"]


def test_measure_tail_refuses_a_prefix_determined_measure():
    """The tail must not just award everything: a measure the dim prefix DETERMINES takes one value
    per group, so it clusters for free under the dim sort and a slot on it orders nothing."""
    con = duckdb.connect()
    _aemo_shaped(con)
    # one price per interval (FD of date,time) instead of five — nothing left for a slot to order.
    con.execute("create or replace table full_t as select date, time, DUID, mw, "
                "(((date - date '2024-01-01')*288 + time) % 4001 * 0.05)::DECIMAL(18,4) as price "
                "from full_t")
    assert "price" not in _key_of(con, "full_t")


def test_profiling_leaves_no_temp_table_behind():
    con = duckdb.connect()
    _aemo_shaped(con)
    _key_of(con, "full_t")
    assert not [r[0] for r in con.sql("show tables").fetchall() if r[0].startswith("_rle")]


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


# ── 9. bytes/row: the byte model behind the one-row-group-per-file write geometry. ──────────────

def _row(dtype, encoding, ndv, est_kb_sorted, column="c"):
    """One profile row in sortkey._SCHEMA order — the shape `bytes_per_row` consumes."""
    return ("sch.tbl", False, 0, column, dtype, encoding, ndv, 0.0, 0, False,
            est_kb_sorted, est_kb_sorted, 0.0)


def test_plain_width_prices_the_type_parquet_actually_writes():
    f = sortkey.plain_width
    assert f("BIGINT") == 8 and f("DOUBLE") == 8 and f("TIMESTAMP") == 8
    assert f("INTEGER") == 4 and f("DATE") == 4 and f("FLOAT") == 4 and f("SMALLINT") == 4
    assert f("HUGEINT") == 16 and f("UUID") == 16
    assert f("BOOLEAN") == 1
    # DECIMAL is priced by precision — the three Parquet physical types it maps onto.
    assert f("DECIMAL(9,2)") == 4 and f("DECIMAL(18,2)") == 8 and f("DECIMAL(38,4)") == 16
    # Variable-width and unrecognised types have no PLAIN ceiling to cap with.
    assert f("VARCHAR") is None and f("BLOB") is None and f("STRUCT(a INT)") is None
    # INTERVAL starts with "INT" but is not a fixed-width number — it must not be priced as one.
    assert f("INTERVAL") is None


def test_plain_width_prices_a_narrowed_decimal_at_its_landed_width():
    # The connection API narrows DECIMAL(p>18,s) -> DECIMAL(18,s) AFTER profiling, so the profile's
    # type string is the 16-byte one while an 8-byte INT64 is what actually lands.
    assert sortkey.plain_width("DECIMAL(38,4)") == 16
    assert sortkey.plain_width("DECIMAL(38,4)", narrow_decimals=True) == 8
    # A scale that leaves no integer digit cannot be narrowed, so it stays 16 either way.
    assert sortkey.plain_width("DECIMAL(38,18)", narrow_decimals=True) == 16


def test_bytes_per_row_charges_a_value_column_for_its_dictionary():
    # THE correction this function exists for. `_col_bytes` prices a unique BIGINT as bit-packed
    # dictionary INDICES and adds nothing for the dictionary itself (_dict_bytes is 0 for value
    # encodings) — 2.75 B/row at 4M rows. Parquet stores the dictionary in the column chunk, so a
    # near-unique column pays for both and the writer falls back to PLAIN. Measured: 6.38 B/row.
    n = 4_000_000
    rows = [_row("BIGINT", "value", n, n * 2.75 / 1024.0)]
    assert sortkey.bytes_per_row(rows, n) == 8.0          # the PLAIN ceiling, not the 2.75 model


def test_bytes_per_row_keeps_a_low_cardinality_value_column_cheap():
    # The dictionary charge must not flatten every value column to PLAIN: a small dictionary is
    # genuinely cheap, so the model's own number survives.
    n = 4_000_000
    rows = [_row("INTEGER", "value", 500, n * 1.125 / 1024.0)]
    bpr = sortkey.bytes_per_row(rows, n)
    assert 1.12 < bpr < 1.13, bpr                          # ~bitpack + 500*4/n, well under PLAIN 4


def test_bytes_per_row_never_exceeds_the_plain_ceiling():
    # An over-modelled column is capped: Parquet cannot spend more than the physical width.
    n = 1_000_000
    rows = [_row("INTEGER", "value", 999_999, n * 50.0 / 1024.0)]
    assert sortkey.bytes_per_row(rows, n) == 4.0


def test_bytes_per_row_leaves_hash_columns_to_the_model():
    # A hash column's dictionary is already charged by _dict_bytes, so it must pass through
    # untouched — and it has no PLAIN width to cap against anyway.
    n = 1_000_000
    rows = [_row("VARCHAR", "hash", 250_000, n * 3.5 / 1024.0)]
    assert abs(sortkey.bytes_per_row(rows, n) - 3.5) < 1e-9


def test_bytes_per_row_is_pure_and_deterministic():
    n = 100_000
    rows = [_row("BIGINT", "value", 1000, 12.0, "a"), _row("VARCHAR", "hash", 50, 30.0, "b")]
    assert sortkey.bytes_per_row(rows, n) == sortkey.bytes_per_row(rows, n)
    assert sortkey.bytes_per_row(list(reversed(rows)), n) == sortkey.bytes_per_row(rows, n)


def test_bytes_per_row_refuses_to_guess_from_an_incomplete_profile():
    # No rows, no row count, or a column carrying no modelled size -> None ("no geometry"), which
    # the engine reads as "change nothing" rather than sizing a write off a hole.
    assert sortkey.bytes_per_row([], 1000) is None
    assert sortkey.bytes_per_row([_row("BIGINT", "value", 10, 1.0)], 0) is None
    assert sortkey.bytes_per_row([_row("BIGINT", "value", 10, None)], 1000) is None


def test_bytes_per_row_over_a_real_profile_is_in_the_right_order_of_magnitude():
    # End to end against a real profile: a 6-column star-schema fact is single-digit bytes/row.
    # Deliberately a band, not a number — the model is calibrated to ~0.5-1.5x (see policy).
    con = duckdb.connect()
    recs, _ = _profile(con, "select (DATE '2020-01-01' + INTERVAL (i%1500) DAY) d, (i%400)::int store, "
                            "(i%9000)::int product, (i%13)::int channel, "
                            "((i%9973)/100.0)::DECIMAL(18,2) amount, (i%97)::int qty "
                            "from range(200000) t(i)")
    rows = [tuple(r[c] for c in _COLS) for r in recs.values()]
    bpr = sortkey.bytes_per_row(rows, 200_000)
    assert 0.5 < bpr < 40.0, bpr


# ── 19. Refactor goldens: the scan-consolidated profiler must return IDENTICAL answers. ──────────
# The profiler's queries were consolidated (widths folded into the NDV scan, per-column skew GROUP
# BYs replaced by chunked GROUPING SETS, near-grain FD confirms prefetched in pairs) purely to cut
# passes over the staged table — measured on a 591.7M-row fact the old shape spent ~49 minutes
# re-reading a 38 GB spill ~25 times. These two fixtures were captured against the PRE-consolidation
# code and pin the full output rows, so any consolidation that changes a single count, byte estimate
# or key decision goes red here. The broader R-rule tests above are the semantic goldens; these are
# the byte-for-byte ones. (Deterministic by construction: HLL and the exact confirms are
# deterministic, so exact float equality is expected; approx() only absorbs FP association noise.)

# Exercises every consolidated path at once: strings + NULLs + an all-NULL column (width merge and
# GROUPING SETS NULL groups), year/month both FD of the leading date (two CONSECUTIVE dim-loop
# confirms at one level), and three near-FD measures (three consecutive tail-loop confirms, all
# refused). 9 low-NDV columns → two GROUPING SETS chunks at the chunk size of 8.
_COMPOSITE_FIXTURE = """
select
  date '2000-01-01' + ((i % 2000)::INTEGER)                        as sale_date,
  year(date '2000-01-01' + ((i % 2000)::INTEGER))                  as sale_year,
  month(date '2000-01-01' + ((i % 2000)::INTEGER))                 as sale_month,
  (i % 24)                                                         as hour,
  case when i % 7 = 0 then null else chr(65 + (i % 5)::INTEGER) end as region,
  cast(null as varchar)                                            as dead,
  (((i % 2000) * 3 + (i % 2)) / 100.0)::DOUBLE                     as price,
  (((i % 24) * 7 + (i % 3)) * 1.5)::DOUBLE                         as amount,
  ((i % 2000) * 100 + (i % 24))::DOUBLE                            as total
from range(200000) t(i)
"""

_COMPOSITE_GOLDEN = [
    ('sch.tbl', True, 1, 'sale_date', 'DATE', 'value', 1600, 0.05, 199900.0, False, 268.6, 7.5, 97.2),
    ('sch.tbl', True, 2, 'region', 'VARCHAR', 'hash', 5, 16.73, 166530.64489, False, 73.2, 10.3, 86.0),
    ('sch.tbl', True, 3, 'hour', 'BIGINT', 'value', 24, 4.17, 191666.66664, False, 122.1, 30.2, 75.3),
    ('sch.tbl', False, 0, 'total', 'DOUBLE', 'value', 5467, 0.02, 199966.66, False, 317.4, 317.4, 0.0),
    ('sch.tbl', False, 0, 'price', 'DOUBLE', 'value', 1978, 0.05, 199900.0, False, 268.6, 268.6, 0.0),
    ('sch.tbl', False, 0, 'amount', 'DOUBLE', 'value', 25, 4.17, 191666.66664, False, 122.1, 122.1, 0.0),
    ('sch.tbl', False, 0, 'sale_month', 'BIGINT', 'value', 13, 8.39, 183211.4, False, 97.7, 97.7, 0.0),
    ('sch.tbl', False, 0, 'sale_year', 'BIGINT', 'value', 6, 17.44, 165124.19999999998, False, 73.2, 73.2, 0.0),
    ('sch.tbl', False, 0, 'dead', 'VARCHAR', 'hash', 0, 100.0, 0.0, False, 0.0, 0.0, 0.0),
]

# A tail slot won THROUGH a confirm: price grows the (date, time) grain by exactly 1.5x — inside
# the _FD_CONFIRM_BELOW zone (so the exact confirm fires) but outside the fd_band (so it survives)
# — and then clears both byte gates. The aemo-shaped tests above award their tails outside the
# confirm zone, so without this fixture the confirm-then-keep path had no golden.
_TAIL_CONFIRM_FIXTURE = """
select date '2024-01-01' + (d)::INTEGER as date, (t*5) as time,
       ((u*7+d*3+t) % 997 * 0.11)::DOUBLE as mw,
       (((d*96+t)*31 % 4001) * 0.05 + case when u % 3 = 0 and t % 2 = 0 then 7.77 else 0 end)::DOUBLE as price
from range(10) tt(d), range(96) t2(t), range(60) u2(u)
"""

_TAIL_CONFIRM_GOLDEN = [
    ('sch.tbl', True, 1, 'date', 'DATE', 'value', 11, 10.0, 51840.0, False, 28.1, 0.0, 99.9),
    ('sch.tbl', True, 2, 'time', 'BIGINT', 'value', 108, 1.04, 57000.0, False, 49.2, 2.6, 94.8),
    ('sch.tbl', True, 3, 'price', 'DOUBLE', 'value', 1657, 0.08, 57553.333333333336, False, 77.3, 4.7, 93.9),
    ('sch.tbl', False, 0, 'mw', 'DOUBLE', 'value', 627, 0.22, 57473.75173611111, False, 70.3, 70.3, 0.0),
]


def _assert_rows_equal(rows, golden):
    assert len(rows) == len(golden), f"row count {len(rows)} != {len(golden)}"
    for actual, expected in zip(rows, golden):
        for a, e in zip(actual, expected):
            if isinstance(e, float):
                assert a == pytest.approx(e, rel=1e-9), (actual, expected)
            else:
                assert a == e, (actual, expected)


def test_consolidated_profile_composite_golden():
    con = duckdb.connect()
    recs, _ = _profile(con, _COMPOSITE_FIXTURE)
    assert _key_order(recs) == ["sale_date", "region", "hour"]
    _assert_rows_equal([tuple(r[c] for c in _COLS) for r in recs.values()], _COMPOSITE_GOLDEN)


def test_consolidated_profile_tail_confirm_golden():
    con = duckdb.connect()
    recs, lines = _profile(con, _TAIL_CONFIRM_FIXTURE)
    assert _key_order(recs) == ["date", "time", "price"]
    assert any("measure tail" in ln and "price" in ln for ln in lines)
    _assert_rows_equal([tuple(r[c] for c in _COLS) for r in recs.values()], _TAIL_CONFIRM_GOLDEN)


def test_skew_sigma_p2_matches_per_column_group_by():
    """The exact Σp² skew must equal the per-column GROUP BY reference for every low-NDV column —
    including NULL-bearing and all-NULL columns, where a grouping-sets rewrite could silently
    diverge (NULL groups as one value, empty set as zero). The reference below IS the
    pre-consolidation query, run by the test itself, so this stays green on either implementation
    only if the numbers agree."""
    con = duckdb.connect()
    recs, _ = _profile(con, _COMPOSITE_FIXTURE)
    n = con.sql("SELECT count(*) FROM t").fetchone()[0]
    for c, r in recs.items():
        s = con.sql(
            f'SELECT COALESCE(SUM(cnt * cnt), 0)::DOUBLE FROM '
            f'(SELECT COUNT(*) AS cnt FROM t GROUP BY "{c}")').fetchone()[0]
        assert r["skew_pct"] == pytest.approx(round(100.0 * s / (n * n), 2)), c
