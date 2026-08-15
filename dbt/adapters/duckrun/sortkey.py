"""The sort-key recommender — the one heuristic in duckrun.

Everything else in this codebase is deterministic plumbing over DuckDB and delta-rs. This file is
the exception: given a Delta table (as a materialized sample plus its Delta-log statistics) it
*decides* a short physical sort key that should minimise the table's in-memory columnar footprint.
There is no exact, checkable answer — the optimal ordering is an NP-hard search — so this is a fast
model-and-rank heuristic, not an optimiser. It is the only place in the repo where "a better
algorithm" is a meaningful ask, which is exactly why it lives on its own.

Input  → a DuckDB connection + the name of a temp table holding a random sample of the Delta table,
         the column list/types, the partition columns, and optional Delta-log column stats.
Output → ``(rows, schema, lines)``: one row per column describing the recommendation
         (``in_sort_key`` / ``sort_position`` are the decision; the rest is the profile that
         justifies it), and ``lines`` — the human-readable advisory the CALLER prints (this module
         does no I/O). The caller wraps ``rows`` into a DataFrame.

The model is a *deterministic function of the sample statistics*; the only run-to-run variance comes
from the sample the caller materialises. That sample is therefore seeded by default
(``DEFAULT_SAMPLE_SEED``, threaded as ``REPEATABLE`` by every caller), so the same table profiled
twice returns the same key.

The recommendation, in order:
  1. sample each column's cardinality (``approx_count_distinct`` HLL — never the BATCHED exact
     ``COUNT(DISTINCT)`` over every column at once, which was the OOM lever), capped by the
     Delta-log value range.
  2. per-column skew Σp² (Simpson index) — exact histogram for low-card columns, ``1/ndv`` above.
  3. an in-memory columnar byte model: each column costs ``min(bit-packed, RLE)`` + dictionary.
  4. greedy short key: eligible dimensions in ascending cardinality (one temporal may lead), drop
     functionally-dependent columns (they cluster for free — screened on the sketch, confirmed
     exactly near the grain, see ``_FD_CONFIRM_BELOW``), stop at the table's grain, cap at 4.
  5. the key-organized special case: a (near-)unique key that barely compresses is sorted for
     join/segment locality instead of the marginal compression key.
  6. measure TAIL slots: below the dim key, greedily append the measures whose in-group ordering
     pays real modeled bytes — correlated/zero-heavy measures collapse into runs there (probe:
     tests/parquet_layout/nyc — ~62% of a run-minimizing writer's money-column win, from ORDER BY
     alone). Tails refine order INSIDE each dim group, so the dims queries filter on are untouched.
"""
import math
import re

# Above this distinct-value count the exact Σp² histogram (a GROUP BY = O(NDV) hash table) is skipped
# for the uniform approximation Σp² ≈ 1/ndv — a high-card column is ~uniform and its runs are
# ndv-bound, so the skew term barely moves, and this keeps the skew pass from rebuilding an OOM.
_SKEW_EXACT_NDV = 100_000

# MODEL_VERSION: bump on ANY change to an R-rule, a threshold default, or the byte model. Pure
# plumbing — sample sizing, the explicit exact flag, seeding, returning lines instead of printing —
# does NOT bump it. Appended to the recommendation header so a recommendation is attributable to the
# model that produced it, making future threshold churn traceable.
# v3: measure tail slots (step 6) — measures may now FOLLOW the dim key, never hold a dim slot.
# v4: R5 decides on EXACT counts inside the noise zone (see _FD_CONFIRM_BELOW) — the HLL ratio it
#     used to trust is worth ±25% at the cardinalities real date columns live at. Step 6 measures
#     in-group cardinality on whole GROUPS read from the source when the sample is too thin per
#     group to see any (see _GROUP_STARVED_FRAC) — it was awarding no tail at all on a real fact.
MODEL_VERSION = "4"

# R5's FD ratio SCREENS on HLL and DECIDES on exact counts below this multiple of the kept grain.
# `approx_count_distinct` is a sketch, and at the NDV where real dimensions live (10²–10⁵) it is
# wrong by ±10–25% — measured on duckdb 1.5.5 against a column and a strict FD of it (true ratio
# EXACTLY 1.0): the sketched ratio ranged 0.88–1.48 across date cardinalities 500…50k, so the 12%
# `fd_band` sat entirely inside the noise. The observed miss: aemo's `year` (a calendar join on
# `date`, so determined by it by construction) took the second key slot on a real 142M-row build.
# HLL is deterministic, so this was systematic, not flaky — the same table wasted the same slot on
# every build. A candidate the sketch says does not even ~double the grain is therefore unresolved,
# not FD, and earns one exact confirming scan; above 2.0 the sketch's error cannot flip the verdict
# and it is trusted as-is. This is NOT the exact COUNT(DISTINCT) that was the OOM lever: that
# batched every high-card column at once, this is one column at a time, only near the grain, over a
# sample already byte-budgeted by `plan_sample`.
_FD_CONFIRM_BELOW = 2.0

# A tail slot must pay for itself twice over: save at least ``tail_gain_frac`` of the measure's own
# modeled bytes (an in-group sort that barely moves a column isn't worth a longer key) AND at least
# this fraction of the whole table's modeled bytes (a near-empty column can pass the relative test
# on savings nobody would ever notice — nyc mta_tax/improvement_surcharge are the measured case).
_TAIL_MIN_TABLE_FRAC = 0.005

# A tail slot asks "how many distinct values does this measure take INSIDE one dim group" — and a
# uniform ROW sample cannot answer that once the dim prefix is fine enough, because it leaves only a
# handful of rows in each group. Measured on the real aemo mart: 142M rows, ~2.6M (date, time)
# groups, an 8M-row sample ⇒ 3.1 sampled rows per group, so EVERY measure looks ~unique per group and
# the tail awards nothing. `price` (a regional price — exactly 5 values per interval) was the case:
# `date, time, price` measures 596 MB against `date, time`'s 779 MB, a 23% saving the picker could
# not see. At that density the uniform sample reads price at 2.68 distinct/group vs mw's 3.63 (both
# saturating the 3.1 rows available); taking WHOLE groups instead reads 5.00 vs 450.00 — the truth.
# So when groups get denser than this fraction of the sample, in-group counts come from a
# GROUP-stratified read of the source (all rows of a 1-in-K slice of groups) rather than the sample.
_GROUP_STARVED_FRAC = 0.05
# Rows to aim for in that stratified read. Whole groups, so it is a hash slice of the SOURCE, not a
# row sample: on the aemo mart K lands ~70 and the read touches ~1.4% of the table, one pass, once
# per profile. Materialized to a temp table so every tail level regroups it locally.
_STRAT_TARGET_ROWS = 2_000_000

# Sample-sizing knobs — NOT the recommendation byte model. A byte budget divided by an estimated
# in-memory row width gives a width-aware row count: a wide table samples fewer rows, a narrow one
# more. These only decide how many rows the caller materializes to profile; they never feed a
# recommendation.
_SAMPLE_BYTE_BUDGET = 256 * 1024 * 1024   # 256 MiB target for the in-memory profiling sample
# The REPEATABLE seed every caller threads into the profiling sample unless it asks for another one.
# `recommend_sort_key` is a deterministic function of that sample, so an UNSEEDED sample is the only
# thing that can make two runs of ONE config choose different keys — and it did: four benchmark runs
# on one adapter version over an identical 592M-row table split 3/1 on which measure won the last
# tail slot, an 8.8% (700 MB) spread from sample luck alone. A constant is enough; the value is
# arbitrary and only its FIXEDNESS matters, so it is not configurable. Sizing only — this never
# feeds the recommendation, so it is not a MODEL_VERSION concern.
DEFAULT_SAMPLE_SEED = 0
_DECOMPRESSION_FACTOR = 3.0               # parquet-on-disk → in-memory expansion, a rule of thumb
# In-memory byte width per column type, for the relation path (no Delta log → no real file sizes).
# Crude on purpose; VARCHAR/BLOB/INTERVAL/structured/unknown fall through to 24. Sizing only.
_TYPE_WIDTHS = (
    ("BOOL", 1), ("TINYINT", 1), ("UTINYINT", 1), ("SMALLINT", 2), ("USMALLINT", 2),
    ("INTEGER", 4), ("UINTEGER", 4), ("BIGINT", 8), ("UBIGINT", 8), ("HUGEINT", 16),
    ("UHUGEINT", 16), ("FLOAT", 4), ("REAL", 4), ("DOUBLE", 8), ("DECIMAL", 8), ("NUMERIC", 8),
    ("DATE", 4), ("TIMESTAMP", 8), ("TIME", 8), ("UUID", 16))

_SCHEMA = (
    "table string, in_sort_key boolean, sort_position int, column string, data_type string, "
    "encoding string, ndv bigint, skew_pct double, current_runs bigint, is_unique boolean, "
    "est_kb_current double, est_kb_sorted double, saved_pct double")


def _qid(name: str) -> str:
    """Quote a SQL identifier (schema/table/view/column name)."""
    return '"' + str(name).replace('"', '""') + '"'


# Accepts the NUMERIC alias and a scale-less DECIMAL(p) (scale 0) besides the canonical
# DECIMAL(p,s) — DuckDB's DESCRIBE normalizes to the latter, but type strings from user config
# or a Delta schema arrive in any of the three spellings.
_DECIMAL_RE = re.compile(r"(?:DECIMAL|NUMERIC)\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", re.IGNORECASE)
# DECIMAL(p, s) with p <= 18 fits INT64; p > 18 forces a 16-byte FIXED_LEN_BYTE_ARRAY, which
# arrow-rs (the delta-rs writer) NEVER dictionary-encodes — such a column is written PLAIN even
# when its value domain is tiny. Narrowing precision back to 18 (scale unchanged) restores INT64,
# and with it dictionary/RLE encoding and a cheap transcode.
_DECIMAL_NARROW_PRECISION = 18


def decimal_narrow_target(type_str, max_abs):
    """Target type for narrowing a wide DECIMAL so it regains dictionary encoding, or ``None``.

    Returns ``"DECIMAL(18,s)"`` iff ``type_str`` is ``DECIMAL(p,s)`` with ``p > 18``, ``s <= 17``
    (so at least one integer digit remains), and the column's true ``max_abs`` fits — i.e.
    ``max_abs < 10**(18 - s)``. A ``None`` ``max_abs`` (all-NULL column) trivially fits. Scale is
    never changed; only precision. Pure — no DB, no I/O — so it is unit-testable directly.

    ``max_abs`` MUST be the exact column maximum (a single aggregate scan), not a sample: the cast
    is unconditional at write time, so a sampled max that missed an outlier would fail the whole
    write. With the exact max there is no overflow risk and no headroom heuristic is needed."""
    m = _DECIMAL_RE.fullmatch(str(type_str).strip())
    if not m:
        return None
    p, s = int(m.group(1)), int(m.group(2) or 0)
    if p <= _DECIMAL_NARROW_PRECISION or s > _DECIMAL_NARROW_PRECISION - 1:
        return None
    if max_abs is not None and abs(max_abs) >= 10 ** (_DECIMAL_NARROW_PRECISION - s):
        return None
    return f"DECIMAL({_DECIMAL_NARROW_PRECISION},{s})"


def plan_sample(total_rows, avg_row_bytes, *, byte_budget=_SAMPLE_BYTE_BUDGET,
                min_rows=100_000, max_rows=8_000_000):
    """Rows to materialize for profiling, and whether that profile will be EXACT (the whole table).

    ``rows = clamp(byte_budget // max(avg_row_bytes, 1), min_rows, max_rows)`` — a byte budget over
    an estimated in-memory row width, so a wide table samples fewer rows and a narrow one more.
    ``exact`` is True only when the table is known to fit within that many rows (then ``rows``
    collapses to ``total_rows`` and the caller skips sampling entirely — no ``USING SAMPLE`` at all).
    ``total_rows=None`` (an unknown size, e.g. a derived relation) is never exact. Pure — no I/O."""
    rows = int(byte_budget // max(avg_row_bytes, 1))
    rows = max(min_rows, min(rows, max_rows))
    if total_rows is not None and total_rows <= rows:
        return int(total_rows), True
    return rows, False


def estimate_row_bytes(types):
    """A crude in-memory row width (bytes) from the schema ALONE — no data read. Used only to size
    the profiling sample on the relation path, where there is no Delta log to give real file sizes.
    Value types map to their fixed width; VARCHAR/BLOB/INTERVAL/anything unknown to 24. Crude on
    purpose — the point is that a 300-column frame no longer samples like a 5-column one. This never
    feeds the recommendation byte model, so it is not a ``MODEL_VERSION`` concern. ``types`` may be a
    ``{col: type}`` mapping or an iterable of type strings."""
    total = 0.0
    for t in (types.values() if isinstance(types, dict) else types):
        u = str(t).upper()
        for prefix, width in _TYPE_WIDTHS:
            if u.startswith(prefix):
                total += width
                break
        else:
            total += 24.0   # VARCHAR / BLOB / INTERVAL / structured / unknown
    return total


def recommend_sort_key(con, sch, tbl, src, cols, types, partition_cols,
                       sort_key_cap=4, min_gain_pct=1.0, key_sort_below_pct=10.0,
                       stats=None, null_excl=0.5, fd_band=0.12, grain_frac=0.5,
                       *, sample_rows, exact, tail_cap=4, tail_gain_frac=0.25,
                       full_src=None, total_rows=None):
    """The sort-key model, run against the materialized sample ``src`` on connection ``con``. All
    counts (``n``, ``ndv``, run estimates) are SAMPLE estimates — enough to rank candidates and test
    functional dependencies, not exact table cardinalities.

    Every count is an ``approx_count_distinct`` HLL sketch (fixed KB of state) — never a batched
    exact ``COUNT(DISTINCT)`` over every high-card column at once, which was the OOM lever (an
    O(NDV·width) hash table per column, all live together). The functional-dependency ratio is the
    one place that is not settled on a sketch: hashing both sides does NOT cancel HLL's error (the
    two sides are different value domains with independent bias), and at real dimension
    cardinalities that error dwarfs ``fd_band`` — so R5 screens on the sketch and confirms a
    near-grain candidate with one exact scan for that column alone (see ``_FD_CONFIRM_BELOW``). ``stats``
    is the optional Delta-log column profile (``engine.delta_column_stats``): ``null_frac`` drops
    mostly-null columns from candidacy (``null_excl``); ``ndv_cap`` (discrete ``max−min+1``) caps the
    HLL estimate for free. ``None`` (pre-write relation path, or an unreadable log) means "no log
    stats". ``fd_band`` is the near-FD tolerance and ``grain_frac`` the stop-at-grain fraction for key
    selection (step 4). This is all heuristic ranking, not exact science.

    ``full_src`` (a FROM-able spelling of the table the sample was drawn from) and ``total_rows``
    are optional and used ONLY by the measure tail (step 6): together they let it read whole dim
    GROUPS from the source when the sample has too few rows per group to see in-group structure at
    all (see ``_GROUP_STARVED_FRAC``). Omit either and the tail falls back to sample-only counts —
    correct on a table whose groups the sample covers densely, blind to a tail slot on one whose
    groups it does not.

    ``sample_rows`` is the number of rows the caller materialized, and ``exact`` says whether that
    sample IS the whole table (the caller knows — it planned the sample). ``exact`` gates the
    uniqueness claim (a sample can't tell a unique key from a merely higher-than-sample column) and
    the "profiled a N-row sample" advisory line. ``tail_cap`` bounds the measure tail (step 6) and
    ``tail_gain_frac`` is its keep-the-slot relative byte gate — tail slots are IN ADDITION to
    ``sort_key_cap``, which stays a dim-only budget.

    Returns ``(rows, schema, lines)`` — one row per column, ``schema`` a DuckDB DDL string, and
    ``lines`` the advisory text for the CALLER to print (this module prints nothing)."""
    stats = stats or {}
    # Exact NDV upper bound per discrete column, straight from the Delta log (zero data read) —
    # approx_count_distinct can overshoot, and a value range caps it exactly.
    ndv_cap = {c: stats[c]["ndv_cap"] for c in cols
               if c in stats and stats[c].get("ndv_cap")}
    # 1) sample ndv per column, one pass, with HLL sketches. NOTE: a random sample has no physical
    # row order, so the table's *actual* current run count can't be measured. current_runs is set
    # below (once skew is known) to the iid / arbitrary-order estimate — the honest neutral for an
    # unknown layout, which matches a freshly-appended unsorted table and drives "does sorting help?".
    agg_sel = ", ".join(
        f"approx_count_distinct({_qid(c)}) AS n{i}" for i, c in enumerate(cols))
    row = con.sql(f"SELECT {agg_sel}, COUNT(*) AS total FROM {src}").fetchone()
    n = row[-1] or 0
    ndv = {}
    for i, c in enumerate(cols):
        v = int(row[i] or 0)
        cap = ndv_cap.get(c)
        ndv[c] = min(v, cap) if cap is not None else v

    # value-encoded = numeric/temporal (no dictionary); hash = strings/blobs (dictionary of ndv
    # distinct values). An in-memory engine may force hash for relationship columns too, but we can't see that.
    def _encoding(t):
        t = t.upper()
        # INTERVAL starts with "INT" but is NOT a value-encoded fixed-width number — it is a
        # duration, ineligible as a key (see _is_interval). Guard it before the "INT" prefix so it is
        # never misclassified as value-encoded; treat it as hash if it ever reaches the byte model.
        if t.startswith("INTERVAL"):
            return "hash"
        return "value" if t.startswith((
            "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT", "INT", "UINT", "BIGINT", "UBIGINT",
            "HUGEINT", "UHUGEINT", "BOOL", "FLOAT", "DOUBLE", "REAL", "DEC", "NUMERIC",
            "DATE", "TIME", "TIMESTAMP")) else "hash"

    # A continuous/additive **measure** (DECIMAL/FLOAT/DOUBLE) is an output you aggregate, not a key
    # you organise by: no query filters an exact price, and sorting a fact by a measure just scrambles
    # the dimensions that queries DO filter. So measures are ineligible as sort-key columns — you
    # shrink them by cutting precision / splitting, not by sorting. (Integer/temporal columns stay
    # eligible: an INT can be a real dimension key, e.g. a HHMM time-of-day.)
    def _is_measure(t):
        return t.upper().startswith(("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL"))

    def _is_temporal(t):
        return t.upper().startswith(("DATE", "TIME", "TIMESTAMP"))

    # An INTERVAL is a duration/offset, not a filterable dimension: no query filters an exact
    # duration and sorting by one clusters nothing queries actually filter — so, like a measure, it
    # is ineligible as a sort-key column (excluded from candidacy in _elig below).
    def _is_interval(t):
        return t.upper().startswith("INTERVAL")

    # 2) per-column skew term Σp_v² (Simpson index, from the value histogram) and, for hash
    # columns, average serialised value width (drives dictionary cost).
    simpson, avg_width = {}, {}
    hash_cols = [c for c in cols if _encoding(types[c]) == "hash"]
    if hash_cols:
        wsel = ", ".join(
            f"avg(octet_length(encode({_qid(c)}::VARCHAR))) AS w{j}"
            for j, c in enumerate(hash_cols))
        wr = con.sql(f"SELECT {wsel} FROM {src}").fetchone()
        avg_width = {c: (wr[j] or 1.0) for j, c in enumerate(hash_cols)}
    # The exact Σp² is a GROUP BY that materialises one row per distinct value — an O(NDV) hash table,
    # the same OOM lever the exact COUNT(DISTINCT) was. Skew only matters (and the histogram is only
    # cheap) for LOW-cardinality columns; a high-card column is ~uniform, so Σp² ≈ 1/ndv (its runs are
    # ndv-bound anyway). Cap the exact histogram at _SKEW_EXACT_NDV distinct values; approximate above.
    for c in cols:
        if ndv[c] > _SKEW_EXACT_NDV:
            simpson[c] = 1.0 / ndv[c] if ndv[c] else 1.0
            continue
        s = con.sql(
            f"SELECT COALESCE(SUM(cnt * cnt), 0)::DOUBLE FROM "
            f"(SELECT COUNT(*) AS cnt FROM {src} GROUP BY {_qid(c)})").fetchone()[0]
        simpson[c] = (s / (n * n)) if n else 1.0

    # 3) in-memory columnar byte model. A column stores min(bit-packed indices, RLE runs) + a dictionary
    # (hash only). RLE run entry ≈ one index (ceil(log2 ndv) bits) + a run length (up to N).
    cnt_bits = max(1, math.ceil(math.log2(n))) if n > 1 else 1

    def _bits(k):
        return max(1, math.ceil(math.log2(k))) if k and k > 1 else 1

    def _dict_bytes(c):
        return 0.0 if _encoding(types[c]) == "value" else ndv[c] * avg_width.get(c, 1.0)

    def _iid_runs(c):  # runs of a column left in ~arbitrary order (skew-governed)
        return min(float(n), max(float(ndv[c]), n * (1.0 - simpson[c])))

    # current layout is unknown from a sample → assume arbitrary (iid) order. For an unsorted table
    # this matches the real physical runs; it drives comp_saved (how much sorting would help) below.
    current_runs = {c: _iid_runs(c) for c in cols}

    def _col_bytes(c, runs):
        b = _bits(ndv[c])
        bitpack = n * b / 8.0
        rle = runs * (b + cnt_bits) / 8.0
        return min(bitpack, rle) + _dict_bytes(c)

    # 4) short sort key. Candidates are the eligible dimensions/keys only (a constant sorts nothing;
    # a measure is never a key), taken in ASCENDING cardinality — the classic rule, which also
    # respects natural hierarchies: a coarse column (date) leads the finer one nested within it
    # (time), so a currently-free coarse column is never stranded behind a higher-card column for a
    # marginal byte win. Each column's runs at its position = exact distinct(prefix incl. it); its
    # own values scatter across every prefix group, so this is >= ndv (the cap only holds at
    # position 1). Keep adding while the next column still compresses AND actually refines the grain
    # (R5); once the prefix reaches the grain everything after is shredded, so stop.
    iid_bytes = {c: _col_bytes(c, _iid_runs(c)) for c in cols}
    baseline_total = sum(iid_bytes.values())
    # S1 — a mostly-null column (Delta-log null share > null_excl) is dropped from candidacy: its
    # nulls already collapse to one run under any order, so a key slot on it clusters little of value
    # and crowds out a denser dimension. Log-only signal; absent stats → empty set → no change.
    null_heavy = {c for c in cols
                  if stats and c in stats and stats[c]["null_frac"] > null_excl}
    # R8: partition columns are excluded as candidates — they lead the ORDER BY (below) but carry no
    # RLE value once Delta strips them from the files; a measure / constant / null-heavy column is out.
    # S2 — near-constant guard: ndv > 1 is not enough. A column with ndv=3 where one value holds
    # 99% of rows (a refresh watermark, a status flag) is a constant for sorting purposes —
    # effective cardinality 1/Σp² ≈ 1. Sorting it clusters nothing and steals a key slot from a
    # real dimension (Simpson index is already in simpson[c], populated in step 2 above).
    def _elig(c):
        eff = (1.0 / simpson[c]) if simpson.get(c) else float(ndv[c])
        return (ndv[c] > 1 and eff >= 1.5
                and not _is_measure(types[c]) and not _is_interval(types[c])
                and c not in partition_cols and c not in null_heavy)
    # R6: ONE non-(near-)unique temporal leads the key — on a fact table, leading with the date keeps
    # natural clustering and incremental framing. Only the single coarsest date gets the tier-0 thumb
    # (DATE-typed first, then lowest ndv, ties by schema order); the OTHER dates fall back to plain
    # ascending cardinality, so the low-card dimensions queries actually filter on aren't stranded
    # behind them. A temporal too fine to survive the grain stop (a raw microsecond timestamp is
    # ~unique — real NYC-taxi tpep_pickup_datetime is ndv≈0.7·n) is NOT lead-eligible: promoting it
    # would grain-stop the very first pick and leave an EMPTY key, when the low-card dims are the key.
    temporals = [c for c in cols if _elig(c) and _is_temporal(types[c])
                 and not (n and ndv[c] >= grain_frac * n)]
    # DATE-typed columns outrank TIMESTAMP-typed for the tier-0 lead: the business calendar is a DATE;
    # a low-ndv TIMESTAMP is almost always an audit/watermark column, and "lowest ndv" alone lets it
    # hijack the lead (observed: a 3-value refresh watermark beat a 3000-value date, shredding date
    # clustering). Tier by type, then ndv, then schema order.
    lead_temporal = min(temporals, key=lambda c: (
        0 if types[c].upper().startswith("DATE") else 1,
        ndv[c], cols.index(c))) if temporals else None
    candidates = sorted(
        (c for c in cols if _elig(c)),
        key=lambda c: (0 if c == lead_temporal else 1, ndv[c]))
    # Greedy prefix build. Each level issues ONE batched sample scan:
    # approx_count_distinct(hash(kept_prefix, c)) for EVERY remaining candidate at once (fixed KB of
    # state per sketch, so batching over high-card columns can't OOM). That sketch RANKS and SCREENS;
    # it does not settle R5 — below _FD_CONFIRM_BELOW × the grain the sketch's own error exceeds
    # fd_band, so the candidate gets one exact confirming scan for itself alone. Scans = one batched
    # per key column kept (≤ sort_key_cap), plus at most one exact per candidate examined near grain.
    def _exact_pair(prefix_cols, c):
        """Exact (distinct(prefix), distinct(prefix, c)) over the sample — the FD verdict's inputs.

        Both sides hashed exactly as the sketch spells them, so the confirm answers the same question
        the screen asked. One column at a time and only near the grain: the OOM lever was the BATCHED
        exact count over every high-card column simultaneously, not a single bounded one."""
        pfx = ", ".join(_qid(x) for x in prefix_cols)
        row = con.sql(f"SELECT count(DISTINCT hash({pfx})), "
                      f"count(DISTINCT hash({pfx}, {_qid(c)})) FROM {src}").fetchone()
        return int(row[0] or 1), int(row[1] or 1)

    sort_key, sorted_runs = [], {}
    remaining = list(candidates)  # ranked (one temporal lead, then ascending ndv), consumed as decided
    kept_ndv = 1                  # grain of the kept prefix (empty prefix → 1)
    while remaining and len(sort_key) < sort_key_cap:
        pfx = ", ".join(_qid(x) for x in sort_key)
        sel = ", ".join(
            f"approx_count_distinct(hash({pfx + ', ' if pfx else ''}{_qid(c)})) AS m{j}"
            for j, c in enumerate(remaining))
        mrow = con.sql(f"SELECT {sel} FROM {src}").fetchone()
        marg = {c: max(int(mrow[j] or 0), kept_ndv) for j, c in enumerate(remaining)}
        chosen, stop = None, False
        for c in list(remaining):
            runs = marg[c]
            # R5 — threshold functional dependency: adding c grows the grain by less than fd_band ⇒ c
            # is ≥ ~(1−fd_band) determined by the prefix (year ← date; subcategory ← category, and now
            # a 99%-near-FD too). Clustered for free by the prefix sort ⇒ no key slot; a later
            # independent column may still refine. (The old exact-equality test missed near-FDs.)
            #
            # The sketched `runs` only SCREENS here: inside the noise zone its error is several times
            # fd_band, which is how a strict FD (aemo `year` ← `date`) read as independent and took a
            # slot. Confirm exactly, then decide — and adopt the exact grain for the levels below, so
            # a sketch's error can't compound down the prefix.
            if sort_key and runs < _FD_CONFIRM_BELOW * kept_ndv:
                kept_ndv, runs = _exact_pair(sort_key, c)
                marg[c] = runs
            if sort_key and runs < kept_ndv * (1.0 + fd_band):
                remaining.remove(c)
                continue
            # Grain stop: c would push the grain past grain_frac·n — it's near the table's own grain,
            # so it behaves like a key, not a clustering dimension (it can't form runs), and since the
            # rank is ascending-cardinality everything after it is finer still. Stop the key here.
            # Left on the sketch deliberately: grain_frac·n is a coarse half-the-table threshold, so
            # sketch error cannot realistically flip it the way it flips the tight fd_band ratio.
            if runs >= grain_frac * n:
                stop = True
                break
            chosen = c
            break
        if stop or chosen is None:
            break
        sort_key.append(chosen)
        sorted_runs[chosen] = marg[chosen]
        kept_ndv = marg[chosen]
        remaining.remove(chosen)

    # 5) assemble. "current" uses current_runs — the iid / arbitrary-order estimate, because a random
    # sample has no physical order to measure (see step 1); it is the honest neutral for an unknown
    # layout. A column in the key uses its prefix runs; everything else its iid estimate.
    est_current = {c: _col_bytes(c, current_runs[c]) for c in cols}
    current_total = sum(est_current.values())

    def _bytes_for(key_runs):
        est = {c: (_col_bytes(c, key_runs[c]) if c in key_runs else iid_bytes[c]) for c in cols}
        return est, sum(est.values())

    # If the table has a (near-)unique KEY and the compression sort barely helps, it is
    # key-organized (a dimension, or a table at its grain): the sensible physical layout is ORDER BY
    # the key (join / segment locality, stable refresh), NOT the marginal compression sort — a unique
    # key leaves nothing for RLE to group, so compression is already at its floor. When sorting *does*
    # compress meaningfully (a real fact) we keep the compression key.
    _, comp_total = _bytes_for(sorted_runs)
    comp_saved = 100.0 * (current_total - comp_total) / current_total if current_total else 0.0
    # is_unique can't be judged from a sample: any column whose true ndv exceeds the sample size
    # saturates to ndv≈n and looks unique, so a high-cardinality measure (an INT64 price) would be
    # falsely flagged and could hijack the key-organized branch below as the sort key. Only trust
    # uniqueness when the profile was EXACT — the caller sampled nothing and handed us the whole
    # table (``exact`` is an explicit argument now: the caller planned the sample and KNOWS whether
    # it covered the table, rather than us inferring it from ``n < sample_rows``, which conflated "the
    # sample covered the table" with "the table is smaller than a constant"). When it truly sampled,
    # claim no unique column — the conservative direction: fall back to the compression key.
    unique_cols = ([c for c in cols if n and ndv[c] >= 0.9 * n and c not in partition_cols]
                   if exact else [])
    note = None
    if unique_cols and comp_saved < key_sort_below_pct:
        pk, comp_alt = unique_cols[0], list(sort_key)  # schema-order first unique col (usually the PK)
        sort_key, sorted_runs = [pk], {pk: ndv[pk]}     # unique key → runs = ndv, no RLE to be had
        note = (f"key-organized (unique key '{pk}') — sorted for join/segment locality; compression "
                f"is at its floor" + (f", best-effort compression sort {', '.join(comp_alt)} only "
                f"~{comp_saved:.1f}%" if comp_alt else ""))

    # 6) measure TAIL slots (probe-backed: tests/parquet_layout/nyc). Within the kept dim prefix,
    # ordering rows BY the measures themselves collapses correlated / zero-heavy measures into runs
    # (nyc: fare/tip/tolls + the near-FD total_amount — ~62% of a run-minimizing writer's
    # money-column advantage, from plain ORDER BY). R7 stands: a measure never holds a DIM slot —
    # a tail slot sits BELOW every dim, refining order inside each dim group, so nothing queries
    # filter on is disturbed and the grain stop above is unaffected. Greedy by MARGINAL modeled
    # byte saving at the growing tail prefix, one batched HLL scan per level exactly like the dim
    # loop — marginal credit is what keeps a wide measure (nyc trip_distance) from grabbing the
    # first tail slot and fragmenting the runs of every measure after it. R5's FD band applies to
    # tails too, against the TAIL prefix's grain: a measure (near-)determined by the prefix
    # (amount ← region) already clusters for free under the dim sort — a slot on it orders nothing
    # and its iid-relative "saving" belongs to the dims, not the slot. A candidate that clears the
    # band but stays near-FD (nyc total_amount ≈ fare+tip+tolls) still earns a slot by breaking
    # the prefix's remaining ties. A candidate whose in-group runs stay ~iid (the dim prefix is
    # already at the table's grain) models a ~zero saving and is never kept — the byte gate is the
    # grain stop here. Key-organized tables skip tails entirely: a unique key leaves no groups.
    tail = []
    if sort_key and note is None:
        t_remaining = [c for c in cols
                       if _is_measure(types[c]) and ndv[c] > 1
                       and c not in null_heavy and c not in partition_cols]
        t_grain = kept_ndv                    # grain of the kept dim prefix (dim loop invariant)
        t_prefix = list(sort_key)
        # Group-stratified read of the SOURCE, when the sample is too thin per dim group to measure
        # in-group structure (see _GROUP_STARVED_FRAC). Takes ALL rows of a 1-in-K hash slice of
        # groups — whole groups are the point; a row sample of the same size answers the wrong
        # question. Materialized once here so every tail level regroups it locally instead of
        # re-reading the source. Best-effort: any failure leaves strat None and the tail falls back
        # to the sample-only counts it used before.
        strat = None
        if full_src and total_rows and kept_ndv > _GROUP_STARVED_FRAC * n:
            k = max(1, -(-int(total_rows) // _STRAT_TARGET_ROWS))
            gpfx = ", ".join(_qid(x) for x in sort_key)
            # Only the columns the tail actually groups by or counts — a fact table is mostly columns
            # this phase never looks at, and materializing them would be the memory cost this whole
            # path is supposed to avoid.
            need = ", ".join(_qid(x) for x in list(dict.fromkeys(sort_key + t_remaining)))
            try:
                con.execute(f"CREATE OR REPLACE TEMP TABLE _rle_strat AS SELECT {need} "
                            f"FROM {full_src} WHERE hash({gpfx}) % {k} = 0")
                if con.sql("SELECT count(*) FROM _rle_strat").fetchone()[0] > 0:
                    strat = "_rle_strat"
            except Exception:
                strat = None

        def _run_fracs(prefix_cols, candidates):
            """Run count as a FRACTION of rows for each candidate, ordered by ``prefix_cols`` then
            itself — plus that same fraction for the bare prefix, read from ``strat``.

            A fraction, not a count, because the byte model works in SAMPLE units while ``strat``
            holds whole groups from the SOURCE: the two have different row counts and a raw count
            would not survive the trip (an earlier cut multiplied group count by per-group distincts
            and saturated at n, hiding the very slot this exists to find). ``strat`` holds every row
            of a 1-in-K slice of groups, so ``Σ distinct-per-group / Σ rows`` is exactly the sorted
            layout's run fraction and **K cancels** — no extrapolation anywhere.

            aemo: price → 5 values in each of 5,760 intervals over 2.59M rows ⇒ ~1.1%, against an
            iid ~100%. mw → 450 per interval ⇒ 100%, no saving, no slot. That is the whole
            discrimination the sample could not make."""
            gb = ", ".join(_qid(x) for x in prefix_cols)
            inner = ", ".join(f"count(DISTINCT {_qid(c)}) AS g{j}" for j, c in enumerate(candidates))
            sel = ", ".join(f"sum(g{j})::DOUBLE / nullif(sum(k), 0) AS f{j}"
                            for j in range(len(candidates)))
            row = con.sql(f"SELECT {sel}, count(*)::DOUBLE / nullif(sum(k), 0) "
                          f"FROM (SELECT {inner}, count(*) AS k FROM {strat} "
                          f"GROUP BY {gb})").fetchone()
            return [float(v) if v is not None else 1.0 for v in row[:-1]], float(row[-1] or 1.0)

        while t_remaining and len(tail) < tail_cap:
            pfx = ", ".join(_qid(x) for x in t_prefix)
            scan = list(t_remaining)          # trow is indexed by THIS order; t_remaining mutates
            sel = ", ".join(
                f"approx_count_distinct(hash({pfx}, {_qid(c)})) AS t{j}"
                for j, c in enumerate(scan))
            trow = con.sql(f"SELECT {sel} FROM {src}").fetchone()
            # Where a stratified read exists, take runs from the layout's true run FRACTION and
            # scale it into the sample units the byte model speaks. `pfrac` is the bare prefix's own
            # fraction — the floor a measure hits when the prefix already determines it.
            fracs, pfrac = _run_fracs(t_prefix, scan) if strat else (None, None)
            best, best_runs, best_save = None, None, 0.0
            for j, c in enumerate(scan):
                runs = max(int(trow[j] or 0), 1)
                # FD of the prefix → clusters for free, permanently out (FD survives prefix growth).
                if fracs:
                    # The stratified read answers the FD question directly: a measure the prefix
                    # determines takes ONE value inside every group, so its run fraction collapses
                    # onto the prefix's own. No sketch, no confirm — and no clobbering these numbers
                    # with sample counts that cannot see in-group structure in the first place.
                    if fracs[j] < pfrac * (1.0 + fd_band):
                        t_remaining.remove(c)
                        continue
                    runs = max(1, min(int(n), int(round(n * fracs[j]))))
                else:
                    # Sample-only path: same screen-then-confirm as the dim loop, because the sketch
                    # cannot resolve fd_band near the grain, and here a false FD reading drops the
                    # candidate PERMANENTLY — it could bin a measure that deserved a tail slot.
                    if runs < _FD_CONFIRM_BELOW * t_grain:
                        t_grain, runs = _exact_pair(t_prefix, c)
                    if runs < t_grain * (1.0 + fd_band):
                        t_remaining.remove(c)
                        continue
                save = iid_bytes[c] - _col_bytes(c, runs)
                if save > best_save:
                    best, best_runs, best_save = c, runs, save
            if (best is None
                    or best_save < tail_gain_frac * iid_bytes[best]
                    or best_save < _TAIL_MIN_TABLE_FRAC * baseline_total):
                break
            tail.append(best)
            sorted_runs[best] = best_runs
            t_grain = best_runs
            t_prefix.append(best)
            t_remaining.remove(best)
        if strat:   # connection-local temp table; this module owns it, so this module clears it
            con.execute("DROP TABLE IF EXISTS _rle_strat")

    # Partition columns lead the physical order, so they end up perfectly grouped (runs = ndv) —
    # and Delta stores them in the path, not the data file. Reflect that clustered state so they
    # don't show a spurious size regression against their already-partitioned current layout.
    for c in partition_cols:
        sorted_runs[c] = ndv[c]
    est_sorted, sorted_total = _bytes_for(sorted_runs)
    pos = {c: i + 1 for i, c in enumerate(sort_key + tail)}
    # a genuine unique key's dictionary is inherent — you cannot "cut" a key's cardinality — so a
    # column flagged dictionary-bound is only the non-key high-card hash columns.
    dict_bound = [c for c in cols if _encoding(types[c]) == "hash" and c not in unique_cols
                  and _dict_bytes(c) > 0.5 * est_sorted[c] and ndv[c] > 0.5 * max(n, 1)]
    # a measure the tail didn't rescue isn't sortable-away; a costly one shrinks by cutting
    # precision / splitting the column. Tail members are excluded — their slot IS the fix.
    heavy_measures = [c for c in cols if _is_measure(types[c]) and c not in tail
                      and est_sorted[c] > 0.15 * max(sorted_total, 1)]

    def _kb(x):
        return round(x / 1024.0, 1)

    def _saved(cur, new):  # clamp: a column already ~free (in load order) balloons to a silly ratio
        return (max(-999.9, round(100.0 * (cur - new) / cur, 1)) + 0.0) if cur else 0.0

    # R8: partition columns lead the printed ORDER BY (write-locality) but hold no key slot. This
    # module PRINTS NOTHING — it collects the advisory into ``lines`` and the caller prints them
    # (``_get_rle`` prints all; the no-arg ``sort()`` path prints only the ORDER BY line).
    order_cols = partition_cols + [c for c in sort_key + tail if c not in partition_cols]
    lines = [f"\nrecommend_sort_key('{sch}.{tbl}') — sort-key recommendation (experimental) "
             f"[model v{MODEL_VERSION}]:"]
    if not exact:
        lines.append(f"  (profiled a {sample_rows:,}-row sample — ndv/skew/runs are estimates)")
    lines.append(f"  ORDER BY {', '.join(order_cols) if order_cols else '(no key pays off)'}")
    if partition_cols:
        lines.append(f"  (partition columns lead the sort but carry no compression weight: "
                     f"{', '.join(partition_cols)})")
    # Deliberately NO projected-size line: this only profiles, it doesn't rewrite, so any "sorted
    # size" would be a model estimate — and an estimate that reads like a measurement is worse than
    # none. The real before/after bytes come from actually rewriting the table
    # (CREATE OR REPLACE TABLE t SORTED BY AUTO AS SELECT * FROM t) and measuring via the Delta log.
    if note:
        lines.append(f"  ({note})")
    if tail:
        lines.append(f"  (measure tail — in-group ordering below the dim key, dims untouched: "
                     f"{', '.join(tail)})")
    if dict_bound:
        lines.append(f"  (dictionary-bound — sort won't help, cut cardinality: {', '.join(dict_bound)})")
    if heavy_measures:
        lines.append(f"  (measures — not sortable; cut precision / split to shrink: "
                     f"{', '.join(heavy_measures)})")
    if null_heavy:
        lines.append(f"  (null-heavy — excluded from the sort key (>{null_excl:.0%} null): "
                     f"{', '.join(sorted(null_heavy))})")

    rest = sorted((c for c in cols if c not in pos), key=lambda c: -est_current[c])
    unique_set = set(unique_cols)  # ndv >= 0.9*n (non-partition) — a dictionary buys nothing here
    rows = [(f"{sch}.{tbl}", c in pos, pos.get(c, 0), c, types[c], _encoding(types[c]), ndv[c],
             round(100.0 * simpson[c], 2), current_runs[c], c in unique_set, _kb(est_current[c]),
             _kb(est_sorted[c]), _saved(est_current[c], est_sorted[c]))
            for c in sort_key + tail + rest]
    return rows, _SCHEMA, lines
