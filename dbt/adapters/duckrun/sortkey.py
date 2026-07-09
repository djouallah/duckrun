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
from the sample the caller materialises — seed that sample (the caller threads a ``REPEATABLE``
seed) and this is reproducible.

The recommendation, in order:
  1. sample each column's cardinality (``approx_count_distinct`` HLL — never an exact
     ``COUNT(DISTINCT)``, which was the OOM lever), capped by the Delta-log value range.
  2. per-column skew Σp² (Simpson index) — exact histogram for low-card columns, ``1/ndv`` above.
  3. an in-memory columnar byte model: each column costs ``min(bit-packed, RLE)`` + dictionary.
  4. greedy short key: eligible dimensions in ascending cardinality (one temporal may lead), drop
     functionally-dependent columns (they cluster for free), stop at the table's grain, cap at 4.
  5. the key-organized special case: a (near-)unique key that barely compresses is sorted for
     join/segment locality instead of the marginal compression key.
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
MODEL_VERSION = "2"

# Sample-sizing knobs — NOT the recommendation byte model. A byte budget divided by an estimated
# in-memory row width gives a width-aware row count: a wide table samples fewer rows, a narrow one
# more. These only decide how many rows the caller materializes to profile; they never feed a
# recommendation.
_SAMPLE_BYTE_BUDGET = 256 * 1024 * 1024   # 256 MiB target for the in-memory profiling sample
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


_DECIMAL_RE = re.compile(r"DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE)
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
    p, s = int(m.group(1)), int(m.group(2))
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
                       *, sample_rows, exact):
    """The sort-key model, run against the materialized sample ``src`` on connection ``con``. All
    counts (``n``, ``ndv``, run estimates) are SAMPLE estimates — enough to rank candidates and test
    functional dependencies, not exact table cardinalities.

    Every count is an ``approx_count_distinct`` HLL sketch (fixed KB of state) — NO exact
    ``COUNT(DISTINCT)`` anywhere: that was the OOM lever (an O(NDV·width) hash table per column, over
    every high-card column at once). The functional-dependency ratio in key selection stays reliable
    by hashing both sides (``hash(prefix)`` vs ``hash(prefix, c)``) so HLL's bias cancels. ``stats``
    is the optional Delta-log column profile (``engine.delta_column_stats``): ``null_frac`` drops
    mostly-null columns from candidacy (``null_excl``); ``ndv_cap`` (discrete ``max−min+1``) caps the
    HLL estimate for free. ``None`` (pre-write relation path, or an unreadable log) means "no log
    stats". ``fd_band`` is the near-FD tolerance and ``grain_frac`` the stop-at-grain fraction for key
    selection (step 4). This is all heuristic ranking, not exact science.

    ``sample_rows`` is the number of rows the caller materialized, and ``exact`` says whether that
    sample IS the whole table (the caller knows — it planned the sample). ``exact`` gates the
    uniqueness claim (a sample can't tell a unique key from a merely higher-than-sample column) and
    the "profiled a N-row sample" advisory line.

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
    # Greedy prefix build, fully HLL — NO exact COUNT(DISTINCT) anywhere. Each level issues ONE sample
    # scan: approx_count_distinct(hash(kept_prefix, c)) for EVERY remaining candidate at once (fixed KB
    # of state per sketch, so batching over high-card columns can't OOM). The grain is measured against
    # the ACTUAL kept prefix, and BOTH sides of the FD ratio go through hash() so HLL's bias cancels in
    # the ratio (measured: hash(a)=112, hash(a,b)=111 → 0.99 for a 99%-FD; the unhashed ndv would read
    # 98 vs 111 → 1.13 and miss it). Scans = one per key column kept (≤ sort_key_cap).
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
            if sort_key and runs < kept_ndv * (1.0 + fd_band):
                remaining.remove(c)
                continue
            # Grain stop: c would push the grain past grain_frac·n — it's near the table's own grain,
            # so it behaves like a key, not a clustering dimension (it can't form runs), and since the
            # rank is ascending-cardinality everything after it is finer still. Stop the key here.
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

    # Partition columns lead the physical order, so they end up perfectly grouped (runs = ndv) —
    # and Delta stores them in the path, not the data file. Reflect that clustered state so they
    # don't show a spurious size regression against their already-partitioned current layout.
    for c in partition_cols:
        sorted_runs[c] = ndv[c]
    est_sorted, sorted_total = _bytes_for(sorted_runs)
    pos = {c: i + 1 for i, c in enumerate(sort_key)}
    # a genuine unique key's dictionary is inherent — you cannot "cut" a key's cardinality — so a
    # column flagged dictionary-bound is only the non-key high-card hash columns.
    dict_bound = [c for c in cols if _encoding(types[c]) == "hash" and c not in unique_cols
                  and _dict_bytes(c) > 0.5 * est_sorted[c] and ndv[c] > 0.5 * max(n, 1)]
    # measures aren't sortable-away; a costly one shrinks by cutting precision / splitting the column.
    heavy_measures = [c for c in cols if _is_measure(types[c])
                      and est_sorted[c] > 0.15 * max(sorted_total, 1)]

    def _kb(x):
        return round(x / 1024.0, 1)

    def _saved(cur, new):  # clamp: a column already ~free (in load order) balloons to a silly ratio
        return (max(-999.9, round(100.0 * (cur - new) / cur, 1)) + 0.0) if cur else 0.0

    # R8: partition columns lead the printed ORDER BY (write-locality) but hold no key slot. This
    # module PRINTS NOTHING — it collects the advisory into ``lines`` and the caller prints them
    # (``_get_rle`` prints all; the no-arg ``sort()`` path prints only the ORDER BY line).
    order_cols = partition_cols + [c for c in sort_key if c not in partition_cols]
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
            for c in sort_key + rest]
    return rows, _SCHEMA, lines
