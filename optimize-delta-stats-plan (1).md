# Plan: stats-first `optimize(sort='experimental')` for duckrun

Agent spec. Target files: `duckrun/session.py` (`_get_rle`, `_rle_from_sample`, `_RLE_SAMPLE_ROWS`), `duckrun/delta_table.py` (`_optimize_experimental_sort`, `optimize`). Engine helpers live in `dbt.adapters.duckrun.engine` (`_delta_table`, `delta_file_summary`, `write_delta`, `optimize`).

## Goal

Replace the current profile pipeline (full-table reservoir sample + exact `COUNT(DISTINCT)` everywhere) with a three-stage pipeline:

1. **Stats pass** — pure Delta-log arithmetic, zero data-file I/O.
2. **Gate** — skip the rewrite entirely when the table is already clustered.
3. **Sampled profile** — file-level sample, sketch-based NDV, run only for what stats can't answer.

Recommendation semantics, output DataFrame schema, writer path (`optimize_layout=True`, `plain_cols`), and partition-cols-lead ordering are unchanged. This is a cost/robustness rewrite, not a re-scoring of the byte model except where flagged (§5.4).

## Non-goals

- No Z-order. No public API change beyond new optional kwargs.
- Writer *encodings* unchanged (dictionary policy, PLAIN cols, compression); writer *layout* (row group / file sizing) IS in scope — see Stage 4.
- No exact NDV anywhere. If a decision seems to need exactness, redesign the decision (see §5.3, §5.5) — the input is a sample of the table, so exactness is false precision by construction.

---

## Stage 0 — Stats pass (`_delta_stats_profile`)

New private method on the session. Input: resolved table path + storage options.

```python
dt   = engine._delta_table(path, storage_options)
adds = dt.get_add_actions(flatten=True)   # Arrow table
con.register("_adds", adds)
```

Columns available per active file: `path, size_bytes, num_records, null_count.<col>, min.<col>, max.<col>`. Caveats to encode as guards:

- Stats cover only the first `dataSkippingNumIndexedCols` (default 32) columns. A column absent from `min.*` is simply "no stats" — fall through to Stage 2, never error.
- String min/max may be truncated → use only as ordering signals, never as NDV bounds.
- Timestamps lose sub-ms precision; irrelevant for our uses.
- Tables written without stats (some writers) → whole stage degrades to no-op, pipeline still works.

Compute, per column where stats exist:

**S1 — null fraction.** `sum(null_count.c) / sum(num_records)`. If > `null_excl` (default 0.5): mark `excluded: null-heavy` (nulls RLE fine in any order).

**S2 — per-file constancy.** Fraction of files with `min.c == max.c`. If ≥ 0.95: column is ingestion-clustered already; mark `free_rider` — never spend a key slot, it compresses under any prefix sort.

**S3 — NDV cap (discrete types only: int/date/bool).** `ndv_cap = global_max − global_min + 1` (date: days; bool: 2). If per-file ranges are near-disjoint (overlap metric S4 ≈ 1), tighten: `ndv_cap = Σ per-file (max−min+1)` capped at global. Seeds ascending-cardinality ranking before any data is read.

**S4 — clustering metric per candidate.** Average interval-overlap count:

```sql
SELECT avg(cnt) FROM (
  SELECT count(*) cnt FROM _adds a JOIN _adds b
    ON a."min.c" <= b."max.c" AND b."min.c" <= a."max.c"
  GROUP BY a.path)
```

`≈1` → files disjoint on c → table physically sorted by c at file granularity. This replaces the iid assumption for `current_runs`: for a column with overlap `o` over `F` files, estimate current file-level clustering and blend into the run model (see §5.4). Guard: O(F²) join — fine to ~20k files; above that, sort by min and count overlaps with a window scan instead.

**S5 — co-monotonicity (near-FD prescreen).** Spearman rank correlation between per-file `min.a` and `min.b` for all candidate pairs with stats (compute in SQL over `_adds`; F rows, trivial). Pairs with |ρ| ≥ 0.95 → record `b co-clustered with a`; the cheaper-NDV column represents the pair in the key search, the other is marked `free_rider`. This is a prescreen, not a proof — Stage 2's threshold-FD still runs on the survivors.

Output: dict per column `{null_frac, constancy, ndv_cap, overlap, free_rider, has_stats}` + table-level `{num_files, total_rows, partition_cols}`.

## Stage 1 — Gate

In `_optimize_experimental_sort`, after Stage 0 but before any sampling:

1. If a cached recommendation exists (§6) and is valid → use its key, skip to step 3.
2. Else run Stage 2 to get the recommended key.
3. **Clustering gate:** if S4 overlap of the recommended lead column ≤ `gate_overlap` (default 1.5) AND file count / avg row-group shape are already sane (reuse `get_stats` footer logic) → return `{"operation": "sortRewrite", "skipped": "already-clustered", ...}` without rewriting. This is the single largest saving in the whole plan — the rewrite is a full read→sort→overwrite.
4. Else rewrite as today; afterwards run the free validation: recompute S4 on the fresh log; lead-key overlap should be ≈1. Include before/after overlap in the returned metrics next to `savedPct`.

## Stage 2 — Sampled profile (rewrite of `_rle_from_sample`)

### 2.1 Sample acquisition — replace reservoir

Delete `USING SAMPLE {_RLE_SAMPLE_ROWS} ROWS` over `delta_scan` (it scans the entire remote table). Instead:

- From `_adds`, pick `k` files (default: enough to cover ≥ `_RLE_SAMPLE_ROWS` rows, min 4, max 16), **stratified**: order files by `min.<lead-candidate>` (best NDV-cap discrete column, else path) and take evenly spaced picks — kills ingestion-order bias.
- `CREATE TEMP TABLE _rle_src AS SELECT * FROM parquet_scan([files...]) USING SAMPLE ... (system)` only if the picked files exceed 2× the target rows; otherwise read them whole.
- Record `sample_exact = (picked rows == table rows)` — replaces the current `n < _RLE_SAMPLE_ROWS` check.
- Column-pruning: only select candidate columns that survived Stage 0 exclusions plus columns needed for reporting. Do not pull excluded wide strings over the network.

### 2.2 Single-pass sketch profile

One query: `approx_count_distinct(c)` for every surviving column + `COUNT(*)`. No exact `COUNT(DISTINCT)`. Fixed ~KBs of state per aggregate vs O(NDV·width) hash tables — this is the OOM fix, not just the CPU fix. Reconcile with S3: `ndv = min(approx, ndv_cap)` where a cap exists.

### 2.3 Deferred Simpson/width

Compute Simpson `Σp²` (the existing per-column `GROUP BY`) and `avg_width` **only** for: (a) key candidates that survive ranking into the top `2 × sort_key_cap`, (b) hash columns needed for the `dict_bound` report. Everything else gets `skew_pct = NULL` in the output frame (document it).

### 2.4 Key selection — threshold-FD, single pass

Rank candidates as today (R6 temporal thumb, ascending `ndv`, partition cols excluded, measures excluded, `free_rider`s excluded). Then:

1. One query computes all cumulative prefix NDVs for the ranked list: `approx_count_distinct(hash(c1)), approx_count_distinct(hash(c1,c2)), …` up to `min(len, sort_key_cap + 4)`.
2. Run greedy selection arithmetically over the returned row. **R5 replacement:** skip `c` when `approx(prefix + c) < approx(prefix) × (1 + fd_band)`, `fd_band` default 0.12 (measured DuckDB HLL error: ~1% at low NDV, ~9.5% at ~700k NDV on a 1M sample). Semantics upgrade, not just tolerance: a column that grows the grain < 12% is ≥ ~88% determined by the prefix ⇒ near-free under the prefix sort ⇒ correctly skipped. The old exact-equality test missed near-FDs entirely.
3. A skip changes downstream prefixes → issue **one** repair query for the adjusted prefixes. Hard cap: 2 scans total for step 4; if more repairs would be needed, accept the current key (log it).

### 2.5 Marginal-gain fix (byte model)

Current flaw: a candidate's gain is scored `iid_bytes[c] − col_bytes(c, prefix_runs)` — i.e. vs the *unsorted* state. A near-FD column therefore shows a huge gain it would have received anyway by riding the prefix. Fix: score vs riding-the-prefix:

```
gain(c) = col_bytes(c, runs_if_riding) − col_bytes(c, runs_if_in_key)
runs_if_riding ≈ distinct(prefix) × iid_runs_within_group   # or approx(prefix+c) when available
```

With the threshold-FD of §2.4 most such columns are skipped before scoring, so this is a consistency fix; implement it, but keep `min_gain_pct` semantics against the same baseline_total.

### 2.6 Uniqueness — split the two uses

- **PLAIN-encoding flag:** trust the sample. Zero duplicates in the sample ⇒ table NDV ≳ sample scale ⇒ dictionary is wrong regardless of true uniqueness. Flag PLAIN whenever `ndv ≥ 0.9 × n_sample`, independent of `sample_exact`.
- **Key-organized branch (promote PK to sort key):** stays conservative — require `sample_exact`, exactly as today. A saturated INT64 measure must not hijack the key.

## Stage 3 — Writer layout: row group is the contract, not file size

Kill any target-file-size logic (the "~1 GB files" note on `optimize_layout=True`). Rationale: Direct Lake maps one Parquet row group to one VertiPaq segment; transcode parallelism is per column-segment, i.e. per row group across file boundaries. File size is invisible to the engine — it was a proxy for "no confetti row groups," and a bad one (a 1 GB file of 100k-row groups is still confetti). The only real constraint file size imposes is that a row group cannot span files.

Rules for the sortRewrite writer path:

- **Row group target:** 8,388,608 rows (2^23, VertiPaq segment size), adaptive downward: `rg_rows = clamp(1M, mem_budget / avg_row_width_uncompressed, 8M)`, with `avg_row_width` taken from the Stage 2 profile (widths of surviving columns + type widths for the rest). On constrained compute (2-core notebook), a smaller segment beats a spill — the uncompressed row group being assembled is the real OOM lever, and this stack has OOM history. Never go below 1M rows/group (Direct Lake guardrails count segments).
- **File layout:** one row group per file (default). Alignment payoff: file-level min/max in the Delta log == per-segment min/max, so the Stage 0 overlap metric measures segment clustering directly; the rewrite parallelizes at segment granularity; other engines get file skipping at the same boundary VertiPaq transcodes at. Allow `rg_per_file=2` as an escape hatch if file-count guardrails or log size ever bite (they shouldn't: 8M-row groups put SF1000 lineitem at ~750 files).
- **Tail handling:** the final partial group per partition is written as-is; do not rebalance across files to pad it.
- Partitioned tables: the rules apply per partition writer; ordering partition cols first (already the case) keeps ~one writer open at a time, which is what makes the memory budget above meaningful.

## Stage 4 — Cache

After a successful sortRewrite, stamp into the commit's `custom_metadata` (delta-rs `CommitProperties`): `{"duckrun.rle.key": [...], "duckrun.rle.rows": N, "duckrun.rle.schemaHash": h}`. On the next `optimize(sort='experimental')`: reuse the cached key when schema hash matches and current rows < 2× cached rows; else re-profile. Invalidate on any schema evolution.

---

## New/changed signatures

```python
Session._delta_stats_profile(path, storage_options) -> StatsProfile         # new
Session._get_rle(table, sort_key_cap=4, min_gain_pct=1.0,
                 key_sort_below_pct=10.0, fd_band=0.12,
                 null_excl=0.5, stats=None) -> DataFrame                     # extended
DeltaTable._optimize_experimental_sort(force=False) -> Dict                 # gate + cache; force skips both
DeltaTable.optimize(..., sort='experimental', force=False)                  # pass-through
```

Output DataFrame: existing columns unchanged; add `ndv_source` (`'approx'|'cap'|'exact'`), `overlap`, `free_rider`. `skew_pct` becomes nullable.

## Tests (extend `tests/`)

Synthetic tables via DuckDB, written with delta-rs so add-action stats exist:

1. **FD + near-FD:** `category → subcategory` exact FD and a 99%-FD pair. Assert: neither consumes a key slot; free_rider/threshold paths both exercised.
2. **Pre-sorted table:** write sorted by date → gate returns `skipped: already-clustered`; `force=True` rewrites anyway.
3. **Unique-key dimension:** sample_exact case → key-organized branch fires; same table above sample size → it must not.
4. **Saturating INT64 measure:** must not be chosen as key; must be flagged PLAIN.
5. **Partitioned table:** partition cols lead physical order, absent from key, no size regression reported.
6. **No-stats table** (strip stats): entire Stage 0 degrades gracefully, pipeline completes.
7. **Wide table (100 cols):** assert ≤ 6 sample scans total and peak memory bounded (no per-column exact distinct).
8. **Cache:** second optimize call issues zero profile queries; schema change invalidates.
9. **Row-group layout:** rewrite output must have ≥95% of rows in full-target-size row groups, one row group per file (verify via `parquet_metadata()`); with a constrained `mem_budget` the adaptive path must shrink groups instead of spilling, never below 1M rows.
10. **Regression:** on TPC-H `lineitem` SF1 the recommended key must still be a temporal-led low-card prefix (e.g. `l_shipdate, l_returnflag, l_linestatus` modulo ties) and `savedPct` within 2 points of the current implementation.

## Acceptance

- No `COUNT(DISTINCT` remains in the profile path (grep-clean, except tests).
- Remote I/O for profiling: Delta log + k sampled files only — never a full-table scan.
- `optimize(sort='experimental')` on an already-clustered table: no data read beyond the log + footers, no write.
- Total sample-table scans ≤ 6 regardless of column count.
- No file-size target anywhere in the rewrite path; row-group row count is the only layout knob, file boundaries fall out of it.
