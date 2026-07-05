# Experimental: optimize (the safe button and the sort rewrite)

!!! warning "Experimental — opt-in, and no promises"
    `conn.table(name).optimize(...)` covers table maintenance in one method, from a safe
    schedule-it button to an opt-in **sort rewrite**. The sort-rewrite heuristic and the writer
    tuning may **change between releases**, and the rewrite is **not guaranteed to make anything
    smaller** — see [No guarantee of improvement](#no-guarantee-of-improvement). It needs a catalog
    table to profile.

## What it does

`optimize()` is a ladder — the bare call is safe, and you opt into the heavier operations.

```python
# ── Bare: the safe button — compact small files + vacuum, NEVER rewrites data ──────────
conn.table("sales").optimize()
# Compacts only partitions with real small-file debt (a byte trigger), then vacuums.
# Commits dataChange=false, safe under concurrent writers, idempotent, schedule-friendly.
# → {'operation': 'compact', 'filesRemoved': 41, 'filesAdded': 3,
#    'partitionsTouched': ['date=2026-07-04'], 'filesVacuumed': 12,
#    'advice': 'run .optimize(analyze=True) to see whether a sort rewrite would compress …'}
# Nothing to do → {'operation': 'noop', 'reason': 'no small-file debt', 'filesVacuumed': 0}

# ── Deliberate: a sort rewrite — profile, ORDER BY, rewrite in the read layout ─────────
conn.table("sales").optimize(rewrite=True)              # auto-profiled sort key
conn.table("sales").optimize("region", "order_date")   # explicit key
conn.table("sales").optimize("order_date", where="year = 2026")   # scoped to matching partitions
# → {'operation': 'sortRewrite', 'sortedBy': ['region', 'order_date'],
#    'sizeBytesBefore': 15_197_312, 'sizeBytesAfter': 6_785_450, 'savedPct': 55.3,
#    'warning': 'commits as dataChange=true — CDF / streaming consumers will see a full-table change'}

# ── Advisory: the profiler, no writes ─────────────────────────────────────────────────
conn.table("sales").optimize(analyze=True)   # sort-key recommendation DataFrame + small-file debt
```

The bare button never rewrites row data; only `rewrite=True` / an explicit key / `where` do, and
those commit `dataChange=true`. Both rewrite paths are snapshot-fenced (full table →
`overwrite_if_unchanged`, scoped → `replaceWhere`), so a concurrent write fails the rewrite loudly
rather than being clobbered.

Note the other `optimize`: `DeltaTable.forName(conn, name).optimize(...)` is the **plain compaction
/ z-order** operation. The safe button and the sort rewrite live on the table's DataFrame
(`conn.table(name).optimize(...)`).

The sort-rewrite pipeline is: **profile the table → pick a sort key → `ORDER BY` → overwrite →
measure.** The returned `savedPct` is the **real, measured** on-disk change — active-file
`size_bytes` read from the Delta log before and after the rewrite, never an estimate. If no key
pays off it falls back to a plain compaction.

## It's a plain global `ORDER BY` — nothing clever

The "sort" is exactly what it sounds like: **one SQL `ORDER BY` over the entire table**, end to
end. Concretely, the rewrite is

```sql
SELECT * FROM delta_scan('…/sales') ORDER BY "region", "order_date"
```

streamed back out as new Delta files. That means:

- It is a **global** ordering across the whole dataset — row 1 of file 1 through the last row of
  the last file are in one continuous sorted sequence. It is **not** a per-file or per-row-group
  local reordering, and it is **not** z-order / space-filling-curve interleaving. Just `ORDER BY`.
- Every existing file is **read and rewritten** (that's why it's a full overwrite, not a
  bin-pack), so the whole table is re-laid-out in a single new Delta version.
- The only decision the feature makes is **which** columns go in the `ORDER BY` and in what
  order. That choice is the one "smart" part, and it's covered below.

## Why a sort shrinks files

A columnar file stores each column independently. For a low-cardinality column, two encodings
compete: **bit-packed dictionary indices** (`ceil(log2 ndv)` bits per row) and **run-length
encoding** (RLE — one entry per contiguous run of equal values). RLE only wins when values
arrive in long runs.

In arbitrary physical order, the number of runs a column breaks into is governed by its value
**skew**:

```
E[runs] ≈ N · (1 − Σ p_v²)
```

where `Σ p_v²` is the Simpson index of the value histogram (the probability two random rows share
a value). A near-uniform column (`Σ p_v² ≈ 1/ndv`) shatters into ≈N runs and falls back to
bit-packing; a skewed column already RLEs well in almost any order.

A global sort **manufactures** those runs: order by a column and its equal values become one
contiguous run, so RLE collapses them and dictionary pages stay compact. This matters more here
than under a heavy compressor: the files are written with **SNAPPY** (chosen for fast cold-load
decode into a columnar reader), which is a light codec — it won't rescue an unsorted layout the way
a maximum-ratio compressor might, so the clustering the sort creates is what does the shrinking.
Ordering the rows also lines up row-group min/max statistics so a reader can skip whole row groups
on a filter.

## How the key is chosen

The key picker looks at each column's cardinality (`ndv`), skew, encoding, type, and Delta-log
statistics, and assembles a **short** sort key from the eligible columns:

- **Partition columns lead** the physical order but take **no key slot** — Delta strips them from
  the data files, so they carry no compression weight; leading with them just keeps roughly one
  partition writer open at a time (less write memory).
- Columns are added in **ascending cardinality** (the classic rule). This also respects natural
  hierarchies — a coarse `date` leads the finer `time` nested within it rather than being
  stranded behind it.
- A **temporal column is favoured to lead** when it actually clusters.
- **Measures** — `DECIMAL` / `FLOAT` / `DOUBLE` values you aggregate but never filter or sort on
  — are **excluded**. Sorting a fact by a measure just scrambles the dimensions queries filter on;
  you shrink a heavy measure by cutting precision or splitting the column, not by sorting.
- **Mostly-null columns are dropped** from the key. A column that is almost entirely NULL already
  clusters for free (nulls RLE in any order), so it would waste a key slot — the profiler detects
  it from the Delta log's null-count statistics without reading the data.
- **Unique / near-unique** columns (`ndv ≥ 0.9·N`) get **no dictionary** — every value is
  distinct, so a dictionary just re-stores the whole column plus an index. Those are written
  **PLAIN**.
- **Key-organized tables** (a dimension, or a fact already at its grain) are a special case: if a
  (near-)unique key exists and sorting for compression barely helps, the recommendation is simply
  `ORDER BY <key>` — the sensible join / segment-locality layout — because a unique key leaves
  nothing for RLE to group.

## Why the key stays short

The key is capped at **4 columns**, and usually ends up shorter. Two rules keep it from bloating,
and both come straight from the profiler:

- **Correlated / functionally-dependent columns don't earn a second slot.** The test is the
  standard `distinct(X) == distinct(X, c) ⇒ X → c`: if adding a column to the current key prefix
  doesn't grow the prefix's distinct-count, that column is **already determined by the prefix**,
  so sorting the prefix already clusters it for free. Take `date`, `year`, `month`: once the key
  orders by `date`, the `year` and `month` columns are already in perfect runs — adding them
  changes nothing, so they are skipped. Sorting by `year, month, date` is no better than `date`
  alone, and it burns slots a genuinely independent column could have used. Same story for
  `category → subcategory`.
- **Past the grain there is nothing left to sort.** A column is added only while it still forms
  meaningfully fewer runs than N at its position. Once the key prefix reaches the table's **grain**
  — the prefix uniquely identifies rows — every group is size 1, there are no runs left to
  cluster, and further columns can't compress anything. The key stops there (or at the 4-column
  cap). A long arbitrary key clusters *worse*, not better.
- **Sometimes there is nothing to sort at all.** A near-uniform table (no skew for RLE to exploit)
  or a table whose only structure is a unique key yields no paying key; the rewrite then **falls
  back to a plain compaction** instead of a pointless global sort.

## No guarantee of improvement

Every dataset is different, and this can legitimately save nothing:

- If values are **near-uniform** (`Σ p_v² ≈ 1/ndv`) or the table is organized by a **(near-)unique
  key**, RLE has nothing to group and savings approach zero.
- If the data is **already roughly ordered** (e.g. loaded in date order), a sort by that same key
  changes little.
- **Skew, cardinality, existing physical order, and partitioning** all move the result, and they
  interact — there is no way to know the outcome without running it.

The key picker's internal cost model reasons about a **dictionary + RLE encoded size**, but the
files are actually written with **SNAPPY** on top. So the realized `savedPct` will not match any
modeled estimate. That mismatch is exactly why the number you get back is **measured from the Delta
log**, not predicted. Treat the feature as *try it and measure*, and keep it for the occasional
maintenance pass it's designed for.

## The read layout it writes

This is the **one** writer configuration duckrun uses for **every file write** — `saveAsTable`,
`insertInto`, `save`, the `_if_unchanged` fenced modes, `replaceWhere`, the post-write compaction,
and this sort rewrite. (MERGE is the sole exception — see below.) The sort rewrite differs from a
plain write only in that it (a) physically orders the rows first and (b) writes genuinely-unique
columns PLAIN. delta-rs (the arrow-rs Parquet writer) does the writing; DuckDB only executes the
`ORDER BY` and streams Arrow into it.

| Property | Value | Why |
| --- | --- | --- |
| **Compression** | `SNAPPY` | Fast to decode, which is what a columnar reader (Direct Lake) wants on cold load — it transcodes the Parquet dictionaries into its own encoding. SNAPPY runs only ~1.3× the size of ZSTD on real data; the sort and the dictionary do the real shrinking, so the codec's job here is decode speed, not maximum ratio. |
| **Row group size** | 6M rows | One Parquet row group maps to one column segment in a Direct Lake reader, and Fabric wants segments in the **1–16M** row band. 6M sits mid-band while bounding write-time memory (arrow-rs buffers a full uncompressed row group per open writer). arrow-rs's ~1M default would mean thousands of tiny segments. |
| **Dictionary page limit** | 8 MB | Caps how big a column's dictionary grows before it overflows to **PLAIN**. Mid-cardinality columns (dict < 8 MB) stay dictionary-encoded — small, cheap, and a columnar reader remaps the dictionary directly. High-cardinality / unique columns overflow to PLAIN, which is *correct*: their dictionary would be as big as the data (no compression), pure overhead. This bound is **load-bearing for MERGE** — a large limit (128 MB) kept high-card columns dictionary-encoded and made a merge *reading* the table materialize ~25 GB of dictionaries; 8 MB keeps that ~4 GB. |
| **Data page size** | 1 MB, **20k-row cap** | Bounded pages. The row-count cap is the real safeguard: without it, a highly compressible column buffers its whole row group as a single page — ~10× write memory and giant pages that blow a reader's memory ([arrow-rs #5797](https://github.com/apache/arrow-rs/issues/5797)). |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level stats just bloat the footer. Long strings are truncated in the stats. |
| **Target file size** | 256 MB | A row group can't span files, so this byte cap is really a segment cap — 256 MB lets a wide fact reach a full 6M-row segment, one row group per file, giving large uniform segments. It's deliberately far below **1 GB**, which forced a whole-file copy-on-write that blew up merges on disk. (With the dictionary bounded, file size is no longer a merge-memory lever — 128/256/512 MB all merge the same.) |

The exact same properties apply on a local path and on any object store (S3 / GCS / ADLS /
OneLake) — storage is neutral.

## The one exception: MERGE

`MERGE` (raw-SQL `MERGE`, `DeltaTable.forName(...).merge(...)`, and dbt incremental merges) writes
with **none** of the above — no writer properties, no target file size — so an incremental merge
stays quick, never rewrites fat files, and never materializes giant dictionaries just to update a
few rows. `DELETE` and `UPDATE` are likewise lean. The threshold-gated post-write **compaction**
then folds the files those operations leave behind up into the read layout on a later pass, off the
merge's critical path. So you still converge on the read layout — you just don't pay for it inside
every merge.
