# Experimental: auto optimize (a sort rewrite)

!!! warning "Experimental — opt-in, and no promises"
    `conn.optimize(name, sort="experimental")` is an opt-in, best-effort **sort rewrite**. It
    profiles the table, picks a short sort key, and rewrites every file physically ordered by
    that key so equal values cluster into long runs that compress well. The heuristic and the
    writer tuning may **change between releases**, and it is **not guaranteed to make anything
    smaller** — see [No guarantee of improvement](#no-guarantee-of-improvement). It is a full
    **read → `ORDER BY` → overwrite** (not a bin-packing compaction), so run it **occasionally**
    (after a batch load, not on every write). `forName` tables only — it needs a catalog table
    to profile. Nothing else in the library reaches for this; normal writes stay boring.

## What it does

```python
r = conn.optimize("sales", sort="experimental")
# {'operation': 'sortRewrite', 'sortedBy': ['region', 'order_date'],
#  'sizeBytesBefore': 15_197_312, 'sizeBytesAfter': 6_785_450, 'savedPct': 55.3}
```

The pipeline is: **profile the table → pick a sort key → `ORDER BY` → overwrite → measure.**
The returned `savedPct` is the **real, measured** on-disk change — active-file `size_bytes`
read from the Delta log before and after the rewrite, never an estimate. If no key pays off it
falls back to a plain compaction.

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
contiguous run, so RLE collapses them, dictionary pages stay compact, and the byte compressor
(ZSTD) then has long repetitive sequences to work on. Ordering the rows also lines up row-group
min/max statistics so a reader can skip whole row groups on a filter.

## How the key is chosen

The key picker looks at each column's cardinality (`ndv`), skew, encoding, and type, and assembles
a **short** sort key from the eligible columns:

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

The key picker's internal cost model reasons about a **dictionary + RLE encoded size with no byte
compressor**, but the files are actually written with **ZSTD** on top. So the realized `savedPct`
will not match any modeled estimate — sometimes ZSTD recovers most of what an unsorted layout
"lost", sometimes the sort helps ZSTD a lot. That mismatch is exactly why the number you get back
is **measured from the Delta log**, not predicted. Treat the feature as *try it and measure*, and
keep it for the occasional maintenance pass it's designed for.

## The Parquet layout it writes

On this rewrite — and only on this rewrite — the files are written with a specific, opinionated
writer configuration. delta-rs (the arrow-rs Parquet writer) does the writing; DuckDB only
executes the `ORDER BY` and streams Arrow into it.

| Property | Value | Why |
| --- | --- | --- |
| **Compression** | `ZSTD` level 3 | A meaningfully smaller footprint than Snappy at a low, fast level. On object storage the read is usually **network-I/O-bound**, so smaller files beat marginally-faster decompression. |
| **Row group size** | ~8M rows | The row group is the unit a columnar reader loads and skips as one range. arrow-rs defaults to ~1M — thousands of tiny row groups mean thousands of tiny scan ranges and a bloated footer. ~8M gives fewer, larger ranges; it costs more write-time memory (a full row group is buffered per open writer), which is why it's confined to this opt-in pass. |
| **Dictionary page limit** | 256 MB | The most consequential knob. The arrow-rs default (~1 MB) makes a **wide, repetitive column silently fall back to plain encoding mid-column** once its dictionary grows — the file looks dictionary-encoded in the first pages and isn't after. Raising the limit keeps the whole column dictionary-encoded, which is what keeps a repetitive column small and cheap to read. 256 MB holds any dictionary worth having and still bounds per-column write memory; a column that overflows it was never a good dictionary candidate (drop / hash / truncate it upstream). |
| **Data page size** | 8 MB | Fewer page headers, and run-length runs survive across page boundaries instead of being chopped at ~1 MB. |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level stats just bloat the footer. |
| **Target file size** | ~1 GB | Few large files rather than many small ones — small files are as bad for a columnar reader as small row groups. (Normal writes leave this unset, so an incremental MERGE never has to rewrite a fat 1 GB file.) |
| **Data pages / bloom filters** | v1 pages, bloom off | The maximally-compatible layout; bloom filters aren't used here and only inflate the footer. |

The exact same properties apply on a local path and on any object store (S3 / GCS / ADLS /
OneLake) — storage is neutral.

## What a normal write does instead

For contrast, every non-experimental path — `saveAsTable`, `insertInto`, `merge`, `append`, the
`_if_unchanged` fenced modes, a plain `conn.optimize(...)` compaction, and z-order — writes a
deliberately plain layout: **ZSTD + ~4M-row row groups, and nothing else**, with the file size
left at the delta-rs default. That keeps the hot write paths cheap and keeps an incremental MERGE
from having to rewrite fat files. If you want the sort rewrite's layout, you ask for it explicitly
with `sort="experimental"`.
