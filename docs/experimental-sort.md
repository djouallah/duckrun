# Experimental: sort rewrite

!!! danger "Experimental, theoretical — and not a guarantee"
    The sort rewrite is a **best-effort heuristic, not an optimizer.** Finding the row ordering that
    truly minimizes a table's on-disk compressed size is a **combinatorial, NP-hard** search: the row
    orderings are factorial in the data, the *column* order inside the key matters too, and the only
    way to know any candidate's real compressed size is to actually write it out and measure it.
    duckrun does **not** attempt that search — nobody wants to wait hours for a provably-optimal
    layout. Instead it profiles a **sample**, applies a handful of cheap rules of thumb to pick a
    **short** column sort key, and rewrites **once**.

    Everything below is the *theory* those rules lean on — read it as motivation, not a promise. The
    heuristic and the writer tuning may **change between releases**, the rewrite is **not guaranteed
    to make anything smaller** (see [No guarantee of improvement](#no-guarantee-of-improvement)), and
    the only number worth trusting is the **measured** `savedPct` the rewrite returns — read from the
    Delta log before and after, never an estimate.

To run it, see [`optimize` in the Connection API](connection-api.md#optimize):
`conn.table(name).optimize(rewrite=True)` (auto key), `.optimize("region", "order_date")` (explicit
key), `.optimize("order_date", where="year = 2026")` (scoped), or `.optimize(analyze=True)` (just the
recommendation, no write). It needs a catalog table to profile, and it lands in
[the read layout](read-layout.md) like every other write.

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
  order. That choice is the one "smart" part, and it's the heuristic covered below.

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
decode into a columnar reader — see [the read layout](read-layout.md)), which is a light codec — it
won't rescue an unsorted layout the way a maximum-ratio compressor might, so the clustering the sort
creates is what does the shrinking. Ordering the rows also lines up row-group min/max statistics so
a reader can skip whole row groups on a filter.

## How the key is chosen (the heuristic)

This is the "smart" part — and it is a **heuristic**, deliberately cheap. The key picker looks at
each column's cardinality (`ndv`), skew, encoding, type, and Delta-log statistics, and assembles a
**short** sort key from the eligible columns:

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

This bears repeating, because the heuristic makes no promise. Every dataset is different, and a sort
rewrite can legitimately save nothing:

- If values are **near-uniform** (`Σ p_v² ≈ 1/ndv`) or the table is organized by a **(near-)unique
  key**, RLE has nothing to group and savings approach zero.
- If the data is **already roughly ordered** (e.g. loaded in date order), a sort by that same key
  changes little.
- **Skew, cardinality, existing physical order, and partitioning** all move the result, and they
  interact — there is no way to know the outcome without running it.

The key picker's internal cost model reasons about a **dictionary + RLE encoded size**, but the
files are actually written with **SNAPPY** on top. So the realized `savedPct` will not match any
modeled estimate — which is exactly why the number you get back is **measured from the Delta log**,
not predicted. Treat the feature as *try it and measure*: run `.optimize(analyze=True)` to see what
the profiler recommends, run the rewrite if it looks worthwhile, and keep it for the occasional
maintenance pass it's designed for — not every write.
