# Automatic sort

`conn.table(name).optimize(rewrite=True)` picks the sort key **for you**. You don't pass a column
list — it profiles the table as it stands (the cardinality, skew, null-density and functional
dependencies it reads from the Delta-log statistics plus a sample of the rows), chooses a short
key from that, and rewrites every file physically ordered by it.

```python
conn.table("sales").optimize(rewrite=True)            # auto: profile, pick the key, rewrite
conn.table("sales").optimize("region", "order_date")  # or say the key yourself
conn.table("sales").optimize(analyze=True)            # just show me what you'd pick, don't write
```

Both forms do the same physical thing — one global `ORDER BY` streamed back out as new Delta files
in [the parquet layout](parquet-layout.md). The only difference is **who chooses the columns**. The rest
of this page is about why letting the machine choose is a genuinely hard problem — and why that
matters when you decide whether to trust it.

## It's just a global `ORDER BY`

The "sort" is exactly what it sounds like: **one SQL `ORDER BY` over the entire table**, end to
end:

```sql
SELECT * FROM delta_scan('…/sales') ORDER BY "region", "order_date"
```

It is a **global** ordering — row 1 of file 1 through the last row of the last file are one
continuous sorted sequence, not a per-file reordering and not z-order interleaving. Every existing
file is read and rewritten, so the whole table is re-laid-out in a single new Delta version. The
only decision the feature makes is **which columns go in that `ORDER BY`, and in what order** — and
that decision is the whole ballgame.

## Why a sort shrinks files

A columnar file stores each column independently. For a low-cardinality column, two encodings
compete: **bit-packed dictionary indices** (`ceil(log2 ndv)` bits per row) and **run-length
encoding** (RLE — one entry per contiguous run of equal values). RLE only wins when values arrive
in long runs.

In arbitrary physical order, the number of runs a column breaks into is governed by its value
**skew**:

```
E[runs] ≈ N · (1 − Σ p_v²)
```

where `Σ p_v²` is the Simpson index of the value histogram (the chance two random rows share a
value). A near-uniform column shatters into ≈N runs and falls back to bit-packing; a skewed column
already RLEs well in almost any order. A global sort **manufactures** runs: order by a column and
its equal values become one contiguous run, so RLE collapses them and the dictionary pages stay
compact. Ordering the rows also lines up row-group min/max statistics so a reader can skip whole
row groups on a filter.

So far so simple — for **one** column. The trouble starts when you have forty.

## Why picking the best key is hard

Choosing the sort order that makes a table smallest is not a tidy optimization with a clean answer.
It is **combinatorially hard — NP-hard in general** — and it stays hard no matter how much compute
you throw at it. Three things stack up:

- **The search space is superexponential.** The key isn't "which columns" — it's *which columns, in
  which order*. For a table with `n` columns you're choosing an **ordered subset**: `n` first-column
  choices, times `n−1` second-column choices, and so on. That's `Σ_k n!/(n−k)!` candidate keys —
  it blows past a million by the time you have ten columns. You cannot enumerate them.
- **You can't score a candidate without building it.** There is no closed form for "how many bytes
  will this ordering compress to." A column's encoded size depends on its **run structure**, which
  depends on the run structure of *every column ahead of it in the key* — the columns interact
  through correlation and functional dependency (sorting by `date` silently clusters `month` and
  `year`; sorting by `city` half-clusters `country`). The only faithful way to know a candidate's
  size is to actually sort and write the whole table that way and measure it. One evaluation is a
  full-table rewrite.
- **Superexponential candidates × a full rewrite each = hours to days.** An exact optimizer would
  materialize an astronomical number of layouts just to compare them. For a table of any real size
  that is not "slow," it is *never finishes*. And what's at stake is a few percent of disk. Nobody
  is going to spend a compute-week to shave 4% off a Parquet folder.

This is the same shape as other well-known hard clustering/ordering problems: minimizing total runs
across multiple columns by reordering rows generalizes problems that are provably NP-hard, so a
polynomial exact algorithm almost certainly doesn't exist. The honest engineering answer isn't "try
harder" — it's **don't try to be optimal.**

## How the automatic picker chooses instead

`rewrite=True` gives up on optimal and uses a cheap, greedy heuristic that runs in one profiling
pass over a sample. It's a stack of rules of thumb, each of which is *usually* right:

- **Ascending cardinality.** Add columns coarse-to-fine — the classic rule that tends to maximize
  total run length. This also respects natural hierarchies (a coarse `date` leads the finer `time`
  nested within it).
- **Partition columns lead but take no key slot** — Delta strips them from the data files, so they
  carry no compression weight; leading with them just keeps ~one partition writer open at a time.
- **Skip functionally-dependent columns.** If adding a column doesn't grow the current prefix's
  distinct-count (`distinct(X) == distinct(X, c) ⇒ X → c`), the prefix already clusters it for free
  — `year`/`month` behind `date` earn no slot.
- **Stop at the grain.** Once the prefix uniquely identifies rows, every group is size 1, there are
  no runs left to make, and further columns can only cluster *worse*. Cap the key at 4 columns.
- **Drop what can't help.** Measures (`DECIMAL`/`FLOAT`/`DOUBLE` you aggregate, never filter on) and
  mostly-null columns are excluded; unique/near-unique columns (`ndv ≥ 0.9·N`) are written **PLAIN**
  because a dictionary just re-stores the whole column plus an index.

The result is a short, sensible key computed in seconds instead of a provably-optimal key computed
never. Because it's a heuristic, it is **not guaranteed to shrink anything** — a near-uniform table
or one already organized by a unique key has no runs to make, and the rewrite falls back to a plain
compaction. That's also why the `savedPct` you get back is **measured from the Delta log** after the
rewrite, not predicted: the picker optimizes a model of the size; only the disk knows the truth.

## Pick your columns yourself

To be honest, I know the implementation is naïve and will probably give worse results than, perhaps,
the natural order of a table — but I find it interesting, because it is not an exact science. It is a
heuristic, and you will get better or worse results depending on your data's cardinality.

Having said that — and I am not even joking — it is my personal test for AGI: the day an AI can give
me a good-enough algorithm that returns an optimal sort order (not a row reordering, that is too much
work) in minimal time, I'll know we have AGI 😊. It is not there yet.
