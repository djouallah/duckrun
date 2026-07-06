# Automatic sort

`CREATE OR REPLACE TABLE <t> SORTED BY AUTO AS SELECT * FROM <t>` picks the sort key **for you**. You
don't pass a column list — duckrun profiles the table (each column's cardinality, skew, null-density,
and functional dependencies), chooses a short key from that, and rewrites every file physically ordered
by it.

```sql
-- auto: profile the table, pick the key, rewrite clustered by it
CREATE OR REPLACE TABLE sales SORTED BY AUTO AS SELECT * FROM sales;

-- or name the key yourself (plain DuckDB CTAS syntax — no AUTO)
CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) AS SELECT * FROM sales;

-- just compact small files, no re-sort
VACUUM sales;
```

Both `SORTED BY` forms do the same physical thing — one global `ORDER BY` streamed back out as new
Delta files in [the parquet layout](parquet-layout.md). The only difference is **who chooses the
columns**: `AUTO` profiles and picks, `(cols)` takes your list. `VACUUM` is the no-sort sibling — it
just bin-packs small files. The rest of this page is about why letting the machine choose is a
genuinely hard problem — and why that matters when you decide whether to trust it.

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
  write out an astronomical number of layouts just to compare them. For a table of any real size
  that is not "slow" — it *never finishes*. And what's at stake is a few percent of disk. Nobody is
  going to spend a compute-week to shave 4% off a Parquet folder.

This is the same shape as other well-known hard clustering/ordering problems: minimizing total runs
across multiple columns by reordering rows generalizes problems that are provably NP-hard, so a
polynomial exact algorithm almost certainly doesn't exist. The honest engineering answer isn't "try
harder" — it's **don't try to be optimal.**

## What the research says — and why its best ideas are too slow

None of this is new. Daniel Lemire and co-authors studied exactly this problem — reordering rows so
run-length compression pays off — in *Sorting improves word-aligned bitmap indexes* (Lemire, Kaser,
Aouiche, 2010). Two of their findings frame the whole trade-off, and both are worth internalizing:

- **Sorting is a huge lever, not a rounding error.** A plain lexicographic sort of the table "can
  divide the index size by 9." Physical row order is one of the biggest compression knobs there is —
  bigger than the choice of codec. This is *why* the feature exists at all.
- **Which columns you sort by, and in what order, is itself a real decision.** Simply permuting the
  columns before sorting changed compression efficiency by about **40%**. The order of columns in the
  key is not cosmetic; it is most of the win.

Now push the second point to its conclusion. If the column order is worth 40%, you'd want the *best*
column order — but there are `n!` of them, and the only faithful way to score one is to sort and
measure. And that's the *easy* version of the problem. The truly optimal layout isn't a column
permutation at all; it's a free reordering of the **rows** to minimize the transitions between
neighbours — the same combinatorial monster from the section above, a cousin of the travelling
salesman problem, NP-hard, no shortcut.

That is exactly why the clever ideas in this literature — Gray-code orderings, Hilbert-curve tuple
orderings, nearest-neighbour (TSP-style) row chaining — are genuinely interesting but **impractical
at warehouse scale**. They reason about *relationships between rows*, which means computing pairwise
distances: quadratic work, or a heavy approximation, over a table that might hold billions of rows.
You do not have unlimited time to reorder stuff. A nightly maintenance job gets **one pass**, not a
week of graph search to shave off a few more percent.

So Lemire's own practical recommendation is the one this code lands on: don't chase the optimum. Do a
single cheap lexicographic sort — `O(n log n)`, affordable every night — and spend your one real
degree of freedom on the **column order**, arranging the key so the lowest-cardinality columns lead.
That captures most of the 9× for almost none of the cost. The heuristic below is that recommendation,
plus a few duckrun-specific rules (lead with a date, stop at the grain, drop measures).

## How the automatic picker chooses instead

`SORTED BY AUTO` gives up on optimal and uses a cheap, greedy heuristic — a stack of rules of thumb,
each of which is *usually* right:

- **A date leads.** One temporal column is given the first key slot ahead of everything else, because
  leading a fact table by its date preserves natural time-clustering and lines up with how these
  tables are queried and refreshed. Only the **coarsest** eligible date gets this — the lowest-
  cardinality one — and a **near-unique** timestamp (so fine it's almost a row id) is *not* allowed
  to lead: it would swallow the whole key and cluster nothing, leaving the real dimensions unsorted.
- **Then ascending cardinality.** The remaining columns are added coarse-to-fine — the classic rule
  that tends to maximize total run length and respects natural hierarchies.
- **Partition columns go outermost, but take no key slot** — Delta strips them from the data files,
  so they carry no compression weight; ordering by them first just keeps ~one writer open at a time.
- **Skip functionally-dependent columns.** If adding a column doesn't grow the current prefix's
  distinct-count (`distinct(X) == distinct(X, c) ⇒ X → c`), the prefix already clusters it for free
  — `year`/`month` behind `date` earn no slot.
- **Stop at the grain.** Once the prefix nearly identifies rows, every group is size 1, there are no
  runs left to make, and further columns can only cluster *worse*. The key stops there, capped at 4.
- **Drop what can't help.** Measures (`DECIMAL`/`FLOAT`/`DOUBLE` you aggregate, never filter on) and
  mostly-null columns are never candidates; unique/near-unique columns are written **PLAIN** because
  a dictionary just re-stores the whole column plus an index.

The result is a short, sensible key. Because it's a heuristic, it is **not guaranteed to shrink
anything** — a near-uniform table or one already organized by a unique key has no runs to make, and
`SORTED BY AUTO` falls back to a plain compaction (the same thing `VACUUM` does). The picker only
optimizes a *model* of the size, so compare `conn.get_stats("sales")` before and after to see the real
change on disk — only the disk knows the truth.

## Pick your columns yourself

To be honest, I know the implementation is naïve and will probably give worse results than, perhaps,
the natural order of a table — but I find it interesting, because it is not an exact science. It is a
heuristic, and you will get better or worse results depending on your data's cardinality.

Having said that — and I am not even joking — it is my personal test for AGI: the day an AI can give
me a good-enough algorithm that returns an optimal sort order (not a row reordering, that is too much
work) in minimal time, I'll know we have AGI 😊. It is not there yet.

## Sources & further reading

- **Daniel Lemire, Owen Kaser, Kamel Aouiche — "Sorting improves word-aligned bitmap indexes"**
  (*Data & Knowledge Engineering* 69(1), 2010). The result this page leans on: physical row order is
  a huge compression lever (a lexicographic sort divided their index size by ~9×), the column order
  within the sort matters a lot (~40%), and a well-chosen cheap lexicographic sort — low-cardinality
  columns first — captures most of the win while the true optimum stays intractable. This is the
  conceptual basis for the column-ordering heuristic here.
  [arXiv:0901.3751](https://arxiv.org/abs/0901.3751) ·
  [author's page](https://lemire.me/en/publication/dke2010/)
- **HyperLogLog** (Flajolet, Fusy, Gandouet, Meunier, 2007). Every cardinality and
  functional-dependency estimate in the picker is a HyperLogLog sketch — DuckDB's
  [`approx_count_distinct`](https://duckdb.org/docs/current/sql/functions/aggregates) — never an exact
  `COUNT(DISTINCT)`, which is what keeps profiling cheap and bounded in memory.
  [HyperLogLog overview](https://en.wikipedia.org/wiki/HyperLogLog)
- The **run-count model** `E[runs] ≈ N·(1 − Σ pᵥ²)` uses the Simpson / Herfindahl index of the value
  histogram — a standard statistical result, not from a single source.
- The **functional-dependency test** `distinct(X) == distinct(X, c) ⇒ X → c` is textbook relational
  database theory.
