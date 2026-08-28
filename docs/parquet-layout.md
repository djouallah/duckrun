# Parquet layout

Two things determine how a duckrun table sits on disk: the **physical file format** every write emits, and the **row order** a sort lays down inside it. This page documents both.

Before anything else, one caveat that applies to the entire page: **every value and every rule described here is a heuristic.** Compression is a function of your data's cardinality, skew, and correlation structure, and those differ from table to table and change over time within the same table. The settings below are defaults that worked well across the workloads duckrun was tuned against; they are starting points, not guarantees. Where a rule can fail, this page says so.

- [Parquet layout](#parquet-layout-the-file-format) — what the layout optimizes for (transcoding), the file-format settings, and the reasoning behind each.
- [Automatic sorting](#automatic-sorting) — `SORTED BY AUTO`: what it does and how it picks a key.
- [The design goal, stated precisely](#the-design-goal-stated-precisely) — the best outcome for Power BI within two hard limits: merge must survive, and the sort key must be the sharpest heuristic a single pass allows.

## Parquet layout (the file format)

The configuration is a compromise between two consumers with opposite needs — a columnar reader that wants large structures, and an incremental merge that wants small ones. Before the settings themselves, it is worth being explicit about what this layout is actually optimizing for, because it is not what most Parquet tuning guides assume.

### What the layout optimizes for: transcoding

Most Parquet advice targets **scan engines** — Spark, DuckDB, a warehouse — where the engine streams the file, decompresses it, and processes it row-group by row-group. That is not the primary consumer here. This layout is tuned for **Direct Lake**, and Direct Lake does not scan Parquet: it **transcodes** it.

The distinction is between two formats that happen to describe the same data:

- **The disk format (Parquet)** is built for storage and transport: compressed pages, dictionary and RLE encodings chosen per column, min/max statistics, immutable files on an object store. Its job is to be small and cheap to move.
- **The engine's in-memory format (VertiPaq column segments)** is built for interactive query: columns held resident in memory in the engine's own encoding, sized and organized for vectorized scans over and over, thousands of times a day, with sub-second expectations.

A scan engine bridges the two formats transiently, per query, and throws the result away. Direct Lake bridges them **once, on cold load**: it reads the Parquet, converts each row group into a resident column segment, remaps dictionaries where it can, and then serves every subsequent query from the in-memory form without touching the file again until the data changes. That one-time conversion — **transcoding** — is the operation this layout exists to make fast and faithful, and it explains settings that would look odd through a scan-engine lens:

- **SNAPPY over ZSTD** — transcoding is decode-bound; a slightly larger file that decodes faster wins, because the size difference is paid once in storage while the decode cost is paid on every cold load.
- **Fixed 6M-row row groups** — a row group becomes a segment as-is, so row-group sizing *is* segment sizing; anything from **1M to 16M rows is a fine segment**, a property of the in-memory engine, not of Parquet. Every write — overwrite, append, `SORTED BY AUTO`, and compaction alike — uses the same fixed **6M-row** ceiling: nothing is derived from the result, so no write pays a plan walk or a row count.
- **Dictionaries retained where sensible** — a Parquet dictionary can be remapped into the engine's dictionary rather than rebuilt from raw values, so keeping mid-cardinality columns dictionary-encoded makes the transcode a cheap remap instead of a full re-encode.
- **The footer must *declare* the dictionaries: `encoding_stats`** — the remap fast path is gated on metadata, not on the pages themselves. Before opening a single page, the transcoder checks `ColumnMetaData.encoding_stats` (the per-chunk page-encoding counts, with the dictionary page listed first) to prove the chunk is 100% dictionary-encoded. If the field is absent it cannot assume anything, so it decodes and re-hashes every value — same bytes, same dictionaries, ~15× slower cold load on a large string column (measured on a 142M-row column: 10.9 s without the field, 0.7 s with it, identical data and geometry). duckrun's writer emits the field natively; DuckDB's own `COPY TO parquet` did not — a gap this project found and fixed upstream in [duckdb#24957](https://github.com/duckdb/duckdb/pull/24957). The practical rule: a dictionary-encoded chunk is worthless to the transcode unless the footer *says* it is dictionary-encoded.

The general point: the "right" Parquet layout depends on **which engine's in-memory format it will be turned into, and how often**. A layout tuned for one-shot scans, for a different segment size, or for a different memory model would legitimately choose different values. This is one more sense in which everything on this page is a heuristic tied to a specific consumer, not a universal recommendation.

With that frame in place, the table below is the full configuration; the [reasoning](#the-logic-behind-the-numbers) follows.

| Property | Value | Rationale |
| --- | --- | --- |
| **Compression** | `SNAPPY` | Fast to decode, which is what a columnar reader (Direct Lake) needs on cold load — it transcodes Parquet dictionaries into its own encoding. On representative data, SNAPPY output is only ~1.3× the size of ZSTD; the sort and the dictionary do most of the shrinking, so the codec is chosen for decode speed rather than maximum ratio. The 1.3× figure is empirical and will vary with your data. |
| **Row group size** | fixed · 6M rows | **A max, not a constant — and a fixed one.** Every write (overwrite, append, `replaceWhere`, `SORTED BY AUTO`) and post-write compaction uses the same **6M-row** ceiling; the 128 MB file roll usually closes the group first. Nothing is sized from the result — the planner-estimate machinery and, later, the `SORTED BY AUTO` byte-model geometry that used to adapt the layout each cost plan walks, counts, and a calibrated bytes/row model on every write for no measured read-side advantage. One Parquet row group maps to one column segment in a Direct Lake reader; anything in the **1–16M** row band is a healthy segment (Power BI's own default segment size is 8M), and 6M keeps segments comfortably inside the band while giving a scan one more group per file to parallelize over. arrow-rs's ~1M default would produce thousands of small segments instead. A good **sort key** remains the larger lever on read performance (see [Automatic sorting](#automatic-sorting)) — order of priority is sort key first, row-group size second. |
| **Dictionary page limit** | 32 MB | Caps how large a column's dictionary grows before the column overflows to **PLAIN**. Mid- and high-cardinality columns (dictionary < 32 MB) stay dictionary-encoded — compact and directly remappable by a columnar reader. Truly unique columns still overflow to PLAIN, the correct outcome: their dictionary would be nearly as large as the data itself, pure overhead. This bound is the main lever on merge memory — a merge *reading* the table materializes those dictionaries, so a larger limit means a larger merge working set. Measured on an 18M-row merge: a 128 MB limit hit ~25 GB, 8 MB ~4 GB, 16 MB ~8.7 GB — 32 MB is a deliberate step up that curve, traded for denser read-layout segments and held in check by the merge spill tests. The exact crossover depends on your column cardinalities. |
| **Data page size** | 1 MB, **1M-row cap** | Bounded pages. The row-count cap is the important safeguard: without it, a highly compressible column buffers its entire row group as a single page — roughly 10× write memory and oversized pages that strain a reader's memory ([arrow-rs #5797](https://github.com/apache/arrow-rs/issues/5797)). |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level statistics only bloat the footer. Long strings are truncated in the statistics. |
| **Target file size** | 128 MB | The same bin size Fabric Spark's optimize-write targets. A row group cannot span files, so this byte cap is effectively a segment cap. A narrow fact reaches a full 6M-row segment well under 128 MB; a wide fact fills 128 MB first, so there the file size — not the row count — sets the segment. It is deliberately far below **1 GB**, which forced whole-file copy-on-write and inflated merge cost on disk. (Not the dominant merge-*memory* lever — the dictionary limit is — but a merge-*disk* lever at scale: more, smaller files cost DataFusion disk spill in a whole-table merge, which is why the merge-spill gate revalidates this value.) |

**Per-model overrides.** A dbt model that knows better than the fixed defaults can declare its
geometry: `max_row_group_size` (rows) and `target_file_size_mb` (MB) pin the row-group ceiling and
file size for that model's writes — the explicit value is honored verbatim and preserved by
post-write compaction. See the
[model config reference](dbt-adapter.md#config-options-table--incremental--delta).

### The logic behind the numbers

Every value above is pulled in two opposite directions:

- **Direct Lake wants it large.** One Parquet row group becomes one column segment, and a reader wants those segments large and few — big row groups, big files, dictionaries retained. Optimizing for the reader alone, every size would be pushed to its maximum.
- **Merge wants it small.** arrow-rs buffers a whole uncompressed row group per open writer, and a large dictionary inflates the memory a merge needs just to *read* the table — a 128 MB dictionary limit made one merge materialize ~25 GB, and a 1 GB file forced whole-file copy-on-write. The merge spill tests act as the guardrail: they fail the moment a setting pushes merge past its memory budget.

If those two consumers were equals, the values would sit in the middle. They are not equals, and this asymmetry decides which way the compromise leans:

- **ETL compute is cheap and invisible.** Merge and compaction run in the background, on a schedule, with no one watching. If a merge takes four minutes instead of three, nothing happens — no user notices, no report stalls. Batch work tolerates being slow; the only hard constraint is that it must *finish* within its memory budget.
- **BI compute is interactive and user-facing.** A Power BI report is a person clicking a slicer and waiting. Latency there is the product: a cold load that takes seconds instead of milliseconds is felt by every user of every report built on the table, on every visual interaction. Worse, interactive compute cannot be amortized or rescheduled — it happens exactly when the user acts, at whatever concurrency the users generate.

So a second of latency is not worth the same on both sides. The layout therefore optimizes for the reader first and concedes to merge only what merge strictly *needs* — not what would make merge fastest, but what keeps it from failing. Each number is set to the **largest value that still passes the merge spill tests**: large enough to serve Direct Lake well, small enough that merge remains stable. 6M-row groups, 128 MB files, a 32 MB dictionary cap, a 1M-row page cap — each is pushed toward the reader as far as the merge spill tests still tolerate, not to the theoretical ideal for read speed. These specific values were validated against duckrun's test workloads — tables with very different width, cardinality, or update patterns may find their own sweet spot elsewhere.

**MERGE takes the compromise one step further and opts out entirely**: it writes with none of these properties (delta_rs defaults), so a merge stays lean and never rewrites large files or materializes large dictionaries to touch a few rows. Merged files are transient — threshold-gated compaction folds them back into this layout on a later pass. Note this covers only a merge that genuinely reaches delta_rs: an *insert-only* merge is routed to a DuckDB anti-join and committed as a plain append, so it gets the full profile.

**Appends do get the profile.** They used to be exempt on the same "compaction will fold it in later" reasoning, but that fold only happens for files small enough to trip the compaction trigger, which is small-file **byte debt**. An append-only fact table writes files that are already a healthy size, so it never trips the trigger, and its row groups stayed at delta_rs's ~1M default permanently — the bottom of the segment band, on exactly the tables that can least afford it. Sizing the append at write time costs nothing on a small increment, because the row-group size is a *ceiling*: an append that never reaches it is written exactly as before.

There is nothing left that differs between them: overwrite, append, a **`replaceWhere`** write (the microbatch strategy, `INSERT … REPLACE WHERE`), and a **`SORTED BY AUTO`** rewrite all keep the same fixed 6M ceiling and let the 128 MB file roll pick the actual group.

### How the numbers are grounded

The values above are not read off a specification. The reader's in-memory format has no public spec, so we treat it as a **black box** — there is no reverse-engineering of its internals. Instead every number is **grounded against a real reference**: for a given dataset we build the same table two ways — once with **Spark's V-Order writer** (the layout Fabric itself produces) and once with duckrun — deploy both as **Direct Lake** semantic models, and compare heavy **DAX** queries over the XMLA endpoint, measuring both the cold Delta→memory transcode and the hot scan. The V-Order table is the target duckrun tries to match; the **DAX timings are the signal**, and the file-size / row-group statistics are a useful sidecar, not the verdict.

The tuning loop is deliberately narrow: change **one** writer property, rebuild, and keep the change only if Direct Lake DAX speed moves **toward** the V-Order reference **and** the merge spill tests still pass. That second clause is the guardrail from [the section above](#the-logic-behind-the-numbers) — a setting that reads faster but breaks merge is rejected.

One caveat dominates everything here, and it is the reason this whole page is framed as heuristics: **this was tuned against a single dataset** — one ~140M-row AEMO fact table. That is one workload, not a representative sample. The numbers are grounded on that one benchmark; a table with very different width, cardinality, or skew may well have its sweet spot elsewhere. The comparison is reproducible — see `tests/parquet_layout/aemo/` and the manual, non-gating `parquet_layout.yml` workflow, which builds `fct_summary_auto_sort` (duckrun) against `fct_summary_vorder` (Spark V-Order) and runs the DAX comparison in `xmla_compare.py` — so the honest way to trust these defaults for *your* data is to run the same comparison on it.

## Automatic sorting

`CREATE OR REPLACE TABLE <t> SORTED BY AUTO AS SELECT * FROM <t>` selects the sort key **for you**. You do not pass a column list — duckrun profiles the table (each column's cardinality, skew, null density, and functional dependencies), chooses a short key from that profile, and rewrites every file physically ordered by it.

The selection is a heuristic built on statistical estimates of your data. It is usually a reasonable key; it is never guaranteed to be the best one, and on some data distributions it will not help at all. See [the picker rules](#how-the-automatic-picker-chooses) and their failure modes below. From dbt, the same picker is reachable as the model config `sort_by: auto` — it profiles the staged model result and writes unsorted when nothing pays off.

The profile decides only the **key**: the write itself lands in [the same fixed Parquet layout](#parquet-layout-the-file-format) as every other write.

```sql
-- auto: profile the table, pick the key, rewrite clustered by it
CREATE OR REPLACE TABLE sales SORTED BY AUTO AS SELECT * FROM sales;

-- or name the key yourself (plain DuckDB CTAS syntax — no AUTO)
CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) AS SELECT * FROM sales;

-- just compact small files, no re-sort
VACUUM sales;
```

Both `SORTED BY` forms perform the same physical operation — one global `ORDER BY` streamed back out as new Delta files in [the Parquet layout above](#parquet-layout-the-file-format). The only difference is **who chooses the columns**: `AUTO` profiles and picks; `(cols)` takes your list. `VACUUM` is the no-sort sibling — it bin-packs small files without reordering rows.

### It is just a global `ORDER BY`

The "sort" is exactly what it sounds like: **one SQL `ORDER BY` over the entire table**, end to end:

```sql
SELECT * FROM delta_scan('…/sales') ORDER BY "region", "order_date"
```

It is a **global** ordering — row 1 of file 1 through the last row of the last file form one continuous sorted sequence. It is not a per-file reordering and not z-order interleaving. Every existing file is read and rewritten, so the whole table is re-laid-out in a single new Delta version. The only decision the feature makes is **which columns go in that `ORDER BY`, and in what order.**

### Why a sort shrinks files

A columnar file stores each column independently. For a low-cardinality column, two encodings compete: **bit-packed dictionary indices** (`ceil(log2 ndv)` bits per row) and **run-length encoding** (RLE — one entry per contiguous run of equal values). RLE only wins when values arrive in long runs.

In arbitrary physical order, the number of runs a column breaks into is governed by its value **skew**:

```
E[runs] ≈ N · (1 − Σ p_v²)
```

where `Σ p_v²` is the Simpson index of the value histogram (the probability that two random rows share a value). A near-uniform column shatters into ≈N runs and falls back to bit-packing; a heavily skewed column already RLE-compresses well in almost any order. A global sort **manufactures** runs: order by a column and its equal values become one contiguous run, so RLE collapses them and the dictionary pages stay compact. Ordering the rows also aligns row-group min/max statistics, allowing a reader to skip whole row groups on a filter.

Note what this model implies: how much a sort can help is entirely a property of *your* data's cardinality and skew. The same key that halves one table can be a no-op on another. This is straightforward for **one** column; the difficulty starts when there are forty.

### How the automatic picker chooses

`SORTED BY AUTO` uses a cheap, greedy heuristic — a stack of rules of thumb, each of which is *usually* right and each of which can be wrong on a given data distribution:

- **A date leads.** One temporal column takes the first key slot, because leading a fact table by its date preserves natural time-clustering and matches how such tables are typically queried and refreshed. Only the **coarsest** eligible date qualifies — the lowest-cardinality one — and a **near-unique** timestamp (so fine-grained it is effectively a row id) is *not* allowed to lead: it would consume the entire key's clustering power and leave the real dimensions unsorted. *Where it can fail:* on a table that is never filtered by time, or whose only temporal column is a technical watermark, the date-first rule spends the most valuable slot on the wrong column.
- **Then ascending cardinality.** Remaining columns are added coarse-to-fine — the classic ordering that tends to maximize total run length and respects natural hierarchies. *Where it can fail:* cardinality is a proxy, not the objective; a mid-cardinality column strongly correlated with the prefix can compress better than a lower-cardinality independent one, and the greedy rule will not see it.
- **Partition columns go outermost, but take no key slot.** Delta strips them from the data files, so they carry no compression weight; ordering by them first simply keeps roughly one writer open at a time.
- **Skip functionally dependent columns.** If adding a column does not grow the current prefix's distinct count (`distinct(X) == distinct(X, c) ⇒ X → c`), the prefix already clusters it for free — `year`/`month` behind `date` earn no slot. Near-dependencies are treated as dependencies (a column ~99% determined by the prefix also clusters for free); that tolerance is deliberate. *Note:* this is the one test not settled on a HyperLogLog sketch. The rule compares two counts whose true ratio is 1.0 for a real dependency, against a tolerance of a few percent — and a sketch's own error at the cardinality a date column lives at (10³–10⁵ distinct values) is several times larger than that tolerance, so it cannot resolve the question it is being asked. The sketch therefore only *screens*: a candidate that looks close to the prefix's grain is confirmed with one exact count for that column alone, and the verdict is taken from the exact numbers. This is bounded — one column at a time, only near the grain — and is not the batched exact count over every column at once that the sketches exist to avoid.
- **Stop at the grain.** Once the prefix nearly identifies rows, every group has size 1, there are no runs left to manufacture, and further columns can only cluster *worse*. The key stops there, capped at 4 dimension columns.
- **Drop what cannot help.** Mostly-null columns are never candidates; unique and near-unique columns are written **PLAIN**, because a dictionary would merely re-store the whole column plus an index.
- **Measures never hold a dimension slot, but they may follow one.** A measure (`DECIMAL`/`FLOAT`/`DOUBLE`) is an output you aggregate, not a key you filter on, so it can never take one of the four dimension slots. It can, however, earn a **tail slot** *below* the whole dimension key (up to 4 more), where ordering only refines rows *inside* an existing dimension group — correlated or zero-heavy measures collapse into runs there while every dimension a query filters on stays exactly as clustered as it was. A tail slot is kept only if it pays for itself twice over: a worthwhile fraction of the measure's own modelled bytes *and* of the whole table's. *Where it can fail:* the model scores only the column in the slot, so clustering a slot induces on **other** columns is invisible to it — a key whose win comes mostly from that induced effect will be passed over.
  - *A note on how this one is measured.* The question a tail slot asks — how many distinct values does this measure take **inside one dimension group** — used to be the one question the picker could not answer, because it profiled a *sample*: once the dimension key is fine there are only a handful of sampled rows left in each group and every measure looks unique. On a real 142M-row fact with ~2.6M `(date, time)` groups, an 8M-row sample leaves ~3 rows per group, and a regional price taking exactly 5 values per interval was indistinguishable from a per-row measure — the tail saw nothing to win and awarded no slot, silently missing a ~23% saving. The picker now profiles every row, so `distinct(prefix, measure)` **is** that measure's run count under the sorted layout and the question is answered directly: on that fact it reads the price at 1.5% of rows against the per-unit measure's 91%.

**Determinism.** The picker reads every row of the table and is a deterministic function of them, so profiling one table twice returns the same key — there is no sampling and no seed involved. It was not always: it used to profile a seeded reservoir sample, and before that seed was defaulted, two builds of one unchanged model could choose different keys purely on sampling luck — measured at 8.8% of a 592M-row table's size when a different measure won the last tail slot. The seed patched that for a table read directly, but never for a model whose SQL contains a `GROUP BY`: DuckDB only guarantees a repeatable sample single-threaded, and parallel hash aggregation reorders the rows reaching the sampler. Reading everything removes the failure mode rather than narrowing it.

**Cost.** Profiling reads the source **once**, into a local temp table that every subsequent pass scans; on remote storage that is one read instead of several. Sampling was removed rather than made cheaper because it was not saving anything: `USING SAMPLE reservoir(N ROWS)` costs superlinearly in `N` (on a 20M-row parquet table where materializing everything took 0.68 s, an 8M-row reservoir took 83 s), it reads every row regardless — DuckDB can only push sampling into its own native storage, never `delta_scan` — and the profiling model's memory is bounded by its sketches, not by how many rows it was given.

The result is a short, sensible key — usually. Because the picker is a heuristic operating on statistical sketches, **it is not guaranteed to shrink anything**. A near-uniform table has no runs to manufacture; a table already organized by a unique key has nothing left to cluster. In those cases `SORTED BY AUTO` degrades to a plain compaction (the same operation as `VACUUM`). The picker optimizes a *model* of the on-disk size, and the model can be wrong for your distribution — always compare `conn.get_stats("sales")` before and after to measure the real change. Only the disk knows the truth.

## The design goal, stated precisely

Everything above serves one objective: **produce the best possible layout for the interactive reader — Power BI — subject to two hard constraints.**

1. **The file layout must pass the merge spill tests.** The reader gets the largest segments, the fastest-decoding codec, and the most remappable dictionaries that merge can survive. The constraint is survival, not merge speed — merge is background compute and its comfort buys nothing.
2. **The sort key must be selectable in a single cheap profiling pass.** Here the constraint is not chosen — it is imposed by mathematics. Finding the *provably* best key is intractable (the next section shows why), so the honest ambition is the strongest one available: **the cleverest column-selection heuristic that can be written** — one that profiles cardinality, skew, dependencies, and grain, and gets as close to the intractable optimum as a single pass allows.

So the goal is not "good enough, found cheaply" as an end in itself. The goal is *maximal* on both axes; "heuristic" describes the ceiling the problem permits, not a lowered bar. What follows is why that ceiling exists.

### Why the provably best key is out of reach

Choosing the sort order that minimizes a table's size is not a tidy optimization with a clean answer. It is **combinatorially hard — NP-hard in general** — and remains hard regardless of available compute. Three factors compound:

- **The search space is superexponential.** The key is not "which columns" — it is *which columns, in which order*. For a table with `n` columns the candidates are **ordered subsets**: `n` first-column choices, times `n−1` second-column choices, and so on — `Σ_k n!/(n−k)!` candidate keys, past a million by ten columns. Enumeration is not feasible.
- **A candidate cannot be scored without building it.** There is no closed form for "how many bytes will this ordering compress to." A column's encoded size depends on its **run structure**, which depends on the run structure of *every column ahead of it in the key* — columns interact through correlation and functional dependency (sorting by `date` silently clusters `month` and `year`; sorting by `city` partially clusters `country`). The only faithful way to score a candidate is to sort and write the entire table that way and measure the result. One evaluation is a full-table rewrite.
- **Superexponential candidates × a full rewrite each.** An exact optimizer would write an astronomical number of layouts just to compare them. For a table of any realistic size this is not merely slow — it does not finish. And the prize is a few percent of disk; no one spends a compute-week to shave 4% off a Parquet folder.

Minimizing total runs across multiple columns by reordering rows generalizes problems that are provably NP-hard, so a polynomial exact algorithm almost certainly does not exist. Exact optimality is therefore not on the menu at any budget — which is exactly why the effort goes into making the *heuristic* as sharp as possible instead.

### What the research says — and why its best ideas are too slow

None of this is new. Lemire, Kaser, and Aouiche studied exactly this problem — reordering rows so run-length compression pays off — in *Sorting improves word-aligned bitmap indexes* (2010). Two of their findings frame the entire trade-off:

- **Sorting is a major lever, not a rounding error.** A plain lexicographic sort of the table reduced their index size by up to roughly 9×. Physical row order is one of the largest compression knobs available — larger than the choice of codec. This is why the feature exists at all.
- **The column order within the sort is itself a substantial decision.** Merely permuting the columns before sorting changed compression efficiency by roughly **40%** in their experiments. The order of columns in the key is not cosmetic; it accounts for much of the win.

Push the second point to its conclusion: if the column order is worth 40%, one would want the *best* column order — but there are `n!` of them, and the only faithful way to score one is to sort and measure. And that is the *easy* version of the problem. The truly optimal layout is not a column permutation at all; it is a free reordering of the **rows** to minimize transitions between neighbours — the same combinatorial problem as above, a relative of the travelling salesman problem, NP-hard, with no shortcut.

This is precisely why the more sophisticated ideas in this literature — Gray-code orderings, Hilbert-curve tuple orderings, nearest-neighbour (TSP-style) row chaining — are genuinely interesting but **impractical at warehouse scale**. They reason about *relationships between rows*, which requires pairwise distances: quadratic work, or a heavy approximation, over a table that may hold billions of rows. A nightly maintenance job gets **one pass**, not a week of graph search to recover a few more percent.

Lemire's practical recommendation is the foundation this implementation builds on: perform a single cheap lexicographic sort — `O(n log n)`, affordable every night — and spend the one real degree of freedom on the **column order**, arranging the key so the lowest-cardinality columns lead. That captures most of the ~9× for a small fraction of the cost. The [picker above](#how-the-automatic-picker-chooses) takes that foundation and sharpens it with everything a profiling pass can learn — coarsest-date leadership, functional-dependency pruning, grain detection, measure exclusion — precisely because when the exact optimum is unreachable, the quality of the heuristic is where all the remaining win lives.

### Prefer your own key when you know the workload

A final, deliberate note of humility. However sharp the heuristic, it is still a heuristic operating on estimates, and there will be tables where it selects a worse key than the one you would choose — occasionally even a worse layout than the table's natural arrival order. Sort-key selection is not an exact science: the outcome is determined by your data's cardinality, skew, and correlation structure, all of which the picker only *estimates* from sketches, and all of which can drift as the table grows.

If you understand your table's grain and query patterns, `SORTED BY (cols)` with an explicit list will often beat `AUTO` — you have information about the workload that no single-pass profile can recover. Treat `AUTO` as a sensible default for tables you have not studied, verify its effect with `conn.get_stats()`, and override it wherever your own knowledge is better. Finding a provably good sort order in minimal time remains an open problem; a heuristic plus measurement is the practical state of the art.

### Sources & further reading

- **Daniel Lemire, Owen Kaser, Kamel Aouiche — "Sorting improves word-aligned bitmap indexes"** (*Data & Knowledge Engineering* 69(1), 2010). The result this page rests on: physical row order is a major compression lever (a lexicographic sort reduced their index size by ~9×), the column order within the sort matters substantially (~40%), and a well-chosen cheap lexicographic sort — low-cardinality columns first — captures most of the win while the true optimum remains intractable. This is the conceptual basis for the column-ordering heuristic here.
  [arXiv:0901.3751](https://arxiv.org/abs/0901.3751) ·
  [author's page](https://lemire.me/en/publication/dke2010/)
- **HyperLogLog** (Flajolet, Fusy, Gandouet, Meunier, 2007). Every cardinality estimate that *ranks* candidates is a HyperLogLog sketch — DuckDB's
  [`approx_count_distinct`](https://duckdb.org/docs/current/sql/functions/aggregates) — rather than an exact `COUNT(DISTINCT)` over every column at once, which is what makes profiling cheap and bounded in memory. The cost is approximation error, and it is real: at 10³–10⁵ distinct values the sketch can be off by tens of percent. That is tolerable for ranking (the order of candidates barely moves) and for the coarse grain stop, but not for the functional-dependency test, which asks whether a ratio is 1.0 within a few percent — hence the exact confirm described [above](#how-the-automatic-picker-chooses). Everywhere else the picker's output remains an estimate rather than a guarantee.
  [HyperLogLog overview](https://en.wikipedia.org/wiki/HyperLogLog)
- The **run-count model** `E[runs] ≈ N·(1 − Σ pᵥ²)` uses the Simpson / Herfindahl index of the value histogram — a standard statistical result, not attributable to a single source.
- The **functional-dependency test** `distinct(X) == distinct(X, c) ⇒ X → c` is textbook relational database theory.
