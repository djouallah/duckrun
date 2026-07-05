# Parquet layout

It is a compromise. The table below is the whole configuration; the
[reasoning](#the-logic-behind-the-numbers) is at the end.

| Property | Value | Why |
| --- | --- | --- |
| **Compression** | `SNAPPY` | Fast to decode, which is what a columnar reader (Direct Lake) wants on cold load — it transcodes the Parquet dictionaries into its own encoding. SNAPPY runs only ~1.3× the size of ZSTD on real data; the sort and the dictionary do the real shrinking, so the codec's job here is decode speed, not maximum ratio. |
| **Row group size** | 6M rows | One Parquet row group maps to one column segment in a Direct Lake reader, and Fabric wants segments in the **1–16M** row band. 6M sits mid-band while bounding write-time memory (arrow-rs buffers a full uncompressed row group per open writer). arrow-rs's ~1M default would mean thousands of tiny segments. |
| **Dictionary page limit** | 8 MB | Caps how big a column's dictionary grows before it overflows to **PLAIN**. Mid-cardinality columns (dict < 8 MB) stay dictionary-encoded — small, cheap, and a columnar reader remaps the dictionary directly. High-cardinality / unique columns overflow to PLAIN, which is *correct*: their dictionary would be as big as the data (no compression), pure overhead. This bound is **load-bearing for merge** — a large limit (128 MB) kept high-card columns dictionary-encoded and made a merge *reading* the table materialize ~25 GB of dictionaries; 8 MB keeps that ~4 GB. |
| **Data page size** | 1 MB, **20k-row cap** | Bounded pages. The row-count cap is the real safeguard: without it, a highly compressible column buffers its whole row group as a single page — ~10× write memory and giant pages that blow a reader's memory ([arrow-rs #5797](https://github.com/apache/arrow-rs/issues/5797)). |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level stats just bloat the footer. Long strings are truncated in the stats. |
| **Target file size** | 256 MB | A row group can't span files, so this byte cap is really a segment cap — 256 MB lets a wide fact reach a full 6M-row segment, one row group per file, giving large uniform segments. It's deliberately far below **1 GB**, which forced a whole-file copy-on-write that blew up merges on disk. (With the dictionary bounded, file size is no longer a merge-memory lever — 128/256/512 MB all merge the same.) |

## The logic behind the numbers

Every value here is pulled in two opposite directions:

- **Direct Lake wants it big.** One Parquet row group becomes one column segment, and a reader
  wants those segments large and few — big row groups, big files, dictionaries kept. Left alone
  you'd push every size straight to the maximum.
- **merge wants it small.** arrow-rs buffers a whole uncompressed row group per open writer, and a
  fat dictionary balloons the memory a merge needs just to *read* the table — a 128 MB dictionary
  limit made one merge materialize ~25 GB, a 1 GB file forced a whole-file copy-on-write. The merge
  spill tests are the guardrail: they fail the moment a size makes merge blow its memory budget.

So each number is set to the **largest value that still passes the merge spill tests** — big enough
to keep Direct Lake happy, small enough that merge doesn't fall over. 6M-row groups, 256 MB files,
an 8 MB dictionary cap, a 20k-row page cap: none of them are the theoretical ideal for read speed,
they're that ideal minus whatever headroom merge needs. merge itself takes the compromise one step
further and opts out entirely — it writes with none of these properties, so an incremental merge
stays lean and never rewrites fat files or materializes giant dictionaries to touch a few rows; the
threshold-gated compaction folds those loose files back into this layout on a later pass.

## The one thing I can't fix: one row group per file

The biggest limitation is that I can't force **exactly one row group per file**. Direct Lake is
happiest when a file is a single clean segment — one file, one row group — but the Parquet writer
picks row-group boundaries from its own row-count and size thresholds, and there is no knob that
says "emit one row group, then start a new file." The best you can do is line up `target_file_size`
and `row_group_size` so they *usually* coincide (a 256 MB file target against a 6M-row group tends
to yield one group per file for a wide fact). On a narrow table, or one with unusual row widths, a
file can still come out with two or three row groups — and nothing in the writer will stop it.
