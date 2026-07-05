# The read layout

Every duckrun **write** lands in **one** Parquet writer configuration — `saveAsTable`, `insertInto`,
`save`, the `_if_unchanged` fenced modes, `replaceWhere`, the threshold-gated post-write compaction,
and the [automatic sort](automatic-sort.md) all use it. It is tuned so a columnar reader (Microsoft
Fabric **Direct Lake**) can transcode the files fast on a cold load, not for maximum compression
ratio. `MERGE` is the sole exception — see [below](#the-one-exception-merge). delta-rs (the arrow-rs
Parquet writer) does the writing; DuckDB only streams Arrow into it.

The exact same properties apply on a local path and on any object store (S3 / GCS / ADLS / OneLake)
— storage is neutral.

| Property | Value | Why |
| --- | --- | --- |
| **Compression** | `SNAPPY` | Fast to decode, which is what a columnar reader (Direct Lake) wants on cold load — it transcodes the Parquet dictionaries into its own encoding. SNAPPY runs only ~1.3× the size of ZSTD on real data; the sort and the dictionary do the real shrinking, so the codec's job here is decode speed, not maximum ratio. |
| **Row group size** | 6M rows | One Parquet row group maps to one column segment in a Direct Lake reader, and Fabric wants segments in the **1–16M** row band. 6M sits mid-band while bounding write-time memory (arrow-rs buffers a full uncompressed row group per open writer). arrow-rs's ~1M default would mean thousands of tiny segments. |
| **Dictionary page limit** | 8 MB | Caps how big a column's dictionary grows before it overflows to **PLAIN**. Mid-cardinality columns (dict < 8 MB) stay dictionary-encoded — small, cheap, and a columnar reader remaps the dictionary directly. High-cardinality / unique columns overflow to PLAIN, which is *correct*: their dictionary would be as big as the data (no compression), pure overhead. This bound is **load-bearing for MERGE** — a large limit (128 MB) kept high-card columns dictionary-encoded and made a merge *reading* the table materialize ~25 GB of dictionaries; 8 MB keeps that ~4 GB. |
| **Data page size** | 1 MB, **20k-row cap** | Bounded pages. The row-count cap is the real safeguard: without it, a highly compressible column buffers its whole row group as a single page — ~10× write memory and giant pages that blow a reader's memory ([arrow-rs #5797](https://github.com/apache/arrow-rs/issues/5797)). |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level stats just bloat the footer. Long strings are truncated in the stats. |
| **Target file size** | 256 MB | A row group can't span files, so this byte cap is really a segment cap — 256 MB lets a wide fact reach a full 6M-row segment, one row group per file, giving large uniform segments. It's deliberately far below **1 GB**, which forced a whole-file copy-on-write that blew up merges on disk. (With the dictionary bounded, file size is no longer a merge-memory lever — 128/256/512 MB all merge the same.) |

## The one exception: MERGE

`MERGE` (raw-SQL `MERGE`, `DeltaTable.forName(...).merge(...)`, and dbt incremental merges) writes
with **none** of the above — no writer properties, no target file size — so an incremental merge
stays quick, never rewrites fat files, and never materializes giant dictionaries just to update a
few rows. `DELETE` and `UPDATE` are likewise lean. The threshold-gated post-write **compaction**
then folds the files those operations leave behind up into the read layout on a later pass, off the
merge's critical path. So you still converge on the read layout — you just don't pay for it inside
every merge.
