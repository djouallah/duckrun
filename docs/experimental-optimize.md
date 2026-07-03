# Experimental: the Direct Lake sort-rewrite

!!! warning "One experimental feature, not two"
    `conn.optimize(name, sort="experimental")` is a single feature that does two things at once:
    it **picks a sort key and physically re-sorts the table**, *and* it writes the result with a
    **bespoke, aggressive Parquet layout**. The sort and the layout are the same pass — you cannot
    get one without the other, and there is no way to ask a normal write for this layout.

    **Normal writes deliberately don't do any of this.** A plain `saveAsTable` / `insertInto` /
    `merge` / `append` — and an ordinary compaction or z-order — writes a boring, safe Delta layout
    (ZSTD, moderate row groups, delta-rs default file size). The tuning below is reached *only*
    through `sort="experimental"`.

## Why it exists

The layout is a **bespoke optimization for [Power BI Direct
Lake](https://learn.microsoft.com/power-bi/enterprise/directlake-overview)** — the mode where Power
BI frames Parquet files straight into its in-memory columnar engine instead of importing or querying
them row-by-row. When the files on disk already look the way that in-memory reader wants them,
framing is cheap and the semantic model is smaller and faster; when they don't, the reader has to do
more work per refresh.

There is **no public spec** to target here, so this is not a standard or a portable optimization —
it's built from **public information** about how Direct Lake reads and segments Parquet, plus direct
measurement of the on-disk result. It happens to also help any other in-memory columnar reader
(DuckDB, Spark, Trino), but Direct Lake is what it's tuned for. Treat the exact heuristic and the
tuning as **experimental**: they may change between releases.

## Using it

```python
r = conn.optimize("sales", sort="experimental")
# {'operation': 'sortRewrite', 'sortedBy': ['region', 'order_date'],
#  'sizeBytesBefore': 15_197_312, 'sizeBytesAfter': 6_785_450, 'savedPct': 55.3}
```

It **profiles the table, rewrites every file physically sorted** by a key it picks automatically,
and reports the **real, measured** on-disk size change — read from the Delta log's active-file
`size_bytes` before and after, never an estimate. It's a **full read → `ORDER BY` → overwrite**, not
a bin-pack, so run it **occasionally** (after a batch load, not on every write). `forName` tables
only — it needs a catalog table to profile. If no key pays off it falls back to a plain compaction.

## It's a global sort — nothing clever

The "sort" here is exactly what it sounds like: **one plain SQL `ORDER BY` over the entire table**,
end to end. Concretely, the rewrite is

```sql
SELECT * FROM delta_scan('…/sales') ORDER BY "region", "order_date"
```

streamed back out as new Delta files. That means:

- It is a **global** ordering across the whole dataset — row 1 of file 1 through the last row of the
  last file are in one continuous sorted sequence. It is **not** a per-file or per-row-group local
  reordering, and it is **not** z-order / space-filling-curve interleaving. Just `ORDER BY`.
- Every existing file is **read and rewritten** (that's why it's a full overwrite, not a bin-pack),
  so the whole table is re-laid-out in a single new Delta version.
- There is no magic beyond picking *which* columns go in the `ORDER BY` and in what order — that
  choice is the only "smart" part, and it's covered next.

Why bother: a global sort clusters equal values into long, contiguous runs, which is what lets
run-length and dictionary encoding actually shrink the file — and it lines the row groups up so a
reader can skip whole segments on a filter.

## How the sort key is chosen

The only decision the feature makes is **which columns to put in that global `ORDER BY`, and in
what order** — a shorter, well-ordered key clusters more than a long arbitrary one. The heuristic,
briefly:

- **Partition columns lead** the physical order but take no key slot (the partition already clusters
  them).
- A **date / temporal column is favoured to lead** — coarse before fine, respecting natural
  hierarchies.
- A column **functionally determined** by one already in the key (e.g. `year` ← `date`) is dropped —
  sorting the key already clusters it for free.
- **Measures** (decimals / floats you aggregate but never filter or sort on) are excluded; they don't
  belong in a sort key.
- **Unique / near-unique columns** get no dictionary — every value is distinct, so a dictionary just
  re-stores the whole column plus an index. Those are written **PLAIN**.

## The Parquet layout it writes

This is the tuned writer configuration applied **only** on the sort-rewrite. delta-rs (the arrow-rs
Parquet writer) does the writing; DuckDB only executes the SQL and streams Arrow into it.

| Property | Value | Why |
| --- | --- | --- |
| **Compression** | `ZSTD` level 3 | A meaningfully smaller footprint than Snappy at a low, fast level. On a lakehouse the read is usually **network-I/O-bound**, so smaller files beat marginally-faster decompression. |
| **Row group size** | ~8M rows | A row group is the unit an in-memory engine loads as one segment. arrow-rs defaults to ~1M — thousands of tiny segments trip reader guardrails. ~8M (one Power BI segment) gives fewer, larger scan ranges; bigger costs more write-time memory (a full row group is buffered per open writer), which is why it's confined to this opt-in pass. |
| **Dictionary page limit** | 256 MB | The single most important knob for fast framing. The arrow-rs default (~1 MB) makes a **wide, repetitive column silently fall back to plain encoding mid-column** once its dictionary grows — the file looks dictionary-encoded in the first pages and isn't after. In-memory readers ingest a Parquet dictionary almost directly into their own hash encoding, so keeping the whole column dictionary-encoded is what makes framing cheap. 256 MB holds any dictionary worth having and still bounds per-column write memory; a column that overflows it was never a good dictionary candidate (drop / hash / truncate it upstream). |
| **Data page size** | 8 MB | Fewer page headers, and run-length runs survive across page boundaries instead of being chopped at ~1 MB. |
| **Statistics** | chunk-level, truncated | Row-group min/max is all a reader needs to skip row groups; page-level stats just bloat the footer. |
| **Target file size** | ~1 GB | Few large files rather than many small ones — small files are as bad for a columnar reader as small row groups. (Normal writes leave this unset, so an incremental MERGE never has to rewrite a fat 1 GB file.) |
| **Data pages / bloom filters** | v1 pages, bloom off | The boring, maximally-compatible layout; bloom filters aren't read here and only inflate the footer. |

The exact same properties apply on a local path and on OneLake / S3 / GCS / ADLS — storage is
neutral.

## What a normal write does instead

For contrast, every non-experimental path — `saveAsTable`, `insertInto`, `merge`, `append`, the
`_if_unchanged` fenced modes, a plain `conn.optimize(...)` compaction, and z-order — writes a
deliberately plain layout: **ZSTD + ~4M-row row groups, and nothing else**, with the file size left
at the delta-rs default. That keeps the hot write paths cheap and keeps an incremental MERGE from
having to rewrite fat files. If you want the Direct Lake layout, you ask for it explicitly with
`sort="experimental"`.
