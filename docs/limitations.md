# Limitations

An honest, consolidated list of what duckrun doesn't do — by design, by an upstream constraint, or
by deliberate caution. Most come with a "do this instead." Deeper detail lives in the linked pages.

## Setup & versions

- **Needs `duckdb >= 1.5.4`.** Older builds — including Microsoft Fabric's bundled stable runtime —
  fail loud at `connect()`. In a Fabric notebook: `!pip install duckrun --upgrade` then restart.
- **Pins `deltalake == 1.5.0`.** delta-rs 1.6.0's MERGE is broken at scale, so duckrun stays on 1.5.0.

## Microsoft Fabric / OneLake

- **delta-rs `> 1.5.0` breaks bulk delete on OneLake.** Since 1.5.1 the batch-delete path drops the
  workspace/artifact ids (*"Either WorkspaceId or ArtifactId are missing in the request"*), so
  `vacuum` and other multi-file deletes fail against OneLake. This is a major reason duckrun pins
  `deltalake == 1.5.0`. See [delta-rs #4401](https://github.com/delta-io/delta-rs/issues/4401).

## Iceberg (`format='iceberg'`)

**Delta is the default and the format duckrun is built around.** Iceberg is opt-in — you get it only
by explicitly passing `format="iceberg"` to `duckrun.connect()` / `conn.attach()`, or setting
`format: iceberg` in a dbt profile. Nothing changes for existing projects.

When you do opt in, duckrun contributes only the OneLake token, the Azure storage secret and the
`ATTACH`; DuckDB's `iceberg` extension is the engine. So its limits are yours (verified against
DuckDB **1.5.4**):

- **Fabric's Iceberg REST catalog is in private preview**, and DuckDB documents its own REST-catalog
  storage support as *"S3, S3 Tables, and Google Cloud Storage (GCS). Support for other storage
  backends is not yet available."* OneLake works — that is why duckrun passes
  `ACCESS_DELEGATION_MODE 'none'` and mints its own Azure secret rather than relying on the catalog
  vending credentials — but it is outside the combination upstream lists as supported. Treat it as
  preview, not production.
- **`UPDATE`/`DELETE` are merge-on-read only.** DuckDB writes positional deletes; copy-on-write is not
  supported, and the operation fails outright if the table sets `write.update.mode` /
  `write.delete.mode` to anything else.
- **None of duckrun's Delta features exist there.** No `SORTED BY AUTO`, no automatic
  compaction/`VACUUM`, no `DESCRIBE DETAIL`/`DESCRIBE HISTORY`, no `RESTORE TABLE`, no `get_stats()`,
  no `refresh()`, no delta-rs snapshot-pinned MERGE. `conn.sql()` is a pass-through — duckrun does not
  parse, rewrite or fence your statements — so concurrency and correctness are between you, DuckDB and
  the catalog.
- **`read_only` is DuckDB's `ATTACH … READ_ONLY` flag**, not a duckrun gate. Whether a given catalog
  honors it is the extension's business; duckrun will not second-guess it by inspecting SQL.
- **The catalog token is fixed at `ATTACH` time.** A notebook session re-attaches automatically when
  the OneLake token nears expiry; a dbt run captures the token when the profile loads, so a build
  running past the token's ~1h life can fail on the Iceberg catalog.
- **dbt: a second `snapshot` run against an attached catalog fails** with DuckDB's *"a single
  transaction can only write to a single attached database"*. This is stock dbt-duckdb behavior —
  identical under `type: duckdb` — not something duckrun's delegation introduces.
- **`write.target-file-size-bytes` / `write.parquet.row-group-size-bytes` are ignored on partitioned
  tables** (upstream).

## SQL DML (`conn.sql`)

- **`UPDATE … FROM` and `DELETE … USING` are rejected** → rewrite as a correlated subquery.
- **One statement per `conn.sql()` call** — multiple statements in a single call are rejected.

The full accepted/rejected matrix is in the [Connection API](connection-api.md#raw-sql-dml-through-connsql).

## dbt & incremental

- **A single `dbt run` is single-threaded** (`threads: 1`): the in-process delta-rs write path isn't
  thread-safe, so models inside one dbt invocation don't parallelize. This is **not** a limit on
  concurrent writers — separate runs / notebooks / jobs writing the same tables at once is fully
  supported and safe (every write is snapshot-pinned and fails loud on a conflict).
- **Some dbt merge configs are rejected, on purpose.** `merge_clauses`,
  `merge_update_set_expressions`, and `merge_on_using_columns` are dbt-duckdb-specific with no
  delta-rs equivalent, so duckrun raises a clear error rather than silently running a plain upsert.

## Schema & constraints

- **Schema evolution is add-only.** delta-rs can't drop columns, so `on_schema_change='sync_all_columns'`
  only *adds* them. Use `append_new_columns` or `fail`.
- **Only `not null` is enforced.** `check` / `primary_key` / `foreign_key` constraints are declared
  but not checked — they can't be enforced against a `delta_scan` view.

## DuckDB catalog

- **No external-table abstraction, so Delta tables are views.** DuckDB's catalog has only native
  tables (own DuckDB storage) and views — no PostgreSQL-style foreign table whose bytes live
  elsewhere but still accepts writes. Since delta-rs owns the data, duckrun registers each Delta
  table as a `CREATE VIEW` over `delta_scan(...)` and routes writes to delta-rs at the cursor.

## Materializations

- **No persistent views.** The Delta spec doesn't define a view, so there's nothing durable to write.
  A `materialized='view'` model *runs* fine — it's a real DuckDB catalog view you can query for the
  rest of the session — but it lives only in that connection and vanishes when it closes; nothing is
  saved to storage, so the next session won't see it. (And swapping a model between `table` and
  `view` isn't supported.)
- **`DROP TABLE` is a soft tombstone, not a physical delete.** `conn.sql("drop table x")` unregisters
  the table and writes a tombstone marker but **does not reclaim the data files** (a deliberate
  precaution — you purge them when you're sure). Address dropped tables by name, not by path.

## Parquet layout

- **`SORTED BY AUTO` picks the key with a naive, lightly-tested heuristic.** The auto sort-key picker
  is a cheap greedy single pass over statistical *sketches* (approximate cardinalities, HyperLogLog
  functional-dependency tests) — a stack of rules of thumb, each of which can be wrong on a given
  distribution. It is not guaranteed to shrink anything and can occasionally pick a worse key than the
  table's natural arrival order, and it has been validated against essentially one dataset, not a broad
  workload sample. When you know your grain and query patterns, prefer an explicit `SORTED BY (cols)`,
  and always compare `conn.get_stats()` before and after. See
  [Automatic sorting](parquet-layout.md#automatic-sorting).
- **Adaptive row-group sizing is a heuristic too, tuned on one dataset.** Row groups are sized from a
  planner row estimate (`ceil(rows / 8)`, capped at 16M) grounded against that same single benchmark —
  a rough rule, not a broadly tested optimum. A table with unusual width, cardinality, or skew may well
  have its sweet spot elsewhere. See
  [How the numbers are grounded](parquet-layout.md#how-the-numbers-are-grounded).

## Memory

- **Two engines share one machine's memory.** DuckDB and delta-rs each keep their own pool in the same
  process; heavy merges split the budget, and that split is fragile (delta-rs's merge spill-to-disk is
  itself flaky). Background in the [Design document](design_document.md).
- **delta-rs hard-codes a 100 GB merge disk-spill ceiling — arguably a bug.** A wide MERGE (one that
  rewrites many partitions) spills to disk, and the DataFusion `DiskManager` under delta-rs caps that
  spill at a **flat 100 GB regardless of how big the disk is** — so a merge aborts with *"Resources
  exhausted … exceeded the allowable limit of 100.0 GB"* even on a machine with terabytes free. It
  should scale to the available disk (or at least be documented and defaulted sanely), not hard-code a
  constant. duckrun works around it by sizing `max_temp_directory_size` to **~80% of the spill disk's
  free space** on every merge (override per model with `merge_max_temp_directory_size`); still, the true
  fix for a merge this large is data layout — keep each batch inside one partition so the span, and thus
  the spill, stays small. See [MERGE at scale](merge-benchmark.md).

---

For the test-by-test picture, see [Conformance](conformance.md); for *why* these trade-offs exist,
see [How it works](overview.md).
