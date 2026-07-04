# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed
- **`optimize` operates on a table, not the session.** Removed the session-level `conn.optimize(name, …)`.
  Compaction and z-order are `DeltaTable.forName(conn, name).optimize()` / `.optimize(zorder_by=[…])`; the
  experimental profiled sort rewrite is `conn.table(name).optimize()` (auto key) or `.optimize("a","b")`.
  The old `sort='experimental'` kwarg on `DeltaTable.optimize()` is gone.
- **Single read-layout writer profile for every file write.** The separate "normal" (ZSTD) and
  "optimize" writer configs are collapsed into one Direct-Lake-friendly profile — SNAPPY, 6M-row groups,
  an **8 MB dictionary page limit** (mid-cardinality columns keep a remappable dictionary; near-unique
  columns overflow to PLAIN — a 128 MB limit instead kept them dictionary-encoded and made a merge reading
  the table materialize 25 GB of dictionaries vs ~4 GB), **data pages bounded to 20k rows** (an unbounded
  page row-count buffers a whole row group as one page on compressible columns — arrow-rs #5797), chunk
  stats, and unique columns written PLAIN — used by append / overwrite / safeappend / compaction / the
  sort-rewrite alike. **MERGE
  is deliberately excluded:** it passes no writer properties and no target file size, so a merge stays
  quick and never rewrites fat files; the threshold-gated post-merge compaction folds merged files up
  into the read layout afterwards.
- **Target file size 1 GB → 256 MB, one row group per file.** A Parquet row group can't span files, so
  a large file-size cap silently truncates the row group (delta-rs closes the file mid-group), leaving
  small, non-uniform Direct Lake column segments; 1 GB also forced the whole-file copy-on-write that blew
  up merges on disk. 256 MB is large enough for a wide fact (lineitem) to reach a full 6M-row segment yet
  far below the 1 GB that hurt merges — and with the dictionary page limit bounded (below), 128/256/512 MB
  all merge in ~16s / ~5 GB (measured), so file size is free to serve the read layout. Applies to every
  file write and to routine post-write compaction.
- **Row group is 6M rows** (was 4M normal / 8M optimize). 6M sits mid-band in Fabric's 1M–16M segment
  guidance while bounding write-time memory (arrow-rs buffers a full uncompressed row group per open
  writer).
- **Auto sort-key profiler drops mostly-null columns** using Delta-log statistics — a column that is
  almost entirely NULL clusters for free and never earns a sort-key slot.
- **TPC-H benchmark ingests through the duckrun write path** (`conn.read.parquet(...).write.saveAsTable`)
  instead of a zero-copy `convert_to_deltalake`, so it exercises the writer and the DuckDB read side of
  the 22 queries end-to-end.

## [0.3.31] - 2026-07-03

### Added
- **`df.sort()` / `df.orderBy()`** — the vanilla Spark DataFrame methods, returning a new *writable*
  DataFrame ordered by a native DuckDB `ORDER BY` (`orderBy` is an alias of `sort`, `ascending=` bool
  or per-column list). Previously these fell through to the raw relation and lost `.write`; now
  `conn.sql(...).sort("a", "b").write…saveAsTable(...)` works and composes with `.partitionBy(...)`.
- **`conn.optimize(name, …)` — experimental sort rewrite.** `conn.optimize(name, sort="experimental")`
  (a one-liner over `DeltaTable.forName(conn, name).optimize(...)`) profiles the table, picks a
  run-length-friendly sort key (partition columns lead but take no key slot; a column functionally
  determined by the key is dropped; measures excluded), and rewrites every file physically sorted with
  the tuned writer properties. Returns the **real measured** on-disk size from the Delta log
  (`sizeBytesBefore` / `sizeBytesAfter` / `savedPct`) — never an estimate. The plain compaction and
  z-order forms are `conn.optimize(name)` / `conn.optimize(name, zorder_by=[...])`.

### Changed
- **Parquet writer properties tuned for columnar / Direct Lake readers** — ZSTD level 3, ~6M-row row
  groups (Power BI segment standard), a 256 MB dictionary-page limit so wide columns stay
  dictionary-encoded (no mid-chunk PLAIN fallback), 8 MB data pages, chunk-level statistics, and a
  ~1 GB target file size. Applied on the initial write **and** on compaction/optimize (compaction
  previously reverted the tuned layout).

## [0.3.30] - 2026-07-03

### Added
- **Storage-neutral Files I/O on the connection API** — `conn.copy()`, `conn.download()`, and
  `conn.list_files()` move loose files to/from any store (local / S3 / GCS / ADLS / OneLake) using
  DuckDB `COPY … (FORMAT BLOB)` over the secret `connect()` already mints. No new dependency; copies
  are byte-verbatim (a `.gz`/`.zst` target is never re-compressed). OneLake enumeration uses the DFS
  REST API (DuckDB can't glob OneLake).
- **`conn.get_stats()`** — per-table Delta statistics (rows, files, row-groups, avg row-group, size,
  VORDER, compression) from the Delta log + parquet footers; `detailed=True` for one row per row
  group. Live files only (tombstoned files excluded).

### Changed
- **`connect()` tolerates any root.** Discovery skips directories that aren't Delta tables (no
  `_delta_log`) instead of hard-failing, so pointing at a Files section or a mixed folder works; a
  genuine unreadable table still fails loud.
- **Unreachable OneLake fails loud.** A wrong-tenant / not-in-workspace store now raises
  `OneLakeAccessError` on both the connection API and the dbt discovery path, instead of silently
  reporting an empty lakehouse.

## [0.3.28] - 2026-07-01

### Fixed
- **Wrong-`deltalake` runtime guard is now exact.** The startup version check only enforced a
  `deltalake >= 1.5.0` floor, but duckrun needs *exactly* 1.5.0 — every newer release breaks
  MERGE-at-scale and batch DELETE. A Microsoft Fabric kernel that keeps a newer `deltalake` loaded
  (installed-but-not-`restartPython()`) previously sailed past the guard and silently ran broken
  merges/deletes; it now raises a loud, actionable error.
- **Single-thread pin is verified, not assumed.** The adapter pins `config.threads = 1` (the Delta
  write path is not thread-safe). If that pin can't take, it now raises instead of silently
  continuing with parallel models that would collide on the shared connection and corrupt tables.
- **`ALTER TABLE … ADD COLUMN <c> <type> NOT NULL`** no longer mis-parses the type: the trailing
  `NOT NULL` is stripped whole instead of leaving `not` glued onto the type name.

### Changed
- Added debug-level traces to two previously-silent best-effort paths (drop-tombstone scan
  failures; the DuckDB-filtered overwrite fallback for `DELETE` predicates with a subquery), so
  they're visible under `--debug`.

## [0.3.27] - 2026-06-26

### Fixed
- **OneLake bearer-token refresh on long-running builds.** A build that outlives the token's ~1h
  lifetime no longer 401s mid-run. The token is captured once at connection-open, so the adapter now
  re-mints it at the universal cursor `execute()` choke point — covering not just per-model writes but
  dbt's test/end-of-run reads, which run on a reused cursor — whenever the JWT is near expiry. The
  fresh token comes from whatever live source is available: a Fabric notebook (`notebookutils`),
  `azure-identity` (Azure CLI / managed identity), or GitHub Actions workload-identity federation. A
  bare static token (`AZURE_STORAGE_TOKEN` with no live credential behind it) still can't self-refresh.

### Changed
- **Adapter version is single-sourced** from the installed package metadata, so it can no longer drift
  from `pyproject.toml`.

## [0.3.26] - 2026-06-26

### Fixed
- **`incremental_strategy='delete+insert'` is now real.** It was silently aliased to `merge`; duckrun
  now performs an actual delete (by `unique_key`) + insert and honors `incremental_predicates`,
  matching dbt-duckdb. Surfaced by the Start Data Engineering parity project.
- **Raw-DML routing hardened.** `INSERT … VALUES` vs `INSERT … SELECT` is detected correctly even when
  a `select` appears inside a string literal, and the statement scanner is dollar-quote-aware, so a
  `;` inside `COMMENT ON … IS $tag$…$tag$` (e.g. Elementary's `persist_docs`) no longer truncates the
  statement.

### Added
- **Multi-statement DML on the dbt-cursor path.** A `delete …; insert …` script (e.g. Elementary's
  delete+insert upsert) is split into its top-level statements (parenthesis- and dollar-quote-aware)
  and each is routed individually — Delta-DML to delta_rs, the rest to the DuckDB cursor. (`conn.sql`
  still runs one statement per call by design.)

## [0.3.23] - 2026-06-23

### Changed
- **`deltalake` hard-pinned to `==1.5.0`.** Every newer release breaks duckrun — `DELETE` is broken
  and OneLake support regresses — and 1.5.0 is the first with the MERGE `max_spill_size` config the
  merge path needs. Do not float until upstream fixes land.
- **`duckdb` upper cap dropped** (`>=1.5.4`, was `>=1.5.4,<1.6.0`). duckdb is only used to read; the
  floor is solely for duckdb-delta's `version =>` pin support, and newer builds read fine.
- **merge-spill recurring gate back to SF=10** (~60M rows). SF=20 (~120M) was verified once in 0.3.22
  (peak 10.5 GB on a 16 GB runner); SF=10 is enough as the per-release gate and keeps release time down.

## [0.3.22] - 2026-06-23

### Added
- **Snapshot-isolated read-modify-write through the `DeltaTable` handle.** `DeltaTable.forName` /
  `forPath` capture the table version once; `merge` / `delete` / `update` through that handle are
  pinned to it and validated under delta-rs OCC, so a conflicting concurrent commit fails loud
  (`CommitFailedError`) instead of silently interleaving. See [docs/snapshot-isolation.md](docs/snapshot-isolation.md).
- **Fenced writer modes** — `mode("append_if_unchanged")` (alias `safeappend`) and
  `mode("overwrite_if_unchanged")`: fail-loud compare-and-swap append / overwrite that commit only
  if the table version hasn't moved since the read.
- **`DeltaTable` maintenance ops** on the connection API — `vacuum`, `optimize`, `restoreToVersion`.
- **Catalog surface fill-in** — `catalog.createTable` (empty managed Delta table from DDL/StructType),
  `refreshTable`, `getTable` / `getDatabase`, `dropTempView`.
- **DataFrame / reader parity** — `df.schema` / `df.printSchema` (Spark shape, DuckDB types), more
  DataFrame actions, `read.schema` (explicit read schema for csv/json), `read.json`.

### Fixed
- Quote-safe identifiers, fail-loud primary authentication, and connection lifecycle on the
  connection API.

### Changed
- **merge-spill release gate restored to SF=20 (~120M rows)** (was SF=10 in 0.3.21).
- CI now also runs on Python 3.12.

## [0.3.21] - 2026-06-22

### Added
- **Full delta-rs `MERGE` parity on the connection API** (`conn.sql` raw `MERGE` + the
  `DeltaTable.merge` builder). Beyond the upsert subset, both surfaces now accept everything delta-rs
  `TableMerger` exposes: `WHEN MATCHED … THEN DELETE`, `WHEN MATCHED … THEN UPDATE SET col = <expr>`
  (arbitrary expressions, incl. `CASE`), `WHEN NOT MATCHED … THEN INSERT (cols) VALUES (<exprs>)`,
  `WHEN NOT MATCHED BY SOURCE … THEN UPDATE/DELETE`, **multiple clauses of the same kind in order**,
  and an arbitrary boolean `ON` predicate (multi-key / range / non-equi). The dbt incremental path and
  its single-snapshot read-pin / OCC concurrency guarantees are unchanged.
- **dbt `merge_clauses` and `merge_update_set_expressions` configs** are now honored — an ordered,
  user-specified clause list and arbitrary `SET col = expr` updates route through the same clause core.
  (`merge_returning_columns` stays rejected — delta-rs `execute()` returns metrics, not rows.)
- **Multiple catalogs in one session.** `conn.attach(path, name=…)` binds a second+ lakehouse root as
  a named catalog, so a single session reads and joins across several lakehouses by three-part
  `catalog.schema.table` name. `read_only` is **per-catalog** — a read-only reference store (e.g. a
  Fabric Warehouse, which is a write-locked Lakehouse) sits next to a writable lakehouse. New catalog
  surface: `catalog.listCatalogs()` / `currentCatalog()` / `setCurrentCatalog(name)`. The primary
  catalog's `name` is derived from the URL (else `data`); `name=` overrides it and is mandatory for a
  GUID-only OneLake path. See [`docs/connection-api.md`](docs/connection-api.md) and the
  [live demo](https://djouallah.github.io/duckrun/multicatalog.html).
- **`conn.createDataFrame(data, schema=None)`** turns in-memory data (list of tuples/scalars, pandas
  `DataFrame`, or pyarrow `Table`/`RecordBatchReader`) into a DataFrame on duckrun's own connection —
  for seeding, demos, or persisting a small result to Delta. No Spark/PySpark dependency.
- **`DeltaTable.convertToDelta(conn, ident, partitionSchema=None)`** — zero-copy conversion of existing
  parquet into Delta (writes a `_delta_log`, no data rewrite).
- **Raw SQL `MERGE` through `conn.sql`** routes to delta-rs (same engine + snapshot pin as the
  `DeltaTable.merge` builder), via the literal `target`/`source` aliases (issue #4).
- **`DeltaTable.history(limit=None)`** — delta-rs commit history (newest-first), to discover versions
  for time travel.

## [0.3.20] - 2026-06-22

### Changed
- **`connect()` is read-only by default.** Every Delta write raises `PermissionError` unless
  `read_only=False` is passed, so an accidental write can't mutate a shared lakehouse. Reads and native
  `CREATE TEMP`/`CREATE VIEW` scratch are always allowed.

### Added
- **`conn.stop()`** closes the underlying DuckDB connection.
- **`df.toArrow()`** returns a streaming `pyarrow.RecordBatchReader` (not a fully-materialized table),
  so large results don't have to fit in memory.

## [0.3.19] - 2026-06-21

### Added
- **`DataFrame.createOrReplaceTempView(name)`** — a native, ephemeral DuckDB view (not Delta, not in
  `conn.catalog`).

### Fixed
- **INSERT fails loud on lossy numeric narrowing** instead of silently truncating values that don't fit
  the target Delta column type (issue #5).

## [0.3.18] - 2026-06-21

### Fixed
- **Cleaner OneLake `delta_scan` errors**, plus a live hint that friendly workspace/lakehouse names hit
  an upstream OneLake read bug — use the GUID form.

## [0.3.17] - 2026-06-21

### Added
- **Storage-neutral `duckrun.connect()` notebook API.** A DataFrame-style surface over DuckDB +
  delta-rs (local / S3 / GCS / ADLS / OneLake) — `conn.sql`, `conn.table`, `conn.read`, `conn.catalog`,
  a `DataFrame` with `.write…saveAsTable()`, and a `DeltaTable` handle (`merge`, `delete`, `update`,
  `version`). See [`docs/connection-api.md`](docs/connection-api.md) and
  [`docs/spark-delta-parity.md`](docs/spark-delta-parity.md).
- **Raw SQL DML through `conn.sql` routes to delta-rs** (`create table as` / `insert` / `update` /
  `delete` / `alter add column` / `drop`), so it works identically on a local path and on OneLake. The
  invariant: every `CREATE TABLE` is Delta-backed; only `CREATE TEMP TABLE` / `CREATE VIEW` stay native.
- **Snapshot pinning by default.** Incremental writes (`merge`, and `mode("safeappend")`) capture the
  target version and validate the commit against it, so a concurrent writer fails loud
  (`CommitFailedError`) rather than silently interleaving (issue #1).
- **Delta-backed dbt snapshots** (`snapshot` materialization via MERGE on `dbt_scd_id`).

### Changed
- **Requires `duckdb` ≥ 1.5.4** (newer than Fabric's bundled stable build) and `deltalake` ≥ 1.5.0;
  `connect()` fails loud with a version guardrail otherwise.

## [0.3.16] - 2026-06-12

### Added
- **dbt sources via the `duckrun` plugin can now read CSV and Parquet, not just Delta.** A source
  with `meta: {plugin: duckrun}` resolves a Delta table (`delta_table_path`), or any `location`
  whose `format` is `csv` / `parquet` / `delta` (inferred from the file extension when `format` is
  omitted). A source declares *location + format* only — CSV parsing is left to `read_csv_auto`'s
  detection; hand-tuned parse options belong in a model's `read_csv(...)`, not the source.

### Fixed
- **Plugin sources failed with `... created by another Connection`.** dbt-duckdb registers the
  plugin's returned `DuckDBPyRelation` and re-registers it on every new per-handle cursor; a
  `DuckDBPyRelation` is bound to its creating connection, so the re-registration threw (and a
  read-only command could miss it entirely). duckrun now registers a plugin source as a
  connection-independent **catalog view** (`CREATE OR REPLACE VIEW … AS delta_scan/read_csv_auto/
  read_parquet(…)`) — the same way it surfaces model Delta tables — so it resolves on every cursor
  and is rebuilt in a fresh process, with no pyarrow and no copying the source into a table.
  Thanks to **Jose Marquez** for reporting the bug.
- **Azure transport for OneLake/ADLS is now set at connection-open**, alongside the bearer-token
  secret in the adapter, instead of relying on a run-only `on-run-start` hook. Read-only commands
  that still open the store — `dbt test` / `show` / `docs generate` — now get the configured
  `azure_transport_option_type` too (driven by `AZURE_TRANSPORT_OPTION_TYPE`; absent → DuckDB's
  default), fixing a OneLake `Problem with the SSL CA cert` failure on `docs generate`.

## [0.3.15] - 2026-06-11

### Fixed
- **Merge: stop silently ignoring valid-but-unsupported config.** A merge config that *passed*
  shape validation but used a key delta-rs can't express (`merge_clauses`,
  `merge_update_set_expressions`, `merge_on_using_columns`) was accepted and then quietly run as a
  plain upsert — a green run that ignored what the user asked for (the same silent-divergence class
  as the WS1 data-loss fix). These keys are now **rejected** with a clear error naming the supported
  alternatives, instead of being dropped.

### Added
- **Merge: honor `merge_update_condition` / `merge_insert_condition`.** These are now applied as
  delta-rs per-clause predicates (gating which matched rows update and which unmatched rows insert),
  rather than ignored.

## [0.3.14] - 2026-06-10

### Fixed
- **Data-loss fix (incremental writes):** `engine.table_exists` swallowed every exception and
  returned `False`, so a *transient* storage error (ADLS/OneLake 503, expired token) at store time
  looked like "no table" and sent an incremental write — already filtered to only-new rows — down
  the overwrite branch, replacing the whole table with just the increment. It now catches only
  `TableNotFoundError`; every other error propagates and fails the run loudly. `delta_version` is
  narrowed the same way (a swallowed error would degrade `safeappend`'s start-of-build pin), and
  `store()` refuses to overwrite when dbt resolved the model as incremental but the table can't be
  opened at store time. Audited every `except Exception` in the adapter to narrow or justify it.

### Added
- **Merge config validation:** invalid merge configs now fail fast with clear messages (ported from
  dbt-duckdb's `validate_merge_config`) before any Delta access, instead of a late generic delta-rs
  "Schema error".
- **Model contracts / constraints:** `config(contract={enforced:true})` now enforces column
  name/type/count (dbt's `assert_columns_equivalent` preflight) and `not null` (a pre-write guard
  on the staged rows that leaves the prior table intact on violation). `check`/`primary_key`/
  `foreign_key` are declared but not enforced against a `delta_scan` view.
- **persist_docs:** model and column descriptions are written into the Delta table's own metadata
  (`set_table_description` / `set_column_metadata`) and re-applied as `COMMENT ON` whenever the view
  is registered, so `dbt docs generate` reports real comments across processes.
- **Catalog:** Delta-backed relations are reported as `BASE TABLE` (not `VIEW`) in `dbt docs
  generate` / `get_catalog`.

### Conformance
- dbt-tests-adapter pass rate raised from 92/135 to 114/135, with a per-push regression gate.

## [0.2.26] - 2026-01-13

### Added
- **`schedule_notebook()`**: Schedule notebooks to run automatically in Microsoft Fabric
  - Supports `interval`, `daily`, `weekly`, and `monthly` schedule types
  - `interval`: Run every X minutes (e.g., `interval_minutes=60` for hourly)
  - `daily`: Run at specific times each day (e.g., `times=["09:00", "18:00"]`)
  - `weekly`: Run on specific days (e.g., `weekdays=["Monday", "Friday"]`)
  - `monthly`: Run on specific day of month (e.g., `day_of_month=1`)
  - `overwrite=False` by default - prevents accidental schedule overwrites
  - Available on connection: `con.schedule_notebook("notebook_name", ...)`

### Note
- Fabric does NOT support traditional cron expressions - uses interval/daily/weekly/monthly instead

## [0.2.17] - 2025-11-01

### Added
- **ZSTD Compression by Default**: All Delta Lake writes now use ZSTD compression instead of Snappy
  - Achieves 30-40% better compression ratios than Snappy
  - Reduces storage costs in OneLake/cloud environments
  - Automatic detection for both PyArrow (0.18.2-0.19.x) and Rust engines (0.20+)
  - Works seamlessly with schema merging, partitioning, and row group optimization

- **Expanded OneLake Connectivity**: Can now connect to multiple Microsoft Fabric item types:
  - Lakehouses (Read/Write)
  - Data Warehouses (Read)
  - Databricks Mirrored Databases (Read)
  - Any OneLake-enabled Fabric item with Delta tables

- **OneLake API Integration**: Now uses OneLake API to List table (no more path parsing)

- **Compression Stats**: Stats now display compression codec information for Delta tables

### Changed
- Refactored writer code to eliminate duplication between `writer.py` and `runner.py`
  - Single source of truth for Delta Lake write configuration
  - Both DataFrame-style API (`.write.saveAsTable()`) and pipeline runner (`run()`) now share the same compression logic


