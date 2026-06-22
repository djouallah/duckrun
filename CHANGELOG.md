# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
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


