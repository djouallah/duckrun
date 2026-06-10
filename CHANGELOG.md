# Changelog

All notable changes to this project will be documented in this file.

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
  - Both Spark-style API (`.write.saveAsTable()`) and pipeline runner (`run()`) now share the same compression logic


