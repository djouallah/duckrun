# Changelog

All notable changes to this project will be documented in this file.

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


