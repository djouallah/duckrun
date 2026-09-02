# Overview

duckrun is **glue over DuckDB + delta-rs + dbt-duckdb**: a notebook `connect()` for SQL over Delta,
and a dbt adapter that materializes models as Delta tables, on one engine split.

## How it works

DuckDB runs every query and reads Delta through `delta_scan` views; delta-rs handles every write;
an Arrow C-stream bridges them; dbt orchestrates on top.

![duckrun architecture: DuckDB executes SQL and reads Delta via delta_scan; an Arrow C-stream bridges to delta-rs, which handles every write and commits against the read version (OCC); dbt orchestrates on top](architecture.png)

Writes are **snapshot-pinned**: the read is fixed at `delta_scan(…, version => N)` and the write
commits against `N`, so a concurrent commit is rejected with `CommitFailedError` instead of silently
overwriting a lost update.

![Two writers race on one table: Writer A reads v5 and computes; Writer B commits v6 in between; A's commit against v5 is rejected with CommitFailedError instead of silently overwriting B](snapshot-timeline.png)

The full model is in [Snapshot isolation](snapshot-isolation.md); the engine-split rationale in the
[Design document](design_document.md).
