# Limitations

What duckrun doesn't do, by design or by upstream constraint, with the workaround where one exists.

## Gaps vs dbt-duckdb

duckrun is a **drop-in for dbt-duckdb** — same DuckDB SQL, same model and config spelling — and [parity](parity.md) proves it by running real, unmodified `type: duckdb` projects verbatim. These are the differences, in full:

| gap | what happens | why |
|---|---|---|
| `type: duckrun` is its own adapter type | the **profile** must say `type: duckrun`; the project itself needs no edit | dbt selects the adapter from `type`; the profile lives outside the project |
| `materialized='view'` | runs, but is a session-scoped DuckDB view — nothing on storage; swapping a model `table` ⇄ `view` isn't supported | Delta defines no view ([below](#materializations)) |
| `materialized='table_function'` | **errors** | a DuckDB table macro is catalog-only state, and duckrun's DuckDB is in-memory |
| `threads` | honored, but concurrent writers share one memory budget and a microbatch model's batches run in order | `memory_limit` is per database, and every batch writes the same table ([below](#dbt-incremental)) |
| `on_schema_change='sync_all_columns'` | only *adds* columns | delta-rs can't drop columns |
| `merge_on_using_columns`, clause `action: error` | rejected with a clear error | no delta-rs equivalent; refusing beats silently running something else |
| `merge_returning_columns` | accepted and ignored | duckrun never surfaces a returned relation |
| naive `TIMESTAMP` columns | written **UTC-adjusted** (Delta `timestamp`) by default; `timestamp_ntz: true` or `DUCKRUN_TIMESTAMP_NTZ=1` restores the verbatim write | Fabric's SQL analytics endpoint omits `timestamp_ntz` columns ([below](#microsoft-fabric-onelake)) |

Everything else carries over: `DuckrunCredentials` subclasses dbt-duckdb's, so the whole profile surface (`attach`, `secrets`, `settings`, `extensions`, `plugins`, `filesystems`, `remote`, `retries`, `external_root`, `module_paths`, `disable_transactions`, …) is the same object. Seeds, snapshots, `unit_tests:`, data tests, exposures, python models, `external_location` sources, `materialized='external'`, and the full merge-config surface (`merge_clauses` with its implicit defaults, `mode`, `by: source`, `insert: {columns, values}`, `merge_update_set_expressions`) behave as upstream. duckrun's additions (`incremental_strategy='insert'`, `partition_by`, `sort_by`, `location`, `catalogs`) are a superset; dbt-duckdb 1.11's `partitioned_by` / `sorted_by` are accepted as aliases, applied to Delta tables rather than DuckLake ones. Test-by-test detail is in [Conformance](conformance.md).

## Setup & versions

- **Needs `duckdb >= 1.5.4`.** Older builds, including Microsoft Fabric's bundled runtime, fail loud at `connect()`. In a Fabric notebook: `!pip install duckrun --upgrade`, then restart.
- **Pins `deltalake == 1.5.0`.** delta-rs 1.6.0's MERGE is broken at scale, and delta-rs `> 1.5.0` breaks bulk delete on OneLake (*"Either WorkspaceId or ArtifactId are missing in the request"*, [delta-rs #4401](https://github.com/delta-io/delta-rs/issues/4401)), which `vacuum` needs.

## Microsoft Fabric / OneLake

- **Naive `TIMESTAMP` columns are UTC-coerced on write** (issue #42). Fabric's SQL analytics endpoint does not support Delta `timestamp_ntz`: the column is silently missing and T-SQL naming it fails with *Invalid column name*. So duckrun rewrites a naive timestamp as `timezone('UTC', col)` — the naive value read as a UTC wall clock, independent of the session `TimeZone` — and lands it as Delta `timestamp`.
  - `timestamp_ntz: true` (per model) or `DUCKRUN_TIMESTAMP_NTZ=1` (whole run / connection API) keeps the verbatim `timestamp_ntz` write.
  - A **pre-existing** `timestamp_ntz` column is matched: appends / merges skip the coercion for it and warn once; a full rebuild (`--full-refresh` / `CREATE OR REPLACE`, and the raw-SQL `DELETE` / `UPDATE` fallbacks and `ALTER` rewrites) retypes it.
  - Timestamps nested in a `STRUCT` / `LIST` still land as `timestamp_ntz`.
  - A raw-SQL `INSERT` of a naive value into an existing tz-aware column keeps DuckDB's own cast semantics (session `TimeZone`).
- **`get_stats` is slow on tables with deletion vectors.** `total_rows` subtracts the DV total to stay equal to `SELECT COUNT(*)`, and the only delta-rs API for that total expands every bitmap (~10 s on a 150M-row table). Only tables written by Fabric Warehouse / Spark carry DVs; delta-rs rewrites files instead.

## SQL DML (`conn.sql`)

- **`UPDATE … FROM` and `DELETE … USING` are rejected** → rewrite as a correlated subquery.
- **One statement per `conn.sql()` call.**

The full matrix is in the [Connection API](connection-api.md#raw-sql-dml-through-connsql).

## dbt & incremental

- **`threads` is honored**, with two differences from a pure-SQL adapter, both because a model writes a real table:
    - **Concurrent writers share one memory budget.** `memory_limit` is pinned once for the run (85% of the container-aware effective limit). delta-rs merges are serialized: one at a time, holding the full merge pool and spill cap, while other threads keep running views, appends, overwrites and insert-only merges. Many independent network-bound models benefit most from more threads.
    - **A microbatch model's batches run in order**, since every batch writes the same table. Different models still run in parallel.
- None of this limits concurrent **writers**: separate runs writing the same tables at once is supported, every write being snapshot-pinned and failing loud on a conflict.

## Schema & constraints

- **Schema evolution is add-only.** delta-rs can't drop columns, so `sync_all_columns` only adds them; use `append_new_columns` or `fail`.
- **Only `not null` is enforced.** `check` / `primary_key` / `foreign_key` are declared, not checked.

## DuckDB catalog

- **Delta tables are views.** DuckDB has no foreign-table abstraction, so each Delta table is a `CREATE VIEW` over `delta_scan(...)` and writes are routed to delta-rs at the cursor.

## Materializations

- **No persistent views.** The Delta spec defines no view, so a `materialized='view'` model is a DuckDB catalog view that lives only in the session that built it.
- **`materialized='external'`** needs `on-run-start: "{{ register_upstream_external_models() }}"` for a later run that reads it without rebuilding it — see the [dbt adapter](dbt-adapter.md#external).
- **`DROP TABLE` is a soft tombstone.** `conn.sql("drop table x")` unregisters the table and writes a marker but does not delete the data files; purge them yourself when sure.

## Parquet layout

- **`SORTED BY AUTO` (dbt: `sort_by: auto`) is a greedy heuristic** over approximate cardinalities and HyperLogLog dependency tests, validated against essentially one dataset. It is not guaranteed to shrink anything and can pick a worse key than arrival order. Prefer `SORTED BY (cols)` when you know the grain, and compare `conn.get_stats()` before and after. See [Automatic sorting](parquet-layout.md#automatic-sorting).
- **Profiling stages the source locally.** Every row up to 30M, a deterministic hash-selected ~30M-row substrate above that (`DUCKRUN_PROFILE_ROWS`; `0` = always exact), staged into a temp table that spills to DuckDB's `temp_directory`, so disk is the ceiling. An explicit key skips profiling.
- **The write geometry is fixed**: a 6M-row row-group ceiling and a 256 MB target file for every write, nothing derived from the result. `max_row_group_size` / `target_file_size_mb` override it per model. See [Write settings](parquet-layout.md#write-settings).

## Memory

- **Two engines share one machine's memory.** DuckDB is bounded by one pinned `memory_limit`, a delta-rs merge by its own spill caps behind a gate; neither is a shared allocator. The merge pool exists only for the `merge` strategy and the raw `MERGE INTO` verb. Background in the [Design document](design_document.md#tradeoffs).
- **`merge` is the write path most likely to run out of memory.** A delta-rs `MERGE` plans a join against the whole pinned target, so its cost scales with the target's partition span, not the batch. For a key-level idempotent append use `incremental_strategy='insert'` (a DuckDB anti-join, no merge, no file rewritten). For a genuine upsert, keep each batch inside as few partitions as possible.
- **delta-rs hard-codes a 100 GB merge disk-spill ceiling.** DataFusion's `DiskManager` caps on-disk spill at a flat 100 GB regardless of disk size, so a wide merge aborts with *"Resources exhausted … exceeded the allowable limit of 100.0 GB"* on a machine with terabytes free. duckrun sizes `max_temp_directory_size` to the spill disk's free space minus `min(20% of free, 8 GiB)` on every merge (override with `merge_max_temp_directory_size`). The real fix is layout: one partition per batch. See [MERGE at scale](merge-benchmark.md).

---

Test-by-test: [Conformance](conformance.md). Why the trade-offs exist: [Design document](design_document.md#tradeoffs).
