# Design: Delta-backed dbt materializations via delta-rs + `delta_scan` views

## Context

duckrun is **glue over DuckDB + dbt-duckdb + delta-rs**. It exists to give dbt Delta Lake support, writing through delta-rs, and to expose the same engine as a SQL-first notebook API (`conn.sql(...)`). It is not an engine and not a DataFrame library: no transform builder, no second SQL dialect; transforms are DuckDB SQL.

All state lives in Delta Lake. Every write goes through delta-rs (`engine.py` + `delta_plugin.py`); DuckDB only reads and runs model logic. Each Delta table is surfaced to dbt as a `delta_scan` view named `database.schema.identifier`, which is what makes `{{ this }}`, `ref()` and `is_incremental()` resolve against real Delta tables across separate `dbt build` processes. Views are recreated at run start by discovering tables on disk, and a table built mid-run becomes visible when its own materialization recreates the view. `location` is deterministic: `root_path/<schema>/<identifier>` or `config(location=…)`.

## Why these choices

- **delta-rs, not DuckDB's Delta writer.** DuckDB's writer is blind `INSERT` only — no `UPDATE` / `DELETE` / `MERGE` — and its direction is writing through Unity Catalog, which defeats Delta's filesystem simplicity.
- **Delta as the write format.** Iceberg writers still need to mature; a POC stalled on table maintenance. `connect(format='iceberg')` reads and writes a Fabric Lakehouse's Iceberg REST catalog through DuckDB natively, with none of the machinery on this page involved.
- **A separate adapter, not a dbt-duckdb PR.** Writing Delta needs the `deltalake` package, and dbt-duckdb deliberately keeps its dependency footprint minimal.
- **A `delta_scan` view, not per-table `ATTACH (TYPE delta)`.** A single-table attach is its own catalog and cannot sit inside dbt's three-part `lake.mart.dim_duid`, so `{{ this }}` would not resolve; a view can be created with the exact name dbt expects and always reads the latest `_delta_log` snapshot, which `is_incremental()` needs.

## The invariant

| Concern            | Mechanism                                                        |
|--------------------|-----------------------------------------------------------------|
| Write Delta        | delta-rs (`engine.write_delta` / `engine.merge_delta`)            |
| Read a Delta table | `delta_scan('<location>')`                                       |
| `{{ this }}` / `ref()` / `is_incremental()` | a DuckDB **view** `db.schema.id` over `delta_scan('<location>')` |
| New table mid-run  | `CREATE OR REPLACE VIEW …` — no attach                           |
| Cross-process state| views recreated at run start from the Delta tables on disk       |

## Design

### 1. Disk discovery → relation cache + read-path views (`impl.py`)

dbt populates its relation cache at run start by calling `list_relations_without_caching(schema_relation)` for every schema in the manifest. For each call duckrun enumerates the Delta table directories under `root_path/<schema>`:

- **Local / `az://`** — a DuckDB `glob('<base>/*/_delta_log/*.json')` (`*.json`, since `00…0.json` is gone after `cleanup_metadata()`); separators are normalized before splitting on `/_delta_log/`.
- **OneLake / `abfss://`** — DuckDB cannot glob `abfss://` (duckdb-azure#174), so directories are listed with the OneLake DFS REST API. A REST listing can name a directory that holds parquet but no `_delta_log` (an interrupted write), so a directory delta-rs failed to open is confirmed with `remote.has_delta_log` before it becomes a relation. Only a positive "no log" answer drops it: a wrongly dropped relation would flip `is_incremental()` off and clobber the table.

Relations are returned with `type=Table` — the physical object is a view, but dbt-core's `is_incremental()` requires `relation.type == 'table'`. Discovery also registers the `delta_scan` view for each table so read-only commands (`dbt test` / `show` / `docs`) have something to query. Views created on that connection do not survive into the model-run phase, so the run-phase `{{ this }}` view is pre-registered separately (step 2). If `root_path` is unset or nothing is found, `super()`'s result is returned unchanged.

### 2. Materialization (`_delta_core.sql`)

`duckrun__build_delta()`: if the Delta table exists, pre-register `{{ this }}` as a `delta_scan` view *before* `run_hooks`, so pre-hooks and the model's own self-reference resolve on the run-phase connection; create the schema; stage the model as a view; hand it to the delta-rs plugin; drop the staging view; then `create or replace view {{ target_relation }} as select * from delta_scan('<location>')` and `persist_docs`. A new table is immediately visible to downstream `ref()` in the same run. `table.sql`, `incremental.sql` and `delta.sql` all call it.

### 2b. Plugin reads on the model's cursor (`delta_plugin.py`)

The staged view, and any `SET VARIABLE` a pre-hook set, live in the session of the cursor dbt ran the model on — dbt-duckdb gives each model its own child cursor via `configure_cursor`. The plugin overrides `configure_cursor(cursor)` to keep that cursor and reads on it in `store()` / `load()`; a fresh child cursor would see `getvariable(...)` as `NULL`.

### 3. Memory: one pin + the delta-rs spill caps (`engine.py`) { #3-memory-one-cap-split-across-two-engines }

DuckDB and delta-rs each manage their own memory. Profiling (`DUCKRUN_MEM_PROFILE`) shows that during a merge delta-rs holds ~99% of process RSS, so duckrun does not divide a budget; it applies two independent guardrails:

- **One DuckDB `memory_limit` pin** per connection: `_MEM_LIMIT_FRACTION` (0.85) of the effective limit, tighten-only. DuckDB's own default is 80% of *host* RAM, blind to cgroups, which gets the process OOM-killed on Fabric / k8s.
- **The delta-rs merge spill caps**: `max_spill_size` at `_MERGE_SPILL_FRACTION` (0.6) of the effective limit for the in-memory pool, `max_temp_directory_size` for on-disk spill.

0.85 + 0.6 deliberately exceed 1.0: the two never peak together, and merges are serialized (`engine._MERGE_GATE`) so one merge holds the whole pool while other threads run the cheap paths. The effective limit (`_effective_mem_limit_bytes`) is the tightest of physical RAM, the cgroup cap and the RAM actually free.

## Cross-process state

1. **Empty store**: discovery finds nothing → `is_incremental()` false → delta-rs overwrites; each model ends as a `delta_scan` view.
2. **Populated store, fresh process**: discovery caches existing tables → `is_incremental()` true; the materialization pre-registers `{{ this }}`; incremental models merge / append via delta-rs.

## Tradeoffs

Two engines across one write cost what a single native engine would not:

- **Memory across two independent systems is a hack.** No shared allocator; a static pin plus per-merge caps derived from a cgroup-aware limit. Size the merge cap wrong and you starve the pool (`Resources exhausted`) or get OOM-killed. One engine spills against its own true peak and needs no tuning constant.
- **The Arrow bridge is not truly zero-copy.** DuckDB's vector format is not Arrow, so producing the stream materializes results into Arrow buffers first.
- **Arrow in memory is uncompressed.** Data crosses the boundary raw, then delta-rs re-encodes and compresses it.
- **Two Parquet readers.** DuckDB reads via `delta_scan`; delta-rs reads and writes the log and files independently.

A single native engine reading and writing Delta would win on the write path and on predictability. It does not exist for the upsert workloads this adapter serves, so the boundary is a deliberate, temporary cost: writes are isolated behind delta-rs and reads behind `delta_scan`, so the writer can be swapped later without touching the read/state model.
