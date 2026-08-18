# Limitations

An honest, consolidated list of what duckrun doesn't do — by design, by an upstream constraint, or
by deliberate caution. Most come with a "do this instead." Deeper detail lives in the linked pages.

## Gaps vs dbt-duckdb

duckrun aims to be a **drop-in for dbt-duckdb** — same DuckDB SQL, same model and config spelling —
and [parity](parity.md) proves that by running real, unmodified `type: duckdb` projects verbatim
on duckrun, their own tests included. These are the places where it is *not* the same, in full:

| gap | what happens | why |
|---|---|---|
| `type: duckrun` is its own adapter type | an existing project's **profile** must say `type: duckrun`; a `type: duckdb` profile cannot be rerouted | dbt selects the adapter from `type`. The project itself needs no edit — in dbt the profile lives outside it (that's how the parity runs work, via `--profiles-dir`) |
| `materialized='view'` | runs, but is a connection-scoped DuckDB view — nothing on storage, gone next session; swapping a model `table` ⇄ `view` isn't supported | Delta defines no view ([below](#materializations)) |
| `materialized='table_function'` | **errors** — duckrun doesn't ship it | a DuckDB table macro is catalog-only state, and duckrun's DuckDB is in-memory, so it could only ever live for the length of one run |
| `threads` | honored, but concurrent writers share one memory budget; a microbatch model's batches still run in order | DuckDB's `memory_limit` is per database, not per model, and a microbatch model's batches all write the same table ([below](#dbt-incremental)) |
| `on_schema_change='sync_all_columns'` | only *adds* columns | delta-rs can't drop columns ([below](#schema-constraints)) |
| `merge_on_using_columns`, clause `action: error` | rejected with a clear error | no delta-rs equivalent — refusing beats silently running something else ([below](#dbt-incremental)) |
| `merge_returning_columns` | accepted and ignored | duckrun never surfaces a returned relation, so it changes no table state |
| naive `TIMESTAMP` columns | written **UTC-adjusted** (Delta `timestamp`) by default, where dbt-duckdb/delta-rs write `timestamp_ntz`; `timestamp_ntz: true` (model config) or `DUCKRUN_TIMESTAMP_NTZ=1` restores the verbatim write | Fabric's SQL analytics endpoint silently **omits** `timestamp_ntz` columns (issue #42) — duckrun's default keeps the table fully readable there ([below](#microsoft-fabric--onelake)) |

Everything else carries over: `DuckrunCredentials` subclasses dbt-duckdb's, so the whole profile
surface (`attach`, `secrets`, `settings`, `extensions`, `plugins`, `filesystems`, `remote`, `retries`,
`external_root`, `module_paths`, `disable_transactions`, …) is the same object, not a reimplementation.
Seeds, snapshots, native `unit_tests:`, data tests, exposures, python models, `external_location`
sources, `materialized='external'` (dbt-duckdb's macro, shipped under duckrun's adapter name — see
[below](#materializations)), and the full merge-config surface (`merge_clauses` — including `do_nothing`, the implicit
clause defaults, `mode`, `by: source`, `insert: {columns, values}` — and
`merge_update_set_expressions`) all behave as they do upstream. duckrun's own additions
(`incremental_strategy='insert'`, `partition_by`, `sort_by`, `location`, `catalogs`) are a superset:
a dbt-duckdb project never has to use them. dbt-duckdb 1.11's canonical `partitioned_by` /
`sorted_by` spellings are accepted as aliases of `partition_by` / `sort_by` (same precedence as
upstream, canonical name first) — with one behavior difference: upstream applies them only to
DuckLake tables (warn-and-ignore otherwise), duckrun applies them to its Delta tables, which is
what a duckrun model means by partitioning/sorting. The other dbt-duckdb 1.11 additions
(DuckLake `partitioned_by`/`sorted_by` alter flows, DuckLake transactions) are DuckLake-specific
and don't apply to a Delta-backed adapter. Test-by-test detail is in
[Conformance](conformance.md).

## Setup & versions

- **Needs `duckdb >= 1.5.4`.** Older builds — including Microsoft Fabric's bundled stable runtime —
  fail loud at `connect()`. In a Fabric notebook: `!pip install duckrun --upgrade` then restart.
- **Pins `deltalake == 1.5.0`.** delta-rs 1.6.0's MERGE is broken at scale, so duckrun stays on 1.5.0.

## Microsoft Fabric / OneLake

- **delta-rs `> 1.5.0` breaks bulk delete on OneLake.** Since 1.5.1 the batch-delete path drops the
  workspace/artifact ids (*"Either WorkspaceId or ArtifactId are missing in the request"*), so
  `vacuum` and other multi-file deletes fail against OneLake. This is a major reason duckrun pins
  `deltalake == 1.5.0`. See [delta-rs #4401](https://github.com/delta-io/delta-rs/issues/4401).

- **Naive `TIMESTAMP` columns are UTC-coerced on write** (issue #42). Fabric's SQL analytics
  endpoint does not support Delta `timestamp_ntz`: the table appears, but the column is silently
  missing and any T-SQL naming it fails with *Invalid column name*. So by default duckrun rewrites
  a naive timestamp as `timezone('UTC', col)` — the naive value read as a **UTC wall clock**,
  independent of the session `TimeZone` — before handing data to delta-rs, landing it as plain
  Delta `timestamp`. Details and edges:
  - `timestamp_ntz: true` (per model) or `DUCKRUN_TIMESTAMP_NTZ=1` (whole run / connection API)
    keeps the old verbatim `timestamp_ntz` write.
  - A **pre-existing** `timestamp_ntz` column is matched, not broken: appends/merges skip the
    coercion for that column and warn once; a full rebuild (`--full-refresh` /
    `CREATE OR REPLACE`) retypes it. Note that any full-rewrite operation goes through the same
    seam — the raw-SQL `DELETE`/`UPDATE` subquery fallbacks and `ALTER` rewrites also retype.
  - Timestamps **nested** in a `STRUCT`/`LIST` are out of scope and still land as
    `timestamp_ntz` (top-level columns only).
  - A raw-SQL `INSERT` of a naive value **into an existing tz-aware column** keeps DuckDB's
    native cast semantics (interpreted in the session `TimeZone`), exactly as it would in plain
    DuckDB — the UTC read applies where duckrun itself decides the type, not to DuckDB's own
    column-cast rules.

- **`get_stats` is slow on tables with deletion vectors.** A parquet footer counts rows a DV has
  already removed, so `total_rows` subtracts the DV total to stay equal to `SELECT COUNT(*)` — and
  the only delta-rs API for that total, `DeltaTable.deletion_vectors()`, expands each bitmap into
  one boolean per row. That is ~10s on a 150M-row table, to recover a number the Delta log already
  stores verbatim as `add.deletionVector.cardinality`; `get_add_actions()` just doesn't surface it.
  **TODO:** read the cardinality from the log instead (needs handling parquet checkpoints too), or
  get a `cardinality` column onto `get_add_actions` upstream. Only affects tables written by Fabric
  Warehouse / Spark — delta-rs rewrites files rather than emitting a DV, and tables whose protocol
  doesn't declare the `deletionVectors` reader feature skip the read entirely.

## SQL DML (`conn.sql`)

- **`UPDATE … FROM` and `DELETE … USING` are rejected** → rewrite as a correlated subquery.
- **One statement per `conn.sql()` call** — multiple statements in a single call are rejected.

The full accepted/rejected matrix is in the [Connection API](connection-api.md#raw-sql-dml-through-connsql).

## dbt & incremental

- **`threads` is honored**, as in dbt-duckdb — dbt's default of `1` applies when the profile omits
  it. Two things behave differently from a pure-SQL adapter, both because a duckrun model writes a
  real table rather than a row set:
    - **Concurrent writers share one memory budget.** DuckDB's `memory_limit` applies to the
      database, not to a model, so duckrun pins it once for the run (85% of the container-aware
      effective limit) — no model sets its own. delta-rs merges are serialized: at most one runs
      at a time and holds the **full** merge pool and disk spill cap, while the other threads keep
      running everything else (views, appends, overwrites, insert-only merges).
      A run of many genuinely small merges therefore won't overlap them; many independent,
      network-bound models benefit most from more threads.
    - **A microbatch model's batches run in order**, even at higher thread counts, because every
      batch writes the same table: they'd serialize on the Delta log anyway. Different *models*
      still run in parallel.
- None of this is a limit on concurrent **writers**: separate runs / notebooks / jobs writing the
  same tables at once is fully supported and safe (every write is snapshot-pinned and fails loud on
  a conflict).
- **Two dbt merge configs are rejected, on purpose.** `merge_on_using_columns` and a clause
  `action: error` are dbt-duckdb spellings with no delta-rs equivalent (delta-rs can't join on a
  USING column list, and can't raise from a merge clause), so duckrun raises a clear error rather
  than silently running something else. The rest of dbt-duckdb's merge surface — `merge_clauses`
  (including `do_nothing` and its implicit clause defaults) and `merge_update_set_expressions` — is
  translated and honored.

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
- **`materialized='external'` needs an on-run-start hook to be visible to a later run.** External
  models work as they do upstream — the model is written as a real parquet/csv/json file by DuckDB's
  `COPY … TO` and surfaced as a view over that file — but duckrun's disk discovery only rebuilds
  **Delta** tables, so the view is gone once the process ends. As on dbt-duckdb, a run that reads an
  external model without rebuilding it (`dbt run --select some_downstream_model`) needs
  `on-run-start: "{{ register_upstream_external_models() }}"` in `dbt_project.yml`. Note also that
  `location` means the output **file** for an external model, and the **Delta table path** for
  `table`/`incremental`.
- **`DROP TABLE` is a soft tombstone, not a physical delete.** `conn.sql("drop table x")` unregisters
  the table and writes a tombstone marker but **does not reclaim the data files** (a deliberate
  precaution — you purge them when you're sure). Address dropped tables by name, not by path.

## Parquet layout

- **`SORTED BY AUTO` picks the key with a naive, lightly-tested heuristic** (also reachable from dbt
  as the model config `sort_by: auto`). The auto sort-key picker
  is a cheap greedy single pass over statistical *sketches* (approximate cardinalities, HyperLogLog
  functional-dependency tests) — a stack of rules of thumb, each of which can be wrong on a given
  distribution. It is not guaranteed to shrink anything and can occasionally pick a worse key than the
  table's natural arrival order, and it has been validated against essentially one dataset, not a broad
  workload sample. When you know your grain and query patterns, prefer an explicit `SORTED BY (cols)`,
  and always compare `conn.get_stats()` before and after. See
  [Automatic sorting](parquet-layout.md#automatic-sorting).
- **Profiling stages the whole source into a local temp table.** It reads the source once and every
  pass scans that copy, which is what keeps the cost off remote storage — but it does mean a full
  local copy of the table (or model result) exists for the duration of the write. It spills to
  DuckDB's `temp_directory` like any other result, so the ceiling is disk rather than RAM; on a very
  large table plan for that disk. An explicit `SORTED BY (cols)` skips profiling entirely.
- **The write geometry is fixed, not adaptive.** Every normal write — overwrite, append,
  `replaceWhere`, and compaction alike — uses the same constants: an **8M-row** row-group ceiling
  (Power BI's native segment size) and a **128 MB** target file size. Nothing is derived from the
  result: no planner estimate, no `count(*)`, no prior-log probe. The cost of the fixed rule is that a
  small table lands fewer, larger segments than an adaptive scheme would give it; the win is that no
  write pays a plan walk or a count, and no bad estimate can mis-size a table. The two exceptions:
  `SORTED BY AUTO` / `sort_by: auto` derives its geometry from the exact count its profile already
  paid for, and the model configs `max_row_group_size` / `target_file_size_mb` declare a geometry
  explicitly and are honored verbatim. See the
  [model config reference](dbt-adapter.md#config-options-table--incremental--delta).

## Memory

- **Two engines share one machine's memory.** DuckDB and delta-rs each keep their own pool in the same
  process. duckrun bounds them independently — DuckDB by one pinned `memory_limit`, a delta-rs merge
  by its own spill caps behind a gate — but a bound is not a shared allocator, and delta-rs's merge
  spill-to-disk is itself flaky. Background in the [Design document](design_document.md). The merge
  pool exists only for the `merge` strategy and the raw `MERGE INTO` verb; `insert`, `delete+insert`,
  `append`, `microbatch` and `overwrite` never run one.
- **`merge` is the write path most likely to run out of memory, and the ceiling is not duckrun's.**
  A delta-rs `MERGE` plans a join against the whole pinned target, so its cost scales with the
  target's *partition span*, not the size of the batch — a batch straddling many monthly partitions
  can exhaust a very large machine. If your model only ever needs to add rows it has never seen (a
  key-level idempotent append), use `incremental_strategy='insert'`: duckrun computes that as a
  DuckDB anti-join and commits a plain append, with no delta-rs merge and no file rewritten. For a
  genuine upsert there is no way around removing old row versions, which means rewriting files — keep
  each batch inside as few partitions as possible.
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
