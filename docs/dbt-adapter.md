# duckrun dbt adapter

duckrun is a thin wrapper around [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb): you keep
everything dbt-duckdb gives you — views, seeds, sources, tests, snapshots, the full plugin ecosystem
— and gain a Delta-backed `table` / `incremental` materialization that writes real Delta tables via
delta_rs. The design rationale (why delta_rs, why Delta not Iceberg, why a separate adapter) is in
[design_document.md](design_document.md).

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # No `threads:` needed — duckrun always runs single-threaded.
      # DuckDB runs in-memory by default — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...).
      # OneLake — address by GUID, not friendly names (see "OneLake: use GUID paths" below):
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables"
      # Or any other store: './warehouse' (local), 's3://...', 'gs://...'.
      # storage_options: {}      # passed through to deltalake for remote stores
```

Persisted models are written to `<root_path>/<schema>/<model>` (e.g. `.../Tables/dbo/orders`), or to
an explicit `config(location=...)`.

### OneLake: use GUID paths for now

Address OneLake tables by **workspace GUID + lakehouse GUID**, not friendly names —
`abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/...`. This sidesteps an
upstream `duckdb-delta` read bug ("No files in log segment") that is **already fixed upstream but
still rolling out to production OneLake**. Friendly-name paths will work again once the fix finishes
deploying.

### Fabric Lakehouse without a schema

A schema-less Lakehouse (tables straight under `Tables/`, no `Tables/<schema>/` grouping) is a **bad
pattern** — you lose the namespace that keeps a warehouse organized — but if you're stuck with one,
no special config is needed. Drop the trailing `Tables` from `root_path` and let the schema fill that
slot:

```yaml
      schema: Tables
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>"
```

Since models are written to `<root_path>/<schema>/<model>`, this lands them at
`<lh>.Lakehouse/Tables/<model>` — exactly the flat layout the schema-less Lakehouse expects. Prefer a
schema-enabled Lakehouse (`root_path: .../Tables`, real schemas) whenever you can.

### Remote stores (S3 / GCS / ADLS)

Point `root_path` at the warehouse location and pass credentials through `storage_options` — these
flow straight to deltalake for writes and merges.

On Azure-backed stores, if `storage_options` carries a `bearer_token` (or `token` / `access_token`),
the adapter also auto-creates a matching DuckDB Azure secret, so `delta_scan()` reads work with no
extra config. In a notebook where the storage secret is already provided to DuckDB, you can leave
`storage_options` empty.

```yaml
    remote:
      type: duckrun
      schema: dbo
      root_path: "s3://my-bucket/warehouse"   # or abfss://... , gs://...
      storage_options:
        aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

Verified end-to-end against real remote object storage: `table` overwrite, `incremental` merge, and
`delta_scan` reads / tests.

## Materializations

| materialized      | backed by                | notes                                                                 |
|-------------------|--------------------------|-----------------------------------------------------------------------|
| **`table`**       | Delta (overwrite)        | DuckDB runs the SQL; delta_rs writes the table fresh each run.         |
| **`incremental`** | Delta (merge / append)   | First run overwrites; later runs apply `incremental_strategy`.         |
| `view`            | in-memory DuckDB         | Ephemeral staging within a run (inherited from dbt-duckdb).            |
| `seed`            | in-memory DuckDB         | CSV fixtures (inherited from dbt-duckdb).                              |
| `delta`           | Delta                    | Alias for `table`; honors `incremental=true`. Kept for convenience.   |

The persisted materializations (`table`, `incremental`, `delta`) register a `delta_scan` view over
the new Delta table, so downstream `ref()` works.

### `table`

```sql
-- models/orders.sql
{{ config(materialized='table') }}

select status, count(*) as n, sum(amount) as total
from {{ ref('stg_orders') }}
group by status
```

### `incremental`

```sql
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

The first run (or `--full-refresh`, or a missing table) overwrites. Later runs apply the
`incremental_strategy`:

| `incremental_strategy`             | behavior                                  | requires     |
|------------------------------------|-------------------------------------------|--------------|
| `merge` (default with `unique_key`) | upsert — update matched, insert new       | `unique_key` |
| `insert`                           | insert only new keys (idempotent append)  | `unique_key` |
| `append` (default without `unique_key`) | blind append                          | —            |
| `safeappend`                       | append, but only if the table is unchanged since the model read it (else fail) — cheap, no dedup scan | — |

### `safeappend`

A cheap append for the common "load only what's new" pattern — when your model SQL **already
guarantees no duplicates** and you don't want to pay for a merge.

```sql
{{ config(materialized='incremental', incremental_strategy='safeappend') }}

select * from read_csv(getvariable('new_files'))
{% if is_incremental() %}
  -- the dedup is your SQL's job: only load files not already in the table
  where file not in (select distinct file from {{ this }})
{% endif %}
```

**Why, reason 1 — performance.** `merge` / `insert` scan the target and join on the key to find what's
new — expensive on a large table. If the SQL above already excludes rows that are present, that work
is redundant. `safeappend` is a plain append: **no target data scan, no key join, and DuckDB keeps
its full memory budget** (the merge memory split is never applied — same as `append` / `overwrite`).
The only thing it reads from the target is one Delta log entry to get the version.

**Why, reason 2 — a concurrency guard a blind `append` doesn't have.** Because the dedup is done in
SQL against `{{ this }}`, a plain `append` is unsafe under concurrency: if another writer commits
between your `not in (... from {{ this }})` read and your write, the file it added isn't excluded and
you get a duplicate. `safeappend` closes that gap — it commits **only if the table version is
unchanged since the model started** (captured *before* it reads `{{ this }}`); if anything committed
in between, it fails with `CommitFailedError` so the run re-runs against the new state. No duplicate
slips in.

This is **optimistic concurrency control** — it never locks the table or blocks other writers; it
appends, then validates at commit with a compare-and-swap on the version and aborts on a mismatch.
Its policy is the strictest of the strategies (abort on *any* concurrent change, rather than reconcile
like `merge` or auto-rebase like `append`), but the mechanism is optimistic, not pessimistic.
Re-running is safe and idempotent: the SQL dedup simply excludes whatever the previous attempt already
loaded.

First run (or `--full-refresh`, or a missing table) overwrites to create the table; `safeappend`
applies on later runs. A real example is the AEMO
[`fct_scada`](../tests/integration_tests/aemo/models/marts/fct_scada.sql) model — the project's largest
table, which loads only not-yet-seen files and so uses `safeappend` instead of an expensive merge.

### Config options (`table` / `incremental` / `delta`)

| option                  | description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `location`              | Delta path. Defaults to `<root_path>/<schema>/<id>`.                        |
| `incremental_strategy`  | `merge` \| `insert` \| `append` \| `safeappend` (incremental only).          |
| `unique_key`            | column(s) to merge on.                                                       |
| `merge_update_columns`  | merge: update only these columns on match (others untouched).               |
| `merge_exclude_columns` | merge: update all columns **except** these on match.                        |
| `merge_max_spill_size`  | merge: memory ceiling in **bytes** for delta_rs's merge pool (not a disk budget). Defaults to ~60% of the **effective** limit — `min(physical RAM, container/cgroup limit, currently-free RAM)` — beyond which delta_rs spills the merge join to disk (like DuckDB's `memory_limit`). The other big consumer, DuckDB itself, is separately pinned to ~30% of the same effective limit on the merge path (it produces the merge source in the same process), so the two budgets sum under the cgroup cap; both log their chosen value at run start. Set `0` to disable. It bounds the merge pool, *not* the whole process (the Arrow source, read buffers, and spill-file page cache sit outside it), so on a tight container with a huge source the total can still exceed the cap — lower it if needed. A cap below the join's minimum (~hundreds of MB) makes the merge raise `Resources exhausted` instead of spilling. Requires deltalake 1.5.0 (pinned). |
| `incremental_predicates`| merge: extra predicates AND-ed into the merge condition (use `target.`/`source.`, or dbt's `DBT_INTERNAL_DEST`/`DBT_INTERNAL_SOURCE`). |
| `on_schema_change`      | `ignore` (default) \| `append_new_columns` \| `fail`. (`sync_all_columns` only *adds* — delta_rs can't drop columns.) |
| `partition_by`          | Delta partition column(s).                                                   |
| `merge_schema`          | allow schema evolution on write.                                            |
| `storage_options`       | per-model override forwarded to deltalake.                                   |

## Reading existing tables/files as sources

A source routed to the `duckrun` plugin can be a Delta table, a CSV, or a Parquet file.
`delta_table_path` always reads Delta; otherwise the path comes from `location` and the format is
taken from `format` (`csv` | `parquet` | `delta`) or inferred from the extension.

```yaml
sources:
  - name: lake
    tables:
      - name: customers           # Delta table
        meta:
          plugin: duckrun
          delta_table_path: 's3://bucket/lake/customers'
      - name: events              # CSV (read_csv_auto)
        meta:
          plugin: duckrun
          format: csv
          location: 's3://bucket/raw/events.csv'
      - name: metrics             # Parquet
        meta:
          plugin: duckrun
          format: parquet
          location: 's3://bucket/raw/metrics.parquet'
```

## How it works

1. dbt compiles your model SQL.
2. The materialization stages it as a DuckDB view.
3. A `dbt-duckdb` plugin (a `store()` hook) hands that relation to deltalake over the Arrow C-stream
   interface (`__arrow_c_stream__`) — no pyarrow required — which `write_deltalake` /
   `DeltaTable.merge` consume natively.
4. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter is a thin subclass of dbt-duckdb declaring `dependencies=['duckdb']`, so `view`, `seed`,
tests, and the rest are inherited directly; only `table` and `incremental` are overridden to write
Delta.

## Table maintenance (compaction & vacuum)

**duckrun maintains your Delta tables automatically — no configuration, no scheduled job, no separate
`OPTIMIZE`/`VACUUM` run to remember.** It happens inline on every write.

This matters because delta_rs has **no** automatic, post-commit maintenance of its own — and it
ignores Databricks-style auto-optimize table properties (`delta.autoOptimize.*`). Left alone, an
incremental table fragments into many small Parquet files and keeps every superseded file version
forever. duckrun runs the maintenance for you, right after each write:

| write | maintenance |
|---|---|
| `table` / overwrite | `vacuum` + metadata cleanup every run |
| `append` | `optimize.compact` + `vacuum` + cleanup once the table exceeds **100 files** |
| `merge` / `insert` | same threshold-gated `compact` + `vacuum` + cleanup after the merge |
| `microbatch` / delete+insert | same threshold-gated maintenance |

Every `vacuum` uses delta_rs's **safe default retention (7 days / 168h)**, so files a concurrent
reader might still be reading are never deleted out from under it. The trade-off is that a superseded
file version lingers for the retention window before it can be reclaimed — duckrun favors read-safety
over immediate disk savings.

## Limitations

These are core design trade-offs, not bugs — they're inherent to gluing DuckDB to delta_rs and won't
be "fixed" away:

- **A single dbt run is single-threaded — but concurrency works fine.** This is purely a dbt-adapter
  implementation detail: *within one dbt process* models run with `threads: 1`, because the in-process
  delta_rs write path isn't thread-safe (parallel writes to a table in the *same* process collide). It
  is **not** a limit on concurrent writers. Multiple independent writers — separate dbt runs,
  notebooks, jobs, whatever — writing the same tables at the same time is fully supported and safe:
  every write uses optimistic concurrency (snapshot-pinned MERGE, `safeappend` compare-and-swap,
  fail-loud on a conflicting commit). So you can absolutely run many writers in parallel; you just
  can't multi-thread the models *inside a single* dbt invocation.
- **Two engines share one machine's memory.** DuckDB executes the SQL and delta_rs materializes the
  Delta table — two separate memory systems in the same process, each with its own pool. Under heavy
  memory pressure (large merges especially) the budget has to be split between them, and getting that
  split right is fragile: delta_rs's merge spill-to-disk is itself flaky, and coordinating two systems
  that don't know about each other's allocations is the hard, unavoidable part of this design.
- **`DROP TABLE` is a soft tombstone, not a physical delete.** delta_rs has no `DROP`, and duckrun
  **deliberately will not delete your data files** — as a precaution it leaves that to you — so
  `conn.sql("drop table x")` overwrites the table with a one-column tombstone marker and unregisters
  it. The table vanishes from `conn.catalog` and discovery, and a later `create table x as …` revives
  the path with real data, but the **files are not reclaimed** (you purge them yourself when you're
  sure). One consequence: reading the path *directly*
  (`conn.read.format("delta").load("…/x")`) bypasses discovery and returns the one-row tombstone
  marker rather than erroring — address dropped tables by name, not by path.
