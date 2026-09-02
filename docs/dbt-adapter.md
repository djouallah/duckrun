---
hide:
  - navigation
---

# duckrun dbt adapter

duckrun is a thin wrapper around [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb): views, seeds, sources, tests, snapshots and the plugin ecosystem are inherited, and `table` / `incremental` write real Delta tables via delta-rs. The rationale is in the [design document](design_document.md).

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # threads: works as usual (dbt defaults to 1); concurrent models share one DuckDB memory budget.
      # DuckDB runs in-memory — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...).
      # OneLake — address by GUID, not friendly names (see below):
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables"
      # Or any other store: './warehouse' (local), 's3://...', 'gs://...'.
      # storage_options: {}      # passed through to deltalake for remote stores
```

Models are written to `<root_path>/<schema>/<model>` (e.g. `.../Tables/dbo/orders`), or to an explicit `config(location=...)`.

### OneLake shorthand for `root_path`

`root_path`, every entry under `catalogs:`, and a source's `location` / `delta_table_path` accept the same `workspace/item` shorthand as `duckrun.connect()`:

```yaml
      root_path: "<workspace_id>/<lakehouse_id>"        # → abfss://…/<lakehouse_id>/Tables
      # or by name: root_path: "my_workspace/sales.Lakehouse"
```

The rules are on the [Connection API](connection-api.md#onelake-shorthand) page.

### OneLake: use GUID paths for now

Address OneLake by **workspace GUID + lakehouse GUID**. Friendly-name paths trip an upstream `duckdb-delta` read bug ("No files in log segment") that is fixed upstream but still rolling out to production OneLake.

### OneLake authentication — tokens are optional

With no `bearer_token` in `storage_options`, duckrun acquires one itself (`azure-identity` is a core dependency) and mints the matching DuckDB Azure secret, so writes and `delta_scan` reads — including `dbt test` / `dbt show` / `dbt docs generate` — work with no credential in the profile. Order: Fabric notebook (`notebookutils`) → `AZURE_STORAGE_TOKEN` → GitHub Actions OIDC (`AZURE_CLIENT_ID` + `AZURE_TENANT_ID`, `id-token: write`) → Azure CLI → interactive browser. Tokens are cached per scope and re-acquired near expiry. An explicit `storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }` takes precedence.

### Fabric Lakehouse without a schema

A schema-less Lakehouse (tables straight under `Tables/`) is a poor pattern, but needs no special config: drop the trailing `Tables` from `root_path` and let the schema fill that slot.

```yaml
      schema: Tables
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>"
```

### Remote stores (S3 / GCS / ADLS)

Point `root_path` at the store and pass credentials through `storage_options`; they flow straight to deltalake. On Azure-backed stores a `bearer_token` (or `token` / `access_token`) in `storage_options` also mints the DuckDB Azure secret for `delta_scan` reads.

```yaml
    remote:
      type: duckrun
      schema: dbo
      root_path: "s3://my-bucket/warehouse"   # or abfss://... , gs://...
      storage_options:
        aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

### Multiple Lakehouses in one project

Declare each extra write root as a named catalog under `catalogs:` and send a model to it with dbt's standard `+database: <alias>`. The default catalog is the top-level `root_path` / `storage_options`; don't set `database:` (dbt-duckdb requires it to match the `:memory:` path).

```yaml
    dev:
      type: duckrun
      root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      catalogs:
        lh_bronze:
          root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
        lh_gold:
          root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables"
```

```sql
-- models/bronze/raw_events.sql — lands in LH_Bronze
{{ config(materialized='incremental', database='lh_bronze', unique_key='id') }}
select ...
```

Each catalog carries its own `storage_options` (a path-scoped DuckDB secret per catalog); `ref()`, `is_incremental()` and `dbt docs generate` work across catalogs.

### Multiple environments (dev / test / prod)

One target per environment, reusing the same alias names so model config never changes:

```yaml
    dev:
      type: duckrun
      root_path: "abfss://dev_ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      catalogs:
        lh_bronze:
          root_path: "abfss://dev_ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
    # test / prod: the same block with that environment's workspace
```

```bash
dbt run --target test
```

#### Following the target from a source

The profile exposes `target.catalog_locations` (`{alias: root_path}` for every entry under `catalogs:`, no tokens), so a source can follow the current target:

```yaml
sources:
  - name: lake
    tables:
      - name: raw_events
        meta:
          plugin: duckrun
          delta_table_path: "{{ target.catalog_locations['lh_bronze'] }}/dbo/raw_events"
```

It is a plain Jinja dict lookup: a missing alias renders as an empty string, so the path silently becomes `/dbo/raw_events` and fails later with `InvalidTableLocationError`. Guard it in an `on-run-start` macro (`'lh_bronze' in target.catalog_locations`) if that is a risk.

## Materializations

| materialized      | backed by                | notes                                                                 |
|-------------------|--------------------------|-----------------------------------------------------------------------|
| **`table`**       | Delta (overwrite)        | DuckDB runs the SQL; delta-rs writes the table fresh each run.        |
| **`incremental`** | Delta (merge / append)   | First run overwrites; later runs apply `incremental_strategy`.        |
| `view`            | in-memory DuckDB         | Session-scoped, nothing on storage (inherited from dbt-duckdb).       |
| `seed`            | Delta (overwrite)        | CSV loaded via DuckDB, persisted as a Delta table, rediscovered in a fresh process. |
| `delta`           | Delta                    | Alias for `table`; honors `incremental=true`.                         |
| `external`        | a parquet / csv / json **file** | Not Delta: DuckDB `COPY … TO` writes the file and the model is a view over it ([below](#external)). |

The persisted materializations register a `delta_scan` view over the new table, so downstream `ref()` works.

### `table`

```sql
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

The first run (or `--full-refresh`, or a missing table) overwrites. Later runs apply:

| `incremental_strategy`             | behavior                                  | requires     |
|------------------------------------|-------------------------------------------|--------------|
| `merge` (default with `unique_key`) | upsert — update matched, insert new       | `unique_key` |
| `insert`                           | insert only new keys — a DuckDB anti-join committed as a **plain append** | `unique_key` |
| `delete+insert`                    | delete the batch's keys, insert the whole batch (duplicates preserved) | `unique_key` |
| `append` (default without `unique_key`) | append; **auto-fenced** when the model reads `{{ this }}` | — |
| `microbatch`                       | `replaceWhere` per dbt-driven `event_time` window | `event_time` |

### `insert` — insert-only, computed in DuckDB

Insert-only never removes a row, so duckrun computes "batch rows whose `unique_key` is not already present" as a DuckDB anti-join and commits `add` actions only. No existing file is rewritten, the target read is projected to the key columns, and the anti-join spills like any DuckDB query, where a delta-rs `MERGE` plans a join against the whole pinned target and does not fully spill. NULL keys insert, as with `IN`.

- **A batch that adds nothing writes no commit**; history records the write as `WRITE`, not `MERGE`.
- **The append is always fenced**: it commits only if the table version is unchanged since the model started, else `CommitFailedError`.
- **The target probe prunes.** For each key column the batch's `min` / `max` is folded into the target read, so files outside the range are skipped. For a partition column declare the equality and get the exact value set instead (`"month_key" IN (202601, 202602)`), which beats a range when a source unions an old backfill with the current feed:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
    partition_by=['month_key'],
    incremental_predicates=['target.month_key = source.month_key'],
) }}
```

`conn.sql("MERGE INTO t USING s ON … WHEN NOT MATCHED THEN INSERT *")` routes to the same anti-join; any other clause shape runs on delta-rs. `merge_streamed_exec: true` forces delta-rs's merge for an insert-only shape.

### `append` that reads `{{ this }}` — the automatic fence

When your SQL already guarantees no duplicates, skip the merge:

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

select * from read_csv(getvariable('new_files'))
{% if is_incremental() %}
  where file not in (select distinct file from {{ this }})
{% endif %}
```

A plain `append` does no target scan and no key join. Because the model reads `{{ this }}`, duckrun commits only if the table version is unchanged since the model started (captured before the read); otherwise it fails with `CommitFailedError` and a re-run dedups against the new state. An `append` that does not read `{{ this }}` is left unfenced. The model is in [Snapshot isolation](snapshot-isolation.md).

### `external`

dbt-duckdb's file export, shipped verbatim: DuckDB runs the SQL, `COPY … TO` writes a parquet / CSV / JSON file, and the model becomes a view over it, so `ref()` works within the run in both directions.

```sql
{{ config(materialized='external', location='exports/orders.parquet') }}

select * from {{ ref('mart_orders') }}
```

| option | description |
|---|---|
| `location` | output **file** path. Defaults to `<external_root>/<identifier>.<format>` (`external_root` is a profile key, `.` if unset). |
| `format` | `parquet` (default) \| `csv` \| `json`; inferred from `location`'s extension when omitted. |
| `options` | dict spliced into `COPY … TO (…)` — e.g. `{'partition_by': 'year,month'}`, `{'compression': 'zstd'}`. |
| `delimiter` | legacy top-level alias, folded into `options`. |
| `parquet_read_options` / `csv_read_options` / `json_read_options` | args for the `read_*` call the view is built from. |
| `plugin`, `glue_register`, `glue_database`, `partition_columns` | register the written file with a dbt-duckdb plugin after the write. |

The view does not survive the process: a later run that reads an external model without rebuilding it needs upstream's hook, exactly as on dbt-duckdb. `--full-refresh` means nothing; every run rewrites the file.

```yaml
# dbt_project.yml
on-run-start: "{{ register_upstream_external_models() }}"
```

### Config options (`table` / `incremental` / `delta`)

| option                  | description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `location`              | Delta path. Defaults to `<root_path>/<schema>/<id>`.                        |
| `incremental_strategy`  | `merge` \| `insert` \| `append` \| `delete+insert` \| `microbatch`.         |
| `unique_key`            | column(s) to merge on.                                                       |
| `merge_update_columns`  | merge: update only these columns on match.                                   |
| `merge_exclude_columns` | merge: update all columns **except** these on match.                         |
| `merge_update_condition` / `merge_insert_condition` | merge: extra predicate AND-ed onto the matched-update / not-matched-insert clause (`target.` / `source.`, or `DBT_INTERNAL_DEST` / `DBT_INTERNAL_SOURCE`). `merge_insert_condition` also applies to `insert`, where it may reference only the source. |
| `merge_clauses` / `merge_update_set_expressions` | merge: dbt-duckdb's custom clause list / per-column `SET` expressions, translated to delta-rs's TableMerger. Same spelling as upstream — `action` `update` / `delete` / `insert` / `do_nothing`, `mode` `by_name` / `by_position` / `star` / `explicit`, `condition`, `insert: {columns, values}`, `update: {include, exclude, set_expressions}`, `by: source` — with upstream's implicit defaults for an omitted `when_matched` / `when_not_matched`, so `{'when_matched': [{'action': 'do_nothing'}]}` is insert-only and takes the `insert` route. Rejected: `merge_on_using_columns` and `action: error`. |
| `when_not_matched_by_source` | merge: duckrun's own top-level `merge_clauses` key (`update` with a `set` map / `delete` / `do_nothing`) for rows the source doesn't carry. A dict using it opts out of the implicit defaults above; use upstream's portable `{'by': 'source', …}` entry inside `when_not_matched` to keep both. |
| `merge_max_spill_size`  | merge: memory ceiling in **bytes** for delta-rs's merge pool, beyond which the join spills to disk. Default ~60% of the effective limit (`min(physical RAM, cgroup limit, free RAM)`); DuckDB is pinned separately at ~85%. It bounds the merge pool, not the process, so on a tight container with a huge source lower it; below the join's minimum (~hundreds of MB) the merge raises `Resources exhausted`. `0` disables. |
| `merge_max_temp_directory_size` | merge: disk cap in bytes for the merge's spill files. Default: the spill disk's free space minus `min(20% of free, 8 GiB)`. |
| `merge_streamed_exec`   | merge: `true` streams a huge merge **source** instead of collecting it (needed for very large sources, especially by-source clauses) at the cost of target-file pruning. Default `false`. |
| `incremental_predicates`| merge / insert: extra predicates AND-ed into the join condition. On `insert`, a `target.<part> = source.<part>` entry also unlocks partition pruning of the target probe. |
| `on_schema_change`      | `ignore` (default) \| `append_new_columns` \| `fail`. `sync_all_columns` only *adds* — delta-rs can't drop columns. |
| `partition_by`          | Delta partition column(s).                                                   |
| `sort_by`               | column(s) to physically `ORDER` the write by — a trailing `ORDER BY` in the model SQL is not honored, this is. The scalar `'auto'` profiles the staged result and picks the key itself, re-profiling every batch and writing unsorted when nothing pays off. Inert on the `merge` / `microbatch` / `delete+insert` paths. See [Automatic sorting](parquet-layout.md#automatic-sorting). |
| `max_row_group_size`    | **rows** — the parquet row-group ceiling for this model's writes (default 6M). Honored verbatim and preserved by post-write compaction. The delta-rs `merge` write keeps delta-rs defaults; post-merge compaction folds its files into this geometry. See [Write settings](parquet-layout.md#write-settings). |
| `target_file_size_mb`   | **megabytes** — target parquet file size (default 256). Same coverage and merge caveat as `max_row_group_size`. |
| `merge_schema`          | allow schema evolution on write.                                             |
| `timestamp_ntz`         | default `false`: a naive `TIMESTAMP` column is written UTC-adjusted and lands as Delta `timestamp`, because Fabric's SQL analytics endpoint omits `timestamp_ntz` columns. `true` (or `DUCKRUN_TIMESTAMP_NTZ=1`) keeps the verbatim `timestamp_ntz` write. Details in [Limitations](limitations.md#microsoft-fabric-onelake). |
| `storage_options`       | per-model override forwarded to deltalake.                                   |

## Reading existing tables/files as sources

A source routed to the `duckrun` plugin can be a Delta table, a CSV, or a Parquet file. `delta_table_path` always reads Delta; otherwise the path comes from `location` and the format from `format` (`csv` | `parquet` | `delta`) or the extension.

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

1. dbt compiles the model SQL; the materialization stages it as a DuckDB view.
2. A dbt-duckdb plugin (`store()`) hands the relation to deltalake over the Arrow C-stream interface, which `write_deltalake` / `DeltaTable.merge` consume natively.
3. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter subclasses dbt-duckdb with `dependencies=['duckdb']`; only `table` and `incremental` are overridden.

## Table maintenance (compaction & vacuum)

delta-rs has no post-commit maintenance of its own, so duckrun runs it inline after each write:

| write | maintenance |
|---|---|
| `table` / overwrite | `vacuum` + metadata cleanup every run |
| `append`, `merge`, `insert`, `microbatch`, `delete+insert` | `optimize.compact` + `vacuum` + cleanup once the small-file byte debt is worth it (≥8 files under half the target size AND ≥2× the target in small bytes) |

Every table duckrun creates is stamped `delta.checkpointInterval = 10` (delta-rs's default is 100), creation-only. Every `vacuum` uses delta-rs's safe retention (7 days), so a file a concurrent reader may still be reading is never deleted; a superseded file lingers for that window.

## Limitations

Consolidated on the [Limitations](limitations.md) page.
