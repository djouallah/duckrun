<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="duckrun">

[![PyPI version](https://badge.fury.io/py/duckrun.svg)](https://badge.fury.io/py/duckrun)

**duckrun** is a [dbt](https://www.getdbt.com/) adapter that runs your model SQL in
**DuckDB** and materializes the results to **Delta Lake** using
[`delta_rs`](https://delta-io.github.io/delta-rs/) (the `deltalake` Python package).

It is a thin layer on top of [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb): you
get everything dbt-duckdb offers (views, seeds, sources, tests, snapshots, the full
plugin ecosystem) **plus** a `delta` materialization that writes real Delta tables —
the one thing dbt-duckdb doesn't do out of the box. Delta writes use ZSTD compression
and row-group sizing tuned for Power BI / DirectLake.

> Note: versions ≤ 0.2.x of `duckrun` were a Microsoft Fabric/OneLake helper library.
> From 0.1.0 of this line onward, duckrun is a dbt adapter. The old API lives in git
> history on `main`.

## Why

DuckDB is a fantastic query engine; Delta Lake is a great open table format. dbt is the
right tool to orchestrate the DAG. `duckrun` simply wires the three together: **DuckDB
executes, delta_rs materializes, dbt orchestrates.**

## Install

```bash
pip install duckrun
```

That single install pulls `dbt-duckdb` (and therefore `duckdb`) and `deltalake`.

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # DuckDB runs in-memory by default — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...)
      root_path: './warehouse'  # local path, or abfss://.../Tables, s3://..., gs://...
      # storage_options: {}     # passed through to deltalake for remote stores
```

In a notebook where the storage secret is already provided to DuckDB, leave
`storage_options` empty.

### Remote stores (Microsoft Fabric OneLake / ADLS / S3 / GCS)

Point `root_path` at the warehouse location and pass credentials via `storage_options`
— these flow straight to deltalake for writes/merges. If `storage_options` carries a
`bearer_token` (or `token` / `access_token`), the adapter also auto-creates a matching
DuckDB Azure secret so `delta_scan()` reads work, no extra config.

```yaml
    onelake:
      type: duckrun
      schema: dbo
      root_path: "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables"
      storage_options:
        bearer_token: "{{ env_var('ONELAKE_TOKEN') }}"   # az account get-access-token --resource https://storage.azure.com
        use_fabric_endpoint: "true"
```

Tables are written as `root_path/<schema>/<model>` (e.g. `…/Tables/dbo/orders`).
Verified end-to-end against real Fabric OneLake: `table` overwrite, `incremental` merge,
and `delta_scan` reads/tests.

## Materializations

| materialized | backed by | notes |
|---|---|---|
| **`table`** | Delta (overwrite) | DuckDB runs the SQL, delta_rs writes the table fresh each run. |
| **`incremental`** | Delta (merge / append) | First run overwrites; later runs apply `incremental_strategy`. |
| `view` | in-memory DuckDB | Ephemeral staging within a run (inherited from dbt-duckdb). |
| `seed` | in-memory DuckDB | CSV fixtures (inherited). |
| `delta` | Delta | Alias for `table`; honors `incremental=true`. Kept for convenience. |

The persisted materializations (`table`, `incremental`, `delta`) write to
`<root_path>/<schema>/<model>` by default, or to `config(location=...)`, and register a
`delta_scan` view so downstream `ref()` works.

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

First run (or `--full-refresh`, or missing table) overwrites. Later runs apply
`incremental_strategy`:

| `incremental_strategy` | behavior | requires |
|---|---|---|
| `merge` (default w/ `unique_key`) | upsert — update matched, insert new | `unique_key` |
| `insert` | insert only new keys (idempotent append / dedupe) | `unique_key` |
| `append` (default w/o `unique_key`) | blind append | — |

### Config options (table / incremental / delta)

| option                 | description                                              |
|------------------------|----------------------------------------------------------|
| `location`             | Delta path. Defaults to `<root_path>/<schema>/<id>`.     |
| `incremental_strategy` | `merge` \| `insert` \| `append` (incremental only).      |
| `unique_key`           | column(s) to merge on.                                   |
| `partition_by`         | Delta partition column(s).                               |
| `merge_schema`         | allow schema evolution on write.                         |
| `storage_options`      | per-model override forwarded to deltalake.               |

## Reading existing Delta tables as sources

```yaml
sources:
  - name: lake
    tables:
      - name: customers
        meta:
          plugin: duckrun
          delta_table_path: 's3://bucket/lake/customers'
```

## How it works

1. dbt compiles your model SQL.
2. The materialization stages it as a DuckDB view.
3. A `dbt-duckdb` plugin (`store()` hook) hands that relation to deltalake via the Arrow
   C-stream interface (`__arrow_c_stream__`) — no pyarrow — which `write_deltalake` /
   `DeltaTable.merge` consume natively.
4. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter is a thin subclass of dbt-duckdb declaring `dependencies=['duckdb']`, so
`view`, `seed`, tests, etc. are inherited directly; `table` and `incremental` are
overridden to write Delta.

## Development

The `integration_tests/` directory is a small dbt project exercised by CI
(`.github/workflows/integration.yml`): `dbt build` (twice) against a local Delta
`./warehouse` — a seed, a `view`, a `table`, and an `incremental` model — where the
second build exercises the incremental merge. Verified to run with **pyarrow not
installed**, on the minimum supported `duckdb` and `deltalake`.

## License

MIT
