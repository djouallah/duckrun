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

## Use the `delta` materialization

```sql
-- models/orders.sql
{{ config(materialized='delta') }}

select status, count(*) as n, sum(amount) as total
from {{ ref('stg_orders') }}
group by status
```

This runs the SQL in DuckDB, writes a Delta table to
`<root_path>/<schema>/orders` (or `config(location=...)`), and registers a
`delta_scan` view so downstream `ref()` works.

### Incremental Delta tables

```sql
{{ config(materialized='delta', incremental=true, unique_key='order_id') }}

select * from {{ ref('stg_orders') }}
```

First run (or `--full-refresh`) overwrites. Later runs **merge** on `unique_key` (or
**append** when no `unique_key` is set) via delta_rs.

### Config options for `materialized='delta'`

| option            | description                                              |
|-------------------|----------------------------------------------------------|
| `location`        | Delta path. Defaults to `<root_path>/<schema>/<id>`.     |
| `incremental`     | `true` to append/merge on later runs (default overwrite).|
| `unique_key`      | column(s) to upsert on (`incremental` + `unique_key`).   |
| `partition_by`    | Delta partition column(s).                               |
| `merge_schema`    | allow schema evolution on write.                         |
| `storage_options` | per-model override forwarded to deltalake.               |

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
2. The `delta` materialization stages it as a DuckDB view.
3. A `dbt-duckdb` plugin (`store()` hook) reads that view as an Arrow record batch and
   calls `write_deltalake(...)`.
4. The model relation becomes a `delta_scan` view over the new Delta table.

Because the adapter declares `dependencies=['duckdb']`, every other materialization
(`view`, `table`, `seed`, `incremental`, `external`, …) is inherited directly from
dbt-duckdb.

## Development

The `integration_tests/` directory is a small dbt project exercised by CI
(`.github/workflows/integration.yml`): `dbt seed && dbt run && dbt test` against a local
Delta `./warehouse`, including an incremental merge.

## License

MIT
