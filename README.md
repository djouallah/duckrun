<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="duckrun">

[![PyPI version](https://badge.fury.io/py/duckrun.svg)](https://badge.fury.io/py/duckrun)

**duckrun** is a [dbt](https://www.getdbt.com/) adapter that runs your model SQL in
**DuckDB** and writes the results to **Delta Lake** using
[`delta_rs`](https://delta-io.github.io/delta-rs/) (the `deltalake` Python package).

It is a thin wrapper around [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb). You
keep everything dbt-duckdb gives you — views, seeds, sources, tests, snapshots, the full
plugin ecosystem — and gain one extra thing: a Delta-backed `table` / `incremental`
materialization that writes real Delta tables

> ### Why a separate adapter instead of a PR to dbt-duckdb?
>
> Writing Delta with delta_rs needs the `deltalake` package. dbt-duckdb deliberately
> keeps a minimal dependency footprint and avoids external dependencies like this — for
> very good reasons — so this doesn't belong upstream. duckrun keeps it isolated here
> instead.
>
> It's also meant to be a **temporary workaround**: DuckDB is gaining native Delta
> *write* support, and once that matures the delta_rs hop should no longer be needed.
> Until then, this adapter fills the gap.

> ### 0.3.0 is a breaking change
>
> Versions ≤ 0.2.x of `duckrun` were a Microsoft Fabric / OneLake helper library. From
> **0.3.0** onward `duckrun` is a dbt adapter. Need the old library? Pin
> `pip install "duckrun<0.3"`, or use the
> [`legacy`](https://github.com/djouallah/duckrun/tree/legacy) branch.

## How it fits together

DuckDB is a great query engine, Delta Lake is a great open table format, and dbt is the
right tool to orchestrate the DAG. duckrun wires the three together:

> **DuckDB executes · delta_rs materializes · dbt orchestrates.**

## Install

```bash
pip install duckrun
```

That single install pulls in `dbt-duckdb` (and therefore `duckdb`) plus `deltalake`.

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # No `threads:` needed — duckrun always runs single-threaded (see Limitations).
      # DuckDB runs in-memory by default — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...).
      root_path: './warehouse'   # local path, or abfss://.../Tables, s3://..., gs://...
      # storage_options: {}      # passed through to deltalake for remote stores
```

Persisted models are written to `<root_path>/<schema>/<model>` (e.g.
`./warehouse/dbo/orders`), or to an explicit `config(location=...)`.

### Remote stores (Fabric OneLake / ADLS / S3 / GCS)

Point `root_path` at the warehouse location and pass credentials through
`storage_options` — these flow straight to deltalake for writes and merges.

If `storage_options` carries a `bearer_token` (or `token` / `access_token`), the adapter
also auto-creates a matching DuckDB Azure secret, so `delta_scan()` reads work with no
extra config. In a notebook where the storage secret is already provided to DuckDB, you
can leave `storage_options` empty.

```yaml
    onelake:
      type: duckrun
      schema: dbo
      root_path: "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables"
      storage_options:
        # az account get-access-token --resource https://storage.azure.com
        bearer_token: "{{ env_var('ONELAKE_TOKEN') }}"
```

Verified end-to-end against real Fabric OneLake: `table` overwrite, `incremental` merge,
and `delta_scan` reads / tests.

## Materializations

| materialized      | backed by                | notes                                                                 |
|-------------------|--------------------------|-----------------------------------------------------------------------|
| **`table`**       | Delta (overwrite)        | DuckDB runs the SQL; delta_rs writes the table fresh each run.         |
| **`incremental`** | Delta (merge / append)   | First run overwrites; later runs apply `incremental_strategy`.         |
| `view`            | in-memory DuckDB         | Ephemeral staging within a run (inherited from dbt-duckdb).            |
| `seed`            | in-memory DuckDB         | CSV fixtures (inherited from dbt-duckdb).                              |
| `delta`           | Delta                    | Alias for `table`; honors `incremental=true`. Kept for convenience.   |

The persisted materializations (`table`, `incremental`, `delta`) register a `delta_scan`
view over the new Delta table, so downstream `ref()` works.

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

### Config options (`table` / `incremental` / `delta`)

| option                  | description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `location`              | Delta path. Defaults to `<root_path>/<schema>/<id>`.                        |
| `incremental_strategy`  | `merge` \| `insert` \| `append` (incremental only).                         |
| `unique_key`            | column(s) to merge on.                                                       |
| `merge_update_columns`  | merge: update only these columns on match (others untouched).               |
| `merge_exclude_columns` | merge: update all columns **except** these on match.                        |
| `incremental_predicates`| merge: extra predicates AND-ed into the merge condition (use `target.`/`source.`, or dbt's `DBT_INTERNAL_DEST`/`DBT_INTERNAL_SOURCE`). |
| `on_schema_change`      | `ignore` (default) \| `append_new_columns` \| `fail`. (`sync_all_columns` only *adds* — delta_rs can't drop columns.) |
| `partition_by`          | Delta partition column(s).                                                   |
| `merge_schema`          | allow schema evolution on write.                                            |
| `storage_options`       | per-model override forwarded to deltalake.                                   |

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
3. A `dbt-duckdb` plugin (a `store()` hook) hands that relation to deltalake over the
   Arrow C-stream interface (`__arrow_c_stream__`) — no pyarrow required — which
   `write_deltalake` / `DeltaTable.merge` consume natively.
4. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter is a thin subclass of dbt-duckdb declaring `dependencies=['duckdb']`, so
`view`, `seed`, tests, and the rest are inherited directly; only `table` and
`incremental` are overridden to write Delta.

## Limitations

- **Single-threaded (enforced).** duckrun's delta_rs write path isn't thread-safe — parallel
  models would collide on the shared DuckDB connection — so the adapter **pins the run to one
  thread**, overriding any `threads:` you set in the profile. There's nothing to configure;
  it's fine for duckrun's intended use (incremental Delta builds on DuckDB) and isn't aimed at
  large-scale concurrent workloads (that's Spark's job, not this).

## Development

The `integration_tests/` directory is a small dbt project exercised by CI
(`.github/workflows/integration.yml`): `dbt build` runs twice against a local Delta
`./warehouse` — a seed, a `view`, a `table`, and an `incremental` model — where the
second build exercises the incremental merge. Verified to run with **pyarrow not
installed**, on the minimum supported `duckdb` and `deltalake`.

`jaffle_shop/` is a self-contained build of the canonical
[dbt-labs jaffle shop](https://github.com/dbt-labs/jaffle-shop) project on duckrun, run by
`.github/workflows/jaffle.yml` as a **gating** end-to-end test over a local Delta warehouse.
It seeds the classic data, builds staging views → a `dim_customers` Delta table → an
incremental `fct_orders`, then drives a **two-pass merge**: pass 1 lands the 99 base orders,
pass 2 applies a late-arriving batch (a restated order plus two new ones) and singular tests
assert the Delta merge upserted correctly (right row count, the existing order UPDATEd, the
new orders INSERTed). It's industry-standard and recognisable, and — unlike the conformance
report — fails the build on a merge regression. It shares no files with `integration_tests/`.

`tests/conformance/` runs the official dbt adapter test suite
([`dbt-tests-adapter`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-tests-adapter))
against duckrun (`.github/workflows/conformance.yml`, results card in the job summary). It runs
**single-threaded (`threads: 1`)** — see [Limitations](#limitations) — as is normal for
adapter conformance suites (e.g. dbt-iceberg does the same).

## License

MIT
