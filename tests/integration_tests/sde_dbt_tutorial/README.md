# sde_dbt_tutorial — a complex dbt project ported onto duckrun

A port of [josephmachado/simple_dbt_project](https://github.com/josephmachado/simple_dbt_project)
(the *Start Data Engineering* dbt tutorial) onto the **duckrun** adapter. It exercises the patterns
duckrun users actually hit — all materialized as **Delta Lake** tables on a local warehouse:

| Pattern | Where |
|---|---|
| **SCD2 snapshot** (timestamp strategy, `dbt_valid_to_current`) | `scd2/dim_customer.sql` |
| **Incremental model** (`merge` on `event_id`, `is_incremental()` window) | `models/silver/fct_clickstream.sql` |
| **Medallion / OBT** with an SCD2 validity-window join | `models/bronze` → `silver` → `gold/orders_obt.sql` |
| **dbt packages** (`dbt_utils.pivot`, `dbt_expectations`) | `models/gold/marketing`, `packages.yml` |
| **Seeds, generic + custom tests, exposures** | `data/`, `tests/`, `models/**/*.yml` |
| **Plugin sources over Delta** | `models/bronze/raw.yml` |

## What changed vs. upstream

- `profiles.yml`: `type: duckdb` → **`type: duckrun`**, `threads: 1` (duckrun isn't thread-safe), a
  local `root_path` Delta warehouse.
- `fct_clickstream`: `incremental_strategy='delete+insert'` → **`'merge'`** (duckrun has no
  delete+insert; same `unique_key='event_id'`).
- Sources: the Python EL lands `raw.*` as **Delta tables**; `raw.yml` declares them as duckrun
  plugin sources (`delta_table_path`) that resolve to `delta_scan` views.
- Run order: upstream's separate `dbt run` / `dbt snapshot` / `dbt run` commands become a single
  **`dbt build`** (dbt orders the snapshot before `orders_obt` by the DAG; avoids relying on DuckDB
  views persisting across processes).
- Dropped the upstream `non_existent_table` entry (an intentional failing-test teaching artifact).

## Run it

```bash
export DUCKRUN_WAREHOUSE=/tmp/sde_wh          # or any path; defaults to ./warehouse
python el/extract_load_pipeline.py            # phase 1: land raw Delta tables
dbt deps && dbt seed && dbt build             # build everything (snapshot + models + tests)

python el/load_new_data.py                    # phase 2: append updated/new rows
dbt build                                     # SCD2 + incremental react to the change
```

After phase 2: `snapshots.dim_customer` has **2 rows** for `customer_id = 82` (old version closed,
new version open) and `fct_clickstream` has grown **100 → 110**.

## Test

```bash
pytest tests/integration_tests/sde_dbt_tutorial/test_sde_dbt_tutorial.py
```

The test drives real dbt one process per phase (so the snapshot/incremental behave as in
production) and asserts the invariants above.
