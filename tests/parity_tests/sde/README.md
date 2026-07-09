# Parity test — Start Data Engineering (sde) on duckrun

Run the upstream **Start Data Engineering** dbt project on duckrun, **unchanged**, and assert
duckrun's Delta output matches dbt-duckdb table-for-table. dbt-duckdb is the oracle; a mismatch is
a duckrun bug.

This is the project that first exposed duckrun silently aliasing `incremental_strategy='delete+insert'`
to `merge` — the old `integration_tests/sde_dbt_tutorial` had to rewrite the model to `merge` to work
around it. duckrun now implements real delete+insert, so the project runs **verbatim**.

## The repo under test

- **Repo:** https://github.com/josephmachado/simple_dbt_project (the *Start Data Engineering* dbt tutorial)
- Patterns: a `delete+insert` incremental model (`fct_clickstream`), an SCD2 snapshot (`dim_customer`),
  a medallion bronze→silver→gold flow, packages (`dbt_utils`, `dbt_expectations`), seeds, generic +
  custom tests, an exposure. Raw data is committed CSVs the repo's `extract_load_pipeline.py` loads
  into a DuckDB file (`./dbt.duckdb`); the project reads them via `sources`.

## The connection (the only thing not in the repo)

sde ingests its sources into a DuckDB file rather than seeds, so the duckrun profile
([profiles.yml](profiles.yml)) sets `path` to that same `./dbt.duckdb` (sources resolve from it,
unchanged) and `root_path` to a Delta warehouse (the models materialize there). The oracle side uses
the repo's **own** `type: duckdb` profile. Both run the repo's own EL. The repo is never modified.

## Run it

```bash
python tests/parity_tests/sde/run_parity.py
```

It clones the repo into two dirs, runs the repo's EL + `dbt build` once per adapter (duckdb oracle,
duckrun), then diffs every persisted table. Exit 0 = parity.

## Result (latest run)

| table                      | rows | duckrun == dbt-duckdb |
|----------------------------|------|:---------------------:|
| main.fct_clickstream (delete+insert) | 100 | ✓ |
| main.fct_orders            | 999  | ✓ |
| main.order_status_code (seed) | 6 | ✓ |
| snapshots.dim_customer (SCD2) | 100 | ✓ * |

\* SCD2 bookkeeping columns (`dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`) are
stamped from run wall-clock / row hashes and differ between two independent runs, so the snapshot is
compared on its business columns. The `bronze_*` / `orders_obt` / `order_status_pivot` models are
`view`s — duckrun has no durable view, so they're intermediate-only and not part of the persisted diff.
