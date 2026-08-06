# Compatibility test — Start Data Engineering (sde) on duckrun

Run the upstream **Start Data Engineering** dbt project on duckrun, **unchanged**. The bar is a
green `dbt build` — the repo's own EL, models, snapshot and tests, verbatim; a failure is a duckrun
bug (fixed in duckrun, never in the project). dbt-duckdb itself is not built — nothing here tests it.

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
unchanged) and `root_path` to a Delta warehouse (the models materialize there). The repo's own EL
runs first, exactly as upstream documents. The repo is never modified.

## Run it

```bash
python tests/parity_tests/sde/run_parity.py
```

It clones the repo fresh, runs the repo's EL, then `dbt deps` + `dbt build` with the duckrun
profile — OneLake in CI (`WAREHOUSE_PATH`), a local Delta warehouse otherwise. Exit 0 = the project
built green.

## Result (latest run)

The full build — EL, the `delete+insert` incremental `fct_clickstream`, `fct_orders`, the seed, the
SCD2 `dim_customer` snapshot, and every test — runs **green on duckrun**, unmodified. The
`bronze_*` / `orders_obt` / `order_status_pivot` models are `view`s — duckrun has no durable view,
so they're intermediate-only; the gold models that depend on them build and test green.
