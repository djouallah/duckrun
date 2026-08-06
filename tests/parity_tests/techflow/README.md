# Compatibility test — TechFlow Analytics (SaaS) on duckrun

Run the upstream **dbt-example** project on duckrun, **unchanged**. The bar is a green `dbt build`
— the project's own 137 data tests and 2 unit tests included; a failure is a duckrun bug (fixed in
duckrun, never in the project). dbt-duckdb itself is not built — nothing here tests it.

This is the first project here with **native dbt `unit_tests:`**, and it stacks several patterns the
others don't: raw data read from committed **parquet** via dbt-duckdb `external_location` sources, an
**incremental** model, two **timestamp snapshots**, `dbt_expectations`, exposures, and a
staging→intermediate→marts layering — all deterministic (committed data, no `random()`).

## The repo under test

- **Repo:** https://github.com/ameijin/dbt-example (the "TechFlow Analytics" SaaS demo)
- Patterns: 10 committed `data/*.parquet` files exposed as sources via
  `external_location: "data/{name}.parquet"`, 3 CSV seeds, ~30 SQL models, 2 unit tests, 137 data
  tests, 2 snapshots, an incremental `fct_mrr_daily`, exposures. No python models.

## The connection (the only thing not in the repo)

In dbt the *profile* lives outside the project, so swapping it in changes nothing in the repo.
[profiles.yml](profiles.yml) here defines the `techflow_analytics` profile as `type: duckrun` and
sets `root_path` to a Delta warehouse (the seeds, snapshots and marts materialize there). The repo
is never modified.

## Run it

```bash
python tests/parity_tests/techflow/run_parity.py
```

It clones the repo fresh and runs `dbt deps` + `dbt build --full-refresh` with the duckrun profile —
OneLake in CI (`WAREHOUSE_PATH`), a local Delta warehouse otherwise. Exit 0 = the project built green.

Why `--full-refresh`: the CI store **persists** across runs, so `fct_mrr_daily` (the incremental
model) would otherwise extend a table left by a prior run — and its `cumulative_mrr` is a
`sum() over(...)` *inside* the model's `where date_day > max(date_day)` incremental filter, so an
incremental run can't see history outside the new-date batch and resets it to 0. dbt-duckdb does the
**exact same thing** — verified — so it's a quirk of the project's SQL, not a duckrun bug.
`--full-refresh` rebuilds via the normal Delta overwrite (a new version, history retained); it never
deletes the OneLake table, and it exercises rebuild-over-a-persistent-store on every run.

## Result (latest run)

The full `dbt build` (seeds, 2 snapshots, ~30 models, **2 unit tests**, 137 data tests, exposures)
runs **green on duckrun**, unmodified — including **dbt_project_evaluator**, a dbt Labs linting
package that introspects the dbt graph. One knowing quirk: the package hardcodes
`+materialized: "{{ 'table' if target.type in ['duckdb'] else 'view' }}"`, and duckrun is its
**own** adapter type (`target.type == 'duckrun'`), so the evaluator's own ~20 bookkeeping models
materialize as views under duckrun. That can't be "fixed": reporting `type: duckdb` is exactly what
makes dbt load dbt-duckdb instead of duckrun. The package still runs green.
`stg_*`/`int_*` models are `view`s — duckrun has no durable view, so they're intermediate-only.

## Also here: the debug session (`run_debug.py`)

`python tests/parity_tests/techflow/run_debug.py` — standalone, read-only, compile-only. It clones
the project source and installs its packages (dbt cannot parse an unresolved `packages.yml`), then
compiles. It builds nothing and writes nothing; `WAREHOUSE_PATH` has to point at a warehouse
techflow has been built into at some point, so that `fct_mrr_daily` exists to be reasoned about.

jaffle_shop's `run_debug.py` covers the relation / CTE / read-only surface of
`duckrun.dbt_project()`. What techflow adds is `fct_mrr_daily`: a real `is_incremental()` branch on a
project with ~30 models and 137 data tests. The session reports which branch it compiled — it cannot
be read off the SQL, since rendering erases the `{% if %}` — and because the target table is already
in the warehouse, that report has a correct answer to be checked against: the
incremental branch by default, the full-refresh branch under `incremental=False`, and different SQL
for the two.

It also asserts neither branch references an ephemeral `__dbt__cte__` it does not define. The branch
answer costs a second compile of the same node against one warm manifest, which is exactly where dbt
stops re-injecting ephemeral parents.
