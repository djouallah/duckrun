# Compatibility test — MRR (subscription revenue) on duckrun

Run the upstream **dbt-mrr-assignment** project on duckrun, **unchanged**. The bar is a green
`dbt build` — the project's own tests and unit tests included; a failure is a duckrun bug (fixed in
duckrun, never in the project). dbt-duckdb itself is not built — nothing here tests it.

This adds a new analytical idiom to the suite: **monthly recurring revenue**. Invoices are
amortized into per-month revenue (`int_invoice_monthly_amortized`), then `fct_mrr` and
`fct_mrr_movements` derive MRR and its movements (new / expansion / contraction / reactivation /
retained) — date/window logic that jaffle_shop (ecommerce), sde (clickstream) and Tuva (healthcare)
don't exercise. It is also the first project here with **native dbt `unit_tests:`** (3 cases on the
amortization model), plus singular tests and an exposure — `dbt build` runs all of them, so a green
run proves duckrun's unit-test / test path works too.

## The repo under test

- **Repo:** https://github.com/Elkadev/dbt-mrr-assignment
- Patterns: committed CSV **seeds** (no EL, no external sources), staging + intermediate **views**,
  mart **tables** (`fct_mrr`, `fct_mrr_movements`), `dbt_utils`, generic + singular tests, native
  `unit_tests:`, and an exposure. No incremental models, no snapshot.

## The connection (the only thing not in the repo)

In dbt the *profile* (warehouse connection) lives outside the project, so swapping it in changes
nothing in the repo. [profiles.yml](profiles.yml) here defines the `mrr_analytics` profile as
`type: duckrun` and sets `root_path` to a Delta warehouse (the seeds + marts materialize there).
The repo is never modified.

## Run it

```bash
python tests/parity_tests/mrr/run_parity.py
```

It clones the repo fresh and runs `dbt deps` + `dbt build` with the duckrun profile — OneLake in CI
(`WAREHOUSE_PATH`), a local Delta warehouse otherwise. Exit 0 = the project built green.

## Result (latest run)

The full `dbt build` (5 seeds, 2 table models, 6 views, 43 data tests, 3 unit tests, 1 exposure)
runs **green on duckrun**, unmodified. The `stg_*` and `int_invoice_monthly_amortized` models are
`view`s — duckrun has no durable view, so they're intermediate-only; the marts that depend on them
build and test green.
