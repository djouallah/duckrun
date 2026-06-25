# Parity test — MRR (subscription revenue) on duckrun

Run the upstream **dbt-mrr-assignment** project on duckrun, **unchanged**, and assert duckrun's
Delta output matches dbt-duckdb table-for-table. dbt-duckdb is the oracle; a mismatch is a duckrun
bug.

This adds a new analytical idiom to the parity suite: **monthly recurring revenue**. Invoices are
amortized into per-month revenue (`int_invoice_monthly_amortized`), then `fct_mrr` and
`fct_mrr_movements` derive MRR and its movements (new / expansion / contraction / reactivation /
retained) — date/window logic that jaffle_shop (ecommerce), sde (clickstream) and Tuva (healthcare)
don't exercise. It is also the first project here with **native dbt `unit_tests:`** (3 cases on the
amortization model), plus singular tests and an exposure — `dbt build` runs all of them on both
adapters, so a green run proves duckrun's unit-test / test path works too.

## The repo under test

- **Repo:** https://github.com/Elkadev/dbt-mrr-assignment
- Patterns: committed CSV **seeds** (no EL, no external sources), staging + intermediate **views**,
  mart **tables** (`fct_mrr`, `fct_mrr_movements`), `dbt_utils`, generic + singular tests, native
  `unit_tests:`, and an exposure. No incremental models, no snapshot.

## The connection (the only thing not in the repo)

In dbt the *profile* (warehouse connection) lives outside the project, so swapping it in changes
nothing in the repo. [profiles.yml](profiles.yml) here defines the `mrr_analytics` profile as
`type: duckrun` and sets `root_path` to a Delta warehouse (the seeds + marts materialize there). The
oracle side uses the repo's **own** `type: duckdb` profile. The repo is never modified.

## Run it

```bash
python parity_tests/mrr/run_parity.py
```

It clones the repo into two dirs, runs `dbt deps` + `dbt build` once per adapter (duckdb oracle,
duckrun), then diffs every persisted table. Exit 0 = parity.

## Result (latest run)

| table              | rows | duckrun == dbt-duckdb |
|--------------------|------|:---------------------:|
| main.fct_mrr            | 1484 | ✓ |
| main.fct_mrr_movements  | 2078 | ✓ |
| main.invoices (seed)    | 2441 | ✓ |
| main.subscriptions (seed) | 666 | ✓ |
| main.customers (seed)   | 292  | ✓ |
| main.schools (seed)     | 292  | ✓ |
| main.products (seed)    | 9    | ✓ |

The `stg_*` and `int_invoice_monthly_amortized` models are `view`s — duckrun has no durable view, so
they're intermediate-only and not part of the persisted diff. The full `dbt build` (5 seeds, 2 table
models, 6 views, 43 data tests, 3 unit tests, 1 exposure) runs **green on duckrun**, unmodified.

## Caveat

This is a personal "assignment" repo (not an official dbt-labs project) and fills no specific
*incremental* feature gap — its value is a new analytical idiom (MRR) plus native unit-test coverage,
under a clean full row-diff.
