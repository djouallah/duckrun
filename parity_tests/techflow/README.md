# Parity test — TechFlow Analytics (SaaS) on duckrun

Run the upstream **dbt-example** project on duckrun, **unchanged**, and assert duckrun's Delta output
matches dbt-duckdb table-for-table. dbt-duckdb is the oracle; a mismatch is a duckrun bug.

This is the first parity project with **native dbt `unit_tests:`**, and it stacks several patterns the
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
sets `root_path` to a Delta warehouse (the seeds, snapshots and marts materialize there). The oracle
side uses the repo's **own** `type: duckdb` profile. The repo is never modified.

## Run it

```bash
python parity_tests/techflow/run_parity.py
```

It clones the repo into two dirs, runs `dbt deps` + `dbt build` once per adapter (duckdb oracle,
duckrun), then diffs every persisted table. Exit 0 = parity.

## Result (latest run)

| table | rows | duckrun == dbt-duckdb |
|-------|------|:---------------------:|
| marts.fct_events | 52232 | ✓ |
| marts.fct_user_engagement_daily | 36092 | ✓ |
| marts.fct_revenue | 3772 | ✓ |
| marts.fct_mrr_daily (incremental) | 2718 | ✓ * |
| marts.fct_subscription_events | 548 | ✓ |
| marts.dim_users / dim_customers | 500 / 500 | ✓ |
| marts.fct_customer_acquisition | 500 | ✓ |
| marts.dim_subscriptions | 475 | ✓ |
| marts.dim_campaigns / rpt_marketing_roi | 277 / 277 | ✓ |
| marts.rpt_feature_adoption | 18 | ✓ |
| snapshots.user_plan_snapshot | 500 | ✓ ** |
| snapshots.subscription_pricing_snapshot | 475 | ✓ ** |
| seeds.plan_catalog / product_features / utm_channel_mapping | 18 / 18 / 20 | ✓ |

The full `dbt build` (seeds, 2 snapshots, ~30 models, **2 unit tests**, 137 data tests, exposures)
runs **green on duckrun**, unmodified. `stg_*`/`int_*` models are `view`s — duckrun has no durable
view, so they're intermediate-only and not part of the persisted diff.

\* `fct_mrr_daily` stamps `loaded_at = current_timestamp` at build time (differs between two runs),
so it is compared excluding `loaded_at`. Its MRR columns are `DOUBLE`s built by a parallel
`GROUP BY sum(...)` feeding a running-`sum()` window, and DuckDB combines partial float sums in a
thread-order that depends on the runner's core count — so two independent builds drift in the last
ULPs (e.g. `24813.09999999998` vs `…10000000025`). dbt-duckdb has the identical behavior, so it is
**not** a duckrun divergence. The values are genuine currency (every one is within ≤3e-12 of an exact
cent), so these columns are compared **rounded to cents** — which drops only the float noise and still
fails on any real ≥ $0.01 difference; keys, dates and `event_count` are compared exactly.
\*\* snapshots are compared on their business columns; the SCD2 bookkeeping columns (`dbt_scd_id`,
`dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to`) are stamped per run.

## What is NOT diffed, and why

The project includes **dbt_project_evaluator**, a dbt Labs *linting* package. It doesn't model the
SaaS data — it introspects the dbt graph and emits tables describing the project and its adapter. Two
of its columns can't match across two different adapters by design:

- `database` — the connection's catalog name (duckdb's `dev` file vs duckrun's in-memory `memory`);
- `materialized` — the package hardcodes
  `+materialized: "{{ 'table' if target.type in ['duckdb'] else 'view' }}"`. duckrun is its **own**
  adapter type (`target.type == 'duckrun'`, not `'duckdb'`), so ~20 of the evaluator's own models
  materialize as views under duckrun and tables under dbt-duckdb. This can't be "fixed" in duckrun:
  reporting `type: duckdb` is exactly what makes dbt load dbt-duckdb instead of duckrun.

So [run_parity.py](run_parity.py) **skips the dbt_project_evaluator models** from the row diff
(identified from the run's `manifest.json` by `package_name`, logged explicitly — not by quietly
trimming columns). The package still builds and runs **green on duckrun**; it's just not row-compared,
because comparing a linting tool's adapter-introspection across two adapters is apples-to-oranges.
