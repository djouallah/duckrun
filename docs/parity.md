# dbt-duckdb compatibility

duckrun is built to be a **drop-in for dbt-duckdb** — same DuckDB SQL, same models, but every
table materializes to Delta Lake via delta-rs. `parity_tests/`
([on GitHub](../tests/parity_tests/)) proves that claim against *real, unmodified* dbt+DuckDB
projects: take a project whose profile says `type: duckdb` and run it **verbatim** on duckrun. The
bar is a green `dbt build` — the project's own models, seeds, snapshots, data tests and unit tests
all pass, with only the connection swapped.

"Unmodified" is literal. The project repo is cloned fresh and not edited — not one line. The only
thing supplied from outside is the connection: a duckrun profile passed via `--profiles-dir`. In
dbt the profile is connection config that lives *outside* the project (that's why projects keep it
in `profiles/<warehouse>/`), so swapping in a duckrun profile changes nothing in the repo. When a
project does expose a gap, the fix lands in **duckrun**, never in the project.

## jaffle_shop — the canonical reference { #jaffle_shop }

[`parity_tests/jaffle_shop/run_parity.py`](../tests/parity_tests/jaffle_shop/run_parity.py) clones
[dbt-labs/jaffle_shop_duckdb](https://github.com/dbt-labs/jaffle_shop_duckdb) and runs `dbt build`
on duckrun. The full build runs green — 28/28: 3 seeds, 2 table models, 3 view models, 20 data
tests — unmodified.

→ **[Browse the jaffle_shop dbt docs](jaffle_shop.html)** — the full dbt documentation site (DAG +
catalog with per-table row/byte stats), generated on duckrun by `dbt docs generate --static`.

## sde — delete+insert + SCD2 + medallion { #sde }

[Start Data Engineering](https://github.com/josephmachado/simple_dbt_project) is a medallion
bronze→silver→gold project with a **`delete+insert` incremental model**, an **SCD2 snapshot**,
packages, and an exposure. It ingests raw CSVs into a DuckDB file via its own EL and reads them as
`sources` — so the duckrun profile sets `path` to that file and `root_path` to a Delta warehouse.
[`run_parity.py`](../tests/parity_tests/sde/run_parity.py) runs the repo's EL then `dbt build`.

This is the project that exposed duckrun silently aliasing `delete+insert` to `merge`; with real
delete+insert it runs **verbatim** and green — the incremental `fct_clickstream`, `fct_orders`, the
seed, the SCD2 `dim_customer` snapshot, and every test.

→ **[Browse the sde dbt docs](sde.html)** — generated on duckrun by `dbt docs generate --static`.

## MRR — subscription revenue + unit tests { #mrr }

[dbt-mrr-assignment](https://github.com/Elkadev/dbt-mrr-assignment) is a **monthly recurring
revenue** model: CSV seeds → staging/intermediate **views** → mart **tables** that amortize invoices
into per-month revenue (`fct_mrr`) and derive MRR **movements** — new / expansion / contraction /
reactivation / retained (`fct_mrr_movements`). It's the first project here with **native dbt
`unit_tests:`** (3 cases on the amortization model) plus singular tests and an exposure, so a green
`dbt build` proves duckrun's unit-test / test path too. No external sources, so the duckrun profile
just points `root_path` at a Delta warehouse.

The full build — 5 seeds, 2 table models, 6 views, 43 data tests, 3 unit tests, 1 exposure — runs
green on duckrun, unmodified.

→ **[Browse the MRR dbt docs](mrr.html)** — generated on duckrun by `dbt docs generate --static`.

## TechFlow — unit tests + parquet sources + snapshots { #techflow }

[ameijin/dbt-example](https://github.com/ameijin/dbt-example) ("TechFlow Analytics") is a SaaS model
that stacks patterns the others don't: raw data read from committed **parquet** via dbt-duckdb
`external_location` sources, an **incremental** model, two **timestamp snapshots**,
`dbt_expectations`, exposures and native `unit_tests:`. The full `dbt build` (seeds, snapshots,
~30 models, 2 unit tests, 137 data tests, exposures) runs green on duckrun, unmodified.

This project also bundles **dbt_project_evaluator**, a dbt Labs *linting* package that introspects
the dbt graph. One knowing quirk: the package hardcodes
`+materialized: "{{ 'table' if target.type in ['duckdb'] else 'view' }}"`, and duckrun is its
**own** adapter type (`target.type == 'duckrun'`), so the package's own bookkeeping models
materialize as views under duckrun. That can't be "fixed" in duckrun — reporting `type: duckdb` is
what makes dbt load dbt-duckdb — and the package still runs green.

→ **[Browse the TechFlow dbt docs](techflow.html)** — generated on duckrun by `dbt docs generate --static`.

## Tuva — a 100+-model real-world project { #tuva }

[Tuva Health](https://github.com/tuva-health/tuva) is a large healthcare claims/clinical data
model (100+ models, snapshots, packages, Elementary observability). Its own `integration_tests`
project — every vertical enabled, synthetic data from S3 — builds **green on duckrun, unmodified**:
`dbt build` runs Tuva's models, snapshots, data-quality and tests, so a clean run means the
DuckDB/Delta port reproduces Tuva's expected results.

| metric | value |
|--------|-------|
| scope | all verticals — claims, clinical, provider attribution, semantic layer, data quality |
| run | `dbt deps` + full `dbt build` (models + snapshots + Elementary + tests) |
| result | ✓ green |
| wall time | 21m 5s on a Linux CI runner |

→ **[Browse the Tuva dbt docs](tuva.html)** — the full dbt documentation site (100+ models +
catalog stats), generated on duckrun in CI by `dbt docs generate --static`.

It runs in CI on Linux ([`.github/workflows/parity.yml`](../.github/workflows/parity.yml), the
`tuva` job, `workflow_dispatch`). It is Linux-only because Tuva consumes itself as a `local: ../`
package, which dbt symlinks on Linux but copies recursively on Windows — a dbt/OS quirk, unrelated
to the adapter.

## Bugs this surfaced

Running real projects unchanged is the best bug-finder duckrun has. The parity work fixed, in
duckrun, two raw-DML routing bugs that only complex projects trigger:

- an `INSERT … VALUES` was mis-read as `INSERT … SELECT` when a `select` appeared inside a string
  literal in the payload (Elementary's `compiled_code` column);
- the statement splitter wasn't PostgreSQL/DuckDB dollar-quote aware, so a `;` inside
  `COMMENT ON … IS $tag$…$tag$` truncated the statement.

Both now have regression tests, and the conformance baseline gate stays green.
