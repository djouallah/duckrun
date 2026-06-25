# dbt-duckdb parity

duckrun is built to be a **drop-in for dbt-duckdb** — same DuckDB SQL, same models, but every
table materializes to Delta Lake via delta-rs. `parity_tests/`
([on GitHub](../parity_tests/)) proves that claim against *real, unmodified* dbt+DuckDB projects:
take a project whose profile says `type: duckdb`, run it **verbatim** on both dbt-duckdb (the
oracle) and duckrun, and check the results match.

"Unmodified" is literal. The project repo is cloned fresh and not edited — not one line. The only
thing supplied from outside is the connection: a duckrun profile passed via `--profiles-dir`. In
dbt the profile is connection config that lives *outside* the project (that's why projects keep it
in `profiles/<warehouse>/`), so swapping in a duckrun profile changes nothing in the repo. When a
project does expose a gap, the fix lands in **duckrun**, never in the project.

## jaffle_shop — full differential { #jaffle_shop }

[`parity_tests/jaffle_shop/run_parity.py`](../parity_tests/jaffle_shop/run_parity.py) clones
[dbt-labs/jaffle_shop_duckdb](https://github.com/dbt-labs/jaffle_shop_duckdb), runs `dbt build`
once on dbt-duckdb and once on duckrun, then diffs **every persisted table** with a row-multiset
`EXCEPT ALL` both ways. dbt-duckdb's tables are ground truth; a mismatch is a duckrun bug.

Result — identical, row for row, both sides 28/28 green:

| table          | rows | duckrun == dbt-duckdb |
|----------------|------|:---------------------:|
| customers      | 100  | ✓ |
| orders         | 99   | ✓ |
| raw_customers  | 100  | ✓ |
| raw_orders     | 99   | ✓ |
| raw_payments   | 113  | ✓ |

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
