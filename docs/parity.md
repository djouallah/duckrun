# dbt-duckdb compatibility

duckrun is built as a **drop-in for dbt-duckdb**. `parity_tests/` ([on GitHub](../tests/parity_tests/))
proves it against real, unmodified `type: duckdb` projects: the repo is cloned fresh and not edited,
and the only thing supplied from outside is a duckrun profile via `--profiles-dir` (in dbt the
profile lives outside the project). The bar is a green `dbt build` — models, seeds, snapshots, data
tests and unit tests. When a project exposes a gap, the fix lands in duckrun, never in the project.

| project | what it exercises | dbt docs |
|---|---|---|
| [jaffle_shop](https://github.com/dbt-labs/jaffle_shop_duckdb) | the canonical reference: 3 seeds, 2 table models, 3 view models, 20 data tests | [browse](jaffle_shop.html) |
| [sde](https://github.com/josephmachado/simple_dbt_project) (Start Data Engineering) | medallion bronze → silver → gold, a `delete+insert` incremental model, an SCD2 snapshot, packages, an exposure; its own EL loads CSVs into a DuckDB file read as `sources` | [browse](sde.html) |
| [MRR](https://github.com/Elkadev/dbt-mrr-assignment) | seeds → staging views → mart tables amortizing invoices into monthly revenue and MRR movements; native `unit_tests:`, singular tests, an exposure | [browse](mrr.html) |
| [TechFlow](https://github.com/ameijin/dbt-example) | parquet `external_location` sources, an incremental model, two timestamp snapshots, `dbt_expectations`, `dbt_project_evaluator`, native `unit_tests:`, 137 data tests | [browse](techflow.html) |
| [Tuva](https://github.com/tuva-health/tuva) | 100+ healthcare models, snapshots, packages, Elementary; its `integration_tests` project with every vertical enabled and synthetic data from S3; 21 min on a Linux CI runner | [browse](tuva.html) |

Every project builds green. Each docs link is the project's own dbt documentation site, generated on
duckrun by `dbt docs generate --static`; the runners are `run_parity.py` under each
`tests/parity_tests/<project>/`, driven by [`parity.yml`](../.github/workflows/parity.yml).

**TechFlow's `dbt_project_evaluator`** hardcodes
`+materialized: "{{ 'table' if target.type in ['duckdb'] else 'view' }}"`, and duckrun is its own
adapter type (`target.type == 'duckrun'`), so the package's bookkeeping models materialize as views
under duckrun. That can't be changed in duckrun, and the package still runs green.

**Tuva is Linux-only** because it consumes itself as a `local: ../` package, which dbt symlinks on
Linux but copies recursively on Windows — a dbt/OS quirk, unrelated to the adapter.
