# tests

Everything test-, repro-, and tooling-related lives under `tests/`, grouped by concern. Each
subfolder is one kind of thing; nothing loose at the top level.

| Folder | What it is | Run by |
| --- | --- | --- |
| [`adapter/`](adapter/) | Unit/integration tests for the **dbt adapter** internals (write engine, remote discovery, the `table_exists` guard). Fast, local, no network. | `.github/workflows/core.yml` (every push/PR + release gate) |
| [`connection_api/`](connection_api/) | Tests for the top-level **`duckrun.connect()`** notebook API. `test_local_filesystem.py` = offline regression suite; `test_method_matrix.py` = one discrete test per method, rendered into a scorecard. | `core.yml` (every push/PR + release gate) · `connection-card.yml` (matrix → method scorecard card) |
| [`conformance/`](conformance/) | The official **dbt adapter conformance suite** (`dbt-tests-adapter`) subclassed against duckrun. | `.github/workflows/conformance.yml` → `pytest tests/conformance` |
| [`integration_tests/aemo/`](integration_tests/aemo/) | A real **dbt project** (the AEMO example) built against OneLake to exercise the adapter end to end. | `.github/workflows/integration.yml` (`aemo` job) |
| [`integration_tests/coffee/`](integration_tests/coffee/) | One coffee-shop scenario for **`duckrun.connect()`** (dims from a public CSV generator, locally-generated fact) exercising every method, parameterized by row count: `test_coffee_local` (local fs, big — stress) and `test_coffee_onelake` (live OneLake, small — **skips** without `WAREHOUSE_PATH` + `ONELAKE_TOKEN`). Knobs: `COFFEE_LOCAL_ROWS`, `COFFEE_ONELAKE_ROWS`. | `pytest tests/integration_tests/coffee` (local) · `.github/workflows/integration.yml` (`coffee` job — manual stress, `scenario`/`rows` inputs) |
| [`correctness/`](correctness/) | Standalone **correctness scripts** for specific invariants (e.g. concurrency / OCC). | `.github/workflows/concurrency-correctness.yml` |
| [`tools/`](tools/) | **Tooling/scripts**: the MERGE TPCH benchmark, conformance baseline/gate/summary, README card injection. | `.github/workflows/merge.yml`, `conformance.yml` |

**What's gitignored:** only runtime outputs (`conformance/_run.log`, the integration dbt
`target/`/`logs/`/`warehouse/`) and **`_local/`** — the one clearly-named folder for local
throwaway (scratch warehouses, ad-hoc run scripts). Test *code* is never ignored.

## Live OneLake tests

`integration_tests/coffee/test_coffee.py::test_coffee_onelake` and the `integration_tests/aemo/` project hit a real Microsoft
Fabric Lakehouse over `abfss://`. They use an **isolated schema** so they never touch real tables,
and read credentials from the environment (no Azure ids hardcoded):

```
WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables   (the …/Tables root)
ONELAKE_TOKEN    a storage bearer token: az account get-access-token \
                   --resource https://storage.azure.com/ --query accessToken -o tsv
```

Without those set, the live tests skip cleanly, so `pytest tests` is safe offline.
