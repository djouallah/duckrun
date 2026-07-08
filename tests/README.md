# tests

Everything test-, repro-, and tooling-related lives under `tests/`, grouped by concern. Each
subfolder is one kind of thing; nothing loose at the top level.

| Folder | What it is | Run by |
| --- | --- | --- |
| [`adapter/`](adapter/) | Unit/integration tests for the **dbt adapter** internals (write engine, remote discovery, the `table_exists` guard). Fast, local, no network. | `.github/workflows/cores.yml` (`adapter` job — every push/PR + release gate) |
| [`connection_api/`](connection_api/) | Tests for the top-level **`duckrun.connect()`** notebook API, all in `test_connection_api.py`: the per-method `Test*` classes (rendered into the scorecard), the offline local-fs contract suite, and the cross-API write-correctness matrix (SQL ≡ DataFrame). The end-to-end coffee scenario lives under `integration_tests/coffee/`. | `cores.yml` (`adapter` job runs the whole suite — every push/PR + release gate; `connection-card` job → method scorecard from the `Test*` classes) |
| [`conformance/`](conformance/) | The official **dbt adapter conformance suite** (`dbt-tests-adapter`) subclassed against duckrun. | `.github/workflows/cores.yml` (`conformance` job) → `pytest tests/conformance` |
| [`aemo/`](../aemo/) | A real **dbt project** (the AEMO example) built against OneLake to exercise the adapter end to end. | `.github/workflows/aemo.yml` |
| [`integration_tests/coffee/`](integration_tests/coffee/) | One coffee-shop scenario for **`duckrun.connect()`** (dims from a public CSV generator, locally-generated fact) exercising every method, parameterized by row count: `test_coffee_local` (local fs, big — stress) and `test_coffee_onelake` (live OneLake, small — **skips** without `WAREHOUSE_PATH` + `ONELAKE_TOKEN`). Knobs: `COFFEE_LOCAL_ROWS`, `COFFEE_ONELAKE_ROWS`. | `pytest integration_tests/coffee` (local) · `.github/workflows/integration_tests_onelake.yml` (`coffee` job, OneLake) · `local_stress_tests.yml` (`coffee-stress` job, local stress) |
| [`correctness/`](correctness/) | Standalone **correctness scripts** for specific invariants (e.g. concurrency / OCC). | `.github/workflows/cores.yml` (`concurrency-correctness` job) |
| [`tools/`](tools/) | **Tooling/scripts**: the MERGE TPCH benchmark, conformance baseline/gate/summary, README card injection. | `.github/workflows/local_stress_tests.yml`, `cores.yml` |

**What's gitignored:** only runtime outputs (`conformance/_run.log`, the integration dbt
`target/`/`logs/`/`warehouse/`) and **`_local/`** — the one clearly-named folder for local
throwaway (scratch warehouses, ad-hoc run scripts). Test *code* is never ignored.

## Live OneLake tests

`integration_tests/coffee/test_coffee.py::test_coffee_onelake` and the `aemo/` project hit a real Microsoft
Fabric Lakehouse over `abfss://`. They use an **isolated schema** so they never touch real tables,
and read credentials from the environment (no Azure ids hardcoded):

```
WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables   (the …/Tables root)
ONELAKE_TOKEN    a storage bearer token: az account get-access-token \
                   --resource https://storage.azure.com/ --query accessToken -o tsv
```

Without those set, the live tests skip cleanly, so `pytest tests` is safe offline.
