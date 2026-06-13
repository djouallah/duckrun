# tests

Everything test-, repro-, and tooling-related lives under `tests/`, grouped by concern. Each
subfolder is one kind of thing; nothing loose at the top level.

| Folder | What it is | Run by |
| --- | --- | --- |
| [`adapter/`](adapter/) | Unit/integration tests for the **dbt adapter** internals (write engine, remote discovery, the `table_exists` guard). Fast, local, no network. | `pytest tests/adapter` (local) |
| [`connection_api/`](connection_api/) | Tests for the top-level **`duckrun.connect()`** notebook API. `test_local_filesystem.py` is the offline, network-free regression suite (tiny synthetic tables). | `pytest tests/connection_api` (local) |
| [`connection_api/coffeeshop/`](connection_api/coffeeshop/) | One coffee-shop scenario (dims from a public CSV generator, locally-generated fact) exercising every method, parameterized by row count: `test_coffee_local` (local fs, big — stress) and `test_coffee_onelake` (live OneLake, small — **skips** without `WAREHOUSE_PATH` + `ONELAKE_TOKEN`). Knobs: `COFFEE_LOCAL_ROWS`, `COFFEE_ONELAKE_ROWS`. | `pytest tests/connection_api/coffeeshop` (local) · `.github/workflows/coffee-stress.yml` (manual stress, `rows` input) |
| [`conformance/`](conformance/) | The official **dbt adapter conformance suite** (`dbt-tests-adapter`) subclassed against duckrun. | `.github/workflows/conformance.yml` → `pytest tests/conformance` |
| [`integration_tests/`](integration_tests/) | A real **dbt project** (the AEMO example) built twice against OneLake to exercise the adapter end to end. | `.github/workflows/aemo.yml` |
| [`correctness/`](correctness/) | Standalone **correctness scripts** for specific invariants (e.g. concurrency / OCC). | `.github/workflows/concurrency-correctness.yml` |
| [`tools/`](tools/) | **Tooling/scripts**: the MERGE TPCH benchmark, conformance baseline/gate/summary, README card injection. | `.github/workflows/merge.yml`, `conformance.yml` |

`.localtest_*` are gitignored local scratch (a throwaway warehouse + run script); ignore them.

## Live OneLake tests

`connection_api/test_onelake_coffee.py` and the `integration_tests/` project hit a real Microsoft
Fabric Lakehouse over `abfss://`. They use an **isolated schema** so they never touch real tables,
and read credentials from the environment (no Azure ids hardcoded):

```
WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables   (the …/Tables root)
ONELAKE_TOKEN    a storage bearer token: az account get-access-token \
                   --resource https://storage.azure.com/ --query accessToken -o tsv
```

Without those set, the live tests skip cleanly, so `pytest tests` is safe offline.
