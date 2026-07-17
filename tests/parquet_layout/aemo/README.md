# aemo — Direct Lake layout benchmark (duckrun vs real V-Order)

A **manual, non-gating** benchmark that answers the only question that matters when tuning
duckrun's parquet write layout: **does the Power BI / Direct Lake engine query a duckrun-written
table as fast as a real Fabric V-Order table of the same data?** File-size / row-group stats are
a useful sidecar, but the real signal is DAX query speed — cold Delta→memory transcode and hot
scan — measured over the XMLA endpoint. Use this *before* changing a WriterProperties default
(row-group size, dictionary-page limit, target file size) so you catch a regression as a query
that got slower vs V-Order, not months later in production.

Ported from the AEMO project `djouallah/dbt_fabric_python_delta` (`benchmark/` + `xmla_compare.yml`).

## What it compares

Two Direct Lake semantic models over the **same** ~140M-row AEMO fact. The duckrun and Spark builds
each read the pristine `mart.fct_summary` (never mutated) **directly and independently** — so the
V-Order input is never routed through a duckrun-written table, and (at the default full-table run)
both hold the same rows, so only **speed** differs:

| Model | Fact table | Layout |
|:--|:--|:--|
| `aemo_electricity_auto_sort` | `tests.fct_summary_auto_sort` | **duckrun** `sorted by auto` — the layout under test (current WriterProperties) |
| `aemo_electricity_vorder` | `tests.fct_summary_vorder` | **Fabric Spark V-Order** — the reference |

## Pipeline (what the workflow runs, in order)

1. **`build_spark_variant.py`** — builds `tests.fct_summary_vorder` on Fabric Spark via
   the Livy API (V-Order is Spark-only), reading `mart.fct_summary` directly (applies `row_limit`).
   Skip-if-exists.
2. **`build_auto_sort.py`** — builds `tests.fct_summary_auto_sort` via duckrun
   `create or replace table … sorted by auto`, reading `mart.fct_summary` directly with its own
   independent `row_limit` read. This exercises the **current tree's** WriterProperties (the workflow
   `pip install -e .`s the checked-out duckrun). Skip-if-exists; set `rebuild=true` after changing a
   default.
3. **`deploy_vorder.py`** — deploys both `*.SemanticModel`s to the workspace via duckrun's own
   `workspace.deploy()` (repoint + create + Direct Lake refresh), no Fabric CLI.
4. **`table_stats.py`** — duckrun `get_stats('fct_summary*')` row-group sidecar → job summary +
   `stats_detailed.csv` artifact (continue-on-error).
5. **`xmla_compare.py`** — the payload: heavy DAX queries per model over XMLA (ADOMD.NET),
   dehydrating per query for true cold timing; reports cold + hot `base/vorder` speedup tables.

## Run it

**CI (recommended):** trigger the **🔍 parquet layout benchmark** workflow via *workflow_dispatch*.
It is manual only and never gates a release. Inputs: `env` (deploy_config section), `runs`,
`cold`, `gap_seconds`, `opt_sort`, `rebuild`.

**Locally:** you need a live Fabric capacity and tokens. Roughly:

```bash
export ONELAKE_TABLES_PATH="abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables"
export ONELAKE_TOKEN=...   # storage.azure.com
export FABRIC_TOKEN=...    # api.fabric.microsoft.com   (livy + deploy)
export PBI_TOKEN=...       # analysis.windows.net/powerbi/api  (XMLA)
export PBI_WORKSPACE="<workspace display name>"
export WS_ID=... LH_ID=... ADOMD_DIR=<dir with AdomdClient.dll>
python tests/parquet_layout/aemo/build_spark_variant.py
python tests/parquet_layout/aemo/build_auto_sort.py
python tests/parquet_layout/aemo/deploy_vorder.py --env main
python tests/parquet_layout/aemo/xmla_compare.py
```

## Tuning workflow (the point of this folder)

1. Run once at the current default → note the cold/hot `auto_sort vs vorder` totals.
2. Change one WriterProperties default in `dbt/adapters/duckrun/engine.py`.
3. Re-run with `rebuild=true` (rebuilds only the duckrun `_auto_sort` table; V-Order is the fixed
   reference and is left alone).
4. Compare the new duckrun numbers against the **unchanged** V-Order row. Keep the change only if
   Direct Lake query speed moved toward (or past) V-Order — and the `merge_spill` gate still passes.

## Prerequisites

- The repo's federated identity (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` secrets) must have access to
  the **AEMO workspace + capacity** in `deploy_config.yml`. Without it, run locally with your own tokens.
- Base `mart.fct_summary` + `dim_duid` + `dim_calendar` must already exist in that lakehouse
  (built by the AEMO pipeline). This benchmark reads them and never writes to the base.
- `deploy_config.yml` holds Fabric **workspace GUIDs** — identifiers, not secrets; auth is the OIDC
  secrets above.
