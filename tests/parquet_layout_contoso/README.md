# parquet_layout_contoso — Direct Lake layout benchmark (duckrun vs real V-Order), Contoso dataset

A second dataset for the Direct Lake query benchmark that lives next door in
[`../parquet_layout_tests/`](../parquet_layout_tests/) (AEMO). Same question, same machinery: **does
the Power BI / Direct Lake engine query a duckrun-written table as fast as a real Fabric V-Order
table of the same data?** — measured as DAX query speed (cold Delta→memory transcode + hot scan)
over the XMLA endpoint. Where the AEMO benchmark reads a pre-built ~140M-row electricity fact, this
one **generates the Contoso star from scratch with SQLBI's Contoso Data Generator**, so the row
count is a knob and the pipeline demonstrates a real generate → land → benchmark flow.

## Credit & sources — SQLBI Contoso Data Generator V2

The data, the star schema, and the ported DAX measures all come from **SQLBI's Contoso Data
Generator V2**, an open-source (MIT) project by [SQLBI](https://www.sqlbi.com/) — Marco Russo &
Alberto Ferrari. This benchmark **runs their tool**; it does not copy their pre-baked data.

- **Generator tool (run by [`build_base.py`](build_base.py)):**
  https://github.com/sql-bi/Contoso-Data-Generator-V2 — release `2.0.1`, self-contained
  `DatabaseGenerator` binaries. The bundled `data.xlsx` seed workbook and the static reference data
  (`Cust.*`, `Exch_*`, `UKPostcodes` — downloaded to the generator's `CACHE/` on first run) are
  SQLBI's.
- **Ready-to-use data + models (not used here, but the source of the model below):**
  https://github.com/sql-bi/Contoso-Data-Generator-V2-Data
- **The semantic model** under [`contoso_auto_sort.SemanticModel/`](contoso_auto_sort.SemanticModel/)
  / [`contoso_vorder.SemanticModel/`](contoso_vorder.SemanticModel/) is **ported from SQLBI's
  `pbit-Import` template** — its tables, star-schema relationships, and the five Sales measures
  (`Sales Amount`, `Total Cost`, `Margin`, `Margin %`, `Total Quantity`) are SQLBI's DAX, verbatim;
  we only rewrote the storage layer to **Direct Lake** pointing at the duckrun-written Delta.
- **Full documentation:** https://docs.sqlbi.com/contoso-data-generator/ ·
  **Tool page:** https://www.sqlbi.com/tools/contoso-data-generator/
- **License:** MIT — see the upstream repositories.

## What it compares

Two Direct Lake semantic models over the **same** Contoso `Sales` fact. The duckrun and Spark builds
each read the pristine `contoso.sales` (never mutated) **directly and independently**, so the
V-Order input is never routed through a duckrun-written table, and both hold the same rows — only
**layout**, hence **speed**, differs:

| Model | Fact table | Layout |
|:--|:--|:--|
| `contoso_auto_sort` | `tests.sales_auto_sort` | **duckrun** `sorted by auto` — the layout under test (current WriterProperties) |
| `contoso_vorder` | `tests.sales_vorder` | **Fabric Spark V-Order** — the reference |

The dimensions (`Customer`, `Product`, `Store`, `Date`, `Currency Exchange`) and the other facts
(`Orders`, `OrderRows`) are generated + loaded to the `contoso` schema and stay stable in both
models — exactly as AEMO swaps only its `fct_summary`. The full Contoso schema is generated
(`SalesOrders: BOTH`); only the `Sales` fact gets the auto_sort-vs-vorder layout swap.

## Pipeline (what the workflow runs, in order)

1. **Create the `sqlbi` lakehouse** (idempotent) — Contoso lands in its own dedicated lakehouse,
   isolated from the AEMO `data` lakehouse.
2. **[`build_base.py`](build_base.py)** — download the `DatabaseGenerator` binary for the runner,
   run it with the vendored [`config.json`](config.json) (`OrdersCount` overridable via the `orders`
   input), and land all 8 Contoso tables to the `contoso` schema. Skip-if-exists.
3. **[`build_spark_variant.py`](build_spark_variant.py)** — build `tests.sales_vorder` on Fabric
   Spark (V-Order is Spark-only) via the Livy API, reading `contoso.sales` directly. Skip-if-exists.
4. **[`build_auto_sort.py`](build_auto_sort.py)** — build `tests.sales_auto_sort` via duckrun
   `create or replace table … sorted by auto`, reading `contoso.sales` directly. This exercises the
   **current tree's** WriterProperties. Skip-if-exists; `rebuild=true` after changing a default.
5. **[`deploy.py`](deploy.py)** — deploy both `*.SemanticModel`s to the workspace via `fab` + refresh.
6. **[`table_stats.py`](table_stats.py)** — duckrun `get_stats('sales*')` row-group sidecar.
7. **[`xmla_compare.py`](xmla_compare.py)** — the payload: heavy DAX per model over XMLA
   (ADOMD.NET), dehydrating per query for true cold timing; reports cold + hot `base/vorder`
   speedups.
8. **[`render_report.py`](render_report.py)** + **[`render_summary.py`](render_summary.py)** —
   render the analysis + specialist findings to the job summary.

## Run it

**CI (recommended):** trigger the **Direct Lake query benchmark — Contoso** workflow via
*workflow_dispatch*. Manual only; never gates a release. Inputs mirror the AEMO benchmark plus
`orders` (the generator's `OrdersCount`).

The shared scripts (`report.py`, the build/deploy/xmla/render modules) are **adapted copies** of the
AEMO benchmark's, retargeted to the Contoso `Sales` fact — so this folder is self-contained and the
AEMO benchmark is untouched.

## Prerequisites

- The repo's federated identity (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` secrets) must have access to
  the workspace in [`deploy_config.yml`](deploy_config.yml). The `sqlbi` lakehouse is created by the
  workflow if absent.
- `data.xlsx` is **not** vendored — the generator's release zip bundles it, and `build_base.py` uses
  the bundled copy, so the seed always matches the tool version.
- Generating the base needs network **once** (the generator downloads its static reference cache
  from SQLBI's `static-files` release).
