# TPC-DI on duckrun

A port of **TPC-DI** — the TPC's data-integration (ETL) benchmark — to duckrun.
Multi-format source files (pipe/CSV text, fixed-width FINWIRE, and CustomerMgmt
XML) are transformed by dbt models running in DuckDB and materialized as Delta
Lake tables, locally or on OneLake. It exercises exactly duckrun's strengths:
SCD Type 2 dimensions, dimensional joins over effective-date windows, and Delta
output that runs identically local ↔ Fabric.

The transforms are ported from the public
[shannon-barrow/databricks-tpc-di](https://github.com/shannon-barrow/databricks-tpc-di)
project (its `Snowflake_CSV` dialect — closest to DuckDB), and its vendored,
standalone **DIGen** generator is fetched at build time (see below).

## What it builds

A single `dbt run` performs the whole load in one pass (Batch1 historical +
Batch2/3 incremental CDC together), producing the dimensional warehouse:

- **Dimensions:** `DimDate`, `DimTime`, `DimBroker`, `DimCompany`*, `DimSecurity`*,
  `DimCustomer`*, `DimAccount`*, `DimTrade`  (`*` = SCD Type 2)
- **Facts:** `FactCashBalances`, `FactHoldings`, `FactWatches`, `FactMarketHistory`
- **Reference / other:** `Industry`, `StatusType`, `TaxRate`, `TradeType`,
  `Financial`, `Prospect`, plus staging (`FinWire`, `ProspectIncremental`,
  `stg_customermgmt`, `BatchDate`)

## Layout

```
models/base/        typed reads of the reference/date files + FinWire split + Prospect
models/silver/      FINWIRE-derived SCD2 dims (DimCompany, DimSecurity) + Financial + DimBroker
models/staging/     stg_customermgmt — CustomerMgmt.xml flattened via the `webbed` extension
models/incremental/ SCD2 DimCustomer/DimAccount/DimTrade, Prospect, and the four facts
macros/tpcdi.sql    shared read_pipe / read_csvfile / read_fixed helpers + xml/sk/status macros
scripts/            generate_data.py (DIGen), run_benchmark.py (driver)
audit/validate.py   coverage/liveness smoke audit of the built warehouse
```

## Running it

Requires a JDK (for DIGen) and network access (to fetch DIGen and install the
`webbed` DuckDB community extension). One command does generate → dbt run → audit:

```bash
python tests/tpc_di/scripts/run_benchmark.py --sf 3           # local ./warehouse
```

OneLake (Delta output to a Fabric Lakehouse):

```bash
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
ONELAKE_TOKEN=<bearer> \
python tests/tpc_di/scripts/run_benchmark.py --sf 3 --target onelake
```

CI (`.github/workflows/tpc_di.yml`) runs the local path on every push under
`tests/tpc_di/**` — that is the verification harness for this port.

## Notes & design choices

- **Data generator.** DIGen is TPC-licensed and not redistributable, so it is not
  vendored. `generate_data.py` shallow-clones the dbx repo (which carries the whole
  standalone DIGen + PDGF toolkit) and runs `java -jar DIGen.jar -sf N -o …`. No
  Spark is involved. Override the source with `DBX_TPCDI_REPO` / `DBX_TPCDI_REF`.
- **XML.** DuckDB has no first-party XML reader, so `stg_customermgmt` uses the
  [`webbed`](https://github.com/teaguesterling/duckdb_webbed) community extension
  (`INSTALL webbed FROM community; LOAD webbed`, via the model's `pre_hook`). It is
  materialized as a table, so only that one model needs the extension.
- **Headerless typed files.** duckrun keeps source scans to auto-detect, so the
  models read the raw files directly with an explicit `read_csv(columns=…)`
  (`macros/tpcdi.sql`) rather than declaring dbt sources.
- **SCD2.** Company/Security/Customer/Account versioning is done with the reference
  project's window-function SQL (effective/end dating driven by batch dates and CDC
  actions), not dbt snapshots — dbt's hash-based snapshot semantics don't match
  TPC-DI's dating rules.

## Not yet covered (follow-ups)

- **Per-batch incremental audit.** This port is single-pass (the dbx *dbt* project
  is too). The 3-batch-with-audit-checkpoints variant is a larger, separate build.
- **Full Appendix-A audit.** `audit/validate.py` is a coverage/liveness gate (every
  required table present and non-empty). The spec's row-count and business-rule
  reconciliation against DIGen's audit CSVs is not yet implemented.
