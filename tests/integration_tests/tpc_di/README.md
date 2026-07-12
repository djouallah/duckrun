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
scripts/            generate_data.py (drives PDGF), upload_to_onelake.py, run_benchmark.py (driver)
audit/validate.py   coverage/liveness smoke audit of the built warehouse
```

## Running it

Requires a JDK (for PDGF) and network access (to fetch the datagen toolkit and
install the `webbed` DuckDB community extension). One command does generate →
dbt run → audit:

```bash
python tests/integration_tests/tpc_di/scripts/run_benchmark.py --sf 3           # local ./warehouse
```

OneLake (Delta output to a Fabric Lakehouse):

```bash
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
ONELAKE_TOKEN=<bearer> \
python tests/integration_tests/tpc_di/scripts/run_benchmark.py --sf 3 --target onelake
```

CI (`.github/workflows/tpc_di.yml`) runs the whole thing **on OneLake**, on every push
under `tests/integration_tests/tpc_di/**`:

1. Create the dedicated **`tpcdi` lakehouse** if it doesn't exist (Fabric REST, schema-enabled).
2. **Check** whether the seed for this scale factor is already in `Files/tpcdi/sf<N>`
   (`check_seed.py` — a listing, no download). If present, **skip generation** — the Java
   generator only runs on a cache miss.
3. On a miss: `generate_data.py` (PDGF, on the runner) → `upload_to_onelake.py` lands
   `Batch1/2/3` in `Files/tpcdi/sf<N>`.
4. `dbt run` reads the seed **from OneLake Files** (globs work over `abfss://`) and writes the
   Delta dimension/fact tables to **`Tables/tpcdi`** — the runner never hosts the data.
5. `validate.py` audits the OneLake warehouse via `duckrun.connect()`.

The scale factor is `${{ inputs.scale_factor }}` (workflow_dispatch) → repo variable `TPCDI_SF`
→ `3`. Changing it uses a fresh `sf<N>` seed cache, so it regenerates once for that SF.

## Notes & design choices

- **Data generator — we drive PDGF, not DIGen.** DIGen is TPC-licensed and not
  vendored; `generate_data.py` shallow-clones the dbx repo (which carries the whole
  standalone DIGen + PDGF toolkit). It does **not** call `DIGen.jar`, though: DIGen
  shells out to `java -jar pdgf.jar …`, but this datagen's `pdgf.jar` manifest omits
  `plugins/tpc-di.jar`, and PDGF discovers its generators/timeframe-modes by scanning
  `java.class.path` — which under `-jar` holds only `pdgf.jar`. So the plugin is never
  seen and the parse dies on `tpc.di.generators.HRJobIdGenerator was not found` (then
  the custom `gen_ReferenceGenerator from="DailyMarket-…"` mode). We instead run
  `java -cp pdgf.jar:plugins/tpc-di.jar:extlib/* pdgf.Controller -sf N000 -start
  -closeWhenDone` — the plugin on `-cp` puts it on `java.class.path` so the scan
  registers everything. We pass **no** `-o` (PDGF splices it into a javassist-compiled
  file template un-quoted, which won't compile) and let PDGF write its default
  `output/Batch{1,2,3}`, which the script moves under the staging dir; and we keep
  stdin open after the license ENTER+YES so PDGF's shell thread can't flood-kill the
  run. `-sf` is the TPC-DI factor ×1000 (what DIGen applies). No Spark. Override the
  source with `DBX_TPCDI_REPO` / `DBX_TPCDI_REF`.
- **Seed is generated once, then cached in OneLake.** `check_seed.py` lists
  `Files/tpcdi/sf<N>/Batch1` (via duckrun's `conn.list_files()`, no download); if it's
  there, generation is skipped. On a miss, `generate_data.py` writes `Batch1/2/3` to a
  local scratch dir and `upload_to_onelake.py` (`conn.copy()`) lands them in
  `Files/tpcdi/sf<N>`. dbt then reads that seed straight from OneLake — the runner only
  hosts the bytes transiently, during the one-time generation.
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
