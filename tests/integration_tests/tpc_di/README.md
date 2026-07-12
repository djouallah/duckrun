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
audit/validate.py   source→target reconciliation + SCD2 integrity audit of the built warehouse
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
5. `validate.py` audits the OneLake warehouse via `duckrun.connect()` — see below.

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
- **CDC is fully processed — the load is single-*pass*, not single-*batch*.** The
  incremental models read `Batch[123]/…` (all three batches: Batch1 historical +
  Batch2/3 CDC inserts/updates), apply the `cdc_flag`, and fold everything into the
  final SCD2 dimensions and facts in one `dbt run`. What we *don't* do is run the three
  batches as three separate, sequentially-audited executions — we compute the final
  end-state directly. The finished warehouse is the same; only the between-batch
  checkpoints are absent (see the audit note).
- **Audit — real data validation, not a liveness gate.** `audit/validate.py` is a
  DuckDB port of the reference project's single-batch `data_validation.sql`. For every
  source→target transition it **re-derives the expected value from the raw source files**
  and asserts the finished table matches (e.g. distinct customers in `CustomerMgmt(NEW)`
  + Batch2/3 `Customer.txt` CDC inserts must equal `DimCustomer`; every `DailyMarket` row
  must survive into `FactMarketHistory`), then checks the invariants a correct
  dimensional load must hold: no NULL surrogate keys, SCD2 windows with no overlaps /
  zero-length gaps / more than one open version, `IsCurrent` consistent with `EndDate`,
  foreign-key SKs contained in a valid parent version, and legal enum/format domains
  (Gender, ExchangeID, TaxID, tax-rate pairs, …). Because expected values are *derived*
  from the same `sf<N>` seed the warehouse was built from, the audit is
  **scale-factor-agnostic** — it self-calibrates; nothing is hard-coded per SF. It reads
  both the Delta tables and the raw source files through one `duckrun.connect()`
  (the OneLake storage secret covers `read_csv` over `Files/` too).

## Not yet covered (follow-ups)

- **Per-batch Appendix-A audit + DImessages.** The spec's *automated audit* (Appendix A)
  and the `DImessages` validation log are inherently per-batch (row counts / phase-complete
  records after each of the 3 batches) and assume the 3-separate-runs execution model. Our
  single-pass load has no between-batch checkpoints, so that audit is a separate, larger
  build. The final-table validation above (Appendix-B style) does not need it.
