# merge_spill — the incremental-MERGE spill benchmark (SQL-only)

A stress test of duckrun MERGEing incremental batches into a large Delta **fact** table
(TPCH `lineitem`) through the **connection API** (`duckrun.connect()` + `conn.sql(...)`) — no dbt.
It builds a chain of Delta tables and runs the full delta-rs MERGE clause set against them (mixed
upsert, insert-only, update-only, idempotent re-merge, CDC delete+update+insert, full-sync by-source
delete, expression update), plus a plain `append` / `safeappend` / `overwrite` of the same batch for
comparison — verifying every UPDATE/INSERT/DELETE lands correctly while staying within the runner's
RAM (the per-merge DuckDB `memory_limit` pin + delta_rs `max_spill_size`).

This used to be a dbt project; it never made sense as a modelling showcase (it's a memory benchmark),
so it's now plain SQL driven by a small Python harness.

## Layout

- `sql/<op>.sql` — one file per operation, plain DuckDB SQL (no Jinja). Each builds its batch into a
  `_batch` TEMP table (so a random sample is evaluated exactly once) and then applies the op:
  a `MERGE` (`USING _batch`), an `INSERT … SELECT` (append), or a `CREATE OR REPLACE TABLE … AS`
  (overwrite). `{schema}` is substituted by the runner. `full_sync.sql` streams its big ~50% source
  as an inline subquery instead of a TEMP table; `safeappend_only.sql` only builds the batch (there
  is no raw-SQL safeappend — the runner appends it via `.write.mode("append_if_unchanged")`).
- The runner — [`tests/tools/merge_tpch_bench.py`](../../tools/merge_tpch_bench.py) — generates the
  TPCH `lineitem` parquet with `tpchgen-cli`, seeds each op's table from the previous one
  (`CREATE OR REPLACE TABLE … AS SELECT * FROM <prev>`), runs each `sql/<op>.sql`, and verifies the
  effect by querying the table. It writes the scorecard to `docs/merge_card.md`. On the heavy local
  gate each op runs in its OWN `duckrun.connect()` worker subprocess (the chain state is all on disk,
  so this is faithful): a finished op's RSS returns to the OS on process exit, so the next op's merge
  spill cap is sampled against the RAM actually free rather than degrading as one long-lived process
  accumulates the chain. The small OneLake smoke keeps the whole chain in one in-process session.

## Run it

```bash
# local stress / release gate (heavy — SF=10 ≈ 60M rows; the SF is configurable)
python tests/tools/merge_tpch_bench.py --dir /mnt/mbench --sf 10

# small OneLake path smoke (abfss merge write/read path)
python tests/tools/merge_tpch_bench.py --dir /tmp/merge_onelake \
  --warehouse "abfss://…@onelake.dfs.fabric.microsoft.com/…/Tables" --sf 1
```

CI: the heavy SF=10 run is the local release gate (`local_stress_tests.yml`); the SF=1 OneLake smoke
runs in `integration_tests_onelake.yml`. Requires `deltalake == 1.5.0` (1.6.0's MERGE is broken at scale).
