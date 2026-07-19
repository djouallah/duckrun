# merge_spill — the incremental-MERGE spill benchmark (SQL-only)

A stress test of duckrun MERGEing incremental batches into a large Delta **fact** table
(TPCH `lineitem`) through the **connection API** (`duckrun.connect()` + `conn.sql(...)`) — no dbt.
It builds a chain of Delta tables and runs the full delta-rs MERGE clause set against them (mixed
upsert, insert-only, update-only, idempotent re-merge, CDC delete+update+insert, full-sync by-source
delete, expression update), plus plain `append`s / an `overwrite` of the same batch for
comparison — verifying every UPDATE/INSERT/DELETE lands correctly while staying within the runner's
RAM (the per-merge DuckDB `memory_limit` pin + delta_rs `max_spill_size`).

This used to be a dbt project; it never made sense as a modelling showcase (it's a memory benchmark),
so it's now plain SQL driven by a small Python harness.

## Layout

- `sql/<op>.sql` — one file per operation, plain DuckDB SQL (no Jinja). Each builds its batch into a
  `_batch` TEMP table (so a random sample is evaluated exactly once) and then applies the op:
  a `MERGE` (`USING _batch`), an `INSERT … SELECT` (append), or a `CREATE OR REPLACE TABLE … AS`
  (overwrite). `{schema}` is substituted by the runner. `full_sync.sql` only builds its big ~50% source
  (`_src`) — the runner then issues a raw-SQL by-source `MERGE … USING _src`, which the router
  auto-streams (a by-source source must be streamed, not collected whole into a non-spillable hash);
  `append_if_unchanged_only.sql` likewise only builds the batch, which the runner appends with a plain
  `INSERT … SELECT` (the version-guard verb is gone; a read-modify-append on the same table is
  auto-fenced instead).
- The runner — [`merge_tpch_bench.py`](merge_tpch_bench.py) — generates the
  TPCH `lineitem` parquet with `tpchgen-cli`, seeds each op's table from the previous one
  (`CREATE OR REPLACE TABLE … AS SELECT * FROM <prev>`), runs each `sql/<op>.sql`, and verifies the
  effect by querying the table. It writes the scorecard to `docs/merge_card.md`.

## Run it

```bash
# local stress / release gate (heavy — SF=10 ≈ 60M rows; the SF is configurable)
python tests/performance/merge_spill/merge_tpch_bench.py --dir /mnt/mbench --sf 10

# small OneLake path smoke (abfss merge write/read path)
python tests/performance/merge_spill/merge_tpch_bench.py --dir /tmp/merge_onelake \
  --warehouse "abfss://…@onelake.dfs.fabric.microsoft.com/…/Tables" --sf 1
```

CI: the heavy SF=10 run is the local release gate (`local_stress_tests.yml`); the SF=1 OneLake smoke
runs in `integration_tests_onelake.yml`. Requires `deltalake == 1.5.0` (1.6.0's MERGE is broken at scale).
