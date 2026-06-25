# Parity test — jaffle_shop on duckrun (dbt-duckdb validation)

**Goal:** prove duckrun is a faithful drop-in for dbt-duckdb. Take a real `type: duckdb` dbt
project, run it **unchanged** two ways — on dbt-duckdb (the oracle) and on duckrun — and assert
every materialized table is **identical, row for row**. dbt-duckdb's output is ground truth; any
mismatch is a duckrun bug (fixed in duckrun, never in the project).

## The repo under test

- **Repo:** https://github.com/dbt-labs/jaffle_shop_duckdb
- Self-contained: in-repo CSV seeds (no network), **no package dependencies**, marts as tables +
  staging as views. The canonical dbt+DuckDB reference project.

Nothing here is copied from the repo. It is cloned fresh and run **verbatim**. The only thing not
in the repo is the connection: [profiles.yml](profiles.yml) defines the `jaffle_shop` profile as
`type: duckrun` and is passed via `--profiles-dir` — in dbt the profile is connection config that
lives outside the project, so the repo is never modified.

## Run it

```bash
python parity_tests/jaffle_shop/run_parity.py
```

The script clones the repo (if needed), runs `dbt build` once with the repo's own duckdb profile
(→ `jaffle_shop.duckdb`) and once with the duckrun profile here (→ a local Delta warehouse), then
diffs every persisted table with a row-multiset `EXCEPT ALL` both ways. Exit 0 = parity.

## Result (latest local run)

Both sides build green (28/28: 3 seeds, 2 table models, 3 view models, 20 tests). Every table the
duckrun side persists matches the duckdb oracle exactly:

| table          | rows | match |
|----------------|------|-------|
| customers      | 100  | ✓     |
| orders         | 99   | ✓     |
| raw_customers  | 100  | ✓     |
| raw_orders     | 99   | ✓     |
| raw_payments   | 113  | ✓     |

Staging models are `view`s; duckrun has no durable view (it materializes only tables to Delta), so
they're intermediate-only and not part of the persisted diff — the marts that depend on them match,
which validates the pipeline.
