# Compatibility test — jaffle_shop on duckrun

**Goal:** prove an existing `type: duckdb` dbt project runs on duckrun **as-is**. Take the real
jaffle_shop repo, swap in nothing but the connection profile, and `dbt build` it VERBATIM — seeds,
models and the project's own data tests. Green build = compatible; a failure is a duckrun bug
(fixed in duckrun, never in the project). dbt-duckdb itself is not built — nothing here tests it.

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
python tests/parity_tests/jaffle_shop/run_parity.py
```

The script clones the repo (if needed) and runs `dbt build` with the duckrun profile here —
materializing to OneLake in CI (`WAREHOUSE_PATH`), or a local Delta warehouse otherwise.
Exit 0 = the project built green.

## Result (latest run)

The full build runs green (28/28: 3 seeds, 2 table models, 3 view models, 20 tests), unmodified.
Staging models are `view`s; duckrun has no durable view (it materializes only tables to Delta), so
they're intermediate-only — the marts that depend on them build and test green, which validates the
pipeline.

## Also here: the debug session (`run_debug.py`)

`python tests/parity_tests/jaffle_shop/run_debug.py` — standalone. It points
`duckrun.dbt_project()` — the notebook debug session — at this project and checks what it hands
back. Read-only end to end: it clones the project source (dbt needs the `.sql` files to compile),
reads the jaffle_shop tables the warehouse **already** holds, and builds nothing. It does not need
`run_parity.py` to have run first — needing a writer to run first would be the opposite of what a
read-only session claims — it only needs `WAREHOUSE_PATH` to point at a warehouse jaffle_shop has
been built into at some point (the parity lakehouse in CI). An empty warehouse is reported as a
failure, never fixed by building one.

Why here rather than in a fixture: every bug that feature shipped with was found by pointing it at a
real project (generic tests made every model name look ambiguous; ephemeral CTEs outnumbered the
model's own). jaffle_shop is the cheapest project that has the shapes that matter — CTE-structured
staging models reading straight from seeds (real Delta tables, so a cold session can execute them),
marts reading from `view` models, and generic tests on everything.

It checks that `show()` returns real DuckDB types and stays lazy, that `ctes()`/`cte()` slice the
compiled SQL verbatim, that a model name resolves despite the tests hanging off it (and that a test
named outright still resolves), and that a `delete` through the session is refused with the live
Delta table left at exactly the same row count.

One check pins a **limitation** rather than a promise: `customers` compiles but cannot be READ from a
cold session, because its parents are `view` models and duckrun has no durable view. Should that ever
change, the check fails and says so.
