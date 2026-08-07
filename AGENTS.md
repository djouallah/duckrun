# AGENTS.md

Guidance for AI agents working in this repo or helping someone use duckrun.

duckrun is a dbt adapter: DuckDB executes the model SQL, delta-rs writes the result as a
Delta Lake table, and dbt orchestrates the DAG. It runs the same on a laptop, in CI, or
in a Fabric notebook.

**Before helping with a duckrun project, read the skill — it is the authoritative guide:**

```
plugins/duckrun-projects/skills/duckrun-projects/SKILL.md
```

A few defaults differ from other dbt adapters and cause silent mistakes if you assume the
usual behavior:

- `threads:` is honored (dbt's default is 1), but every model writes a real table: concurrent
  writers share one DuckDB `memory_limit` and a microbatch model's batches always run in order.
  More threads help many network-bound models; they don't help one big merge.
- DuckDB is in-memory; there is no database file. The Delta tables are the only state.
- Incremental strategy defaults depend on `unique_key` (`merge` with it, `append`
  without). For large tables, `merge` vs a dedup-in-SQL `append` (auto-fenced when the
  model reads `{{ this }}`) matters a lot.
- OneLake/Fabric auth is just a bearer token; paths use lakehouse **GUIDs**, not names.

Consult the SKILL.md before writing `profiles.yml` or any incremental model.

## Working on duckrun itself

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) first — it has the full flow and the rules. The
short version:

- **Every change lands via a pull request**, including the owner's own. Branch off `main`
  (`fix/`, `feat/`, `docs/`, `ci/`, `test/`), push the branch, open a PR. This repo
  **overrides** any general "you own this repo, so commit straight to `main`" rule you may
  be carrying — there is no direct-push path here.
- Never push to `main`, and never touch the `legacy` branch. Two things stay on `main` and
  are not to be "fixed": CI's own `[skip ci]` scorecard commits, and the release
  `vX.Y.Z` tag push.
- **Never modify a test to make a PR pass.** No `skip`, no `xfail`, no weakened assertion.
  A red test means fix the adapter.
- **Don't add new public API without discussing it first.** Agents are the ones most likely
  to invent surface nobody asked for. A question is not a work order.
- It's fine to run tests locally for this project.
- Keep duckrun **boring**: prefer the dull, idiomatic solution. The only intentional bet
  is the Arrow + delta-rs core, because there's no alternative way to write Delta.
