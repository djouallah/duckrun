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

- Single-threaded by design — do **not** set `threads:`.
- DuckDB is in-memory; there is no database file. The Delta tables are the only state.
- Incremental strategy defaults depend on `unique_key` (`merge` with it, `append`
  without). For large tables, `merge` vs `safeappend` matters a lot.
- OneLake/Fabric auth is just a bearer token; paths use lakehouse **GUIDs**, not names.

Consult the SKILL.md before writing `profiles.yml` or any incremental model.

## Working on duckrun itself

- It's fine to run tests locally for this project.
- Commit straight to `main` on `origin` (this repo is owned, no fork/PR needed).
- Keep duckrun **boring**: prefer the dull, idiomatic solution. The only intentional bet
  is the Arrow + delta-rs core, because there's no alternative way to write Delta.
