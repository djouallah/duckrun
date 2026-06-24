# Limitations

An honest, consolidated list of what duckrun doesn't do — by design, by an upstream constraint, or
by deliberate caution. Most come with a "do this instead." Deeper detail lives in the linked pages.

## Setup & versions

- **Needs `duckdb >= 1.5.4`.** Older builds — including Microsoft Fabric's bundled stable runtime —
  fail loud at `connect()`. In a Fabric notebook: `!pip install duckrun --upgrade` then restart.
- **Pins `deltalake == 1.5.0`.** delta-rs 1.6.0's MERGE is broken at scale, so duckrun stays on 1.5.0.

## Microsoft Fabric / OneLake

- **Address OneLake by GUID, not friendly names.** Use the workspace GUID + lakehouse GUID; friendly
  names currently hit an upstream OneLake read bug. See [dbt adapter → OneLake: use GUID paths](dbt-adapter.md).
- **A Fabric Warehouse is read-only.** Attach it with `read_only=True` (it's a write-locked lakehouse);
  duckrun reads/joins it but does not write to a Warehouse. See the [Connection API](connection-api.md).

## SQL DML (`conn.sql`)

- **`UPDATE … FROM` and `DELETE … USING` are rejected** → rewrite as a correlated subquery, or use
  `DeltaTable.forName(conn, name)`.
- **One statement per `conn.sql()` call** — multiple statements in a single call are rejected.

The full accepted/rejected matrix is in the [Connection API](connection-api.md#raw-sql-dml-through-connsql).

## dbt & incremental

- **A single `dbt run` is single-threaded** (`threads: 1`): the in-process delta-rs write path isn't
  thread-safe, so models inside one dbt invocation don't parallelize. This is **not** a limit on
  concurrent writers — separate runs / notebooks / jobs writing the same tables at once is fully
  supported and safe (every write is snapshot-pinned and fails loud on a conflict).
- **Some dbt merge configs are rejected, on purpose.** `merge_clauses`,
  `merge_update_set_expressions`, and `merge_on_using_columns` are dbt-duckdb-specific with no
  delta-rs equivalent, so duckrun raises a clear error rather than silently running a plain upsert.

## Schema & constraints

- **Schema evolution is add-only.** delta-rs can't drop columns, so `on_schema_change='sync_all_columns'`
  only *adds* them. Use `append_new_columns` or `fail`.
- **Only `not null` is enforced.** `check` / `primary_key` / `foreign_key` constraints are declared
  but not checked — they can't be enforced against a `delta_scan` view.

## Materializations

- **No persistent views.** The Delta spec doesn't define a view, so there's nothing durable to write.
  A `materialized='view'` model *runs* fine — it's a real DuckDB catalog view you can query for the
  rest of the session — but it lives only in that connection and vanishes when it closes; nothing is
  saved to storage, so the next session won't see it. (And swapping a model between `table` and
  `view` isn't supported.)
- **`DROP TABLE` is a soft tombstone, not a physical delete.** `conn.sql("drop table x")` unregisters
  the table and writes a tombstone marker but **does not reclaim the data files** (a deliberate
  precaution — you purge them when you're sure). Address dropped tables by name, not by path.

## Memory

- **Two engines share one machine's memory.** DuckDB and delta-rs each keep their own pool in the same
  process; heavy merges split the budget, and that split is fragile (delta-rs's merge spill-to-disk is
  itself flaky). Background in the [Design document](design_document.md).

---

For the test-by-test picture, see [Conformance](conformance.md); for *why* these trade-offs exist,
see [How it works](overview.md).
