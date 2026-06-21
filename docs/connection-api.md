# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, DataFrame-style `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — DuckDB SQL over the discovered Delta tables, including time travel
  (`delta_scan('…', version => N)`). Reads pass straight through; **raw DML** (`create table … as`,
  `insert`, `update`, `delete`, `alter add column`, `drop`) is applied to the Delta table via
  delta_rs (works local AND on OneLake) — see the [DML matrix](#raw-sql-dml-through-connsql) below.
- a `DataFrame` with a DataFrame-style `.write…saveAsTable()` — modes `overwrite` / `append` /
  `safeappend` / `ignore` — plus `conn.read` and `conn.catalog`.
- a `DeltaTable` handle (`conn.delta_table(name)` / `DeltaTable.forName`) mirroring the `DeltaTable` API:
  `.merge(...)`, `.delete()`, `.update()`, `.replaceWhere()`, `.version()`.

`merge` is **snapshot-pinned by default** — single-snapshot MERGE, with no extra arguments:
the target version is captured and the commit validates against it, so a concurrent writer fails the
commit loudly instead of silently interleaving. `mode("safeappend")` applies the same fail-loud
compare-and-swap to a plain append (identical to the dbt `safeappend` strategy): it commits only if
the table is unchanged since the call, else raises `CommitFailedError`.

```python
import duckrun
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
conn.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")
conn.table("orders_copy").show()

conn.delta_table("orders").delete("region = 'eu'")   # delete / update / replaceWhere

src = conn.sql("select * from updates")
conn.delta_table("orders").merge(src, "target.id = source.id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()   # pinned automatically
```

## Raw SQL DML through `conn.sql`

`conn.sql` doesn't only read — raw SQL DML against a discovered (Delta-backed) table is intercepted
and applied **via delta_rs only**, then the view is refreshed, so it works identically on a local
path and on OneLake. The invariant: **every `CREATE TABLE` is Delta-backed; only `CREATE TEMP TABLE`
and `CREATE VIEW` stay native DuckDB** (ephemeral, session-local scratch). Forms that delta_rs can't
express are rejected up front with a pointer to the write API, rather than failing cryptically.

| Statement | What happens |
| --- | --- |
| `CREATE [OR REPLACE] TABLE x [IF NOT EXISTS] AS <query>` | Delta overwrite (`<query>` = a `select`, a `WITH … select`, or `(select …)`); `IF NOT EXISTS` over a live table is a no-op |
| `CREATE TABLE x (<col defs>)` | empty Delta table |
| `INSERT INTO x [(cols)] SELECT/VALUES …` | Delta append — columns matched by name, projected/cast onto the target schema, unsupplied columns filled with typed `NULL` |
| `[WITH …] INSERT INTO x SELECT …` | Delta append (the CTE is re-attached to the body) |
| `UPDATE x SET … [WHERE …]` | delta_rs update |
| `DELETE FROM x [WHERE …]` | delta_rs delete |
| `ALTER TABLE x ADD COLUMN …` | Delta overwrite, widening the schema |
| `DROP TABLE x` | **tombstone** — marks the table dropped (a one-column marker) without deleting data; files persist for a human to purge, a later `create … as` revives it |
| `CREATE TEMP/TEMPORARY TABLE …`, `CREATE VIEW …` | **native DuckDB** — ephemeral, session-local; not a Delta artifact |
| `MERGE …` | rejected → use `conn.delta_table(name).merge(...)` or `df.write.saveAsTable(...)` |
| `UPDATE … FROM`, `DELETE … USING` | rejected → rewrite as a correlated subquery, or use `conn.delta_table(...)` |
| multiple statements in one call | rejected → one statement per `conn.sql()` |

Leading `--` / `/* … */` comments are fine. The exact behaviour is pinned, statement-by-statement,
in [`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py)
(the cross-API write-correctness matrix + the `TestSqlDml` class).

The card below — every public method with a ✅ — is regenerated on every push by
the `connection-card` job in [`cores.yml`](../.github/workflows/cores.yml) from the `Test*` classes of
[`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py).

<!-- CONNECTION_API:START -->

## duckrun connection API — method scorecard

```
┌───────────────────────────┐
│ ✅ 52 passed   ❌ 0 failed  │
│ 52 methods · 100% passing │
└───────────────────────────┘
```

### DataFrame API — 36/36 ✅

> Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.

| Surface | Methods | Pass |
| --- | --- | :-: |
| `DuckSession` | `sql`, `table`, `read`, `catalog` | 4/4 ✅ |
| `Catalog` | `listTables`, `listDatabases`, `currentDatabase`, `setCurrentDatabase`, `tableExists`, `tableExists_is_fresh`, `databaseExists`, `listColumns` | 8/8 ✅ |
| `DataFrame` | `collect`, `count`, `columns`, `show`, `toPandas` | 5/5 ✅ |
| `DataFrameReader` | `format/load`, `table`, `parquet`, `csv` | 4/4 ✅ |
| `DataFrameWriter` | `saveAsTable`, `mode`, `option`, `partitionBy`, `format`, `save_by_path`, `save_modes`, `save_mode_error_when_exists` | 8/8 ✅ |
| `DeltaTable` | `forName`, `forPath`, `merge`, `version`, `delete`, `update`, `replaceWhere` | 7/7 ✅ |

### duckrun-specific helpers — 16/16 ✅

> Conveniences with no DataFrame-API equivalent (session plumbing + two shortcuts).

| Method | Surface | Pass |
| --- | --- | :-: |
| `connect` | `DuckSession` | ✅ |
| `refresh` | `DuckSession` | ✅ |
| `connection` | `DuckSession` | ✅ |
| `table_path` | `DuckSession` | ✅ |
| `__getattr__` | `DataFrame` | ✅ |
| `delta` | `DataFrameReader` | ✅ |
| `SELECT (passthrough)` | `sql()` | ✅ |
| `version-pinned read` | `sql()` | ✅ |
| `create table as` | `sql()` | ✅ |
| `insert…select` | `sql()` | ✅ |
| `insert…values` | `sql()` | ✅ |
| `update` | `sql()` | ✅ |
| `delete` | `sql()` | ✅ |
| `alter add column` | `sql()` | ✅ |
| `drop (tombstone)` | `sql()` | ✅ |
| `merge guard (→ builder)` | `sql()` | ✅ |

<!-- CONNECTION_API:END -->
