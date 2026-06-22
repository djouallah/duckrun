# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, DataFrame-style `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — DuckDB SQL over the discovered Delta tables, including time travel
  (`delta_scan('…', version => N)`). Reads pass straight through; **raw DML** (`create table … as`,
  `insert`, `update`, `delete`, `alter add column`, `drop`) is applied to the Delta table via
  delta_rs (works local AND on OneLake) — see the [DML matrix](#raw-sql-dml-through-connsql) below.
- a `DataFrame` with a DataFrame-style `.write…saveAsTable()` — modes `overwrite` / `append` /
  `safeappend` / `ignore`, plus `option("replaceWhere", …)` for an atomic slice overwrite — plus
  `conn.read` and `conn.catalog`.
- a `DeltaTable` handle (`DeltaTable.forName(conn, name)`) mirroring the `DeltaTable` API:
  `.merge(...)`, `.delete()`, `.update()`, `.version()`, `.history()`.
- **multiple catalogs**: `connect()` binds one lakehouse root (the primary catalog — named from the
  URL, e.g. the OneLake lakehouse / local folder name, or `data` when a GUID-only path gives nothing
  to derive; pass `name=` to override); attach more with `conn.attach(path, name=…)` and address them
  as `catalog.schema.table`. `conn.catalog.listCatalogs()`
  / `currentCatalog()` / `setCurrentCatalog(name)` switch which catalog unqualified / 2-part names
  resolve in. `name` is derived from a friendly path (mandatory for a GUID-only OneLake path); one URL
  maps to one name. `attach(..., read_only=True)` fences writes to *that* catalog independently of the
  session, so a read-only reference store (e.g. a Fabric Warehouse) can sit next to a writable lakehouse.
  Raw `conn.sql()` DML targets the current catalog — cross-catalog writes go through the DataFrame API
  (`df.write.saveAsTable("cat.schema.t")`) or `DeltaTable.forName(conn, "cat.schema.t")`.

`connect()` is **read-only by default**: every Delta write (`saveAsTable` / `insertInto` / `save` /
`merge` / `insert` / `update` / `delete` / `replaceWhere`) raises `PermissionError`, so an accidental
write can't mutate a shared lakehouse. Pass `read_only=False` to enable writes; reads and native
`CREATE TEMP`/`CREATE VIEW` scratch are always allowed.

For a method-by-method map of this surface against PySpark / Delta-on-Spark — what maps 1:1, what's
duckrun-flavored, and what's deliberately out of scope (SQL-first, no Spark runtime — by design) —
see [Coverage vs the Spark / Delta API](spark-delta-parity.md).

`merge` is **snapshot-pinned by default** — single-snapshot MERGE, with no extra arguments:
the target version is captured and the commit validates against it, so a concurrent writer fails the
commit loudly instead of silently interleaving. `mode("safeappend")` applies the same fail-loud
compare-and-swap to a plain append (identical to the dbt `safeappend` strategy): it commits only if
the table is unchanged since the call, else raises `CommitFailedError`.

```python
import duckrun
# writable session — read_only=False opts in (the default is read-only)
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo",
                       read_only=False)
conn.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")
conn.table("orders_copy").show()

from duckrun import DeltaTable
DeltaTable.forName(conn, "orders").delete("region = 'eu'")   # delete / update / version

# overwrite just one slice, atomically
conn.sql("select * from corrections").write.option("replaceWhere", "region = 'eu'") \
    .mode("overwrite").saveAsTable("orders")

src = conn.sql("select * from updates")
DeltaTable.forName(conn, "orders").merge(src, "target.id = source.id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()   # pinned automatically
```

## In-memory data with `conn.createDataFrame`

`conn.createDataFrame(data, schema=None)` turns in-memory data into a DataFrame on duckrun's own
connection — handy for seeding, demos, or persisting a small Python/pandas result to Delta. `data`
is a list of tuples/lists (a list of scalars becomes one column), a pandas `DataFrame`, or a
pyarrow `Table`/`RecordBatchReader`. `schema` is `None` (names inferred — `_1, _2, …` for tuples),
a list of column names, or a DDL string (`"id int, name string"`; `name: type` is also accepted).
Empty data needs a DDL schema. It's a plain DuckDB build — no Spark/PySpark dependency.

```python
conn.createDataFrame([(1, "a"), (2, "b")], "id int, name string") \
    .write.mode("overwrite").saveAsTable("seeded")

conn.createDataFrame(pandas_df).show()
conn.createDataFrame([], "id int, name string")   # typed empty frame
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
| `MERGE …` | rejected → use `DeltaTable.forName(conn, name).merge(...)` or `df.write.saveAsTable(...)` |
| `UPDATE … FROM`, `DELETE … USING` | rejected → rewrite as a correlated subquery, or use `DeltaTable.forName(conn, name)` |
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
│ ✅ 61 passed   ❌ 0 failed  │
│ 61 methods · 100% passing │
└───────────────────────────┘
```

### DataFrame API — 44/44 ✅

> Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.

| Surface | Methods | Pass |
| --- | --- | :-: |
| `DuckSession` | `sql`, `table`, `read`, `catalog` | 4/4 ✅ |
| `Catalog` | `listTables`, `listDatabases`, `currentDatabase`, `setCurrentDatabase`, `tableExists`, `tableExists_is_fresh`, `databaseExists`, `listColumns`, `listCatalogs`, `currentCatalog`, `setCurrentCatalog` | 11/11 ✅ |
| `DataFrame` | `collect`, `count`, `columns`, `show`, `toPandas`, `toArrow` | 6/6 ✅ |
| `DataFrameReader` | `format/load`, `table`, `parquet`, `csv`, `versionAsOf`, `timestampAsOf_rejected` | 6/6 ✅ |
| `DataFrameWriter` | `saveAsTable`, `mode`, `option`, `insertInto`, `insertInto_requires_existing`, `partitionBy`, `format`, `save_by_path`, `save_modes`, `save_mode_error_when_exists` | 10/10 ✅ |
| `DeltaTable` | `forName`, `forPath`, `merge`, `version`, `history`, `delete`, `update` | 7/7 ✅ |

### duckrun-specific helpers — 17/17 ✅

> Conveniences with no DataFrame-API equivalent (session plumbing + two shortcuts).

| Method | Surface | Pass |
| --- | --- | :-: |
| `connect` | `DuckSession` | ✅ |
| `refresh` | `DuckSession` | ✅ |
| `connection` | `DuckSession` | ✅ |
| `stop` | `DuckSession` | ✅ |
| `table_path` | `DuckSession` | ✅ |
| `attach` | `DuckSession` | ✅ |
| `__getattr__` | `DataFrame` | ✅ |
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
