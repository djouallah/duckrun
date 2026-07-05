# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, SQL-first `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake). The surface is small and everything is
SQL:

- `conn.sql(...)` — DuckDB SQL over the discovered Delta tables, returning DuckDB's **native
  relation** (`.show()`, `.df()`, `.arrow()`, `.pl()`, `.fetchall()`, `.filter()`, … all maintained
  upstream by DuckDB). Reads pass straight through, including time travel
  (`delta_scan('…', version => N)`). **Raw DML** (`create table … as`, `insert`, `update`, `delete`,
  `alter add column`, `drop`, `merge`) is applied to the Delta table via delta_rs (works local AND on
  OneLake) — see the [DML matrix](#raw-sql-dml-through-connsql) below.
- **multiple catalogs**: `connect()` binds one lakehouse root (the primary catalog); attach more with
  `conn.attach(path, name=…)` and read/join/write across them by three-part `catalog.schema.table`
  name — see [Multiple catalogs with `conn.attach`](#multiple-catalogs-with-connattach) below.
- **file utilities**: `conn.copy()`, `conn.download()`, `conn.list_files()` (OneLake Files / any
  store), `conn.get_stats()` (physical layout inspection), and `conn.convert_to_delta()` (zero-copy
  parquet→Delta ingest).

`connect()` is **read-only by default**: every Delta write (`create table … as` / `insert` /
`update` / `delete` / `merge`) raises `PermissionError`, so an accidental write can't mutate a shared
lakehouse. Pass `read_only=False` to enable writes; reads and native `CREATE TEMP`/`CREATE VIEW`
scratch are always allowed.

For the exact list of supported methods, see the [API reference](api-reference.md).

`MERGE` is **snapshot-pinned by default** — single-snapshot MERGE, no extra arguments: the target
version is captured and the commit validates against it, so a concurrent writer fails the commit
loudly (`CommitFailedError`) instead of silently interleaving. The full model and a cross-engine
comparison are in [Snapshot isolation](snapshot-isolation.md).

```python
import duckrun
# writable session — read_only=False opts in (the default is read-only)
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo",
                       read_only=False)
conn.sql("CREATE OR REPLACE TABLE orders_copy AS SELECT * FROM orders")
conn.sql("SELECT * FROM orders_copy").show()
```

## In-memory data — no `createDataFrame` needed

A local pandas / polars / pyarrow object is directly queryable by name via DuckDB's replacement
scan — no wrapper, no import of a duckrun type:

```python
import pandas as pd
seeded = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

conn.sql("CREATE OR REPLACE TABLE seeded AS SELECT * FROM seeded")   # the local df is the source
conn.sql("SELECT * FROM seeded").df()                               # back out as pandas
```

## Multiple catalogs with `conn.attach`

`connect()` binds one lakehouse root as the **primary catalog**; `conn.attach(path, name=…)` binds
more, so a single session can read, join, and write across several lakehouses by **three-part name**
(`catalog.schema.table`). In Microsoft Fabric a **Warehouse** and a **Lakehouse** are the same thing
to duckrun — both are Delta in OneLake — *except a Warehouse is locked to writes*, so attach it
`read_only=True` and keep the lakehouse writable. On OneLake tokens are automatic (nothing to pass in
a Fabric notebook).

```python
import duckrun

# primary catalog: a writable lakehouse. name= reads next to the others (else derived from the URL).
conn = duckrun.connect(
    "abfss://ws@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables",
    read_only=False, name="lakehouse")

# attach a read-only Fabric Warehouse and a plain local folder as more catalogs
conn.attach("abfss://ws@onelake.dfs.fabric.microsoft.com/warehouse.Warehouse/Tables",
            name="warehouse", schema="mart", read_only=True)
conn.attach("/data/reference", name="local")

# join across all three in one query — facts from the warehouse, dim from the lakehouse, a lookup
# from local — and write the mart back to the (writable) lakehouse by three-part name
conn.sql("""
    CREATE OR REPLACE TABLE lakehouse.dbo.mart_generation_by_state AS
    SELECT d.state, sum(f.mw) AS total_mw
    FROM warehouse.mart.fct_summary f
    JOIN mart.dim_duid d                ON d.duid = f.duid
    LEFT JOIN local.dbo.fuel_factors lf ON lower(lf.fuel) = lower(d.fuel)
    GROUP BY d.state
""")

# the warehouse is read-only — a write into it is refused, not silently dropped
conn.sql("CREATE TABLE warehouse.mart.nope AS SELECT 1 AS x")   # -> PermissionError
```

Naming: a table can be addressed 3-part (`warehouse.mart.fct_summary`, from anywhere), 2-part
(`mart.fct_summary`, in the current catalog) or bare (`fct_summary`, in the current catalog + schema).
`name` is derived from a friendly path and is **mandatory for a GUID-only OneLake path**; one URL maps
to one name (re-attaching either raises). `read_only` is **per-catalog**, independent of the session —
a read-only reference store sits safely next to a writable lakehouse. A `create table … as` with a
three-part target writes to that catalog; the attached catalogs are visible to native DuckDB catalog
introspection (`SHOW DATABASES`, `information_schema`, `duckdb_databases()`).

See the full runnable walkthrough in
[`integration_tests/multicatalog/demo_multicatalog.py`](../integration_tests/multicatalog/demo_multicatalog.py)
(published as a [live report](https://djouallah.github.io/duckrun/multicatalog.html)).

## Raw SQL DML through `conn.sql`

`conn.sql` doesn't only read — raw SQL DML against a discovered (Delta-backed) table is intercepted
and applied **via delta_rs only**, then the view is refreshed, so it works identically on a local
path and on OneLake. The invariant: **every `CREATE TABLE` is Delta-backed; only `CREATE TEMP TABLE`
and `CREATE VIEW` stay native DuckDB** (ephemeral, session-local scratch). Forms that delta_rs can't
express are rejected up front, rather than failing cryptically.

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
| `MERGE INTO x [a] USING s [b] ON a.k = b.k WHEN …` | delta_rs upsert (snapshot-pinned like every write). Write it like standard SQL — the `ON`/`WHEN` clauses may use **your own aliases or the table/relation names** (the literal `target`/`source` also work); fully-unqualified columns (`ON k = k`) are ambiguous and unsupported. Supports `UPDATE SET *` / `UPDATE SET col = <src>.col`, `INSERT *`, `WHEN NOT MATCHED BY SOURCE THEN DELETE`, and per-clause `AND` predicates |
| `CREATE TEMP/TEMPORARY TABLE …`, `CREATE VIEW …` | **native DuckDB** — ephemeral, session-local; not a Delta artifact |
| `UPDATE … FROM`, `DELETE … USING` | rejected → rewrite as a correlated subquery |
| multiple statements in one call | rejected → one statement per `conn.sql()` |

Leading `--` / `/* … */` comments are fine. The exact behaviour is pinned, statement-by-statement,
in [`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py)
(the `TestSqlDml` class).

## Table layout — sort, partition, maintenance

<!-- PLACEHOLDER: the SQL-only maintenance surface (optimize / vacuum / history / restore / version /
stats) is being reworked as part of the DataFrame-API removal and will be documented here. Sorting a
write is `CREATE TABLE t AS SELECT … ORDER BY …`; there is no z-order (bit-interleaving destroys the
run-length runs a columnar reader relies on — cluster with a lexicographic key instead). See
[Automatic sort](automatic-sort.md) and [the parquet layout](parquet-layout.md). -->

Sorting a write clusters equal values together, which is what makes run-length and dictionary
encoding pay off — smaller files and faster column scans. Do it in SQL:

```python
conn.sql("CREATE OR REPLACE TABLE sales AS SELECT * FROM stg_sales ORDER BY region, order_date")
```

The remaining maintenance operations (compaction, vacuum, history, restore, version, stats) are being
reworked onto a SQL-native surface and will be documented here when that lands.
