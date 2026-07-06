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

`MERGE`, `UPDATE`, and `DELETE` are **snapshot-pinned by default** — the target version is captured
and the commit validates against it, so a concurrent writer fails the commit loudly
(`CommitFailedError`) instead of silently interleaving. A **read-modify-append on the same table**
(`INSERT INTO a SELECT … FROM a`) is fenced the same way automatically; a plain append of new data
(a `VALUES` list, or a `SELECT` over *other* tables) is unfenced (last-writer-wins / additive). The
full model and a cross-engine comparison are in [Snapshot isolation](snapshot-isolation.md).

```python
import duckrun
# writable session — read_only=False opts in (the default is read-only)
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo",
                       read_only=False)
conn.sql("CREATE OR REPLACE TABLE orders_copy AS SELECT * FROM orders")
conn.sql("SELECT * FROM orders_copy").show()
```

## It's just DuckDB SQL (plus three Delta bits)

Everything you run through `conn.sql()` is **standard DuckDB SQL, parsed and executed by DuckDB** —
reads, CTEs, `SHOW`/`DESCRIBE`, `CREATE TEMP`/`CREATE VIEW` scratch, all of it. There is no second SQL
dialect and no DataFrame API to learn; `conn.sql()` hands back DuckDB's own relation.

duckrun layers exactly two things on top:

1. **Write DML runs against Delta, not a DuckDB table.** `CREATE TABLE … AS`, `INSERT`, `UPDATE`,
   `DELETE`, `MERGE`, `ALTER TABLE … ADD/DROP/RENAME COLUMN`, and `DROP TABLE` are written in ordinary
   DuckDB SQL syntax, but duckrun routes them to delta-rs so they land on the Delta table (local or
   OneLake) with snapshot fencing — see the [DML matrix](#raw-sql-dml-through-connsql).
2. **A few Delta-specific extensions** DuckDB has no syntax for. These are the *only* places duckrun's
   SQL isn't vanilla DuckDB:

   | Extension | What it is |
   |---|---|
   | `CREATE TABLE … SORTED BY AUTO AS …` | duckrun profiles the data and picks the clustering key. (`SORTED BY (cols)` and `PARTITIONED BY (cols)` without `AUTO` are DuckDB's *own* CTAS syntax — not extensions.) |
   | `VACUUM <table>` | DuckDB's `VACUUM` verb, repurposed to compact + vacuum the Delta table (plain DuckDB `VACUUM` is a stats no-op). |
   | `INSERT INTO <t> REPLACE WHERE <pred> SELECT …` | delta_rs `replaceWhere` — an atomic slice overwrite (the Spark/Delta spelling). |
   | `INSERT WITH SCHEMA EVOLUTION INTO <t> SELECT …` | append that widens the table with the source's new columns (existing rows → NULL), instead of dropping them — delta_rs `schema_mode='merge'` (the Spark/Delta spelling). |
   | `DESCRIBE DETAIL <table>` | the table's `location` (storage path), `partitionColumns`, `numFiles`, `sizeInBytes`, `version` — read from the Delta log (the Spark/Delta verb; plain `DESCRIBE <table>` stays DuckDB's column view). |
   | `DESCRIBE HISTORY <table>` | one row per Delta commit (`version`, `timestamp`, `operation`, `operationMetrics`), newest first. |

Everything else is portable DuckDB SQL — the same query runs on plain DuckDB.

**Time travel** is `delta_scan('<location>', version => N)` — get `<location>` from `DESCRIBE DETAIL`,
and the versions from `DESCRIBE HISTORY`.

## In-memory data — `conn.register`

Register a local pandas / polars / pyarrow object (or a DuckDB relation) under a name, then read it
in SQL — no wrapper, no import of a duckrun type. Registration is explicit because DuckDB's
replacement scan only sees the immediate calling frame (the `conn.sql` method), not yours, so a bare
`conn.sql("FROM df")` can't find a caller-local `df`.

```python
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

conn.register("df", df)
conn.sql("SELECT * FROM df").df()                                # query it
conn.sql("CREATE OR REPLACE TABLE seeded AS SELECT * FROM df")   # or persist it to Delta
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

## Table layout — sort & partition on write

Clustering equal values together on write is what makes run-length and dictionary encoding pay off —
smaller files, faster column scans. duckrun reads DuckDB's own `CREATE TABLE … SORTED BY (…)` and
`PARTITIONED BY (…)` layout clauses and applies them to the Delta write:

```python
# cluster the write by a lexicographic key (no z-order — bit-interleaving destroys the run-length
# runs a columnar reader relies on; use a lexicographic key instead)
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) AS SELECT * FROM stg_sales")

# Hive-partitioned Delta (delta-rs writes col=value/ folders, strips the column from the data files)
conn.sql("CREATE OR REPLACE TABLE sales PARTITIONED BY (region) AS SELECT * FROM stg_sales")

# both compose — partition columns should lead the sort so delta-rs keeps ~one partition writer open
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) PARTITIONED BY (region) "
         "AS SELECT * FROM stg_sales")

# SORTED BY AUTO — duckrun profiles the query and picks a run-length-friendly key for you (a
# heuristic, not an optimizer; the optimal choice is NP-hard). See Automatic sort.
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY AUTO AS SELECT * FROM stg_sales")
```

`SORTED BY (cols)` and `PARTITIONED BY (cols)` are DuckDB's native syntax; `SORTED BY AUTO` is a
duckrun extension. A plain `… AS SELECT … ORDER BY …` also clusters the write. See
[Automatic sort](automatic-sort.md) and [the parquet layout](parquet-layout.md).

`VACUUM <table>` — DuckDB's `VACUUM` verb, repurposed for Delta maintenance: it compacts the table's
small files and vacuums files tombstoned past the retention window (compaction also runs automatically
after every write; this is the manual button). Read-only sessions refuse it.

`INSERT INTO <t> REPLACE WHERE <pred> SELECT …` — delta_rs `replaceWhere` (the Spark/Delta spelling):
atomically overwrite **only** the rows matching `<pred>` with the SELECT's rows, in a single fenced
commit (pinned to the version read — a concurrent write fails it loud; no torn delete-then-append
window). `<pred>` is a CAST-free expression over the target's columns; partition columns are preserved.

> **Gaps (for now):** `RESTORE` (roll a table back to an earlier version) has no SQL surface yet —
> though `CREATE OR REPLACE TABLE t AS SELECT * FROM delta_scan('<location>', version => N)` does it as
> a new overwrite. History/version/location are covered by `DESCRIBE HISTORY` / `DESCRIBE DETAIL`;
> `conn.get_stats(table)` inspects the physical layout.
