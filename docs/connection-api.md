# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, DataFrame-style `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — DuckDB SQL over the discovered Delta tables, including time travel
  (`delta_scan('…', version => N)`). Reads pass straight through; **raw DML** (`create table … as`,
  `insert`, `update`, `delete`, `alter add column`, `drop`, `merge`) is applied to the Delta table via
  delta_rs (works local AND on OneLake) — see the [DML matrix](#raw-sql-dml-through-connsql) below.
- a `DataFrame` with a DataFrame-style `.write…saveAsTable()` — modes `overwrite` / `append` /
  `append_if_unchanged` / `overwrite_if_unchanged` / `ignore` (the `_if_unchanged` modes are the
  fenced, fail-loud siblings), plus
  `option("replaceWhere", …)` for an atomic slice overwrite — plus `conn.read` and `conn.catalog`.
- a `DeltaTable` handle (`DeltaTable.forName(conn, name)`) mirroring the `DeltaTable` API:
  `.merge(...)`, `.delete()`, `.update()`, `.version()`, `.history()`.
- **multiple catalogs**: `connect()` binds one lakehouse root (the primary catalog); attach more with
  `conn.attach(path, name=…)` and read/join across them by three-part `catalog.schema.table` name —
  see [Multiple catalogs with `conn.attach`](#multiple-catalogs-with-connattach) below.

`connect()` is **read-only by default**: every Delta write (`saveAsTable` / `save` /
`merge` / `insert` / `update` / `delete` / `replaceWhere`) raises `PermissionError`, so an accidental
write can't mutate a shared lakehouse. Pass `read_only=False` to enable writes; reads and native
`CREATE TEMP`/`CREATE VIEW` scratch are always allowed.

For a method-by-method map of this surface against PySpark / Delta-on-Spark — what maps 1:1, what's
duckrun-flavored, and what's deliberately out of scope (SQL-first, no Spark runtime — by design) —
see [Coverage vs the Spark / Delta API](spark-delta-parity.md).

`merge` is **snapshot-pinned by default** — single-snapshot MERGE, with no extra arguments:
the target version is captured and the commit validates against it, so a concurrent writer fails the
commit loudly instead of silently interleaving. `mode("append_if_unchanged")`
and `mode("overwrite_if_unchanged")` apply the same fail-loud compare-and-swap to a plain append /
full overwrite: they commit only if the table is unchanged since the version read, else raise
`CommitFailedError`. The full model and a cross-engine comparison are in
[Snapshot isolation](snapshot-isolation.md).

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

## Multiple catalogs with `conn.attach`

`connect()` binds one lakehouse root as the **primary catalog**; `conn.attach(path, name=…)` binds
more, so a single session can read and join across several lakehouses by **three-part name**
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

conn.catalog.listCatalogs()          # ['lakehouse', 'warehouse', 'local']

# join across all three in one query — facts from the warehouse, dim from the lakehouse,
# a lookup from local — and write the mart back to the (writable) lakehouse
conn.sql("""
    SELECT d.state, sum(f.mw) AS total_mw
    FROM warehouse.mart.fct_summary f
    JOIN mart.dim_duid d                ON d.duid = f.duid
    LEFT JOIN local.dbo.fuel_factors lf ON lower(lf.fuel) = lower(d.fuel)
    GROUP BY d.state
""").write.mode("overwrite").saveAsTable("mart_generation_by_state")

# the warehouse is read-only — a write into it is refused, not silently dropped
conn.sql("SELECT 1 AS x").write.saveAsTable("warehouse.mart.nope")   # -> PermissionError
```

Naming: a table can be addressed 3-part (`warehouse.mart.fct_summary`, from anywhere), 2-part
(`mart.fct_summary`, in the current catalog) or bare (`fct_summary`, in the current catalog + schema).
`conn.catalog.setCurrentCatalog(name)` / `setCurrentDatabase(db)` move where unqualified names
resolve. `name` is derived from a friendly path and is **mandatory for a GUID-only OneLake path**; one
URL maps to one name (re-attaching either raises). `read_only` is **per-catalog**, independent of the
session — a read-only reference store sits safely next to a writable lakehouse. Raw `conn.sql()` DML
targets the current catalog; cross-catalog writes go through the DataFrame API
(`df.write.saveAsTable("cat.schema.t")`) or `DeltaTable.forName(conn, "cat.schema.t")`.

See the full runnable walkthrough in
[`integration_tests/multicatalog/demo_multicatalog.py`](../integration_tests/multicatalog/demo_multicatalog.py)
(published as a [live report](https://djouallah.github.io/duckrun/multicatalog.html)).

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
| `MERGE INTO x [a] USING s [b] ON a.k = b.k WHEN …` | delta_rs upsert (same engine + snapshot pin as the `DeltaTable.merge` builder). Write it like standard SQL — the `ON`/`WHEN` clauses may use **your own aliases or the table/relation names** (the literal `target`/`source` also work); fully-unqualified columns (`ON k = k`) are ambiguous and unsupported. Supports `UPDATE SET *` / `UPDATE SET col = <src>.col`, `INSERT *`, `WHEN NOT MATCHED BY SOURCE THEN DELETE`, and per-clause `AND` predicates |
| `CREATE TEMP/TEMPORARY TABLE …`, `CREATE VIEW …` | **native DuckDB** — ephemeral, session-local; not a Delta artifact |
| `UPDATE … FROM`, `DELETE … USING` | rejected → rewrite as a correlated subquery, or use `DeltaTable.forName(conn, name)` |
| multiple statements in one call | rejected → one statement per `conn.sql()` |

Leading `--` / `/* … */` comments are fine. The exact behaviour is pinned, statement-by-statement,
in [`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py)
(the cross-API write-correctness matrix + the `TestSqlDml` class).

## Table layout — sort, partition, compaction

### Sorting a write

`df.sort(*cols)` / `df.orderBy(*cols)` are the vanilla Spark DataFrame methods — `orderBy` is an
alias of `sort`, and both return a **new, writable** DataFrame ordered by a native DuckDB `ORDER BY`
(`ascending=` is a bool or a per-column list). Sorting a write physically clusters equal values
together, which is what makes run-length and dictionary encoding pay off — smaller files and faster
column scans for any analytics reader.

```python
# sorted write — clusters low-cardinality columns for better compression
conn.sql("select * from stg_sales") \
    .orderBy("region", "order_date") \
    .write.mode("overwrite").saveAsTable("sales")

# composes with partitioning: partition columns should lead the sort so delta-rs keeps
# ~one partition writer open at a time (less write memory); it writes the folders either way
conn.sql("select * from stg_sales") \
    .sort("region", "order_date") \
    .write.partitionBy("region").saveAsTable("sales")
```

Partitioning itself is `df.write.partitionBy(*cols)` — delta-rs writes Hive-style `col=value/`
folders and strips the column from the data files (it's re-materialized from the path on read). Sort
and partition are orthogonal: partitioning decides the folder layout, sorting decides row order.

### Table maintenance { #optimize }

Maintenance operates on a **table**, not the session. There are two `optimize` methods.

**`conn.table(name).optimize(...)`** — the maintenance ladder. The bare call is safe; you opt into
the heavier operations by argument:

```python
# Safe button — compact small files + vacuum, NEVER rewrites row data. Only partitions carrying
# real small-file debt are bin-packed (a byte trigger). Commits dataChange=false, safe under
# concurrent writers, idempotent — schedule it.
conn.table("sales").optimize()
# → {'operation': 'compact', 'filesRemoved': 41, 'filesAdded': 3,
#    'partitionsTouched': ['date=2026-07-04'], 'filesVacuumed': 12, 'advice': '…'}
# nothing to do → {'operation': 'noop', 'reason': 'no small-file debt', 'filesVacuumed': 0}

# Sort rewrite — profile → ORDER BY → rewrite in the parquet layout. Commits dataChange=true.
conn.table("sales").optimize(rewrite=True)             # auto-profiled sort key
conn.table("sales").optimize("region", "order_date")  # explicit key
conn.table("sales").optimize("order_date", where="year = 2026")   # scoped to matching partitions
# → {'operation': 'sortRewrite', 'sortedBy': [...], 'sizeBytesBefore': …, 'sizeBytesAfter': …,
#    'savedPct': 55.3, 'warning': 'commits as dataChange=true — CDF/streaming consumers see a change'}

# Advisory — the sort-key recommendation as a DataFrame + small-file debt, commits nothing.
conn.table("sales").optimize(analyze=True)
```

The bare button never touches row data; only `rewrite=True` / an explicit key / `where` do. Both
rewrite paths are snapshot-fenced (full table → `overwrite_if_unchanged`, scoped → `replaceWhere`),
so a concurrent write fails the rewrite loudly rather than being clobbered. With `rewrite=True` the
key is picked automatically by profiling the table — a heuristic, not an optimizer, because the
optimal choice is an NP-hard problem — see [Automatic sort](automatic-sort.md). Every write (this
one included) lands in [the parquet layout](parquet-layout.md).

**`DeltaTable.forName(conn, name).optimize()`** — the plain delta-rs `OPTIMIZE` (bin-packing
compaction):

```python
DeltaTable.forName(conn, "sales").optimize()   # bin-packing compaction
```

There is no z-order (bit-interleaving destroys the run-length runs a columnar reader relies on; use a
lexicographic key via `conn.table(name).optimize(...)` instead).

Names resolve like everywhere else — bare = current schema, `schema.table` or
`catalog.schema.table` to be explicit.
