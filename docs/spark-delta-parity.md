# Coverage vs the Spark / Delta `DeltaTable` API

duckrun's connection API (`duckrun.connect()`) deliberately targets **parity with the surface most
data engineers already know** — PySpark's `SparkSession` / `DataFrame` family and Delta Lake's
`DeltaTable` (the "Delta-on-Spark") API — so notebook code reads the same. Under the hood it is
**DuckDB + delta-rs**, not Apache Spark: there is no Spark runtime, no JVM, no cluster.

This page tracks how far the surface matches. Two things it is **not**: a Spark runtime, and a
fluent DataFrame transform builder. Both are **design decisions, not gaps** — duckrun is SQL-first
(transforms are written as SQL through `conn.sql(...)`), and there is no cluster to configure. Those
rows are marked 🚫 below precisely so they read as "we chose not to," not "still missing."

> Everywhere else in the docs this is called the **DataFrame API** (it is not full Spark). This one
> page names Spark on purpose, because it is a side-by-side comparison.

The companion to this page is the **method scorecard** in
[`connection-api.md`](connection-api.md) (auto-generated from
[`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py)):
that says *which mapped methods actually pass their tests*; this page says *what maps to what*.

**Legend**

- ✅ implemented, parity-faithful
- 🟡 implemented, duckrun-flavored or a convenience shortcut
- 🚫 **by design** — deliberately not offered (SQL-first, or no Spark runtime). **Not a gap.**
- ➖ not wired yet — could be added if asked for

## `SparkSession` ↔ `duckrun.connect()` / `DuckSession`

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `SparkSession.builder…getOrCreate()` | `duckrun.connect(path, storage_options=…, schema=…)` | 🟡 | Entry point is a storage **path**, not a builder — one connection bound to one lakehouse root. |
| `spark.sql(str)` | `conn.sql(str)` | ✅ | Plus raw **DML** routing to delta-rs (`create table as` / `insert` / `update` / `delete` / `alter add column` / `drop`). |
| `spark.table(name)` | `conn.table(name)` | ✅ | |
| `spark.read` | `conn.read` | ✅ | → `DataFrameReader`. |
| `spark.catalog` | `conn.catalog` | ✅ | → `Catalog` (see below). |
| `spark.createDataFrame(rows)` | `conn.sql("SELECT * FROM (VALUES …) t(…)")` | ➖ | TODO |
| `spark.range(n)` | `conn.sql("SELECT … FROM range(n)")` | ➖ | TODO |
| — | `conn.delta_table(name)` | 🟡 | duckrun shortcut for `DeltaTable.forName(conn, name)`. |
| — | `conn.table_path(schema, table)` | 🟡 | duckrun plumbing: locate a table's storage path. |
| — | `conn.resolve(name)` | 🟡 | duckrun plumbing: resolve a name to `(schema, table)`. |
| — | `conn.refresh()` | 🟡 | duckrun plumbing: re-discover the catalog. |
| — | `conn.connection` | 🟡 | The raw DuckDB connection — escape hatch. |

## `DataFrame`

duckrun's `DataFrame` is a **thin handle over a DuckDB relation**. Row/column **transforms are done
in SQL** (`conn.sql(...)`), not via chained DataFrame methods — a deliberate SQL-first design choice
(see the [DuckDB Spark-API spike decision](design_document.md)), not missing coverage. The methods
below are the action/output verbs, plus a passthrough to the underlying relation.

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `df.write` | `df.write` | ✅ | → `DataFrameWriter`. |
| `df.collect()` | `df.collect()` | ✅ | |
| `df.toPandas()` | `df.toPandas()` | ✅ | |
| `df.count()` | `df.count()` | ✅ | |
| `df.show()` | `df.show()` | ✅ | |
| `df.createOrReplaceTempView(name)` | `df.createOrReplaceTempView(name)` | ✅ | Native, ephemeral DuckDB view — not Delta, not in `conn.catalog`. |
| `df.columns` | (passthrough) | 🟡 | Not reimplemented — `__getattr__` forwards to the DuckDB relation; a list of names, like Spark. |
| `df.dtypes` | (passthrough) | 🟡 | Passthrough to the DuckDB relation — returns DuckDB types, not Spark `(name, type)` tuples. |
| `df.schema` | — | ➖ | TODO |

## `DataFrameReader` (`conn.read`)

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `read.format(fmt)` | `read.format(fmt)` | ✅ | `delta` (default), `parquet`, `csv`. |
| `read.option(k, v)` | `read.option(k, v)` | ✅ | |
| `read.load(path)` | `read.load(path)` | ✅ | Honors the chosen `format`. |
| `read.format("delta").load(path)` | `read.format("delta").load(path)` | ✅ | Identical — `delta` is the default format. |
| `read.parquet(path)` | `read.parquet(path)` | ✅ | |
| `read.csv(path)` | `read.csv(path)` | ✅ | |
| `read.table(name)` | `read.table(name)` | ✅ | |
| `read.schema(…)` | — | ➖ | TODO |
| `read.json` | — | ➖ | TODO |
| `read.orc` | — | ➖ | TODO |
| `read.text` | — | ➖ | TODO |

## `DataFrameWriter` (`df.write`)

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `write.format(fmt)` | `write.format(fmt)` | ✅ | `delta`. |
| `write.mode(m)` | `write.mode(m)` | ✅ | `overwrite` / `append` / `ignore` / `error` (default). |
| `write.option(k, v)` | `write.option(k, v)` | ✅ | `overwriteSchema`, `mergeSchema`. |
| `write.partitionBy(*cols)` | `write.partitionBy(*cols)` | ✅ | |
| `write.save(path)` | `write.save(path)` | ✅ | Write Delta by **path**. |
| `write.saveAsTable(name)` | `write.saveAsTable(name)` | ✅ | Write Delta by **catalog name**. |
| — | `write.mode("safeappend")` | 🟡 | duckrun extra: a fail-loud compare-and-swap append (no Spark equivalent). |
| `write.insertInto(name)` | `df.write.mode("append").saveAsTable(name)` | ➖ | TODO |
| `write.bucketBy` | — | 🚫 | Delta doesn't bucket; partitioning is `partitionBy`. |
| `write.sortBy` | — | 🚫 | Delta doesn't bucket; partitioning is `partitionBy`. |

## `Catalog` (`conn.catalog`)

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `catalog.listDatabases()` | `catalog.listDatabases()` | ✅ | (= schemas in the lakehouse root). |
| `catalog.listTables(dbName)` | `catalog.listTables(dbName)` | ✅ | |
| `catalog.listColumns(table, dbName)` | `catalog.listColumns(table, dbName)` | ✅ | |
| `catalog.currentDatabase()` | `catalog.currentDatabase()` | ✅ | |
| `catalog.setCurrentDatabase(db)` | `catalog.setCurrentDatabase(db)` | ✅ | |
| `catalog.tableExists(t, db)` | `catalog.tableExists(t, db)` | ✅ | |
| `catalog.databaseExists(db)` | `catalog.databaseExists(db)` | ✅ | |
| `catalog.cacheTable` | — | ➖ | TODO |
| `catalog.clearCache` | — | ➖ | TODO |
| `catalog.dropTempView` | `conn.sql("DROP VIEW name")` | ➖ | TODO |
| `catalog.refreshTable` | `conn.refresh()` | ➖ | TODO |
| `catalog.recoverPartitions` | — | ➖ | TODO (delta-rs gap) |

## `DeltaTable` (Delta-on-Spark) ↔ `conn.delta_table(name)` / `DeltaTable`

The write/mutate side. **`merge` is snapshot-pinned by default** (single-snapshot MERGE): the target
version is captured at build time and the commit validates against it, so a concurrent writer fails
loudly (`CommitFailedError`) rather than silently interleaving.

| Spark / Delta | duckrun | | Notes |
| --- | --- | :-: | --- |
| `DeltaTable.forName(spark, name)` | `DeltaTable.forName(conn, name)` | ✅ | |
| `DeltaTable.forPath(spark, path)` | `DeltaTable.forPath(conn, path)` | ✅ | |
| `.merge(source, condition)` | `.merge(source, condition)` | ✅ | Returns a builder; snapshot-pinned automatically. |
| `.whenMatchedUpdate(set=…)` | `.whenMatchedUpdate(set=…)` | ✅ | |
| `.whenMatchedUpdateAll()` | `.whenMatchedUpdateAll()` | ✅ | |
| `.whenNotMatchedInsertAll()` | `.whenNotMatchedInsertAll()` | ✅ | |
| `.whenNotMatchedBySourceDelete()` | `.whenNotMatchedBySourceDelete()` | ✅ | |
| `.whenMatchedDelete()` | — | ➖ | TODO |
| `.whenNotMatchedInsert(values=…)` | — | ➖ | TODO |
| `.whenNotMatchedBySourceUpdate(set=…)` | — | ➖ | TODO |
| `.delete(predicate)` | `.delete(predicate)` | ✅ | delta-rs predicates take literals (not `IN (SELECT …)`). |
| `.update(set, where)` | `.update(set=…, where=…)` | ✅ | |
| `df.write.option("replaceWhere", …)` / `INSERT OVERWRITE` | `.replaceWhere(source, predicate)` | ➖ | TODO |
| `.history()` | `.version()` | ➖ | TODO |
| `spark.read.option("versionAsOf", N)` | `conn.sql("… delta_scan(path, version => N)")` | ➖ | TODO |
| `.vacuum()` | — | ➖ | TODO |
| `.optimize()` | — | ➖ | TODO |
| `.generate()` | — | ➖ | TODO |
| `.restoreToVersion()` | — | ➖ | TODO |
| `.clone()` | — | ➖ | TODO |
| `convertToDelta` | — | ➖ | TODO |
