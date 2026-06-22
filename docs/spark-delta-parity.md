# Coverage vs the Spark / Delta API

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

## Bespoke — duckrun-only, no Spark/Delta equivalent

The **entire** invented surface is the three entries below — a path-based entry point and two
runtime primitives. Everything else on this page either maps to a real Spark/delta-rs method, is a
deliberate 🚫 omission, or is a ➖ TODO. There is nothing else masquerading as Spark.

| duckrun | what it is | why there's no Spark name |
| --- | --- | --- |
| `duckrun.connect(path, storage_options=…, schema=…)` | open a session bound to one lakehouse root, addressed by a storage **path** | Spark's entry point is `SparkSession.builder…getOrCreate()` against a cluster — there's no cluster and no builder; one connection binds to one storage root |
| `conn.refresh()` | re-discover the Delta tables under the store and re-register their views | duckrun finds tables by globbing storage for `_delta_log`; Spark's metastore is authoritative, so it never needs a "rescan the store" call |
| `write.mode("safeappend")` | fail-loud compare-and-swap append (commits only if the table version hasn't moved) | no Spark `SaveMode` for it — it's the duckrun/dbt concurrency primitive |

Private plumbing (`_resolve`, `_table_path`, `_connection`) is bespoke too but intentionally not
part of the public surface, so it isn't tracked below. One naming note: `DeltaTable.delete(predicate)`
uses delta-rs's parameter name (`predicate`) rather than delta-spark's (`condition`) — real API,
not invented.

## `SparkSession` ↔ `duckrun.connect()` / `DuckSession`

| Spark | duckrun | | Notes |
| --- | --- | :-: | --- |
| `spark.sql(str)` | `conn.sql(str)` | ✅ | Plus raw **DML** routing to delta-rs (`create table as` / `insert` / `update` / `delete` / `alter add column` / `drop`). |
| `spark.table(name)` | `conn.table(name)` | ✅ | |
| `spark.read` | `conn.read` | ✅ | → `DataFrameReader`. |
| `spark.catalog` | `conn.catalog` | ✅ | → `Catalog` (see below). |
| `spark.createDataFrame(rows)` | — | ➖ | TODO |

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
| `df.toArrow()` | `df.toArrow()` | 🟡 | Spark collects a whole `pyarrow.Table`; duckrun returns a **streaming** `pyarrow.RecordBatchReader` (`to_arrow_reader()`) so big results don't materialize. |
| `df.count()` | `df.count()` | ✅ | |
| `df.show()` | `df.show()` | ✅ | |
| `df.first()` | — | ➖ | TODO |
| `df.head(n=1)` / `df.take(n)` | — | ➖ | TODO |
| `df.isEmpty()` | — | ➖ | TODO |
| `df.createOrReplaceTempView(name)` | `df.createOrReplaceTempView(name)` | ✅ | Native, ephemeral DuckDB view — not Delta, not in `conn.catalog`. |
| `df.columns` | (passthrough) | 🟡 | Not reimplemented — `__getattr__` forwards to the DuckDB relation; a list of names, like Spark. |
| `df.dtypes` | (passthrough) | 🟡 | Passthrough to the DuckDB relation — returns DuckDB types, not Spark `(name, type)` tuples. |
| `df.schema` | — | ➖ | TODO |
| `df.printSchema()` | — | ➖ | TODO |


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
| `read.option("versionAsOf", N).load(path)` | `read.option("versionAsOf", N).load(path)` | ✅ | Time travel via duckdb-delta `version =>`. |
| `read.option("timestampAsOf", ts)` | — | 🚫 | duckdb-delta time-travels by version only; rejected (use `versionAsOf`). |
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
| `write.insertInto(name)` | `write.insertInto(name)` | ✅ | Appends to an **existing** table (errors if missing); `overwrite=True` replaces all rows. |
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
| `catalog.getTable(t, db)` | — | ➖ | TODO — peer of `tableExists` / `listTables`. |
| `catalog.getDatabase(db)` | — | ➖ | TODO — peer of `databaseExists` / `listDatabases`. |
| `catalog.dropTempView(name)` | — | ➖ | TODO — inverse of `df.createOrReplaceTempView`. |
| `catalog.createTable` / `createExternalTable` | — | ➖ | TODO — today use `df.write.saveAsTable`. |
| `catalog.cacheTable` / `uncacheTable` / `isCached` / `clearCache` | — | ➖ | TODO — closest is materializing a TEMP table. |
| `catalog.refreshTable` | — | ➖ | TODO — per-table; `conn.refresh()` (bespoke) rediscovers the whole store. |
| `catalog.recoverPartitions` | — | ➖ | TODO (delta-rs gap). |
| `catalog.refreshByPath` | — | 🚫 | Path reads aren't cached — nothing to refresh. |
| `catalog.currentCatalog` / `setCurrentCatalog` / `listCatalogs` | — | ➖ | TODO — would map each attached lakehouse root to a catalog (multi-lakehouse); single-root today. |
| `catalog.functionExists` / `listFunctions` / `registerFunction` | — | 🚫 | DuckDB owns the function namespace; not a duckrun catalog concept. |
| `catalog.dropGlobalTempView` | — | 🚫 | No global-temp namespace in duckrun. |

## `DeltaTable` (Delta-on-Spark) ↔ `DeltaTable.forName(conn, name)`

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
| `.delete(predicate)` | `.delete(predicate)` | ✅ | delta-rs param name (`predicate`); takes literals, not `IN (SELECT …)`. |
| `.update(condition, set)` | `.update(condition=…, set=…)` | ✅ | delta-spark signature. |
| `df.write.option("replaceWhere", …)` / `INSERT OVERWRITE` | `df.write.option("replaceWhere", pred).mode("overwrite").save()` / `.saveAsTable()` | ✅ | Single atomic commit; snapshot-fenced. |
| `.history()` | — | ➖ | TODO — delta-rs exposes table history just fine. |
| `.vacuum()` | — | ➖ | TODO |
| `.optimize()` | — | ➖ | TODO |
| `.generate()` | — | ➖ | TODO |
| `.restoreToVersion()` | — | ➖ | TODO |
| `.clone()` | — | ➖ | TODO |
| `convertToDelta` | — | ➖ | TODO |
