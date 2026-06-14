# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, PySpark-shaped `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — **read-only** DuckDB SQL over the discovered Delta tables, including time travel
  (`delta_scan('…', version => N)`). A write statement is rejected with a pointer to the write API.
- a `DataFrame` with a Spark-style `.write…saveAsTable()` — modes `overwrite` / `append` /
  `safeappend` / `ignore` — plus `conn.read` and `conn.catalog`.
- a `DeltaTable` handle (`conn.delta_table(name)` / `DeltaTable.forName`) mirroring Delta-on-Spark:
  `.merge(...)`, `.delete()`, `.update()`, `.replaceWhere()`, `.version()`.

`merge` is **snapshot-pinned by default** — Spark's single-snapshot MERGE, with no extra arguments:
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

The card below — every public method with a ✅ — is regenerated on every push by
[`connection-card.yml`](../.github/workflows/connection-card.yml) from
[`tests/connection_api/test_method_matrix.py`](../tests/connection_api/test_method_matrix.py).

<!-- CONNECTION_API:START -->

## duckrun connection API — method scorecard

```
┌──────────────────────────┐
│ ✅ 41 passed   ❌ 1 failed │
│ 42 methods · 98% passing │
└──────────────────────────┘
```

### Spark / Delta-on-Spark API — 33/33 ✅

> Methods that mirror PySpark (and Delta Lake's `DeltaTable` on Spark) 1:1.

| Surface | Methods | Pass |
| --- | --- | :-: |
| `DuckSession` | `sql`, `table`, `read`, `catalog` | 4/4 ✅ |
| `Catalog` | `listTables`, `listDatabases`, `currentDatabase`, `setCurrentDatabase`, `tableExists`, `tableExists_is_fresh`, `databaseExists`, `listColumns` | 8/8 ✅ |
| `DataFrame` | `collect`, `count`, `columns`, `show`, `toPandas` | 5/5 ✅ |
| `DataFrameReader` | `format/load`, `table`, `parquet`, `csv` | 4/4 ✅ |
| `DataFrameWriter` | `saveAsTable`, `mode`, `option`, `partitionBy`, `format` | 5/5 ✅ |
| `DeltaTable` | `forName`, `forPath`, `merge`, `version`, `delete`, `update`, `replaceWhere` | 7/7 ✅ |

### duckrun-specific helpers — 8/9 ✅

> Conveniences with no Spark equivalent (session plumbing + two shortcuts).

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
| `read-only guard` | `sql()` | ❌ |

<!-- CONNECTION_API:END -->
