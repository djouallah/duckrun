# Connection API (notebook)

Besides the dbt adapter, duckrun ships a storage-neutral, PySpark-shaped `duckrun.connect()` for
interactive/notebook use (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — run DuckDB SQL over the discovered Delta tables. `CREATE TABLE AS`, `INSERT`,
  `UPDATE`, and `DELETE` are routed to Delta (delta-rs); everything else reads through.
- a `DataFrame` with a Spark-style `.write…saveAsTable()`, plus `conn.read` and `conn.catalog`.
- a `DeltaTable.merge(...)` upsert builder mirroring Delta-on-Spark.

```python
import duckrun
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
conn.sql("CREATE TABLE orders_copy AS SELECT * FROM orders")
conn.table("orders_copy").show()
```

The card below — every public method with a ✅ — is regenerated on every push by
[`connection-card.yml`](../.github/workflows/connection-card.yml) from
[`tests/connection_api/test_method_matrix.py`](../tests/connection_api/test_method_matrix.py).

<!-- CONNECTION_API:START -->

## duckrun connection API — method scorecard

```
┌───────────────────────────┐
│ ✅ 44 passed   ❌ 0 failed  │
│ 44 methods · 100% passing │
└───────────────────────────┘
```

### Spark / Delta-on-Spark API — 38/38 ✅

> Methods that mirror PySpark (and Delta Lake's `DeltaTable` on Spark) 1:1.

| Surface | Methods | Pass |
| --- | --- | :-: |
| `DuckSession` | `sql`, `table`, `read`, `catalog` | 4/4 ✅ |
| `Catalog` | `listTables`, `listDatabases`, `currentDatabase`, `setCurrentDatabase`, `tableExists`, `tableExists_is_fresh`, `databaseExists`, `listColumns` | 8/8 ✅ |
| `DataFrame` | `collect`, `count`, `columns`, `show`, `toPandas` | 5/5 ✅ |
| `DataFrameReader` | `format/load`, `table`, `parquet`, `csv` | 4/4 ✅ |
| `DataFrameWriter` | `saveAsTable`, `mode`, `option`, `partitionBy`, `format` | 5/5 ✅ |
| `DeltaTable` | `forName`, `forPath`, `merge_upsert`, `merge_update_columns`, `merge_insert_only`, `update_only_rejected` | 6/6 ✅ |
| `sql()` | `CREATE TABLE AS`, `INSERT`, `DELETE`, `UPDATE`, `SELECT (passthrough)`, `multi-statement guard` | 6/6 ✅ |

### duckrun-specific helpers — 6/6 ✅

> Conveniences with no Spark equivalent (session plumbing + two shortcuts).

| Method | Surface | Pass |
| --- | --- | :-: |
| `connect` | `DuckSession` | ✅ |
| `refresh` | `DuckSession` | ✅ |
| `connection` | `DuckSession` | ✅ |
| `table_path` | `DuckSession` | ✅ |
| `__getattr__` | `DataFrame` | ✅ |
| `delta` | `DataFrameReader` | ✅ |

<!-- CONNECTION_API:END -->
