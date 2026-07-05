# Supported API

The exact contract: every public method of `duckrun.connect()`. The list below is **introspected
from the shipped classes** — not hand-maintained and not derived from test names — so it's the real
public surface, nothing more, nothing less. It's regenerated on every push by the `connection-card`
job in [`cores.yml`](../.github/workflows/cores.yml); the same job runs the full
[`test_connection_api.py`](../tests/connection_api/test_connection_api.py) suite and **fails if any
test regresses**, so the green badge vouches the surface works. Prose, examples, and the design
rationale live in [Connection API](connection-api.md).

<!-- CONNECTION_API:START -->

## duckrun connection API — supported methods

```
┌──────────────────────────────┐
│ ✅ 77 public methods          │
│ suite: 152/152 tests passing │
└──────────────────────────────┘
```

> Introspected from the shipped classes — this is the exact public surface of `duckrun.connect()`, not a hand-maintained list. The green suite ([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works.

### DataFrame / Delta API — 67 methods

> Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.

| Surface | Methods | # |
| --- | --- | :-: |
| `DuckSession` | `catalog`, `read`, `sql`, `table` | 4 |
| `Catalog` | `createTable`, `currentCatalog`, `currentDatabase`, `databaseExists`, `dropTempView`, `getDatabase`, `getTable`, `listCatalogs`, `listColumns`, `listDatabases`, `listTables`, `refreshTable`, `setCurrentCatalog`, `setCurrentDatabase`, `tableExists` | 15 |
| `DataFrame` | `collect`, `count`, `createOrReplaceTempView`, `first`, `head`, `isEmpty`, `orderBy`, `printSchema`, `schema`, `show`, `sort`, `take`, `toArrow`, `toPandas`, `write` | 15 |
| `DataFrameReader` | `csv`, `format`, `json`, `load`, `option`, `parquet`, `schema`, `table` | 8 |
| `DataFrameWriter` | `format`, `mode`, `option`, `partitionBy`, `save`, `saveAsTable` | 6 |
| `DeltaTable` | `convertToDelta`, `delete`, `forName`, `forPath`, `history`, `merge`, `optimize`, `restoreToVersion`, `update`, `vacuum`, `version` | 11 |
| `DeltaMergeBuilder` | `execute`, `whenMatchedDelete`, `whenMatchedUpdate`, `whenMatchedUpdateAll`, `whenNotMatchedBySourceDelete`, `whenNotMatchedBySourceUpdate`, `whenNotMatchedInsert`, `whenNotMatchedInsertAll` | 8 |

### duckrun-specific helpers — 10 methods

> Conveniences with no DataFrame-API equivalent (session plumbing + shortcuts). `conn.sql()` also routes raw Delta DML — see the DML matrix on the [Connection API](connection-api.md) page.

| Surface | Methods | # |
| --- | --- | :-: |
| `DuckSession` | `attach`, `connect`, `copy`, `createDataFrame`, `download`, `get_stats`, `list_files`, `refresh`, `stop` | 9 |
| `DataFrame` | `optimize` | 1 |

<!-- CONNECTION_API:END -->
