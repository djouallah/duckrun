# API reference

<!-- CONNECTION_API:START -->

## duckrun connection API — supported methods

✅ **58 public methods** · 152/152 tests passing

> Introspected from the shipped classes — the exact public surface of `duckrun.connect()`, signatures and all, not a hand-maintained list. The green suite ([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works. `conn.sql()` also routes raw Delta DML — see the DML matrix on the [Connection API](connection-api.md) page.

| Surface | Method | Parameters |
| --- | --- | --- |
| `duckrun` | `connect` | `path, storage_options=None, schema=None, read_only=True, name=None` |
| `DuckSession` | `attach` | `path, name=None, storage_options=None, schema=None, read_only=None` |
| `DuckSession` | `catalog` | *accessor* |
| `DuckSession` | `copy` | `local_folder, remote_folder, file_extensions=None, overwrite=False` |
| `DuckSession` | `createDataFrame` | `data, schema=None` |
| `DuckSession` | `download` | `remote_folder='', local_folder='./downloaded_files', file_extensions=None, overwrite=False` |
| `DuckSession` | `get_stats` | `source=None, detailed=False` |
| `DuckSession` | `list_files` | `remote_folder='', file_extensions=None` |
| `DuckSession` | `read` | *property* |
| `DuckSession` | `refresh` | `quiet=False, catalog=None` |
| `DuckSession` | `sql` | `query` |
| `DuckSession` | `stop` | *(none)* |
| `DuckSession` | `table` | `name` |
| `Catalog` | `createTable` | `tableName, schema` |
| `Catalog` | `currentCatalog` | *(none)* |
| `Catalog` | `currentDatabase` | *(none)* |
| `Catalog` | `databaseExists` | `dbName` |
| `Catalog` | `dropTempView` | `viewName` |
| `Catalog` | `getDatabase` | `dbName` |
| `Catalog` | `getTable` | `tableName, dbName=None` |
| `Catalog` | `listCatalogs` | *(none)* |
| `Catalog` | `listColumns` | `tableName, dbName=None` |
| `Catalog` | `listDatabases` | *(none)* |
| `Catalog` | `listTables` | `dbName=None` |
| `Catalog` | `refreshTable` | `tableName` |
| `Catalog` | `setCurrentCatalog` | `catalogName` |
| `Catalog` | `setCurrentDatabase` | `dbName` |
| `Catalog` | `tableExists` | `tableName, dbName=None` |
| `DataFrame` | `collect` | *(none)* |
| `DataFrame` | `count` | *(none)* |
| `DataFrame` | `createOrReplaceTempView` | `name` |
| `DataFrame` | `first` | *(none)* |
| `DataFrame` | `head` | `n=None` |
| `DataFrame` | `isEmpty` | *(none)* |
| `DataFrame` | `optimize` | `*keys, rewrite=False, where=None, analyze=False, seed=None` |
| `DataFrame` | `orderBy` | `*cols, ascending=None, seed=None` |
| `DataFrame` | `printSchema` | *(none)* |
| `DataFrame` | `schema` | *property* |
| `DataFrame` | `show` | `*a, **k` |
| `DataFrame` | `sort` | `*cols, ascending=None, seed=None` |
| `DataFrame` | `take` | `n` |
| `DataFrame` | `toArrow` | *(none)* |
| `DataFrame` | `toPandas` | *(none)* |
| `DataFrame` | `write` | *property* |
| `DataFrameReader` | `csv` | `path` |
| `DataFrameReader` | `format` | `fmt` |
| `DataFrameReader` | `json` | `path` |
| `DataFrameReader` | `load` | `path` |
| `DataFrameReader` | `option` | `key, value` |
| `DataFrameReader` | `parquet` | `path` |
| `DataFrameReader` | `schema` | `schema` |
| `DataFrameReader` | `table` | `name` |
| `DataFrameWriter` | `format` | `fmt` |
| `DataFrameWriter` | `mode` | `mode` |
| `DataFrameWriter` | `option` | `key, value` |
| `DataFrameWriter` | `partitionBy` | `*cols` |
| `DataFrameWriter` | `save` | `path` |
| `DataFrameWriter` | `saveAsTable` | `name` |

<!-- CONNECTION_API:END -->
