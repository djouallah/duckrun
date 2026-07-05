# Supported API

The exact contract: every public method of `duckrun.connect()` with a ✅ is a supported,
tested method — nothing else is promised. The card below is regenerated on every push by the
`connection-card` job in [`cores.yml`](../.github/workflows/cores.yml) from the `Test*` classes of
[`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py), and
the job **fails if any method regresses** — so it's the live, verified surface, not a hand-maintained
wishlist. Prose, examples, and the design rationale for this surface live in
[Connection API](connection-api.md).

<!-- CONNECTION_API:START -->

## duckrun connection API — method scorecard

```
┌────────────────────────────┐
│ ✅ 113 passed   ❌ 0 failed  │
│ 113 methods · 100% passing │
└────────────────────────────┘
```

### DataFrame API — 84/84 ✅

> Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.

| Surface | Methods | Pass |
| --- | --- | :-: |
| `DuckSession` | `sql`, `table`, `read`, `catalog` | 4/4 ✅ |
| `Catalog` | `listTables`, `listDatabases`, `currentDatabase`, `setCurrentDatabase`, `tableExists`, `tableExists_is_fresh`, `drop_parity_across_surfaces`, `reader_table_absent_after_drop`, `databaseExists`, `listColumns`, `refreshTable`, `createTable_ddl`, `createTable_from_struct`, `createTable_bad_schema`, `getTable`, `getDatabase`, `dropTempView`, `listCatalogs`, `currentCatalog`, `setCurrentCatalog` | 20/20 ✅ |
| `DataFrame` | `collect`, `count`, `columns`, `show`, `toPandas`, `toArrow`, `first`, `head`, `take`, `isEmpty`, `schema`, `printSchema` | 12/12 ✅ |
| `DataFrameReader` | `format/load`, `table`, `parquet`, `csv`, `json`, `schema_csv_ddl`, `schema_ddl_with_comma_type`, `schema_json_struct`, `schema_rejected_for_delta`, `versionAsOf`, `timestampAsOf_rejected` | 11/11 ✅ |
| `DataFrameWriter` | `saveAsTable`, `mode`, `option`, `insertInto_removed`, `partitionBy`, `sort`, `orderBy_alias_and_desc`, `sort_no_args_auto_key`, `sort_then_partition_write`, `format`, `save_by_path`, `save_modes`, `save_mode_error_when_exists` | 13/13 ✅ |
| `DeltaTable` | `forName`, `forPath`, `convertToDelta`, `merge`, `version`, `history`, `delete`, `update`, `optimize`, `table_optimize_auto_keys`, `table_optimize_user_keys`, `table_optimize_where_scopes_partitions`, `table_optimize_rejects_query_frame`, `optimize_refuses_on_sorted_frame`, `self_overwrite_guard`, `sort_no_args_on_table_uses_log_stats`, `bare_table_optimize_still_works`, `table_optimize_rewrite_refuses_on_concurrent_commit`, `table_optimize_maintain_noop`, `table_optimize_maintain_compacts_small_files`, `table_optimize_analyze`, `get_rle_scan_count_is_constant`, `vacuum`, `restoreToVersion` | 24/24 ✅ |

### duckrun-specific helpers — 29/29 ✅

> Conveniences with no DataFrame-API equivalent (session plumbing + two shortcuts).

| Method | Surface | Pass |
| --- | --- | :-: |
| `connect` | `DuckSession` | ✅ |
| `createDataFrame` | `DuckSession` | ✅ |
| `refresh` | `DuckSession` | ✅ |
| `connection` | `DuckSession` | ✅ |
| `stop` | `DuckSession` | ✅ |
| `table_path` | `DuckSession` | ✅ |
| `attach` | `DuckSession` | ✅ |
| `copy` | `DuckSession` | ✅ |
| `download` | `DuckSession` | ✅ |
| `list_files` | `DuckSession` | ✅ |
| `get_stats` | `DuckSession` | ✅ |
| `get_stats_detailed` | `DuckSession` | ✅ |
| `get_stats_glob` | `DuckSession` | ✅ |
| `get_stats_vorder_flag` | `DuckSession` | ✅ |
| `__getattr__` | `DataFrame` | ✅ |
| `SELECT (passthrough)` | `sql()` | ✅ |
| `version-pinned read` | `sql()` | ✅ |
| `create table as` | `sql()` | ✅ |
| `insert…select` | `sql()` | ✅ |
| `insert…values` | `sql()` | ✅ |
| `update` | `sql()` | ✅ |
| `delete` | `sql()` | ✅ |
| `sql_update_where_inside_set_literal` | `sql()` | ✅ |
| `sql_update_subquery_predicate` | `sql()` | ✅ |
| `sql_delete_subquery_predicate` | `sql()` | ✅ |
| `alter add column` | `sql()` | ✅ |
| `sql_alter_add_column_not_null` | `sql()` | ✅ |
| `drop (tombstone)` | `sql()` | ✅ |
| `merge` | `sql()` | ✅ |

<!-- CONNECTION_API:END -->
