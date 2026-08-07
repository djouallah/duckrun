# Vendored Delta tables

Real tables written by engines duckrun cannot itself produce. delta-rs never enables column
mapping, so a table with it on can only come from Spark/Databricks or Fabric — and the whole point
of these fixtures is that nothing about them is hand-stamped.

## `table_with_column_mapping/`

Copied verbatim from [delta-io/delta-rs](https://github.com/delta-io/delta-rs), path
`crates/test/tests/data/table_with_column_mapping`, at commit
`857bd0e1e0c55c0337d500986667c7034c45ce87`. Licensed Apache-2.0, like delta-rs itself.

A Databricks write with `delta.columnMapping.mode = name` (protocol 2/5), 5 rows in 2 files,
partitioned. Its schema:

| logical name | physical name | role |
|---|---|---|
| `Company Very Short` | `col-173b4db9-b5ad-427f-9e75-516aae37fbbb` | partition column |
| `Super Name` | `col-3877fd94-0973-4941-ac6b-646849a1ff65` | data column |

Which makes it the right referee for [#32](https://github.com/djouallah/duckrun/issues/32):
the parquet footer, `add.stats` and `add.partitionValues` are all keyed by the **physical** name
while `metaData.partitionColumns` is keyed by the **logical** one, the partition column is absent
from the parquet, and a logical name contains a space.
