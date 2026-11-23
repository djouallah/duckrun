# Connection API (notebook)

`duckrun.connect()` is a storage-neutral, SQL-first session over Delta tables (local, S3, GCS, ADLS, OneLake):

- `conn.sql(...)` — DuckDB SQL over the discovered Delta tables, returning DuckDB's native relation (`.show()`, `.df()`, `.arrow()`, `.pl()`, `.fetchall()`, `.filter()`, …). Reads pass straight through, including time travel (`delta_scan('…', version => N)`). Raw DML (`create table … as`, `insert`, `update`, `delete`, `alter`, `drop`, `merge`) is applied to the Delta table via delta-rs — see the [DML matrix](#raw-sql-dml-through-connsql).
- **Multiple catalogs**: `connect()` binds one lakehouse root; `conn.attach(path, name=…)` binds more, addressed as `catalog.schema.table` ([below](#multiple-catalogs-with-connattach)).
- **Files**: `conn.copy()`, `conn.download()`, `conn.list_files()` for loose files on any store ([below](#files-conncopy-conndownload-connlist_files)).
- **Inspection**: `conn.get_stats(source=None, detailed=False)` — one row per table (`total_rows`, `num_files`, `num_row_groups`, `avg_row_group`, `size_mb`, `vorder`, `compression`), or one row per parquet row group with `detailed=True`; `source` is a table, a schema, or a wildcard like `mart.fct_*`.
- **Ingest**: `conn.convert_to_delta("parquet.`<path>`")` writes a `_delta_log` over an existing parquet directory without rewriting it; `conn.refresh()` then surfaces it.
- **Session**: `conn.refresh(catalog=None)` re-discovers tables written out-of-band; `conn.close()` (or `with duckrun.connect(...) as conn:`) closes the DuckDB connection.
- **Iceberg**: `connect(path, format="iceberg")` / `attach(..., format="iceberg")` open a Fabric Lakehouse's Iceberg REST catalog instead of its Delta tables. DuckDB is the whole engine there — it lists, reads and writes (`CREATE TABLE AS`, `INSERT`, `DROP`) — so none of the Delta routing below applies, and `read_only` is DuckDB's own `ATTACH … (READ_ONLY)`.

`connect()` is **read-only by default**: every Delta write raises `PermissionError`. Pass `read_only=False` to enable writes; reads and native `CREATE TEMP` / `CREATE VIEW` scratch are always allowed.

`MERGE`, `UPDATE` and `DELETE` are **snapshot-pinned**: the target version is captured and the commit validates against it, so a concurrent writer fails loud (`CommitFailedError`). A read-modify-append on the same table (`INSERT INTO a SELECT … FROM a`) is fenced the same way; a plain append of new data is not. See [Snapshot isolation](snapshot-isolation.md).

```python
import duckrun
conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo",
                       read_only=False)
conn.sql("CREATE OR REPLACE TABLE orders_copy AS SELECT * FROM orders")
conn.sql("SELECT * FROM orders_copy").show()
```

The exact method list is in the [API reference](api-reference.md).

### OneLake shorthand

On OneLake, pass `workspace/item[/schema]` instead of the `abfss://` URL:

```python
duckrun.connect("ws/lh.Lakehouse")                    # → abfss://ws@…/lh.Lakehouse/Tables
duckrun.connect("ws/lh.Lakehouse/dbo")                # …/Tables/dbo  (catalog "lh")
duckrun.connect("<workspace-guid>/<lakehouse-guid>")  # catalog "data" (pass name= to rename)
```

Only two shapes are shorthand: an item with a `.Lakehouse` / `.Warehouse` suffix, or a workspace-GUID/item-GUID pair; a suffix-less `ws/lh` stays an ordinary local path. Spell `Files` to address the file side (`ws/lh.Lakehouse/Files/raw`); `Tables` is the default. `conn.attach()` and the dbt profile's `root_path` take the same shorthand. Prefer the GUID form on OneLake: friendly names can trip a `delta_scan` bug (duckdb-delta#307).

## It's just DuckDB SQL

Everything through `conn.sql()` is standard DuckDB SQL, parsed and executed by DuckDB — reads, CTEs, `SHOW` / `DESCRIBE`, `CREATE TEMP` / `CREATE VIEW`. Write DML (`CREATE TABLE … AS`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `ALTER TABLE … ADD/DROP/RENAME COLUMN`, `DROP TABLE`) keeps DuckDB's syntax but is routed to delta-rs. The only non-DuckDB spellings are these Delta extensions; a read-only session refuses the ones that write:

| Extension | What it does |
|---|---|
| `CREATE TABLE … SORTED BY AUTO AS …` | duckrun profiles the data and picks the sort key. `SORTED BY (cols)` and `PARTITIONED BY (cols)` without `AUTO` are DuckDB's own CTAS syntax. See [Automatic sorting](parquet-layout.md#automatic-sorting). |
| `VACUUM <table>` | DuckDB's verb, repurposed to compact small files and vacuum tombstoned files past retention (also runs automatically after writes). Bare `VACUUM` stays DuckDB's no-op. |
| `INSERT INTO <t> REPLACE WHERE <pred> SELECT …` | delta-rs `replaceWhere`: atomically overwrite only the rows matching `<pred>`, in one fenced commit. `<pred>` is a CAST-free expression over the target's columns. |
| `INSERT WITH SCHEMA EVOLUTION INTO <t> SELECT …` | append that widens the table with the source's new columns (existing rows → `NULL`) — delta-rs `schema_mode='merge'`. |
| `DESCRIBE DETAIL <table>` | one row: `format`, `id`, `name`, `location`, `partitionColumns`, `numFiles`, `sizeInBytes`, `version`, from the Delta log. Plain `DESCRIBE <table>` stays DuckDB's column view. |
| `DESCRIBE HISTORY <table>` | one row per commit (`version`, `timestamp`, `operation`, `operationMetrics`), newest first. |
| `RESTORE TABLE <t> TO VERSION AS OF <n>` / `TO TIMESTAMP AS OF '…'` | delta-rs `restore` — a new commit on top of history, itself revertible. |

Time travel is DuckDB's own `delta_scan('<location>', version => N)`; get `<location>` from `DESCRIBE DETAIL` and the versions from `DESCRIBE HISTORY`.

## In-memory data — `conn.register`

Register a pandas / polars / pyarrow object (or a DuckDB relation) under a name, then query it. Registration is explicit because DuckDB's replacement scan sees only the immediate calling frame, not yours.

```python
import pandas as pd
df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

conn.register("df", df)
conn.sql("SELECT * FROM df").df()
conn.sql("CREATE OR REPLACE TABLE seeded AS SELECT * FROM df")   # persist to Delta
```

## Files — `conn.copy` / `conn.download` / `conn.list_files`

`conn.copy(local_folder, remote_folder)` uploads a directory tree, `conn.download(remote_folder, local_folder)` pulls one back, `conn.list_files(remote_folder)` lists relative paths. `remote_folder` is relative to the lakehouse **Files** section on OneLake (the catalog root elsewhere), or a full `…://` URL. Everything streams through obstore over the credentials `connect()` holds; `file_extensions` filters by suffix and `overwrite=False` (default) skips files already present.

Two opt-in flags make `copy` a deployment tool for a dbt project that runs inside Fabric (the project lives in `Files/<folder>`, a notebook is only the runner):

- `git_only=True` uploads only the files git tracks — `git ls-files` run inside `local_folder` — so `dbt_packages/`, `target/`, caches and ignored local secrets never leave the machine. It is exactly git's index: a new file must be `git add`ed to ship. Outside a git checkout it falls back to the full directory walk with a warning.
- `sync=True` removes remote files that no longer exist locally, as a per-file diff scoped to `remote_folder` and to the same `file_extensions` filter — never a folder wipe. Uploads run first and deletes last, and an empty local set is refused rather than treated as "delete everything".

```python
conn.copy("dbt", "dbt", git_only=True, sync=True, overwrite=True)
```

`sync` decides which files exist remotely; `overwrite` decides whether an existing file's content is replaced. A deploy that expects edited models to land needs both.

## Multiple catalogs with `conn.attach`

`connect()` binds the primary catalog; `conn.attach(path, name=…)` binds more, so one session reads, joins and writes across lakehouses by three-part name. A Fabric **Warehouse** is Delta in OneLake too, but locked to writes, so attach it `read_only=True`.

```python
conn = duckrun.connect("ws/lakehouse.Lakehouse", read_only=False, name="lakehouse")
conn.attach("ws/warehouse.Warehouse", name="warehouse", schema="mart", read_only=True)

conn.sql("""
    CREATE OR REPLACE TABLE lakehouse.dbo.mart_generation_by_state AS
    SELECT d.state, sum(f.mw) AS total_mw
    FROM warehouse.mart.fct_summary f
    JOIN mart.dim_duid d ON d.duid = f.duid
    GROUP BY d.state
""")

conn.sql("CREATE TABLE warehouse.mart.nope AS SELECT 1 AS x")   # -> PermissionError
```

A table can be addressed 3-part from anywhere, 2-part in the current catalog, or bare in the current catalog and schema. `name` is derived from a friendly path and mandatory for a GUID-only path; one URL maps to one name. `read_only` is per catalog. Attached catalogs are visible to `SHOW DATABASES`, `information_schema` and `duckdb_databases()`. Runnable walkthrough: [`demo_multicatalog.py`](../tests/integration_tests/multicatalog/demo_multicatalog.py) ([live report](https://djouallah.github.io/duckrun/multicatalog.html)).

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

```python
# sort the write by a lexicographic key (no z-order: bit-interleaving destroys run-length runs)
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) AS SELECT * FROM stg_sales")

# Hive-partitioned Delta (delta-rs writes col=value/ folders, strips the column from the data files)
conn.sql("CREATE OR REPLACE TABLE sales PARTITIONED BY (region) AS SELECT * FROM stg_sales")

# both compose — partition columns should lead the sort so delta-rs keeps ~one writer open
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY (region, order_date) PARTITIONED BY (region) "
         "AS SELECT * FROM stg_sales")

# let duckrun pick the key
conn.sql("CREATE OR REPLACE TABLE sales SORTED BY AUTO AS SELECT * FROM stg_sales")
```

`SORTED BY (cols)` and `PARTITIONED BY (cols)` are DuckDB's native syntax; `SORTED BY AUTO` is duckrun's. A plain `… AS SELECT … ORDER BY …` also sorts the write. Details in [Parquet layout](parquet-layout.md).

> **Gap:** changing a column's *type* has no SQL surface — delta-rs schema evolution is add-only, so `ADD` / `DROP` / `RENAME COLUMN` work and a type change does not.
