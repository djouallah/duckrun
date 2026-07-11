# API reference

<!-- CONNECTION_API:START -->

## duckrun connection API — supported methods

✅ **11 public methods** · 56/56 tests passing

> Introspected from the shipped classes — the exact public surface of `duckrun.connect()`, signatures and all, not a hand-maintained list. The green suite ([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works. `conn.sql()` also routes raw Delta DML — see the DML matrix on the [Connection API](connection-api.md) page.

| Surface | Method | Parameters |
| --- | --- | --- |
| `duckrun` | `connect` | `path, storage_options=None, schema=None, read_only=True, name=None` |
| `DuckSession` | `attach` | `path, name=None, storage_options=None, schema=None, read_only=None` |
| `DuckSession` | `close` | *(none)* |
| `DuckSession` | `convert_to_delta` | `identifier, partition_schema=None` |
| `DuckSession` | `copy` | `local_folder, remote_folder, file_extensions=None, overwrite=False` |
| `DuckSession` | `download` | `remote_folder='', local_folder='./downloaded_files', file_extensions=None, overwrite=False` |
| `DuckSession` | `get_stats` | `source=None, detailed=False` |
| `DuckSession` | `list_files` | `remote_folder='', file_extensions=None` |
| `DuckSession` | `refresh` | `quiet=False, catalog=None` |
| `DuckSession` | `register` | `name, obj` |
| `DuckSession` | `sql` | `query` |

<!-- CONNECTION_API:END -->

## Delta SQL extensions (not in DuckDB)

Everything you run through `conn.sql()` is standard DuckDB SQL — reads, CTEs, `SHOW`/`DESCRIBE`,
`CREATE TEMP`/`CREATE VIEW`, and the write DML (`CREATE TABLE … AS`, `INSERT`, `UPDATE`, `DELETE`,
`MERGE`, `ALTER TABLE`, `DROP TABLE`) are all written in ordinary DuckDB syntax; duckrun only routes
the writes to delta-rs so they land on the Delta table (see the
[DML matrix](connection-api.md#raw-sql-dml-through-connsql)).

The forms below are the **only** places where the SQL is *not* vanilla DuckDB — new verbs and clauses
DuckDB has no syntax for (Spark/Delta spellings). A read-only session refuses the ones that write.

| Extension | What it does | Writes? |
|---|---|---|
| `CREATE [OR REPLACE] TABLE <t> SORTED BY AUTO AS <query>` | duckrun profiles the query and picks a run-length-friendly clustering key for you (a heuristic, not an optimizer). Only the `AUTO` keyword is the extension — `SORTED BY (cols)` and `PARTITIONED BY (cols)` are DuckDB's own CTAS syntax. See [Automatic sorting](parquet-layout.md#automatic-sorting). | ✅ |
| `INSERT INTO <t> REPLACE WHERE <pred> SELECT …` | delta_rs `replaceWhere` — atomically overwrite **only** the rows matching `<pred>` with the SELECT's rows, in one fenced commit (no torn delete-then-append window). `<pred>` is a CAST-free expression over the target's columns. | ✅ |
| `INSERT WITH SCHEMA EVOLUTION INTO <t> SELECT …` | append that **widens** the table with the source's new columns (existing rows → `NULL`) instead of dropping them — delta_rs `schema_mode='merge'`. | ✅ |
| `RESTORE TABLE <t> TO VERSION AS OF <n>` <br> `RESTORE TABLE <t> TO TIMESTAMP AS OF '…'` | delta_rs `restore` — roll the table back to an earlier version/timestamp. It's a new commit on top of history, so the restore is itself revertible. | ✅ |
| `DESCRIBE DETAIL <table>` | the Delta table's `location` (storage path), `partitionColumns`, `numFiles`, `sizeInBytes`, `version` — read from the Delta log. Plain `DESCRIBE <table>` stays DuckDB's column view. | — |
| `DESCRIBE HISTORY <table>` | one row per Delta commit (`version`, `timestamp`, `operation`, `operationMetrics`), newest first. | — |

**Not extensions** — these are legit DuckDB verbs/syntax; duckrun only changes what they do against a
Delta table, without adding any new spelling:

- `VACUUM <table>` — DuckDB's own `VACUUM` verb, repurposed to compact small files and vacuum files
  tombstoned past the retention window (compaction also runs automatically after every write; this is
  the manual button). Bare `VACUUM` with no table stays DuckDB's stats no-op.
- `SORTED BY (cols)` / `PARTITIONED BY (cols)` — DuckDB's native CTAS layout clauses, applied to the
  Delta write.
- **Time travel** — DuckDB's own `delta_scan('<location>', version => N)`. Get `<location>` from
  `DESCRIBE DETAIL` and the versions from `DESCRIBE HISTORY`.

> **Portability:** everything above except the extensions table is plain DuckDB SQL — the same query
> runs on stock DuckDB. The full write-DML behaviour is pinned statement-by-statement in
> [`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py).
