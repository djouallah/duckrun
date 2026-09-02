# API reference

<!-- CONNECTION_API:START -->

## duckrun connection API — supported methods

✅ **11 public methods** · 67/67 tests passing

> Introspected from the shipped classes — the exact public surface of `duckrun.connect()`, signatures and all, not a hand-maintained list. The green suite ([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works. `conn.sql()` also routes raw Delta DML — see the DML matrix on the [Connection API](connection-api.md) page.

| Surface | Method | Parameters |
| --- | --- | --- |
| `duckrun` | `connect` | `path, storage_options=None, schema=None, read_only=True, name=None, format='delta'` |
| `DuckSession` | `attach` | `path, name=None, storage_options=None, schema=None, read_only=None, format='delta'` |
| `DuckSession` | `close` | *(none)* |
| `DuckSession` | `convert_to_delta` | `identifier, partition_schema=None` |
| `DuckSession` | `copy` | `local_folder, remote_folder, file_extensions=None, overwrite=False, git_only=False, sync=False` |
| `DuckSession` | `download` | `remote_folder='', local_folder='./downloaded_files', file_extensions=None, overwrite=False` |
| `DuckSession` | `get_stats` | `source=None, detailed=False` |
| `DuckSession` | `list_files` | `remote_folder='', file_extensions=None` |
| `DuckSession` | `refresh` | `quiet=False, catalog=None` |
| `DuckSession` | `register` | `name, obj` |
| `DuckSession` | `sql` | `query` |

<!-- CONNECTION_API:END -->

<!-- WORKSPACE_API:START -->

## duckrun workspace API — Fabric artifact deploy

🗂️ **16 public methods** · deploy · run · schedule

> Introspected from the shipped classes — the exact public surface of `duckrun.workspace()` and the `Workspace` handle it returns, signatures and all, not a hand-maintained list. It drives Microsoft Fabric (create lakehouses, deploy notebooks / semantic models / pipelines / variable libraries, run and schedule them) — see the [Workspace (Fabric)](workspace.md) page. Exercised by the manual deploy demo ([`tests/deploy_testing`](../tests/deploy_testing)).

| Surface | Method | Parameters |
| --- | --- | --- |
| `duckrun` | `workspace` | `workspace, token=None` |
| `Workspace` | `create_lakehouse` | `name, schemas=True, folder=None` |
| `Workspace` | `create_warehouse` | `name, folder=None` |
| `Workspace` | `deploy` | `source, lakehouse=None, variables=None, name=None, overwrite=False, notebook=None, warehouse=None, folder=None, mode=None` |
| `Workspace` | `display_name` | *property* |
| `Workspace` | `download` | `folder='.', name=None, overwrite=False` |
| `Workspace` | `id` | *accessor* |
| `Workspace` | `lakehouse_id` | `name` |
| `Workspace` | `list_items` | `kind=None` |
| `Workspace` | `list_lakehouses` | *(none)* |
| `Workspace` | `name` | *accessor* |
| `Workspace` | `run` | `name` |
| `Workspace` | `run_python` | `script, *, lakehouse=None, args=None, env=None, cores=None, pip=None, setup=None, entry=None, name=None, attempts=3, keep_notebook=False` |
| `Workspace` | `schedule` | `name, every=None, daily=None, weekly=None, at=None, tz='UTC'` |
| `Workspace` | `sql_endpoint` | `warehouse=None` |
| `Workspace` | `warehouse_id` | `name` |

<!-- WORKSPACE_API:END -->

## Delta SQL extensions

Everything through `conn.sql()` is DuckDB SQL; duckrun only routes the write DML to delta-rs. The
few Delta-specific spellings DuckDB has no syntax for — `SORTED BY AUTO`, `REPLACE WHERE`,
`WITH SCHEMA EVOLUTION`, `DESCRIBE DETAIL` / `HISTORY`, `RESTORE TABLE` — and the repurposed `VACUUM`
are listed once, on the [Connection API](connection-api.md#its-just-duckdb-sql) page; the write-DML
matrix is [there too](connection-api.md#raw-sql-dml-through-connsql). The behaviour is pinned
statement-by-statement in [`tests/connection_api/test_connection_api.py`](../tests/connection_api/test_connection_api.py).
