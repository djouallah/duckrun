# API reference

<!-- CONNECTION_API:START -->

## duckrun connection API — supported methods

✅ **11 public methods** · 152/152 tests passing

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
