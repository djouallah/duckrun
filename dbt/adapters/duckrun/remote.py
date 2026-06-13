"""Enumerate Delta tables on a remote OneLake/ADLS store for relation discovery.

DuckDB's azure extension can't reliably glob/list OneLake (``abfss://``) directories
(duckdb/duckdb-azure#174 — trailing-slash 403s — still unreleased; in practice ``glob``
returns zero rows or errors there). Glob-based discovery therefore comes back empty on
OneLake and read-only commands fail with "schema does not exist". So for ``abfss://`` we
list the schema's table directories with the ADLS Gen2 / OneLake DFS "List Paths" REST API
instead, using the same bearer token that authenticates the Delta reads/writes. Local and
``az://`` stores keep using DuckDB ``glob`` (which works there).
"""
from typing import List, Optional, Tuple

from .secret import bearer_token

# OneLake / ADLS Gen2 data-plane REST API version (List Paths).
_DFS_API_VERSION = "2023-11-03"


def is_abfss(root_path: Optional[str]) -> bool:
    return bool(root_path) and root_path.startswith("abfss://")


def _parse_abfss(root_path: str) -> Tuple[str, str, str]:
    """``abfss://<filesystem>@<host>/<path...>`` -> (filesystem, host, path)."""
    rest = root_path[len("abfss://"):]
    fs_host, _, path = rest.partition("/")
    filesystem, _, host = fs_host.partition("@")
    return filesystem, host, path.strip("/")


def list_delta_tables_via_glob(cursor, root_path: str, schema: str) -> List[str]:
    """Table names under ``<root_path>/<schema>`` on a local / az:// / s3:// / gs:// store, via
    DuckDB ``glob``. ``cursor`` is any live DuckDB cursor/connection that can run ``execute`` —
    the caller is responsible for having minted whatever store secret the glob needs first
    (az://, s3, gcs). OneLake (``abfss://``) can't be globbed; use ``list_delta_tables`` there.

    Returns ``[]`` if nothing matches or the glob errors (e.g. the schema dir doesn't exist yet)."""
    base = root_path.rstrip("/") + "/" + str(schema).strip('"')
    # `*` matches one path segment (the table dir); every committed Delta table has at least one
    # commit json (00..0.json is unreliable after cleanup_metadata()).
    pattern = (base + "/*/_delta_log/*.json").replace("'", "''")
    try:
        rows = cursor.execute(f"SELECT DISTINCT file FROM glob('{pattern}')").fetchall()
    except Exception:  # missing dir / unsupported store -> no tables (caller may log)
        return []

    marker = "/_delta_log/"
    names: List[str] = []
    for (file_path,) in rows:
        # glob returns OS-native separators (backslashes on Windows); normalize so the marker
        # match and table-name split work regardless of platform / store.
        fp = file_path.replace("\\", "/")
        idx = fp.find(marker)
        if idx == -1:
            continue
        name = fp[:idx].rsplit("/", 1)[-1]
        if name and name not in names:
            names.append(name)
    return names


def list_delta_tables(root_path: str, schema: str, storage_options) -> List[str]:
    """Immediate sub-directory names under ``<root_path>/<schema>`` on a OneLake/ADLS store
    — each a candidate Delta table. Requires a bearer token in ``storage_options``.

    Returns ``[]`` if the schema directory doesn't exist yet (HTTP 404) or there's no token.
    Raises on any other transport/HTTP error so discovery can log it rather than let an empty
    result masquerade as "no tables".
    """
    import requests  # dbt dependency; imported lazily so non-remote paths don't need it

    token = bearer_token(storage_options)
    if not token:
        return []
    filesystem, host, base = _parse_abfss(root_path)
    directory = "/".join(p for p in (base, schema) if p)
    resp = requests.get(
        f"https://{host}/{filesystem}",
        params={"resource": "filesystem", "recursive": "false", "directory": directory},
        headers={"Authorization": f"Bearer {token}", "x-ms-version": _DFS_API_VERSION},
        timeout=30,
    )
    if resp.status_code == 404:
        return []  # schema directory not created yet -> no tables
    resp.raise_for_status()
    names = []
    for entry in resp.json().get("paths", []) or []:
        # Directories carry isDirectory == "true"; files omit the field. We only want the
        # immediate table directories under the schema.
        if str(entry.get("isDirectory", "")).lower() != "true":
            continue
        name = str(entry.get("name", "")).rstrip("/").rsplit("/", 1)[-1]
        if name:
            names.append(name)
    return names
