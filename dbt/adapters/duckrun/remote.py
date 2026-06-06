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
