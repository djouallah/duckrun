"""Enumerate Delta tables on a remote OneLake/ADLS store for relation discovery.

DuckDB's azure extension can't reliably glob/list OneLake (``abfss://``) directories
(duckdb/duckdb-azure#174 — trailing-slash 403s — still unreleased; in practice ``glob``
returns zero rows or errors there). Glob-based discovery therefore comes back empty on
OneLake and read-only commands fail with "schema does not exist". So for ``abfss://`` we
list the schema's table directories with the ADLS Gen2 / OneLake DFS "List Paths" REST API
instead, using the same bearer token that authenticates the Delta reads/writes. Local and
``az://`` stores keep using DuckDB ``glob`` (which works there).
"""
import os
import re
from typing import List, Optional, Tuple

from .secret import bearer_token

# OneLake / ADLS Gen2 data-plane REST API version (List Paths).
_DFS_API_VERSION = "2023-11-03"

# Strict 8-4-4-4-12 hex GUID. THE one GUID shape test for both surfaces (the dbt adapter and the
# connect/remote package): the looser validators it replaced disagreed with each other (one accepted
# `1-2-3-4-5`), so the same string classified differently between the two stacks.
_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def looks_like_guid(value) -> bool:
    """True when ``value`` is a full GUID (so a OneLake segment is an id, not a friendly name)."""
    return bool(_GUID.match(str(value or "")))


class OneLakeAccessError(RuntimeError):
    """The store itself is unreachable (wrong tenant, item not in workspace, missing filesystem) —
    distinct from a genuinely-empty directory. A distinct type so callers that otherwise treat a
    listing failure as 'no tables' (the dbt adapter's best-effort discovery) can still let a real
    access failure fail loud instead of masquerading as an empty lakehouse."""


def is_abfss(root_path: Optional[str]) -> bool:
    return bool(root_path) and root_path.startswith("abfss://")


# OneLake's data-plane host — the constant half of every abfss lakehouse URL.
ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
# Fabric item-type suffixes that make a path segment unambiguously a OneLake item, not a folder.
_ITEM_SUFFIXES = (".lakehouse", ".warehouse")


def expand_onelake_shorthand(path: Optional[str]) -> Optional[str]:
    """Expand the OneLake shorthand ``<workspace>/<item>[/…]`` into a full ``abfss://`` URL.

    Two shapes qualify, and ONLY these two — everything else (a real URL, an absolute path, a bare
    two-segment relative path like ``data/lh``) is returned verbatim, so no existing path changes
    meaning:

    * the item segment carries a Fabric item suffix — ``ws/sales.Lakehouse``, ``ws/ref.Warehouse``;
    * both segments are GUIDs — ``<ws-guid>/<item-guid>`` (the form we recommend on OneLake anyway,
      since friendly names can trip delta_scan — duckdb-delta#307).

    Segments after the item are kept as-is under ``…/Tables``, so ``ws/lh.Lakehouse/dbo`` and
    ``ws/lh.Lakehouse/Tables/dbo`` are the same thing. Spell ``Files`` explicitly
    (``ws/lh.Lakehouse/Files/raw``) to address the lakehouse's file side instead — the ``Tables``
    default is only applied when the caller named neither section.

    Both entry points route through here — ``duckrun.connect``/``attach`` (via
    ``session._split_root_schema``) and the dbt profile's ``root_path`` (via
    ``DuckrunCredentials.__post_init__``) — so the two surfaces can't drift.
    """
    if not path or "://" in path:
        return path
    if path[0] in "/\\" or os.path.splitdrive(path)[0]:
        return path                                   # absolute / drive / UNC → a real local path
    parts = path.replace("\\", "/").rstrip("/").split("/")
    if len(parts) < 2 or any(p in ("", ".", "..") for p in parts):
        return path
    workspace, item, rest = parts[0], parts[1], parts[2:]
    if not (item.lower().endswith(_ITEM_SUFFIXES)
            or (_GUID.match(workspace) and _GUID.match(item))):
        return path
    section = "Tables"
    if rest and rest[0].lower() in ("tables", "files"):
        section, rest = rest[0], rest[1:]      # keep the caller's spelling; don't double it
    return "/".join([f"abfss://{workspace}@{ONELAKE_HOST}/{item}/{section}", *rest])


# A 404 from the DFS API is only benign when the *directory itself* is absent (a schema / folder not
# created yet → genuinely no tables/files). Every other 404 means the store is unreachable — the
# workspace/item isn't there or the token's tenant can't see it (TargetItemNotInWorkspace,
# FilesystemNotFound). Those MUST fail loud, not masquerade as "empty" (a silent 404 made a
# wrong-tenant token look like an empty lakehouse).
_BENIGN_404_CODES = {"PathNotFound", "SourcePathNotFound"}


def _error_code(resp) -> str:
    code = resp.headers.get("x-ms-error-code")
    if code:
        return code
    try:
        return str(resp.json().get("error", {}).get("code", ""))
    except Exception:
        return ""


def _benign_404_or_raise(resp, directory: str) -> None:
    """Return (caller then yields ``[]``) only for a genuinely-absent directory; otherwise raise a
    clear error so an auth/access failure can't hide behind an empty result."""
    code = _error_code(resp)
    if code in _BENIGN_404_CODES:
        return
    raise OneLakeAccessError(
        f"duckrun: OneLake path not accessible (HTTP {resp.status_code} {code or '?'}) at "
        f"{directory!r} — check the workspace/lakehouse name and that your token's tenant can reach "
        f"it. Server said: {resp.text[:200]}"
    )


# Transient DFS statuses worth retrying: OneLake throttles (429, usually with Retry-After) and
# returns transient 5xx. Everything else (404, 403, auth failures) propagates immediately — a single
# throttle response must not kill a `dbt test`/`docs` run.
_RETRY_STATUS = {429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 3


def _retry_delay(resp, attempt: int) -> float:
    """Seconds to wait before the next attempt: honor a numeric ``Retry-After`` if present, else
    exponential backoff (1s, 2s, …)."""
    ra = resp.headers.get("Retry-After")
    if ra and str(ra).strip().isdigit():
        return float(ra)
    return float(2 ** attempt)


def _sleep(seconds: float) -> None:
    """Indirection so tests can stub out the wait."""
    import time
    time.sleep(seconds)


def retry_request(method: str, url: str, *, headers: dict, params: Optional[dict] = None,
                  json_body: Optional[dict] = None, timeout: int = 30):
    """A bounded-retry HTTP call — THE one retry loop for every OneLake/Fabric REST call (DFS data
    plane here, Fabric control plane via ``fabric_remote``): retries transient 429/5xx (see
    ``_RETRY_STATUS``) honoring a numeric ``Retry-After``, else exponential backoff. Non-retryable
    responses (including 404) are returned as-is for the caller's own status handling; after the
    last attempt the final (still-transient) response is returned so the caller's
    ``raise_for_status`` fails loud."""
    import requests  # dbt dependency; imported lazily so non-remote paths don't need it

    # Dispatch via the module-level helpers (requests.get/head/post/…), not requests.request:
    # tests stub exactly those, and the two are behaviorally identical. `json` is only passed when
    # there IS a body, so GET/HEAD keep their original (url, params, headers, timeout) call shape.
    fn = getattr(requests, method.lower())
    kwargs = {"params": params, "headers": headers, "timeout": timeout}
    if json_body is not None:
        kwargs["json"] = json_body
    resp = None
    for attempt in range(_MAX_ATTEMPTS):
        resp = fn(url, **kwargs)
        if resp.status_code not in _RETRY_STATUS:
            return resp
        if attempt < _MAX_ATTEMPTS - 1:
            _sleep(_retry_delay(resp, attempt))
    return resp


def _dfs_request(method: str, url: str, headers: dict, params: Optional[dict] = None,
                 timeout: int = 30):
    """A DFS GET/HEAD through :func:`retry_request` (kept as the DFS-side entry point)."""
    return retry_request(method, url, headers=headers, params=params, timeout=timeout)


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
    result masquerade as "no tables". Follows ``x-ms-continuation`` so a schema with more tables
    than one DFS page (≤5,000 entries) is fully enumerated, not silently truncated.
    """
    token = bearer_token(storage_options)
    if not token:
        return []
    filesystem, host, base = _parse_abfss(root_path)
    directory = "/".join(p for p in (base, schema) if p)
    headers = {"Authorization": f"Bearer {token}", "x-ms-version": _DFS_API_VERSION}
    names: List[str] = []
    continuation: Optional[str] = None
    while True:
        params = {"resource": "filesystem", "recursive": "false", "directory": directory}
        if continuation:
            params["continuation"] = continuation
        resp = _dfs_request("GET", f"https://{host}/{filesystem}", headers, params=params, timeout=30)
        if resp.status_code == 404:
            _benign_404_or_raise(resp, directory)  # absent schema dir -> []; access failure -> raise
            return []
        resp.raise_for_status()
        for entry in resp.json().get("paths", []) or []:
            # Directories carry isDirectory == "true"; files omit the field. We only want the
            # immediate table directories under the schema.
            if str(entry.get("isDirectory", "")).lower() != "true":
                continue
            name = str(entry.get("name", "")).rstrip("/").rsplit("/", 1)[-1]
            if name:
                names.append(name)
        continuation = resp.headers.get("x-ms-continuation")
        if not continuation:
            return names


def list_files(dir_url: str, storage_options) -> List[str]:
    """Recursively list FILE paths (not directories) under a OneLake/ADLS directory, as full
    ``abfss://`` URLs. DuckDB's azure extension can't glob OneLake (duckdb-azure#174), so file
    transfer (``conn.download``) enumerates via the DFS REST API instead — same mechanism as
    :func:`list_delta_tables`. Returns ``[]`` if the directory doesn't exist yet (HTTP 404) or
    there's no token; raises if the store is unreachable (wrong tenant / item not found). Follows
    ``x-ms-continuation`` so a directory with more files than one DFS page is fully enumerated —
    a truncated list here would silently drop files from a download / the append_if_unchanged ingest."""
    token = bearer_token(storage_options)
    if not token:
        return []
    filesystem, host, directory = _parse_abfss(dir_url)
    headers = {"Authorization": f"Bearer {token}", "x-ms-version": _DFS_API_VERSION}
    files: List[str] = []
    continuation: Optional[str] = None
    while True:
        params = {"resource": "filesystem", "recursive": "true", "directory": directory}
        if continuation:
            params["continuation"] = continuation
        resp = _dfs_request("GET", f"https://{host}/{filesystem}", headers, params=params, timeout=60)
        if resp.status_code == 404:
            _benign_404_or_raise(resp, directory)  # absent folder -> []; access failure -> raise
            return []
        resp.raise_for_status()
        for entry in resp.json().get("paths", []) or []:
            if str(entry.get("isDirectory", "")).lower() == "true":
                continue
            name = str(entry.get("name", "")).lstrip("/")  # full path within the filesystem
            if name:
                files.append(f"abfss://{filesystem}@{host}/{name}")
        continuation = resp.headers.get("x-ms-continuation")
        if not continuation:
            return files


def has_delta_log(table_url: str, storage_options) -> bool:
    """True if the directory at ``table_url`` contains a ``_delta_log`` — i.e. it really IS a Delta
    table. Discovery uses this to tell a non-Delta directory (skip it, don't error) from a genuine
    table that merely fails to read (fail loud — the delta-kernel #307 case). Same REST mechanism as
    :func:`list_delta_tables`, so a table found via REST is always confirmable via REST."""
    token = bearer_token(storage_options)
    if not token:
        return False
    filesystem, host, base = _parse_abfss(table_url)
    resp = _dfs_request(
        "GET", f"https://{host}/{filesystem}",
        {"Authorization": f"Bearer {token}", "x-ms-version": _DFS_API_VERSION},
        params={"resource": "filesystem", "recursive": "false",
                "directory": f"{base}/_delta_log", "maxResults": 1},
        timeout=30,
    )
    if resp.status_code == 404:
        return False  # no _delta_log → not a Delta table
    resp.raise_for_status()
    return bool(resp.json().get("paths", []))


def file_exists(file_url: str, storage_options) -> bool:
    """True if a single file exists on a OneLake/ADLS store (a HEAD on the DFS path). Used by the
    ``overwrite=False`` guard in ``conn.copy`` — DuckDB glob can't be used on OneLake."""
    token = bearer_token(storage_options)
    if not token:
        return False
    filesystem, host, path = _parse_abfss(file_url)
    resp = _dfs_request(
        "HEAD", f"https://{host}/{filesystem}/{path}",
        {"Authorization": f"Bearer {token}", "x-ms-version": _DFS_API_VERSION},
        timeout=30,
    )
    return resp.status_code == 200
