"""Storage-neutral loose-file transfer for ``conn.copy`` / ``conn.download`` / ``conn.list_files``.

Every backend (local / s3 / gcs / az / OneLake ``abfss://``) goes through **obstore** — the Python
binding over the same Rust ``object_store`` crate that ``deltalake`` uses. That shared lineage is the
whole point: the ``storage_options`` dict duckrun already forwards to delta-rs for Delta writes *is*
an ``object_store`` config, so it builds an obstore store verbatim — no per-backend auth code, no
DuckDB Azure secret, no OneLake DFS REST enumeration (obstore lists OneLake natively).

Uploads stream from an open file handle, so obstore does a multipart PUT for large files instead of
materializing the whole file in memory — the fix for the old ``read_blob``/``COPY … (FORMAT BLOB)``
path, which held each file as one in-RAM value and OOM'd on multi-GB blobs. Transfers are
byte-verbatim: obstore never re-encodes by extension, so a ``.gz``/``.zst`` file is copied as-is.
"""
import os
from typing import Dict, List, Optional

from .secret import bearer_token


def build_store(base_url: str, storage_options: Optional[Dict[str, str]]):
    """An obstore store rooted at ``base_url`` — keys passed to the transfer helpers are relative to
    it. A bare path (no ``://``) is a local directory (created if absent) served by ``LocalStore``;
    anything with a scheme is dispatched by ``obstore.store.from_url`` to the matching backend
    (S3/GCS/Azure), configured from ``storage_options``. For OneLake/Azure the bearer token — under
    any of duckrun's accepted aliases — is normalized to obstore's ``bearer_token`` config key; the
    alias keys are dropped so obstore doesn't reject them as unknown."""
    import obstore.store as st

    if "://" not in base_url:
        os.makedirs(base_url, exist_ok=True)
        return st.LocalStore(prefix=base_url)
    config = {k: v for k, v in (storage_options or {}).items()
              if k not in ("token", "access_token", "bearer_token")}
    tok = bearer_token(storage_options)
    if tok:
        config["bearer_token"] = tok
    return st.from_url(base_url, config=config or None)


def exists(store, key: str) -> bool:
    """True if ``key`` is present in ``store`` (a HEAD). Backs the ``overwrite=False`` skip."""
    import obstore

    try:
        obstore.head(store, key)
        return True
    except FileNotFoundError:
        return False


def list_keys(store) -> List[str]:
    """Every object key under ``store``, recursively, as ``/``-separated paths relative to its root
    (e.g. ``['a.csv', 'sub/b.parquet']``)."""
    import obstore

    keys: List[str] = []
    for batch in obstore.list(store):
        for obj in batch:
            keys.append(obj["path"])
    return keys


def upload(store, key: str, local_path: str) -> None:
    """Stream ``local_path`` to ``key`` in ``store``. The open file handle lets obstore do a
    multipart PUT for large files rather than buffering the whole file in memory."""
    import obstore

    with open(local_path, "rb") as f:
        obstore.put(store, key, f)


def download(store, key: str, local_path: str) -> None:
    """Stream ``key`` from ``store`` to ``local_path`` (parent dirs created), chunk by chunk so a
    large file never lands in memory whole."""
    import obstore

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    result = obstore.get(store, key)
    with open(local_path, "wb") as f:
        for chunk in result.stream():
            f.write(chunk)
