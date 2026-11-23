"""Storage-neutral loose-file transfer for ``conn.copy`` (incl. its ``sync`` deletes) /
``conn.download`` / ``conn.list_files``.

Every backend (local / s3 / gcs / az / OneLake ``abfss://``) goes through **obstore** — the Python
binding over the same Rust ``object_store`` crate that ``deltalake`` uses. That shared lineage is the
whole point: the ``storage_options`` dict duckrun already forwards to delta-rs for Delta writes *is*
an ``object_store`` config, so it builds an obstore store verbatim — no per-backend auth code, no
DuckDB Azure secret, no OneLake DFS REST enumeration (obstore lists OneLake natively). The one
exception is a local folder, which :func:`delete_tree` removes with ``shutil.rmtree``.

Uploads stream from an open file handle, so obstore does a multipart PUT for large files instead of
materializing the whole file in memory — the fix for the old ``read_blob``/``COPY … (FORMAT BLOB)``
path, which held each file as one in-RAM value and OOM'd on multi-GB blobs. Transfers are
byte-verbatim: obstore never re-encodes by extension, so a ``.gz``/``.zst`` file is copied as-is.
"""
import os
from typing import Dict, List, Optional

from .secret import TOKEN_KEYS, bearer_token


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
              if k not in TOKEN_KEYS}
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


def delete(store, key: str, base_url: str = "", storage_options: Optional[Dict[str, str]] = None) -> None:
    """Delete the single object ``key`` from ``store`` (every backend, OneLake included: OneLake now
    accepts object_store's Azure delete, which it used to reject — arrow-rs object_store #701). A
    missing key raises ``FileNotFoundError``; the caller only deletes keys it listed a moment
    earlier, so a miss means another writer got there first — let it surface. ``base_url`` and
    ``storage_options`` are unused and kept for the callers' signature."""
    import obstore

    obstore.delete(store, key)


def delete_tree(base_url: str, storage_options: Optional[Dict[str, str]] = None) -> None:
    """Delete everything under ``base_url`` (a table folder), and the folder itself.

    * local: ``shutil.rmtree``;
    * s3 / gcs / Azure / OneLake: list the prefix and delete every key with obstore (batched). On
      OneLake that is not the end: it keeps real folders, so the table folder, ``_delta_log/`` and
      the partition folders would stay behind, empty — and Fabric shows a folder under ``Tables/``
      without a Delta log as an "Unidentified" table. So the folders the keys sat in are deleted
      too, deepest first, then the table folder. They come from the key paths themselves, because
      object_store's listing skips folder entries. OneLake keeps deleted files for 7 days (soft
      delete), restorable with Azure Storage Explorer or the Blob ``Undelete`` API.

    A folder that is already gone is not an error."""
    if "://" not in base_url:
        import shutil

        if os.path.exists(base_url):
            shutil.rmtree(base_url)
        return
    import obstore
    from . import remote

    store = build_store(base_url, storage_options)
    keys = list_keys(store)
    for i in range(0, len(keys), 1000):
        obstore.delete(store, keys[i:i + 1000])
    if not remote.is_abfss(base_url):
        return
    folders = {k.rsplit("/", 1)[0] for k in keys if "/" in k}
    while True:                       # every ancestor folder, not just each key's direct parent
        more = {f.rsplit("/", 1)[0] for f in folders if "/" in f} - folders
        if not more:
            break
        folders |= more
    parent, name = base_url.rstrip("/").rsplit("/", 1)
    for folder in sorted(folders, key=lambda f: f.count("/"), reverse=True) + [None]:
        try:
            if folder is None:
                obstore.delete(build_store(parent, storage_options), name)
            else:
                obstore.delete(store, folder)
        except FileNotFoundError:
            pass


def list_keys(store) -> List[str]:
    """Every object key under ``store``, recursively, as ``/``-separated paths relative to its root
    (e.g. ``['a.csv', 'sub/b.parquet']``)."""
    import obstore

    keys: List[str] = []
    for batch in obstore.list(store):
        for obj in batch:
            keys.append(obj["path"])
    return keys


def upload(store, key: str, local_path: str, single_shot: bool = False) -> None:
    """Stream ``local_path`` to ``key`` in ``store``. The open file handle lets obstore do a
    multipart PUT for large files rather than buffering the whole file in memory.

    ``single_shot=True`` forces one non-multipart PUT (obstore ``use_multipart=False``) — the whole
    file is buffered in memory and written as a single request. This is the OneLake **overwrite**
    path. obstore replaces happily by default (``put`` is ``mode="overwrite"``), but on OneLake
    the default multipart commit stages blocks (``Put Block``) that OneLake rejects over an
    already-committed blob (409 ``BlobOperationNotSupported``). A single ``Put Blob``, which OneLake
    honors as an atomic replace, is the path that overwrites there."""
    import obstore

    with open(local_path, "rb") as f:
        if single_shot:
            obstore.put(store, key, f, use_multipart=False)
        else:
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
