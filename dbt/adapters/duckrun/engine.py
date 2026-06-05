"""
Delta Lake write engine for the duckrun dbt adapter.

Ported from the original duckrun ``writer.py`` / ``runner.py``: DuckDB produces an
Arrow record batch, ``deltalake`` (delta_rs) materializes it as a Delta table, and the
table is maintained (vacuum / compact / cleanup). The version-aware argument builder is
kept verbatim so the row-group sizing that matters for Power BI / DirectLake is preserved.
"""
from typing import Any, Dict, List, Optional

from deltalake import DeltaTable, write_deltalake, __version__ as deltalake_version

# Try to import WriterProperties for Rust engine (available in 0.18.2+)
try:
    from deltalake.writer import WriterProperties
    _HAS_WRITER_PROPERTIES = True
except ImportError:
    _HAS_WRITER_PROPERTIES = False

# Try to import PyArrow dataset for old PyArrow engine
try:
    import pyarrow.dataset as ds
    _HAS_PYARROW_DATASET = True
except ImportError:
    _HAS_PYARROW_DATASET = False


# Row Group configuration for optimal Delta Lake performance (Power BI / DirectLake)
RG = 8_000_000

# Version 0.18.x / 0.19.x support the ``engine`` param and row-group optimization.
# Version 0.20+ removed these (rust only, no row groups).
_DELTALAKE_VERSION = tuple(map(int, deltalake_version.split(".")[:2]))
_IS_OLD_DELTALAKE = _DELTALAKE_VERSION < (0, 20)


def build_write_deltalake_args(
    path: str,
    data,
    mode: str,
    schema_mode: Optional[str] = None,
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build kwargs for ``write_deltalake`` based on the installed deltalake version.

    deltalake 0.18.2 - 0.19.x:
      - has ``engine`` param (defaults to 'pyarrow') and row-group params
      - mergeSchema -> schema_mode='merge' + engine='rust', no row-group params
      - otherwise -> row-group params (pyarrow default)
      - compression ZSTD via writer_properties (rust) or file_options (pyarrow)

    deltalake 0.20+:
      - rust only, no ``engine`` param, no row-group params
      - mergeSchema -> schema_mode='merge'
      - compression ZSTD via writer_properties
    """
    args: Dict[str, Any] = {
        "table_or_uri": path,
        "data": data,
        "mode": mode,
    }

    if partition_by:
        args["partition_by"] = partition_by

    if storage_options:
        args["storage_options"] = storage_options

    if schema_mode == "merge":
        args["schema_mode"] = "merge"
        if _IS_OLD_DELTALAKE:
            # 0.18.2-0.19.x: must also set engine='rust' for schema merging.
            args["engine"] = "rust"
            if _HAS_WRITER_PROPERTIES:
                args["writer_properties"] = WriterProperties(compression="ZSTD")
        else:
            if _HAS_WRITER_PROPERTIES:
                args["writer_properties"] = WriterProperties(compression="ZSTD")
    else:
        if _IS_OLD_DELTALAKE:
            # 0.18.2-0.19.x: pyarrow default + row-group optimization.
            args["max_rows_per_file"] = RG
            args["max_rows_per_group"] = RG
            args["min_rows_per_group"] = RG
            if _HAS_PYARROW_DATASET:
                args["file_options"] = ds.ParquetFileFormat().make_write_options(
                    compression="ZSTD"
                )
        else:
            if _HAS_WRITER_PROPERTIES:
                args["writer_properties"] = WriterProperties(compression="ZSTD")

    return args


def _delta_table(path: str, storage_options: Optional[Dict[str, str]]) -> DeltaTable:
    if storage_options:
        return DeltaTable(path, storage_options=storage_options)
    return DeltaTable(path)


def table_exists(path: str, storage_options: Optional[Dict[str, str]] = None) -> bool:
    """Return True if a Delta table already exists at ``path``."""
    try:
        _delta_table(path, storage_options)
        return True
    except Exception:
        return False


def write_delta(
    path: str,
    data,
    mode: str = "overwrite",
    *,
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """
    Materialize an Arrow record batch / table to Delta and maintain it.

    Mirrors the original ``runner._run_sql`` behavior:
      - overwrite: write, then vacuum(retention=0) + cleanup_metadata
      - append:    write, then compact/vacuum/cleanup if file count exceeds threshold
      - ignore:    write only if the table does not already exist
    """
    if mode not in {"overwrite", "append", "ignore"}:
        raise ValueError(f"Invalid mode '{mode}'. Use: overwrite, append, or ignore")

    schema_mode = "merge" if merge_schema else None

    if mode == "ignore":
        if table_exists(path, storage_options):
            return
        mode = "overwrite"

    args = build_write_deltalake_args(
        path, data, mode,
        schema_mode=schema_mode,
        partition_by=partition_by,
        storage_options=storage_options,
    )
    write_deltalake(**args)

    dt = _delta_table(path, storage_options)
    if mode == "overwrite":
        dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
        dt.cleanup_metadata()
    else:  # append
        if len(dt.file_uris()) > compaction_threshold:
            dt.optimize.compact()
            dt.vacuum(dry_run=False)
            dt.cleanup_metadata()


def merge_delta(
    path: str,
    data,
    unique_key,
    *,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Upsert an Arrow batch into an existing Delta table on ``unique_key`` using delta_rs.

    ``unique_key`` may be a single column name or a list of column names.
    """
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    predicate = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    dt = _delta_table(path, storage_options)
    (
        dt.merge(
            source=data,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
