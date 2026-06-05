"""
Delta Lake write engine for the duckrun dbt adapter.

DuckDB produces the data and ``deltalake`` (delta_rs) materializes it. We pass the
DuckDB relation straight through: deltalake 1.x consumes any object exposing the Arrow
C-stream interface (``__arrow_c_stream__``), which DuckDB relations do — so there is no
pyarrow dependency.
"""
from typing import Any, Dict, List, Optional

from deltalake import DeltaTable, write_deltalake

try:  # deltalake 1.x exposes WriterProperties at the top level
    from deltalake import WriterProperties
except ImportError:  # pragma: no cover - older layouts
    try:
        from deltalake.writer import WriterProperties
    except ImportError:
        WriterProperties = None


def _writer_properties():
    # ZSTD compression for good Parquet footprint (Power BI / DirectLake friendly).
    if WriterProperties is not None:
        try:
            return WriterProperties(compression="ZSTD")
        except Exception:
            return None
    return None


def build_write_deltalake_args(
    path: str,
    data,
    mode: str,
    schema_mode: Optional[str] = None,
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build kwargs for ``write_deltalake`` (deltalake >= 1.2)."""
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
    wp = _writer_properties()
    if wp is not None:
        args["writer_properties"] = wp
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
    Materialize ``data`` (a DuckDB relation / Arrow C-stream) to Delta and maintain it.

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
    insert_only: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Merge ``data`` into an existing Delta table on ``unique_key`` using delta_rs.

    ``unique_key`` may be a single column name or a list of column names.

    - insert_only=False (default): upsert — update matched rows, insert new ones.
    - insert_only=True: insert only rows whose key is not present (idempotent
      append / dedupe; never touches existing rows).
    """
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    predicate = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    dt = _delta_table(path, storage_options)
    merger = dt.merge(
        source=data,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
    )
    if insert_only:
        merger = merger.when_not_matched_insert_all()
    else:
        merger = merger.when_matched_update_all().when_not_matched_insert_all()
    merger.execute()
