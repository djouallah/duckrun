"""
Delta Lake write engine for the duckrun dbt adapter.

DuckDB produces the data and ``deltalake`` (delta_rs) materializes it. We pass the
DuckDB relation straight through: deltalake 1.x consumes any object exposing the Arrow
C-stream interface (``__arrow_c_stream__``), which DuckDB relations do — so there is no
pyarrow dependency.
"""
import ctypes
import os
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


def _total_ram_bytes() -> Optional[int]:
    """Total physical RAM in bytes, cross-platform; None if it can't be determined.

    This is *physical* RAM only; a container can be capped well below it — callers should
    go through _effective_mem_limit_bytes(), which also folds in the cgroup limit.
    """
    # POSIX (Linux, macOS): pages * page size.
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, AttributeError, OSError):
        pass
    # Windows: GlobalMemoryStatusEx -> ullTotalPhys.
    try:
        class _MemStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        stat = _MemStatusEx()
        stat.dwLength = ctypes.sizeof(_MemStatusEx)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return int(stat.ullTotalPhys)
    except Exception:
        pass
    return None


def _cgroup_mem_limit_bytes() -> Optional[int]:
    """Memory limit imposed by the current cgroup (i.e. a container), or None if unlimited
    or not on Linux. This is what matters on Fabric/Spark/k8s, where physical RAM is the
    host's but the kernel OOM-kills us at the (much lower) container limit.

    cgroup v2: the tightest finite ``memory.max`` walking up our cgroup to the root.
    cgroup v1: ``memory/memory.limit_in_bytes`` (huge sentinel == unlimited)."""
    # cgroup v2 (unified hierarchy): /proc/self/cgroup is a single "0::<relpath>" line.
    try:
        rel = None
        with open("/proc/self/cgroup") as fh:
            for line in fh:
                parts = line.strip().split(":", 2)
                if len(parts) == 3 and parts[0] == "0":
                    rel = parts[2]
                    break
        if rel is not None:
            base = "/sys/fs/cgroup"
            cur = os.path.join(base, rel.lstrip("/"))
            limits = []
            while True:
                try:
                    with open(os.path.join(cur, "memory.max")) as fh:
                        val = fh.read().strip()
                    if val.isdigit():  # "max" (unlimited) is not a digit string
                        limits.append(int(val))
                except OSError:
                    pass
                if os.path.normpath(cur) == os.path.normpath(base):
                    break
                cur = os.path.dirname(cur)
            if limits:
                return min(limits)
    except OSError:
        pass
    # cgroup v1.
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as fh:
            val = int(fh.read().strip())
        if 0 < val < 2 ** 62:  # v1 "unlimited" is ~2**63; reject it
            return val
    except (OSError, ValueError):
        pass
    return None


def _effective_mem_limit_bytes() -> Optional[int]:
    """The memory we may actually use: min(physical RAM, cgroup/container limit). None if
    neither can be determined."""
    vals = [v for v in (_total_ram_bytes(), _cgroup_mem_limit_bytes()) if v]
    return min(vals) if vals else None


# Fraction of the effective memory limit to hand delta_rs as the merge pool. Deliberately
# below 1.0 to leave headroom for memory that sits *outside* the pool and still counts toward
# a cgroup limit: the streamed source, Delta read buffers, and — on cgroup v2 — the page
# cache of the spill files the merge writes. 0.7 survived a hard MemoryMax in CI where 0.8
# left too little margin once that spill page cache grew.
_MERGE_SPILL_FRACTION = 0.7


def _default_merge_spill_size() -> Optional[int]:
    """delta_rs merge ``max_spill_size`` default: ~70% of the *effective* memory limit
    (min of physical RAM and the cgroup/container cap), so the merge spills to disk instead
    of being OOM-killed. None if the limit is unknown (then the merge runs unbounded, as it
    did before).

    Caveat: this bounds delta_rs's merge *pool*, not the whole process — the Arrow source,
    read buffers, and spill-file page cache live outside it — so on a tight container with a
    large source the total can still exceed the cap. Override with ``merge_max_spill_size``."""
    limit = _effective_mem_limit_bytes()
    return int(limit * _MERGE_SPILL_FRACTION) if limit else None


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


def delta_columns(path: str, storage_options: Optional[Dict[str, str]] = None) -> List[str]:
    """Column names of the existing Delta table at ``path`` (for on_schema_change)."""
    return [f.name for f in _delta_table(path, storage_options).schema().fields]


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


def delete_insert_window(
    path: str,
    data,
    *,
    column: str,
    start: str,
    end: str,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Microbatch delete+insert for one batch window: delete the rows already in
    ``[start, end)`` on ``column``, then append ``data`` (the batch's rows).

    This is the Delta-native equivalent of dbt's microbatch ``delete from target where
    event_time in window; insert ...``. ``start``/``end`` are naive ``YYYY-MM-DD HH:MM:SS``
    strings (UTC batch bounds from dbt). The column is cast to TIMESTAMP so the same
    predicate works whether ``event_time`` is a DATE or a TIMESTAMP.
    """
    dt = _delta_table(path, storage_options)
    # delta_rs parses this with datafusion and coerces the string literal to the column's
    # type. Keep it CAST-free: delta_rs can't serialize a CAST expression back to a string
    # ("Unable to convert expression to string"), which a wrapping CAST would trigger.
    predicate = f"{column} >= '{start}' AND {column} < '{end}'"
    dt.delete(predicate)

    args = build_write_deltalake_args(path, data, "append", storage_options=storage_options)
    write_deltalake(**args)

    dt = _delta_table(path, storage_options)
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
    update_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
    predicates: Optional[List[str]] = None,
    merge_schema: bool = False,
    max_spill_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Merge ``data`` into an existing Delta table on ``unique_key`` using delta_rs.

    ``unique_key`` may be a single column name or a list of column names. The merge
    condition is ``target.k = source.k`` for each key, AND-ed with any extra
    ``predicates`` (dbt ``incremental_predicates``); predicates should reference the
    ``target``/``source`` aliases.

    - insert_only=True: insert only rows whose key is not present (idempotent append /
      dedupe; never touches existing rows). Mutually exclusive with the update options.
    - default upsert: update matched rows, insert new ones. Narrow the update with
      ``update_columns`` (only these) or ``exclude_columns`` (all but these) — dbt's
      ``merge_update_columns`` / ``merge_exclude_columns``.
    - merge_schema=True lets delta_rs evolve the table schema (new columns), backing
      ``on_schema_change='append_new_columns'`` / ``'sync_all_columns'``.
    - max_spill_size caps the merge's in-memory pool (bytes); beyond it delta_rs spills the
      join to disk instead of OOMing. None -> default to ~80% of RAM (_default_merge_spill_size);
      pass 0 (or any falsy non-None) to disable the cap and run unbounded.
    """
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    conditions = [f"target.{k} = source.{k}" for k in keys]
    if predicates:
        extra = predicates if isinstance(predicates, (list, tuple)) else [predicates]
        conditions.extend(p for p in extra if p)
    predicate = " AND ".join(conditions)

    if max_spill_size is None:
        max_spill_size = _default_merge_spill_size()
    # Only forward the kwarg when we have a positive cap: delta_rs builds a spilling session
    # only when max_spill_size is set, so omitting it preserves the prior unbounded behavior
    # (e.g. RAM undetectable, or caller explicitly passed 0 to opt out).
    spill_kwargs = {"max_spill_size": max_spill_size} if max_spill_size else {}

    dt = _delta_table(path, storage_options)
    merger = dt.merge(
        source=data,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
        merge_schema=merge_schema,
        **spill_kwargs,
    )
    if insert_only:
        merger = merger.when_not_matched_insert_all()
    else:
        if update_columns:
            updates = {c: f"source.{c}" for c in update_columns}
            merger = merger.when_matched_update(updates=updates)
        elif exclude_columns:
            merger = merger.when_matched_update_all(except_cols=list(exclude_columns))
        else:
            merger = merger.when_matched_update_all()
        merger = merger.when_not_matched_insert_all()
    merger.execute()
