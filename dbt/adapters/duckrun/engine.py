"""
Delta Lake write engine for the duckrun dbt adapter.

DuckDB produces the data and ``deltalake`` (delta_rs) materializes it. We pass the
DuckDB relation straight through: deltalake 1.x consumes any object exposing the Arrow
C-stream interface (``__arrow_c_stream__``), which DuckDB relations do — so there is no
pyarrow dependency.
"""
import ctypes
import os
import re
from typing import Any, Dict, List, Optional

from dbt.adapters.events.logging import AdapterLogger
from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

logger = AdapterLogger("Duckrun")

try:  # deltalake 1.x exposes WriterProperties at the top level
    from deltalake import WriterProperties
except ImportError:  # pragma: no cover - older layouts
    try:
        from deltalake.writer import WriterProperties
    except ImportError:
        WriterProperties = None


# 6 row groups' worth of delta-rs's default (1,048,576 rows) per group. Bigger row groups
# give Power BI / DirectLake fewer, larger scan ranges at the cost of more write-time memory.
_MAX_ROW_GROUP_SIZE = 1_048_576 * 6


def _writer_properties():
    # ZSTD compression for a good Parquet footprint (Power BI / DirectLake friendly), plus
    # larger row groups. If a WriterProperties build ever rejects max_row_group_size, fall
    # back to compression-only rather than losing ZSTD entirely (ZSTD is the property we most
    # care about keeping).
    if WriterProperties is None:
        return None
    try:
        return WriterProperties(compression="ZSTD", max_row_group_size=_MAX_ROW_GROUP_SIZE)
    except Exception:  # best-effort: any build rejection falls back to compression-only below
        pass
    try:
        return WriterProperties(compression="ZSTD")
    except Exception:  # best-effort: if even ZSTD-only is rejected, write without writer props
        return None


def _win_mem_status():
    """GlobalMemoryStatusEx result (total + available physical RAM), or None off-Windows /
    on failure. Shared by _total_ram_bytes and _available_ram_bytes."""
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
            return stat
    except Exception:  # best-effort: off-Windows / no ctypes.windll -> caller treats RAM as unknown
        pass
    return None


def _total_ram_bytes() -> Optional[int]:
    """Total physical RAM in bytes, cross-platform; None if it can't be determined.

    This is *physical* RAM only; a container can be capped well below it, and on a shared box
    most of it may already be in use — callers should go through _effective_mem_limit_bytes(),
    which also folds in the cgroup limit and the RAM actually free at startup.
    """
    # POSIX (Linux, macOS): pages * page size.
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, AttributeError, OSError):
        pass
    # Windows: GlobalMemoryStatusEx -> ullTotalPhys.
    stat = _win_mem_status()
    return int(stat.ullTotalPhys) if stat else None


def _available_ram_bytes() -> Optional[int]:
    """Physical RAM the kernel reports as currently allocatable (free + reclaimable),
    cross-platform; None if it can't be determined.

    On a busy shared box — a Fabric notebook sharing the node with the Spark runtime and a
    background DuckDB job — this sits far below *total* RAM, and it's the number the budget must
    respect: total RAM would overcommit a process that doesn't own the whole node. Read FRESH on
    every call (no startup snapshot): _effective_mem_limit_bytes() is sampled right before each
    job (e.g. at the top of merge_delta, before the source relation is materialized), so the cap
    reflects the memory actually free at that moment — after whatever earlier models, a Spark
    runtime, or a background DuckDB job have already taken — instead of a stale value frozen at
    connection setup.
    """
    # Linux: the kernel's own estimate, which already discounts reclaimable page cache.
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024  # value is in kB
    except (OSError, ValueError, IndexError):
        pass
    # Windows: GlobalMemoryStatusEx -> ullAvailPhys.
    stat = _win_mem_status()
    return int(stat.ullAvailPhys) if stat else None


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
    """The memory we may actually use, recomputed FRESH on every call: the tightest of physical
    RAM, the cgroup/container cap, and the RAM currently free (_available_ram_bytes). None if none
    of them can be determined.

    Sampled per job (right before each merge, before its source is materialized) rather than once
    at startup — so the cap tracks the memory actually free *now*, after earlier models / a Spark
    runtime / a background DuckDB job have taken their share, instead of a stale connection-time
    snapshot.

    The available-RAM term is also what catches Fabric: there the cgroup is the unlimited *root*
    (`/proc/self/cgroup` = `0::/`, `memory.max` = `max`), so the cap would otherwise fall back to
    *total* node RAM — ignoring that the Spark runtime, the kernel, and any background DuckDB job
    already hold most of it. Available RAM reflects that pressure; total RAM does not."""
    vals = [v for v in (_total_ram_bytes(), _cgroup_mem_limit_bytes(),
                        _available_ram_bytes()) if v]
    return min(vals) if vals else None


def _effective_mem_limit_source() -> str:
    """Which signal currently bounds _effective_mem_limit_bytes() — for the run-start log line."""
    eff = _effective_mem_limit_bytes()
    if not eff:
        return "unknown"
    avail = _available_ram_bytes()
    if avail and avail <= eff:
        return "available RAM"
    cgroup = _cgroup_mem_limit_bytes()
    if cgroup and cgroup <= eff:
        return "cgroup/container limit"
    return "physical RAM"


# How the effective memory limit is split between the two big consumers that can peak at the
# same time during a merge — DuckDB (producing the source relation) and delta_rs (the merge
# pool). They share one cap, so the shares must sum *under* 1.0 or we've just moved the OOM; each
# consumer spills to disk past its share rather than OOM-killing the container. The merge gets the
# larger share: when DuckDB is only *streaming* the merge source it needs far less than the merge's
# hash-join pool, and starving that pool makes delta_rs raise "Resources exhausted" (the merge can't
# fit its working set) — the opposite failure from an OOM. DuckDB also spills gracefully under a
# tight limit, whereas the delta_rs pool is brittle, so the merge gets the bulk. 0.3 + 0.6 leaves
# ~10% slack for Python, Arrow buffers, and page cache.
_DUCKDB_MEM_FRACTION = 0.3   # DuckDB's memory_limit (see set_merge_memory_limit)
_MERGE_SPILL_FRACTION = 0.6  # delta_rs merge max_spill_size

# Write path (overwrite/append/safeappend/microbatch): DuckDB runs alone — no competing delta_rs
# merge pool — so it gets the bulk of the cap, not a 0.3 share. But it must still be BOUNDED to the
# effective limit, because DuckDB's own default memory_limit is 80% of *physical* RAM, and on a
# container (Fabric/Spark/k8s) physical RAM is the whole node, not our slice — so the default
# overcommits and the kernel OOM-kills us. 0.7 of the effective limit (which folds in available RAM,
# the only signal that reflects the container on Fabric where the cgroup is the unlimited root)
# leaves ~30% for the Arrow source stream, the Parquet writer, and page cache outside DuckDB's pool.
_WRITE_MEM_FRACTION = 0.7


def _default_merge_spill_size() -> Optional[int]:
    """delta_rs merge ``max_spill_size`` default: ~40% of the *effective* memory limit
    (the tightest of physical RAM, the cgroup/container cap, and the RAM free at startup), so
    the merge spills to disk instead of being OOM-killed. None if the limit is unknown (then the
    merge runs unbounded, as it did before).

    Caveat: this bounds delta_rs's merge *pool*, not the whole process — the Arrow source,
    read buffers, and spill-file page cache live outside it — so on a tight container with a
    large source the total can still exceed the cap. Override with ``merge_max_spill_size``."""
    limit = _effective_mem_limit_bytes()
    return int(limit * _MERGE_SPILL_FRACTION) if limit else None


# Units DuckDB emits from current_setting('memory_limit') (e.g. "25.0 GiB"). Binary (GiB) and
# decimal (GB) both occur; bare "B"/"" is bytes.
_BYTE_UNITS = {
    "": 1, "B": 1,
    "KIB": 2 ** 10, "MIB": 2 ** 20, "GIB": 2 ** 30, "TIB": 2 ** 40,
    "KB": 10 ** 3, "MB": 10 ** 6, "GB": 10 ** 9, "TB": 10 ** 12,
}


def _parse_byte_size(text: Optional[str]) -> Optional[int]:
    """Parse a DuckDB byte-size string ("25.0 GiB", "1073741824B", "0 bytes") to bytes;
    None if it doesn't look like one."""
    m = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*", text or "")
    if not m:
        return None
    unit = m.group(2).upper().rstrip("S")  # "bytes" -> "BYTE" -> "BYTE"; handle "B"/"BYTE"
    unit = "B" if unit in ("B", "BYTE") else unit
    mult = _BYTE_UNITS.get(unit)
    return int(float(m.group(1)) * mult) if mult is not None else None


def configure_duckdb_session(con) -> None:
    """Always-on DuckDB tuning for duckrun's Delta write path, applied once per connection:
    ``preserve_insertion_order=false`` and a ``temp_directory`` to spill to. These are NOT the
    memory split — they're write-path correctness that helps every materialization.

    preserve_insertion_order=false: with DuckDB's default (true), streaming a large result into
    delta_rs makes DuckDB buffer the *whole* result to keep row order, which OOMs big writes /
    merges. Delta tables are unordered and explicit ORDER BY still works, so duckrun turns it
    off by default — users no longer need to set it in their profile ``settings``.

    The DuckDB ``memory_limit`` is deliberately left ALONE here; it's set per model in ``store()``.
    The write path clamps it to ``_WRITE_MEM_FRACTION`` of the effective limit
    (``set_write_memory_limit``) — DuckDB has no competing delta_rs pool there so it gets the bulk,
    but still bounded so its host-physical-RAM default can't OOM-kill a container. The merge path
    tightens further to its 0.3 share (``set_merge_memory_limit``)."""
    try:
        con.execute("SET preserve_insertion_order=false")
    except Exception:  # best-effort tuning: a failed SET must not abort connection setup
        pass
    # An in-memory DuckDB can default to an empty temp_directory and then *cannot* spill; give it
    # one so a tight memory_limit (set later for a merge) degrades to disk instead of an error.
    try:
        tmp = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    except Exception:  # best-effort: if we can't read it, skip overriding rather than guess
        tmp = "skip"  # couldn't read it; don't risk overriding
    if not tmp:
        spill_dir = os.path.join(os.getcwd(), ".duckrun_duckdb_spill")
        try:
            os.makedirs(spill_dir, exist_ok=True)
            con.execute(f"SET temp_directory='{spill_dir}'")
        except Exception:  # best-effort spill dir: failure just leaves the default in place
            pass


def read_memory_limit(con) -> Optional[str]:
    """DuckDB's current ``memory_limit`` as it reports it (e.g. '25.0 GiB'), or None if
    unreadable. Captured once at connection setup as the baseline to restore on write paths."""
    try:
        return con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    except Exception:  # best-effort: unreadable limit -> None baseline (write path skips clamping)
        return None


def set_merge_memory_limit(con) -> None:
    """Tighten DuckDB's ``memory_limit`` to its merge share (``_DUCKDB_MEM_FRACTION`` of the
    effective limit) right before a merge, so DuckDB (producing the source) and the delta_rs merge
    pool (``max_spill_size`` = ``_MERGE_SPILL_FRACTION``) fit together in one process. This is the
    *only* place the split is applied — overwrite/append never call it. Tighten-only: never raises
    a lower current/profile limit; no-op when the effective limit is unknown."""
    limit = _effective_mem_limit_bytes()
    if not limit:
        return
    target = int(limit * _DUCKDB_MEM_FRACTION)
    current = _parse_byte_size(read_memory_limit(con))
    # Tighten only: skip when an explicit limit is already at/below our target.
    if current is None or current > target:
        try:
            con.execute(f"SET memory_limit='{target}B'")
            logger.info(
                f"merge: DuckDB memory_limit set to {target / 2 ** 30:.2f} GiB "
                f"({int(_DUCKDB_MEM_FRACTION * 100)}% of {limit / 2 ** 30:.2f} GiB effective limit)"
            )
        except Exception:  # best-effort tuning: a failed SET leaves the prior limit, no abort
            pass


def restore_memory_limit(con, baseline: Optional[str]) -> None:
    """Restore DuckDB's ``memory_limit`` to the connection ``baseline`` — its profile value, or
    DuckDB's own default if none. No-op if the baseline is unknown."""
    if not baseline:
        return
    try:
        con.execute(f"SET memory_limit='{baseline}'")
    except Exception:  # best-effort restore: a failed SET leaves the current limit, no abort
        pass


def set_write_memory_limit(con, baseline: Optional[str]) -> None:
    """Write-path (overwrite/append/safeappend/microbatch) ``memory_limit``: the connection
    ``baseline``, clamped DOWN to ``_WRITE_MEM_FRACTION`` of the effective limit. The write path
    has no competing delta_rs pool, so DuckDB gets the bulk — but still bounded, because DuckDB's
    own default is 80% of *physical* RAM, which on a container is the whole node, not our slice, and
    OOM-kills us (the bug this fixes on Fabric). Also undoes any tightening a prior merge left on the
    shared connection, by setting the limit absolutely (not tighten-only) from the baseline.

    Set to ``min(baseline, _WRITE_MEM_FRACTION * effective)`` so an explicit lower profile limit is
    respected and the bogus host default is capped. Falls back to restoring the baseline string when
    the effective limit is unknown; no-op when neither is known."""
    eff = _effective_mem_limit_bytes()
    target = int(eff * _WRITE_MEM_FRACTION) if eff else None
    base_bytes = _parse_byte_size(baseline)
    candidates = [v for v in (base_bytes, target) if v]
    if not candidates:
        restore_memory_limit(con, baseline)
        return
    final = min(candidates)
    try:
        con.execute(f"SET memory_limit='{final}B'")
        if target is not None and final == target:
            logger.info(
                f"write: DuckDB memory_limit set to {final / 2 ** 30:.2f} GiB "
                f"({int(_WRITE_MEM_FRACTION * 100)}% of {eff / 2 ** 30:.2f} GiB effective limit)"
            )
    except Exception:  # best-effort tuning: a failed SET leaves the prior limit, no abort
        pass


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
    # "merge" evolves the schema (adds columns); "overwrite" replaces it wholesale (overwrite
    # mode only) — delta-rs's own schema_mode values, passed straight through.
    if schema_mode in ("merge", "overwrite"):
        args["schema_mode"] = schema_mode
    wp = _writer_properties()
    if wp is not None:
        args["writer_properties"] = wp
    return args


def _delta_table(path: str, storage_options: Optional[Dict[str, str]]) -> DeltaTable:
    if storage_options:
        return DeltaTable(path, storage_options=storage_options)
    return DeltaTable(path)


def table_exists(path: str, storage_options: Optional[Dict[str, str]] = None) -> bool:
    """Return True if a Delta table already exists at ``path``.

    Catch ONLY ``TableNotFoundError`` (the table genuinely isn't there) → False. Every other
    error — a transient ADLS/OneLake 503, an expired token, a permissions blip — is RE-RAISED.
    Swallowing those was a silent data-loss bug: a transient open failure at store time made an
    incremental (already row-filtered) write fall into the overwrite branch, replacing the table
    with just the increment. A real error must fail the run loudly, not look like "no table".
    """
    try:
        _delta_table(path, storage_options)
        return True
    except TableNotFoundError:
        return False


# Delta column-metadata key under which we stash a dbt column description, and the dollar-quote
# label used to embed arbitrary comment text (newlines, quotes, dollar signs) in COMMENT ON SQL.
_DELTA_COMMENT_KEY = "comment"
_COMMENT_DOLLAR_TAG = "$duckrun_comment$"


def persist_docs_to_delta(
    path: str,
    relation_docs: Optional[str],
    column_docs: Optional[Dict[str, str]],
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Persist a model's relation/column descriptions into the Delta table's own metadata so they
    survive across processes (a later ``dbt docs generate`` runs in a fresh DuckDB and rebuilds
    the views from disk — see ``read_delta_docs`` / view registration). Table description via
    ``alter.set_table_description``; column descriptions via per-field ``alter.set_column_metadata``
    under ``_DELTA_COMMENT_KEY``. Best-effort and idempotent; a docs-only failure must never fail
    the model build."""
    if not relation_docs and not column_docs:
        return
    dt = _delta_table(path, storage_options)
    if relation_docs:
        try:
            dt.alter.set_table_description(relation_docs)
        except Exception as exc:  # best-effort: docs persistence must not fail the build
            logger.debug(f"duckrun: could not set Delta table description at {path!r}: {exc}")
    if column_docs:
        existing = {f.name for f in dt.schema().fields}
        for col, desc in column_docs.items():
            if col not in existing or not desc:
                continue
            try:
                dt.alter.set_column_metadata(col, {_DELTA_COMMENT_KEY: desc})
            except Exception as exc:  # best-effort per column
                logger.debug(f"duckrun: could not set Delta column metadata for {col!r}: {exc}")


def read_delta_docs(
    path: str, storage_options: Optional[Dict[str, str]] = None
):
    """Read back (relation_description, {column: description}) stored by ``persist_docs_to_delta``.
    Returns ``(None, {})`` when the table is absent or carries no docs. Best-effort: a read failure
    yields empty docs rather than aborting view registration."""
    try:
        dt = _delta_table(path, storage_options)
    except Exception:  # best-effort: no table / unreadable -> no docs to restore
        return None, {}
    try:
        relation_docs = dt.metadata().description or None
    except Exception:  # best-effort
        relation_docs = None
    column_docs = {}
    try:
        for f in dt.schema().fields:
            desc = (f.metadata or {}).get(_DELTA_COMMENT_KEY)
            if desc:
                column_docs[f.name] = desc
    except Exception:  # best-effort
        pass
    return relation_docs, column_docs


def comment_on_sql(relation_render: str, relation_type: str,
                   relation_docs: Optional[str],
                   column_docs: Optional[Dict[str, str]]) -> List[str]:
    """Build ``COMMENT ON {VIEW|TABLE} ...`` / ``COMMENT ON COLUMN ...`` statements that re-apply
    persisted docs to a (re-registered) DuckDB relation. Comment text is dollar-quoted so newlines,
    single quotes and dollar signs in the description can't break the literal. Column names are
    double-quoted. Returns an empty list when there's nothing to comment."""
    out: List[str] = []

    def _lit(text: str) -> Optional[str]:
        # Dollar-quoting handles arbitrary text; bail (skip) only if the tag itself appears.
        return None if _COMMENT_DOLLAR_TAG in text else f"{_COMMENT_DOLLAR_TAG}{text}{_COMMENT_DOLLAR_TAG}"

    if relation_docs:
        lit = _lit(relation_docs)
        if lit is not None:
            out.append(f"comment on {relation_type} {relation_render} is {lit}")
    for col, desc in (column_docs or {}).items():
        if not desc:
            continue
        lit = _lit(desc)
        if lit is None:
            continue
        quoted = '"' + str(col).replace('"', '""') + '"'
        out.append(f"comment on column {relation_render}.{quoted} is {lit}")
    return out


def delta_columns(path: str, storage_options: Optional[Dict[str, str]] = None) -> List[str]:
    """Column names of the existing Delta table at ``path`` (for on_schema_change)."""
    return [f.name for f in _delta_table(path, storage_options).schema().fields]


def table_version(path: str, storage_options: Optional[Dict[str, str]] = None) -> int:
    """Current (HEAD) Delta version of the table at ``path``. The ``vB`` a caller captures before
    reading a source, to pin a later merge/replace to the same snapshot (Spark MERGE semantics)."""
    return _delta_table(path, storage_options).version()


def _maintain(dt: DeltaTable, compaction_threshold: int) -> None:
    """Threshold-gated upkeep shared by the append / merge / delete+insert paths: once the table
    has more than ``compaction_threshold`` files, compact small files, vacuum tombstoned old
    versions (safe 7-day default retention — no concurrent reader broken), and clean up expired
    log entries. Without it a table written on every run fragments into small files and keeps old
    versions forever. (The overwrite path vacuums unconditionally instead and does not use this.)"""
    if len(dt.file_uris()) > compaction_threshold:
        dt.optimize.compact()
        dt.vacuum(dry_run=False)
        dt.cleanup_metadata()


def write_delta(
    path: str,
    data,
    mode: str = "overwrite",
    *,
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """
    Materialize ``data`` (a DuckDB relation / Arrow C-stream) to Delta and maintain it.

      - overwrite: write, then vacuum (safe 7-day default) + cleanup_metadata
      - append:    write, then compact/vacuum/cleanup if file count exceeds threshold
      - ignore:    write only if the table does not already exist

    ``merge_schema`` evolves the schema (adds columns); ``overwrite_schema`` replaces it wholesale
    (overwrite mode only — Spark/Delta's ``overwriteSchema``). They are mutually exclusive.
    """
    if mode not in {"overwrite", "append", "ignore"}:
        raise ValueError(f"Invalid mode '{mode}'. Use: overwrite, append, or ignore")

    if merge_schema:
        schema_mode = "merge"
    elif overwrite_schema and mode != "append":  # schema replacement only makes sense on a rewrite
        schema_mode = "overwrite"
    else:
        schema_mode = None

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
        dt.vacuum(dry_run=False)  # safe default 168h retention (no concurrent reader broken)
        dt.cleanup_metadata()
    else:  # append
        _maintain(dt, compaction_threshold)


def append_if_unchanged(
    path: str,
    data,
    *,
    read_version: Optional[int] = None,
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """
    Optimistic ("safe") append: append ``data`` only if the table version has not moved since
    we read it — otherwise refuse with ``CommitFailedError``.

    delta_rs has no native conditional / compare-and-swap commit. A plain append normally
    auto-rebases onto the latest version (appends are non-conflicting), so it can never fail on
    a concurrent write. We instead pin the write to the snapshot we read — a ``DeltaTable``
    loaded at ``read_version`` (or current HEAD) — and pass ``max_commit_retries=0`` so delta_rs
    does NOT rebase: if any commit landed since that snapshot, the append's target version is
    already taken and the commit fails. That is compare-and-swap on the table version.

    Dedup is NOT performed — that is the model SQL's job. This only guarantees the append is
    atomic with respect to the version it was computed against; on a conflict the caller should
    re-run the model against the new HEAD. After a successful append, run the same threshold-
    gated maintenance as the plain append path.
    """
    dt = _delta_table(path, storage_options)
    if read_version is not None:
        dt.load_as_version(read_version)
    pinned = dt.version()

    schema_mode = "merge" if merge_schema else None
    args = build_write_deltalake_args(
        path, data, "append",
        schema_mode=schema_mode,
        partition_by=partition_by,
        storage_options=storage_options,
    )
    # Pin to the snapshot we read and disable rebasing so a concurrent commit fails the append
    # instead of silently landing on top of it. storage_options live on the DeltaTable already.
    args["table_or_uri"] = dt
    args.pop("storage_options", None)
    args["commit_properties"] = CommitProperties(max_commit_retries=0)
    try:
        write_deltalake(**args)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"safeappend: table '{path}' changed since version {pinned} "
            f"(a concurrent write committed); append refused. Re-run the model."
        ) from e

    _maintain(_delta_table(path, storage_options), compaction_threshold)


def replace_where(
    path: str,
    data,
    predicate: str,
    *,
    read_version: Optional[int] = None,
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Spark ``replaceWhere`` / ``INSERT OVERWRITE`` as a SINGLE atomic Delta commit: atomically
    replace the rows matching ``predicate`` with ``data``. One commit, not a delete-then-append
    pair — so there is no torn-read window (a reader never sees the range emptied-but-not-refilled)
    and no half-applied failure state.

    ``predicate`` is a delta_rs/datafusion SQL expression. Keep it CAST-free: delta_rs can't
    serialize a CAST expression back to a string ("Unable to convert expression to string").

    When ``read_version`` is given, the overwrite is pinned to that snapshot and committed with
    ``max_commit_retries=0`` (compare-and-swap): a concurrent writer that lands since ``vB`` fails
    the commit loudly instead of silently interleaving. ``None`` keeps blind-HEAD behavior.
    Maintenance always runs at a fresh HEAD afterward (never pinned)."""
    args = build_write_deltalake_args(
        path, data, "overwrite", partition_by=partition_by, storage_options=storage_options
    )
    args["predicate"] = predicate  # replaceWhere: overwrite ONLY the rows matching the predicate
    if read_version is not None:
        # Pin to the read snapshot and disable rebasing, so a concurrent commit since vB fails this
        # overwrite (CAS) instead of landing on top of it. storage_options live on the DeltaTable
        # already, so drop the kwarg form (mirrors append_if_unchanged).
        dt = _delta_table(path, storage_options)
        dt.load_as_version(read_version)
        args["table_or_uri"] = dt
        args.pop("storage_options", None)
        args["commit_properties"] = CommitProperties(max_commit_retries=0)
    try:
        write_deltalake(**args)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"replaceWhere: table '{path}' changed since version {read_version} "
            f"(a concurrent write committed); replace refused. Re-run."
        ) from e

    # Maintenance ALWAYS at a fresh HEAD — never the pinned snapshot (a stale file list would
    # compact/vacuum files live versions still reference and corrupt the table).
    _maintain(_delta_table(path, storage_options), compaction_threshold)


def replace_window(
    path: str,
    data,
    *,
    column: str,
    start: str,
    end: str,
    read_version: Optional[int] = None,
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Microbatch window replace: atomically replace the rows in ``[start, end)`` on ``column``
    with ``data`` (the batch's rows) — the Delta-native equivalent of dbt's microbatch "delete the
    window, insert the batch", as ONE atomic commit (Spark ``replaceWhere``). ``start``/``end`` are
    naive ``YYYY-MM-DD HH:MM:SS`` strings (UTC batch bounds from dbt). Delegates to
    :func:`replace_where` with the window predicate; ``read_version`` pins/fences the commit."""
    # CAST-free window predicate — see replace_where. delta_rs coerces the string literals to the
    # column's type, so this works whether event_time is a DATE or a TIMESTAMP.
    predicate = f"{column} >= '{start}' AND {column} < '{end}'"
    replace_where(
        path, data, predicate,
        read_version=read_version, partition_by=partition_by,
        storage_options=storage_options, compaction_threshold=compaction_threshold,
    )


def delete_rows(
    path: str,
    predicate: Optional[str] = None,
    *,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Delete rows matching ``predicate`` (a delta_rs/datafusion SQL expression), or every row
    when ``predicate`` is None. The Delta-native ``DELETE FROM`` for the connection API; same
    delta_rs ``dt.delete`` the microbatch window path uses. Then threshold-gated maintenance."""
    dt = _delta_table(path, storage_options)
    dt.delete(predicate) if predicate else dt.delete()
    _maintain(_delta_table(path, storage_options), compaction_threshold)


def update_rows(
    path: str,
    updates: Dict[str, str],
    predicate: Optional[str] = None,
    *,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Update ``{column: expression}`` for rows matching ``predicate`` (delta_rs/datafusion SQL),
    or every row when ``predicate`` is None. The Delta-native ``UPDATE`` for the connection API.
    Then threshold-gated maintenance."""
    dt = _delta_table(path, storage_options)
    dt.update(updates=updates, predicate=predicate)
    _maintain(_delta_table(path, storage_options), compaction_threshold)


def merge_delta(
    path: str,
    data,
    unique_key,
    *,
    insert_only: bool = False,
    update_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
    predicates: Optional[List[str]] = None,
    update_condition: Optional[str] = None,
    insert_condition: Optional[str] = None,
    merge_schema: bool = False,
    max_spill_size: Optional[int] = None,
    streamed_exec: bool = False,
    read_version: Optional[int] = None,
    delete_unmatched_by_source=None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
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
    - update_condition / insert_condition (dbt ``merge_update_condition`` /
      ``merge_insert_condition``): per-clause predicates gating which matched rows update and
      which unmatched rows insert. Reference the ``target``/``source`` aliases (the caller has
      already rewritten dbt's DBT_INTERNAL_DEST/SOURCE).
    - merge_schema=True lets delta_rs evolve the table schema (new columns), backing
      ``on_schema_change='append_new_columns'`` / ``'sync_all_columns'``.
    - max_spill_size caps the merge's in-memory pool (bytes); beyond it delta_rs spills the
      join to disk instead of OOMing. None -> default to ~40% of RAM (_default_merge_spill_size);
      pass 0 (or any falsy non-None) to disable the cap and run unbounded.
    - read_version: pin the merge TARGET to this Delta version (the model's ``vB``), instead of
      opening HEAD. delta_rs then validates OCC over ``(vB, HEAD]`` — the exact window the model's
      pinned read of ``{{ this }}`` could not have seen — so the read and the commit share one
      snapshot (Spark single-snapshot MERGE semantics). None opens HEAD (the prior behavior, still
      safe-by-ordering). NOTE: only the merge target is pinned; the post-merge maintenance below
      always reopens a fresh HEAD and must NEVER receive this version.
    - streamed_exec: delta_rs's flag for how it reads the source. Its default (True) STREAMS the
      source and so cannot compute source statistics, which means it cannot derive an early
      pruning predicate — it scans the *whole* target. We default it to False: collect the source
      so delta_rs uses its min/max to prune target files to the ones the source can actually
      touch. That's the right trade for the incremental pattern (small source, large target):
      collecting a small delta is cheap and the prune avoids a full-target scan. For a merge whose
      *source* is itself huge, pass streamed_exec=True (``merge_streamed_exec``) so it isn't
      materialized — at the cost of no pruning.
    - delete_unmatched_by_source: Spark "WHEN NOT MATCHED BY SOURCE THEN DELETE" — also remove
      target rows the source doesn't carry. True deletes every unmatched target row (full sync); a
      string deletes only those matching that predicate; None (default, used by the dbt incremental
      strategies) adds no such clause. Used by the connection-API ``df.merge`` full-sync option.

    After the merge, run the same threshold-gated maintenance the append/delete+insert paths
    use: when the file count exceeds ``compaction_threshold``, compact small files, vacuum
    tombstoned old versions (safe 7-day default retention), and clean up expired log entries.
    Without this an incremental table that is merged on every run grows old files forever.
    """
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    conditions = [f"target.{k} = source.{k}" for k in keys]
    if predicates:
        extra = predicates if isinstance(predicates, (list, tuple)) else [predicates]
        conditions.extend(p for p in extra if p)
    predicate = " AND ".join(conditions)

    # Sample the effective limit ONCE so the cap we apply and the cap we log can't disagree:
    # free RAM is read live on every call, so two separate reads would drift on a busy box.
    eff_limit = _effective_mem_limit_bytes()
    if max_spill_size is None:
        max_spill_size = int(eff_limit * _MERGE_SPILL_FRACTION) if eff_limit else None
    # Only forward the kwarg when we have a positive cap: delta_rs builds a spilling session
    # only when max_spill_size is set, so omitting it preserves the prior unbounded behavior
    # (e.g. RAM undetectable, or caller explicitly passed 0 to opt out).
    spill_kwargs = {"max_spill_size": max_spill_size} if max_spill_size else {}

    # Make the spill decision observable: this is the only way to confirm, from a normal dbt
    # run, that the cgroup-aware cap is actually being applied (and what value it picked).
    if spill_kwargs:
        logger.info(
            f"merge spill cap: {max_spill_size / 2**30:.2f} GiB "
            f"({int(_MERGE_SPILL_FRACTION * 100)}% of {(eff_limit or 0) / 2**30:.2f} GiB "
            f"{_effective_mem_limit_source()})"
        )
    else:
        logger.info("merge spill cap: disabled (memory limit undetectable or opted out) — merge runs unbounded")
    logger.info(
        "merge target pruning: "
        + ("on (source stats derive an early filter)" if not streamed_exec
           else "off (streamed_exec=True — source streamed, whole target scanned)")
    )

    dt = _delta_table(path, storage_options)
    # Pin the target to the snapshot the model read (vB) so OCC validates (vB, HEAD] — one snapshot
    # for both the read and the commit. None leaves it at HEAD (prior safe-by-ordering behavior).
    if read_version is not None:
        dt.load_as_version(read_version)
    merger = dt.merge(
        source=data,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
        merge_schema=merge_schema,
        streamed_exec=streamed_exec,
        **spill_kwargs,
    )
    # dbt merge_update_condition / merge_insert_condition gate which matched rows update and which
    # unmatched rows insert. delta_rs expresses these as per-clause predicates (referencing the
    # target/source aliases), so they are honored for real here — not silently dropped. The caller
    # has already rewritten dbt's DBT_INTERNAL_DEST/SOURCE aliases to target/source.
    if insert_only:
        merger = merger.when_not_matched_insert_all(predicate=insert_condition)
    else:
        if update_columns:
            updates = {c: f"source.{c}" for c in update_columns}
            merger = merger.when_matched_update(updates=updates, predicate=update_condition)
        elif exclude_columns:
            merger = merger.when_matched_update_all(
                except_cols=list(exclude_columns), predicate=update_condition
            )
        else:
            merger = merger.when_matched_update_all(predicate=update_condition)
        merger = merger.when_not_matched_insert_all(predicate=insert_condition)
    # Spark "WHEN NOT MATCHED BY SOURCE THEN DELETE": optionally remove target rows the source
    # doesn't carry (full sync). True = all unmatched; a string = only those matching the predicate.
    # Composes with both branches above; default None adds nothing (dbt incremental paths unaffected).
    if delete_unmatched_by_source:
        by_source_pred = delete_unmatched_by_source if isinstance(delete_unmatched_by_source, str) else None
        merger = merger.when_not_matched_by_source_delete(predicate=by_source_pred)
    merger.execute()

    # Same threshold-gated maintenance as the append / delete+insert paths: a merged-on-every-run
    # incremental table fragments into small files and leaves tombstoned old versions otherwise.
    _maintain(_delta_table(path, storage_options), compaction_threshold)
