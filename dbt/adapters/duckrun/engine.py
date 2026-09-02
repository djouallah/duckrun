"""
Delta Lake write engine for the duckrun dbt adapter.

DuckDB produces the data and ``deltalake`` (delta_rs) materializes it. We pass the
DuckDB relation straight through: deltalake 1.x consumes any object exposing the Arrow
C-stream interface (``__arrow_c_stream__``), which DuckDB relations do — so there is no
pyarrow dependency.
"""
import ctypes
import hashlib
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

from dbt.adapters.events.logging import AdapterLogger
from deltalake import CommitProperties, DeltaTable, convert_to_deltalake, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

from dbt.adapters.duckrun.policy import (MaintenancePolicy, CHECKPOINT_INTERVAL,
                                         DEFAULT_TARGET_FILE_SIZE, ROW_GROUP_DEFAULT_ROWS)
from dbt.adapters.duckrun import sortkey

logger = AdapterLogger("Duckrun")

try:  # deltalake 1.x exposes WriterProperties at the top level
    from deltalake import WriterProperties
except ImportError:  # pragma: no cover - older layouts
    try:
        from deltalake.writer import WriterProperties
    except ImportError:
        WriterProperties = None

try:  # per-column knobs (dictionary_enabled, statistics_enabled) — deltalake 1.x
    from deltalake import ColumnProperties
except ImportError:  # pragma: no cover - older layouts
    try:
        from deltalake.writer import ColumnProperties
    except ImportError:
        ColumnProperties = None


# The FIXED row-group ceiling (6M rows) every write uses — see policy.ROW_GROUP_DEFAULT_ROWS.
# This alias is the default max_row_group_size _writer_properties uses when no explicit ceiling is
# passed; only a per-model max_row_group_size config moves it.
_ROW_GROUP_SIZE = ROW_GROUP_DEFAULT_ROWS
# Dictionary page limit: 32 MB. Caps how large one column's dictionary grows before its values
# overflow to PLAIN. A bigger limit keeps more MID/HIGH-cardinality columns dictionary-encoded, which
# shrinks the written files and gives the columnar reader denser, more uniform segments — the read
# layout this write path is tuned for. The cost is merge memory: a delta_rs MERGE reads the table and
# materializes those per-column dictionaries, so a larger limit means a larger merge working set.
# Measured on an 18M-row merge: the old 128 MB limit hit ~25 GB RSS, 8 MB ~4 GB, 16 MB ~8.7 GB — so
# 32 MB is a deliberate step further up the merge-memory curve, bought for read-layout density. Truly
# near-unique join keys (e.g. l_orderkey / l_comment) still overflow to PLAIN — their dictionary would
# be as big as the data, so no loss. This is the knob most likely to regress the merge-spill stress
# gate; tests/parquet_layout/aemo/ is the harness to confirm the read-side win against a real Direct Lake
# reference before trusting it.
_DICT_PAGE_SIZE_LIMIT = 32 * 1024 * 1024
# Data page byte limit (1 MB). Secondary bound only — the byte cap NEVER fires on a highly compressible
# column, so it can't cap the page on its own.
_DATA_PAGE_SIZE_LIMIT = 1_048_576
# Data page ROW-COUNT limit — the backstop for columns the byte cap can't see: the byte cap is checked
# on ENCODED page bytes, so a highly compressible column would otherwise buffer its whole row group
# as a single page (~10x write memory, and giant pages blow the merge's read-side spill cap → out of
# disk; arrow-rs #5797 / #4973). At 20k (arrow-rs's intended default) this cap fired FIRST on every
# column, overriding the 1 MB byte cap into ~20k-row micro-pages — and page count is pure read-side
# overhead: each page costs the reader a header parse, a decompressor setup, and an encoding-run
# restart. 1M lets the byte cap shape dense columns into ~1 MB pages (the page size every mainstream
# parquet writer targets) while still bounding the degenerate compressible case. Measured on the
# layout benchmark (tests/parquet_layout/aemo): ~40x fewer pages per chunk, cold column loads
# consistently faster, write memory unchanged (dense pages are bounded by bytes, not rows).
_DATA_PAGE_ROW_LIMIT = 1_000_000
# Target file size: 256 MB. A Parquet row group can't span files, so this byte cap is really a
# segment cap — and every file's LAST row group is truncated wherever the roll lands. That is why
# 128 MB (tried in 0.4.64, reverted in 0.4.65) lost: it doubled the file count, so the aemo mart
# went from ~2 roll-truncated tail groups to 4 of 25 (one a 1.25M-row runt at the bottom of the
# healthy 1-16M segment band). Revisit only when delta-rs can align the file roll to a row-group
# boundary. Not a merge-MEMORY lever (that was the dictionary page limit, see
# _DICT_PAGE_SIZE_LIMIT; 128/256/512 MB all merged in ~16s at ~4.5-5.2 GB RSS in the small-scale
# measurement) — but it IS a merge-DISK lever at scale (128 MB's doubled file count pushed the
# update-only merge's DataFusion spill from <59 to 60-100 GB). Deliberately far below 1 GB, which
# forced the whole-file copy-on-write that blew up merges on disk. Applied by every file write
# (build_write_deltalake_args) and the routine post-write compaction. MERGE is the exception — it
# sets no target_file_size. Defined in policy.py (the one read-layout target); aliased here so the
# many in-module references stay put.
_TARGET_FILE_SIZE = DEFAULT_TARGET_FILE_SIZE

# Max distinct partition values folded into a merge's `target.p IN (…)` prune predicate. Past this the
# IN list stops buying pruning (the source spans most of the table) and we let delta_rs plan on its own.
_PART_PRUNE_MAX = 400

def _writer_properties(row_group_rows=None):
    # The single read-layout writer config, used by every FILE write (append/overwrite/if_unchanged),
    # compaction, and the optimize sort-rewrite: SNAPPY, the fixed 6M-row group ceiling, a 32 MB dictionary page limit
    # (mid-card columns keep a remappable dictionary; high-card ones overflow to PLAIN — see
    # _DICT_PAGE_SIZE_LIMIT, the load-bearing merge-memory knob), a 1 MB data-page byte cap that shapes
    # dense columns into ~1 MB pages (the 1M-row cap only backstops ultra-compressible columns — see
    # _DATA_PAGE_ROW_LIMIT), and chunk-level stats.
    # MERGE deliberately does NOT use this — it passes no writer_properties (delta_rs defaults) so a
    # merge stays quick and never rewrites fat files, and so the known OOM-prone path does not also
    # take on a large write buffer; post-merge compaction folds merged files up into this layout
    # later. That exemption covers only a merge that actually reaches delta_rs: an insert-only merge
    # is routed to a DuckDB anti-join + plain append (see insert_delta) and IS written with this
    # profile. Degrade gracefully if the pinned wheel rejects a newer parameter (last rung:
    # SNAPPY-only).
    if WriterProperties is None:
        return None
    rg = row_group_rows if row_group_rows is not None else _ROW_GROUP_SIZE
    col_props = None
    if ColumnProperties is not None:
        try:
            col_props = ColumnProperties(dictionary_enabled=True, statistics_enabled="CHUNK")
        except Exception:  # best-effort: fall through to writer-level props without per-column knobs
            col_props = None
    for kwargs in (
        dict(compression="SNAPPY",
             max_row_group_size=rg,
             dictionary_page_size_limit=_DICT_PAGE_SIZE_LIMIT,
             data_page_size_limit=_DATA_PAGE_SIZE_LIMIT,
             data_page_row_count_limit=_DATA_PAGE_ROW_LIMIT,
             statistics_truncate_length=64,
             default_column_properties=col_props),
        dict(compression="SNAPPY", max_row_group_size=rg),
        dict(compression="SNAPPY"),
    ):
        try:
            return WriterProperties(**{k: v for k, v in kwargs.items() if v is not None})
        except Exception:  # best-effort: any build rejection tries the next, simpler rung
            continue
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

    On a busy shared box — a Fabric notebook sharing the node with another runtime and a
    background DuckDB job — this sits far below *total* RAM, and it's the number the budget must
    respect: total RAM would overcommit a process that doesn't own the whole node. Read FRESH on
    every call (no startup snapshot): _effective_mem_limit_bytes() is sampled right before each
    job (e.g. at the top of merge_delta, before the source relation is materialized), so the cap
    reflects the memory actually free at that moment — after whatever earlier models, a co-tenant
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
    or not on Linux. This is what matters on Fabric/k8s, where physical RAM is the
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
    at startup — so the cap tracks the memory actually free *now*, after earlier models / a co-tenant
    job have taken their share, instead of a stale connection-time
    snapshot.

    The available-RAM term is also what catches Fabric: there the cgroup is the unlimited *root*
    (`/proc/self/cgroup` = `0::/`, `memory.max` = `max`), so the cap would otherwise fall back to
    *total* node RAM — ignoring that another runtime, the kernel, and any background DuckDB job
    already hold most of it. Available RAM reflects that pressure; total RAM does not.

    But *our own* resident memory (DuckDB buffers, delta_rs pool, spill-file page cache) also lowers
    the available term, so a naive fresh sample would ratchet DOWN each model — each cap smaller partly
    because the previous model's memory hasn't been reclaimed yet, i.e. counting the process against
    itself. So add this process's RSS back into the available term: that memory is ours to reuse for
    the next job. The final min() with total/cgroup still clamps the result, so adding RSS back can
    never exceed the real physical/container ceiling — it just stops the self-throttling ratchet."""
    avail = _available_ram_bytes()
    if avail is not None:
        rss = _proc_rss_bytes()
        if rss:
            avail += rss  # reclaimable-by-us; the min() below re-clamps to total/cgroup
    vals = [v for v in (_total_ram_bytes(), _cgroup_mem_limit_bytes(), avail) if v]
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


# --------------------------------------------------------------- memory profiling (opt-in)
# A merge that OOMs has three suspects sharing one process: DuckDB (producing the source), the
# Arrow buffers delta_rs collects when streamed_exec=False, and delta_rs's own merge pool. RSS
# alone can't tell them apart. With DUCKRUN_MEM_PROFILE set, mem_profile() samples this process's
# RSS *and* DuckDB's own allocation through a write/merge and logs the split, so "who's the slob"
# is measured, not inferred. Off by default: no thread, no samples, no overhead in production.

def _proc_rss_bytes() -> Optional[int]:
    """Resident set size of THIS process in bytes — the number the OOM-killer actually watches;
    None if it can't be read. Linux: VmRSS from /proc/self/status. Windows: WorkingSetSize."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024  # value is in kB
    except (OSError, ValueError, IndexError):
        pass
    try:  # Windows: GetProcessMemoryInfo -> WorkingSetSize
        from ctypes import wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
                (n, ctypes.c_size_t) for n in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                    "PagefileUsage", "PeakPagefileUsage")
            ]
        # argtypes are required: GetCurrentProcess returns the pseudo-handle (-1), which overflows
        # ctypes' default int marshalling unless the parameter is typed as a HANDLE.
        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi = ctypes.windll.psapi
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(_PMC), ctypes.c_ulong]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        p = _PMC()
        p.cb = ctypes.sizeof(_PMC)
        if psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(p), p.cb):
            return int(p.WorkingSetSize)
    except Exception:
        pass
    return None


def _duckdb_mem_bytes(con):
    """(allocated_bytes, temp_spill_bytes) DuckDB currently holds, via duckdb_memory(); None on any
    error. Runs on a *separate* cursor so it's safe to call while another query streams on `con` —
    and this is a diagnostic-only path, so it must never raise into the real write/merge."""
    if con is None:
        return None
    try:
        cur = con.cursor()  # duckdb's cursor() is a new connection on the same instance
        try:
            row = cur.execute(
                "SELECT coalesce(sum(memory_usage_bytes), 0), "
                "coalesce(sum(temporary_storage_bytes), 0) FROM duckdb_memory()"
            ).fetchone()
        finally:
            cur.close()  # sampled every 0.1s while a profile runs — don't pile up cursors
        return (int(row[0]), int(row[1]))
    except Exception:
        return None


class _MemSampler:
    """Background RSS / DuckDB-memory sampler for one write or merge. See mem_profile()."""

    def __init__(self, label: str, con=None, interval: float = 0.1):
        self.label = label
        self.con = con
        self.interval = interval
        self._thread = None
        self._stop = None
        self.samples = 0
        self.peak_rss = 0
        self.duckdb_at_rss_peak = None        # DuckDB alloc at the instant RSS peaked
        self.duckdb_spill_at_rss_peak = None
        self.peak_duckdb = 0                  # DuckDB's own high-water, independently

    def __enter__(self):
        if not os.environ.get("DUCKRUN_MEM_PROFILE"):
            return self  # disabled: no thread, no overhead
        import threading
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"duckrun-mem-{self.label}", daemon=True)
        self._thread.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            rss = _proc_rss_bytes()
            dd = _duckdb_mem_bytes(self.con)
            self.samples += 1
            if dd is not None and dd[0] > self.peak_duckdb:
                self.peak_duckdb = dd[0]
            if rss is not None and rss > self.peak_rss:
                self.peak_rss = rss
                if dd is not None:
                    self.duckdb_at_rss_peak, self.duckdb_spill_at_rss_peak = dd
            self._stop.wait(self.interval)

    def __exit__(self, *exc):
        if self._thread is None:
            return False
        self._stop.set()
        self._thread.join(timeout=2.0)

        def mb(n):
            return "n/a" if n is None else f"{n / 2 ** 20:,.0f} MB"

        non_duck = None
        if self.peak_rss and self.duckdb_at_rss_peak is not None:
            non_duck = max(0, self.peak_rss - self.duckdb_at_rss_peak)
        logger.info(
            f"mem[{self.label}]: peak RSS={mb(self.peak_rss)} | "
            f"DuckDB peak={mb(self.peak_duckdb)} "
            f"(at RSS-peak {mb(self.duckdb_at_rss_peak)}, spill {mb(self.duckdb_spill_at_rss_peak)}) | "
            f"non-DuckDB~={mb(non_duck)} (delta_rs + Arrow) | samples={self.samples}"
        )
        return False


def mem_profile(label: str, con=None, interval: float = 0.1):
    """Context manager that profiles a write/merge's memory when DUCKRUN_MEM_PROFILE is set, else a
    no-op. Wraps an engine call so RSS, DuckDB's allocation, and the delta_rs/Arrow remainder are
    measured for that phase and logged once on exit. `con` (the DuckDB connection) enables the
    DuckDB-vs-delta_rs split; omit it to log RSS only. Diagnostic only — never affects the write."""
    return _MemSampler(label, con=con, interval=interval)


# The two delta_rs merge spill caps. ``max_spill_size`` bounds the merge's in-memory pool — the
# consumer that actually OOMs: profiling (DUCKRUN_MEM_PROFILE) measured that during a merge
# delta_rs holds ~99% of process RSS while DuckDB sits near ~15 MB. Sized to the WHOLE effective
# limit, not a per-thread slice — ``_MERGE_GATE`` (below) keeps that honest by letting only one
# delta_rs merge run at a time. Past the cap the merge spills to disk instead of OOM-killing the
# container; too small a cap makes delta_rs raise "Resources exhausted" instead (the merge can't
# fit its working set), so the cap gets the bulk of the budget.
_MERGE_SPILL_FRACTION = 0.6  # delta_rs merge max_spill_size (in-memory pool)
_MERGE_TEMP_DIR_FRACTION = 0.8  # delta_rs merge max_temp_directory_size, as a share of free spill disk
_MERGE_TEMP_DIR_RESERVE = 8 * 2 ** 30  # ceiling on the disk-spill reserve (see _default_merge_temp_dir_size)

# The one DuckDB memory_limit pin, applied once per connection (see _pin_memory_limit). It exists
# because DuckDB's own default memory_limit is 80% of *physical* RAM, and on a container
# (Fabric/k8s) physical RAM is the whole node, not our slice — so the default overcommits and the
# kernel OOM-kills us. 0.85 of the effective limit (which folds in available RAM, the only signal
# that reflects the container on Fabric where the cgroup is the unlimited root) leaves ~15% for the
# Parquet writer's row-group buffers, the Arrow batch in flight, and — the real reason for the
# margin — DuckDB overshooting its own *soft* memory_limit. 0.85 + the 0.6 merge cap above
# deliberately "sum past 1.0": they are not shares of a divided budget but two independent
# guardrails on different consumers, and measurement shows they never peak together (DuckDB is
# near-idle while a delta_rs merge runs). Safe because the job owns the machine (effective folds in
# currently-free RAM, so the OS/other processes are already excluded); on a shared box, drop back
# toward 0.8 to leave slack for other tenants growing mid-run.
_MEM_LIMIT_FRACTION = 0.85

# How many dbt models can be writing at once — dbt's `threads`, published here by the adapter
# (see set_run_threads). Used to divide the discovery pool (pool_workers) and by the plugin's
# multi-thread cursor guard. 1 for the connection API and for any dbt run that doesn't set
# `threads`. Process-global like POOL_WORKERS: one dbt run per process.
RUN_THREADS = 1


def set_run_threads(threads) -> None:
    """Record the run's dbt thread count. Called once by the adapter at startup."""
    global RUN_THREADS
    try:
        RUN_THREADS = max(1, int(threads))
    except (TypeError, ValueError):  # unset/odd value -> single-threaded, the safe default
        RUN_THREADS = 1


def pool_workers(n_items: int) -> int:
    """Worker count for a discovery pool of ``n_items``. Divided by the run's thread count so N
    concurrent dbt models don't multiply into N x POOL_WORKERS in-flight requests; floored at 4 so
    a wide project still gets some concurrency per model."""
    per_thread = POOL_WORKERS if RUN_THREADS <= 1 else max(4, POOL_WORKERS // RUN_THREADS)
    return max(1, min(per_thread, n_items))


# At most ONE delta_rs merge runs at a time, and it holds the FULL merge budget (the RAM pool
# and the disk spill cap below). The merge is the only write whose working set is a large share
# of the whole process budget — delta_rs holds ~99% of merge RSS while DuckDB sits near-idle —
# so the alternative, dividing the caps by the thread count, charges every merge for concurrency
# that usually isn't happening (one big merge at threads=4 would get a quarter of the budget with
# the other three threads running views and appends). Concurrent merges queue here instead; the
# other dbt threads keep running everything cheap (anti-join inserts, appends, overwrites) while
# a merge holds the gate. Two big merges overlapping would also thrash the one spill disk.
_MERGE_GATE = threading.Lock()


def _merge_spill_dir() -> str:
    """Where delta_rs (DataFusion) writes merge spill files: TMPDIR when set — the adapter points it
    at the roomy Fabric work disk in configure_duckdb_session — else the platform temp dir. This is
    the disk whose free space bounds the merge's on-disk spill."""
    import tempfile
    return os.environ.get("TMPDIR") or tempfile.gettempdir()


def _default_merge_temp_dir_size() -> Optional[int]:
    """delta_rs merge ``max_temp_directory_size`` default: the FREE space on the spill disk minus a
    reserve of ``min(20% of free, 8 GiB)``.

    DataFusion's DiskManager otherwise hard-caps on-disk merge spill at a flat 100 GB regardless of
    how big the disk is — so a wide-partition merge aborts with "Resources exhausted ... exceeded the
    allowable limit of 100.0 GB" on a box with terabytes free (exactly the Fabric work disk, ~1.9 TiB).
    This is a SEPARATE limit from ``max_spill_size`` (which bounds the in-*memory* pool): this one
    bounds bytes on disk. Sizing it to the actual disk lets a big merge spill as far as the disk allows.

    A reserve, not all of it, because DuckDB spills to the same disk during the merge (source
    collection) and the dbt target dir + delta_rs write staging also live there. The reserve's job
    needs gigabytes, not gigabytes-per-gigabyte: a purely proportional 20% stranded ~15 GB on a
    75 GB CI disk while a merge died at the cap with room left (v0.4.58 release gate), and ~380 GB
    on the Fabric work disk. So the proportional reserve holds below 40 GiB free (small disks keep
    exactly the old slack) and flattens to 8 GiB above it. Not divided by the thread count:
    ``_MERGE_GATE`` serializes delta_rs merges, so at most one is spilling at a time and it gets
    the whole disk. None if free space can't be read (then delta_rs keeps its 100 GB default).
    Override with ``merge_max_temp_directory_size``."""
    import shutil
    try:
        free = shutil.disk_usage(_merge_spill_dir()).free
    except Exception:
        return None
    # Derived from the same int(free * 0.8) the proportional rule always used, so below the
    # 40 GiB crossover the cap is byte-identical to the old behavior (no float-slop drift).
    proportional_reserve = free - int(free * _MERGE_TEMP_DIR_FRACTION)
    reserve = min(proportional_reserve, _MERGE_TEMP_DIR_RESERVE)
    return (free - reserve) or None


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
    """Always-on DuckDB tuning for duckrun, applied once per connection:
    ``preserve_insertion_order=false``, ``parquet_metadata_cache=true``, and a ``temp_directory`` to
    spill to. These are NOT the memory split — they're correctness/perf tuning for every connection.

    preserve_insertion_order=false: with DuckDB's default (true), streaming a large result into
    delta_rs makes DuckDB buffer the *whole* result to keep row order, which OOMs big writes /
    merges. Delta tables are unordered and explicit ORDER BY still works, so duckrun turns it
    off by default — users no longer need to set it in their profile ``settings``.

    parquet_metadata_cache=true: DuckDB defaults this OFF because a cached parquet footer goes stale
    if a file is overwritten in place — but Delta never rewrites a data file (a change writes NEW
    files with new names and tombstones the old), so the path→metadata mapping can't go stale here.
    Turning it on lets repeated ``delta_scan`` reads in a session reuse row-group stats instead of
    re-parsing every footer each scan — a real win over OneLake/remote where each footer is a
    round-trip. (DuckDB's data cache — ``enable_external_file_cache`` — is already on by default.)

    The ``memory_limit`` is pinned ONCE here, for every connection at any thread count
    (``_pin_memory_limit``): 85% of the container-aware effective limit, tighten-only so an
    explicit lower profile limit wins. There is no per-model or per-path memory dance beyond
    this — delta_rs merges are bounded separately by their own spill caps (``_merge_spill_caps``)
    and serialized on ``_MERGE_GATE``."""
    _pin_memory_limit(con)
    try:
        con.execute("SET preserve_insertion_order=false")
    except Exception:  # best-effort tuning: a failed SET must not abort connection setup
        pass
    try:
        # Safe because Delta data files are immutable (never overwritten in place), so a cached
        # footer can't go stale; lets repeated delta_scans reuse row-group metadata. Best-effort —
        # an older build without the setting must not abort connection setup.
        con.execute("SET parquet_metadata_cache=true")
    except Exception:
        pass
    # Spill location for BOTH DuckDB (temp_directory) and delta_rs / Python tempfile (TMPDIR). On a
    # Fabric notebook /home/trusted-service-user/work is a ~135 GiB local disk while / and /tmp are a
    # cramped ~19 GiB overlay; delta_rs stages writes to TMPDIR (default /tmp) and DuckDB spills to
    # temp_directory, so a large merge fills /tmp. Point both at the work disk. delta_rs (Rust) only
    # reads the TMPDIR env — SET temp_directory alone won't move it — so export TMPDIR too. Off Fabric
    # the work dir is absent: keep the cwd-based DuckDB spill and leave TMPDIR (system temp) untouched.
    _work = "/home/trusted-service-user/work"
    on_fabric = os.path.isdir(_work)
    spill_dir = os.path.join(_work if on_fabric else os.getcwd(), ".duckrun_spill")
    try:
        os.makedirs(spill_dir, exist_ok=True)
        if on_fabric:
            os.environ.setdefault("TMPDIR", spill_dir)  # delta_rs + tempfile off the tiny /tmp
    except Exception:  # best-effort: never abort connection setup over a spill dir
        pass
    # An in-memory DuckDB defaults to an empty temp_directory and then *cannot* spill; give it one so
    # the session's pinned memory_limit degrades to disk instead of erroring. Respect an explicit
    # temp_directory (a file-backed DB, or a user override).
    try:
        tmp = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    except Exception:  # best-effort: if we can't read it, skip overriding rather than guess
        tmp = "skip"  # couldn't read it; don't risk overriding
    if not tmp:
        try:
            con.execute(f"SET temp_directory='{spill_dir}'")
        except Exception:  # best-effort spill dir: failure just leaves the default in place
            pass


def read_memory_limit(con) -> Optional[str]:
    """DuckDB's current ``memory_limit`` as it reports it (e.g. '25.0 GiB'), or None if
    unreadable."""
    try:
        return con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    except Exception:  # best-effort: unreadable limit -> None (the pin then applies unconditionally)
        return None


def _pin_memory_limit(con) -> None:
    """Pin DuckDB's ``memory_limit`` once, at connection setup, at any thread count: 85%
    (``_MEM_LIMIT_FRACTION``) of the container-aware effective limit. This is the only place
    duckrun touches ``memory_limit`` — there is no per-model or per-path tightening. Bounding
    DuckDB matters because its own default is 80% of *host* physical RAM, blind to
    cgroups/containers (the original Fabric OOM); bounding it *further* around merges does not:
    profiling showed delta_rs holds ~99% of merge RSS, and delta_rs is bounded by its own spill
    caps (``_merge_spill_caps``) behind ``_MERGE_GATE``, not by DuckDB's limit.

    Tighten-only: an explicit lower profile limit wins; no-op when the effective limit is
    unknown (leave DuckDB's default alone rather than guess)."""
    limit = _effective_mem_limit_bytes()
    if not limit:
        return
    target = int(limit * _MEM_LIMIT_FRACTION)
    current = _parse_byte_size(read_memory_limit(con))
    if current is not None and current <= target:
        return  # an explicit lower profile limit wins
    try:
        con.execute(f"SET memory_limit='{target}B'")
        logger.info(
            f"DuckDB memory_limit pinned to {target / 2 ** 30:.2f} GiB "
            f"({int(_MEM_LIMIT_FRACTION * 100)}% of {limit / 2 ** 30:.2f} GiB "
            f"{_effective_mem_limit_source()}); delta_rs merges serialize on a gate with a "
            f"{int(_MERGE_SPILL_FRACTION * 100)}% spill cap"
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
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
) -> Dict[str, Any]:
    """Build kwargs for ``write_deltalake`` (deltalake >= 1.2).

    EVERY write through here — overwrite, replace-where and append alike — gets the one read-layout
    profile: the tuned writer properties plus the 256 MB ``target_file_size``. MERGE does not go
    through here at all and keeps delta_rs defaults.

    Append used to be excluded on the theory that appends are transient increments which
    threshold-gated compaction folds into the read layout on a later pass. That fold does not
    happen: compaction fires on small-file BYTE debt (>= 8 files under half-target, see
    ``policy.MaintenancePolicy``), so append files that are already a healthy size are invisible to
    the trigger and keep delta_rs's 1,048,576-row groups and 100 MB files forever — the bottom of
    the segment band, permanently, on exactly the append-only fact tables that can least afford it
    (issue #22). Writing the append with the profile is what closes that, and it costs
    nothing on a small append: ``max_row_group_size`` is a CEILING, and an increment that never
    reaches it is written exactly as before.

    ``row_group_rows`` is an EXPLICIT ceiling only (a per-model ``max_row_group_size``).
    ``None`` — every normal write — keeps the fixed ``_ROW_GROUP_SIZE`` ceiling and lets the
    file roll pick the group; nothing is derived.

    ``target_file_size`` (bytes) overrides the 256 MB default for THIS write — the per-model
    ``target_file_size_mb`` dbt config lands here. ``None`` keeps ``_TARGET_FILE_SIZE``.
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
    # "merge" evolves the schema (adds columns); "overwrite" replaces it wholesale (overwrite
    # mode only) — delta-rs's own schema_mode values, passed straight through.
    if schema_mode in ("merge", "overwrite"):
        args["schema_mode"] = schema_mode
    args["target_file_size"] = target_file_size if target_file_size is not None else _TARGET_FILE_SIZE
    # Applied only when this write CREATES the table; delta-rs ignores it on an existing one
    # (verified on the pinned wheel — an existing table's configuration is never touched, so a
    # user-set property can't be clobbered from here).
    args["configuration"] = {"delta.checkpointInterval": str(CHECKPOINT_INTERVAL)}
    wp = _writer_properties(row_group_rows=row_group_rows)
    if wp is not None:
        args["writer_properties"] = wp
    return args


def _delta_table(path: str, storage_options: Optional[Dict[str, str]]) -> DeltaTable:
    if storage_options:
        return DeltaTable(path, storage_options=storage_options)
    return DeltaTable(path)


# Worker cap for the discovery pools (delta-rs log opens here, delta_scan view binds in the
# adapter). The work is latency-bound, not CPU-bound — each unit is a handful of sequential
# HTTPS round trips at 30-100ms+ from outside Azure — so the cap sets how many waves a
# project-wide discovery pays: ~80 tables at 8 workers was 10 waves, at 32 it's 3 (#16).
# 32 concurrent requests is still modest for OneLake; throttling (429) is retried inside
# delta-rs / the DFS layer anyway.
POOL_WORKERS = 32


def open_delta_tables(targets):
    """One :class:`DeltaTable` (or None on failure) per ``(location, storage_options)`` in
    ``targets``, in order. Opens run concurrently: each is a Delta-log replay — several network
    round trips on a remote store — and discovery does one per relation, so serialized they were
    the dominant startup cost of read-only commands on OneLake (issue #16). Workers only
    construct the DeltaTable (a read-only delta-rs open); callers keep all DuckDB cursor work on
    their own thread. THE one concurrent-open helper for both the dbt adapter's discovery and
    the connection API's catalog refresh, so the two can't drift."""
    from concurrent.futures import ThreadPoolExecutor

    def _open(target):
        loc, so = target
        try:
            return _delta_table(loc, so)
        except Exception as exc:
            logger.debug(f"duckrun: could not open {loc!r} via delta-rs: {exc}")
            return None

    if len(targets) <= 1:
        return [_open(t) for t in targets]
    with ThreadPoolExecutor(max_workers=pool_workers(len(targets))) as pool:
        return list(pool.map(_open, targets))


def tmp_name(tag: str, key) -> str:
    """A deterministic temp-relation name for ``key`` (a table location or identifier tuple): a
    12-hex-digit SHA-1 prefix, replacing the earlier 32-bit-truncated ``hash()`` whose collision
    between two distinct paths mutated on one cursor would have had ``create or replace temp
    table`` silently clobber the other operation's temp mid-flight."""
    digest = hashlib.sha1(str(key).encode("utf-8", "surrogatepass")).hexdigest()[:12]
    return f"__duckrun_{tag}_{digest}"


def quote_ident(name) -> str:
    """A double-quoted SQL identifier from a possibly already-quoted, possibly padded name: strip
    whitespace and any quotes the caller put on, then quote and escape embedded quotes. Accepted by
    both DuckDB and datafusion — the one quoting shape for join keys / sort columns."""
    return '"' + str(name).strip().strip('"').replace('"', '""') + '"'


_BARE_COLUMN = re.compile(r"[A-Za-z_]\w*")


def quote_ident_if_needed(name) -> str:
    """``name`` verbatim when it is a bare identifier (``[A-Za-z_]\\w*`` — delta_rs binds those as-is,
    mixed case included), else :func:`quote_ident`. For the merge UPDATE/INSERT column maps: datafusion
    parses an unquoted name, so a column with a space or other punctuation ("Total Amount") only
    binds quoted, while the plain spelling is the shape every existing caller and test expects."""
    n = str(name).strip()
    if n.startswith('"') and n.endswith('"'):
        return quote_ident(n)  # normalize an already-quoted name (escape inner quotes)
    return n if _BARE_COLUMN.fullmatch(n) else quote_ident(n)


# DuckDB's naive timestamp spellings. Top-level columns only: a naive timestamp nested in a
# STRUCT/LIST types as "STRUCT(...)" and is left alone (documented gap in docs/limitations.md).
_NAIVE_TS_TYPES = {"TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"}


def _ntz_field_names(dt: DeltaTable) -> set:
    """Lower-cased names of the table's ``timestamp_ntz`` columns, from the Delta schema."""
    out = set()
    for f in dt.schema().fields:
        t = getattr(f.type, "type", None) or str(f.type)
        if str(t).lower() in {"timestamp_ntz", "timestampntz"}:
            out.add(f.name.lower())
    return out


def resolve_timestamp_ntz(timestamp_ntz: Optional[bool] = None) -> bool:
    """THE one resolution of the keep-NTZ escape hatch: an explicit per-model/caller value wins,
    else the ``DUCKRUN_TIMESTAMP_NTZ=1`` env var (the connect()/notebook spelling). True = keep
    naive timestamps as Delta ``timestamp_ntz`` (pre-#42 behavior)."""
    return (timestamp_ntz if timestamp_ntz is not None
            else os.environ.get("DUCKRUN_TIMESTAMP_NTZ", "0") == "1")


def coerce_naive_timestamps(data, *, path: str,
                            storage_options: Optional[Dict[str, str]] = None,
                            retype_ok: bool = False,
                            timestamp_ntz: Optional[bool] = None,
                            existing_dt: Optional[DeltaTable] = None):
    """Reproject naive TIMESTAMP columns to UTC-adjusted TIMESTAMPTZ before a Delta write
    (issue #42): delta_rs types a naive Arrow timestamp as Delta ``timestamp_ntz`` and stamps
    the ``timestampNtz`` table feature, and Fabric's SQL analytics endpoint silently OMITS such
    columns. ``timezone('UTC', col)`` — not a bare ``::TIMESTAMPTZ`` cast — so the naive value
    is read as a UTC wall clock regardless of the session ``TimeZone``. The inner
    ``CAST(.. AS TIMESTAMP)`` normalizes TIMESTAMP_S/_MS/_NS to microseconds (Delta stores
    microseconds; nanoseconds truncate).

    The result is another DuckDB relation (``data.query``), never an Arrow-level cast — the
    downstream seams keep relying on relation behavior (``.columns``,
    ``.limit(0)``, ``.create_view``).

    ``timestamp_ntz`` True keeps NTZ (the per-model escape hatch); None falls back to the
    ``DUCKRUN_TIMESTAMP_NTZ=1`` env var — one resolution point for all three write surfaces.

    Unless ``retype_ok`` (the write replaces the schema wholesale), columns that are ALREADY
    ``timestamp_ntz`` in the existing table are skipped — an append/merge must keep matching
    the target it lands in, so a pre-#42 table keeps working untouched — with one warning
    naming them and the ``--full-refresh`` way out. ``existing_dt`` reuses an open handle;
    otherwise one ``open_if_exists`` probe (None on a fresh create → coerce everything).

    Idempotent and cheap to re-invoke: an already-coerced relation has no naive columns left
    and returns unchanged without touching the log."""
    if not (hasattr(data, "types") and hasattr(data, "query")):
        return data  # not a DuckDB relation (e.g. a pyarrow Table on a fallback path)
    naive = [c for c, t in zip(data.columns, data.types)
             if str(t).upper() in _NAIVE_TS_TYPES]
    if not naive:
        return data
    if resolve_timestamp_ntz(timestamp_ntz):
        return data
    if not retype_ok:
        tgt = existing_dt if existing_dt is not None else open_if_exists(path, storage_options)
        if tgt is not None:
            ntz = _ntz_field_names(tgt)
            skipped = [c for c in naive if c.lower() in ntz]
            if skipped:
                logger.warning(
                    f"duckrun: column(s) {skipped} stay timestamp_ntz to match the existing "
                    f"table at '{path}' — Fabric's SQL analytics endpoint omits such columns. "
                    f"Rebuild (--full-refresh / CREATE OR REPLACE) to retype them to "
                    f"UTC-adjusted timestamp, or set timestamp_ntz: true to silence this."
                )
                naive = [c for c in naive if c.lower() not in ntz]
            if not naive:
                return data
    # Direct quoting, NOT quote_ident: these names come from data.columns verbatim, and
    # quote_ident's strip-then-requote (built for possibly-pre-quoted config input) would mangle
    # a name with a leading/trailing quote character.
    def _q(c):
        return '"' + str(c).replace('"', '""') + '"'
    repl = ", ".join(f"timezone('UTC', CAST({_q(c)} AS TIMESTAMP)) AS {_q(c)}" for c in naive)
    return data.query("__duckrun_tsutc", f"SELECT * REPLACE ({repl}) FROM __duckrun_tsutc")


def merge_on_predicate(unique_key, predicates: Optional[List[str]] = None) -> str:
    """The canonical MERGE ``ON`` predicate: ``target."k" = source."k" [AND …]`` for each join key,
    plus any extra predicates. THE one builder for both merge surfaces — :func:`merge_delta` and the
    dbt incremental plugin's clause-core path — so the two cannot drift on quoting or shape. Keys are
    quoted (mixed case / reserved word / spaces would otherwise emit invalid datafusion SQL)."""
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    conditions = [f"target.{quote_ident(k)} = source.{quote_ident(k)}" for k in keys]
    if predicates:
        conditions.extend(p for p in predicates if p)
    return " AND ".join(conditions)


def _fenced_write(args: Dict[str, Any], path: str, storage_options: Optional[Dict[str, str]],
                  read_version: int, *, refusal_prefix: str, refusal_suffix: str,
                  record_read_version: bool = False) -> None:
    """The single fenced (compare-and-swap) write primitive behind every pinned write: load the
    snapshot the caller read, hand ``write_deltalake`` the pinned ``DeltaTable`` (dropping the
    ``storage_options`` kwarg — they live on the table), and disable rebasing
    (``max_commit_retries=0``) so any commit that landed since ``read_version`` already took our
    target version and this write fails LOUDLY instead of silently landing on top of it.

    ``record_read_version`` stamps ``duckrun.readVersion`` into commitInfo so the fence is
    observable in the log. The refusal message is
    ``"<prefix>: table '<path>' changed since version <vB> (a concurrent write committed)<suffix>"``.
    """
    dt = _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    args["table_or_uri"] = dt
    args.pop("storage_options", None)
    kwargs: Dict[str, Any] = {"max_commit_retries": 0}
    if record_read_version:
        kwargs["custom_metadata"] = {"duckrun.readVersion": str(read_version)}
    args["commit_properties"] = CommitProperties(**kwargs)
    try:
        write_deltalake(**args)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"{refusal_prefix}: table '{path}' changed since version {read_version} "
            f"(a concurrent write committed){refusal_suffix}"
        ) from e


def table_exists(path: str, storage_options: Optional[Dict[str, str]] = None) -> bool:
    """Return True if a Delta table already exists at ``path``.

    Catch ONLY ``TableNotFoundError`` (the table genuinely isn't there) → False. Every other
    error — a transient ADLS/OneLake 503, an expired token, a permissions blip — is RE-RAISED.
    Swallowing those was a silent data-loss bug: a transient open failure at store time made an
    incremental (already row-filtered) write fall into the overwrite branch, replacing the table
    with just the increment. A real error must fail the run loudly, not look like "no table".
    """
    return open_if_exists(path, storage_options) is not None


def open_if_exists(path: str, storage_options: Optional[Dict[str, str]] = None) -> Optional[DeltaTable]:
    """The opened ``DeltaTable`` at ``path``, or None when the table genuinely isn't there — the
    same fail-loud contract as :func:`table_exists` (ONLY ``TableNotFoundError`` → None; transient/
    credential errors re-raise). Returning the handle lets a caller reuse ONE log open for the
    existence check, the read-version pin, and the operation itself — on OneLake every open is
    log-listing round trips, so a raw-SQL DELETE was paying for four."""
    try:
        return _delta_table(path, storage_options)
    except TableNotFoundError:
        return None


def delta_stats(cur, path: str, storage_options: Optional[Dict[str, str]] = None,
                dt: Optional[DeltaTable] = None):
    """Cheap table statistics for ``dbt docs generate``, read from the Delta **log** (no data scan).

    ``DeltaTable.get_add_actions()`` carries per-file ``num_records`` / ``size_bytes`` /
    ``modification_time``; summing rows+bytes and taking the latest mtime gives the whole table's
    stats without opening any data file. Aggregation goes through the DuckDB cursor (``cur``) via a
    replacement scan over the arro3 table — no pyarrow dependency.

    Returns ``{"num_rows", "bytes", "last_modified"}`` (last_modified = epoch milliseconds), or
    ``None`` on ANY failure (a drop-tombstone, a missing table, an unreachable/credential-less remote
    store). Best-effort by design: a statless catalog is fine, but a docs build must never break.
    ``dt`` reuses an already-opened handle instead of a fresh log open.
    """
    try:
        dtx = dt if dt is not None else _delta_table(path, storage_options)
        add_actions = dtx.get_add_actions()  # noqa: F841 (replacement scan)
        row = cur.sql(
            "select coalesce(sum(num_records), 0)::bigint, "
            "coalesce(sum(size_bytes), 0)::bigint, "
            "max(modification_time)::bigint from add_actions"
        ).fetchone()
    except Exception as exc:  # best-effort: docs stats must never fail catalog generation
        logger.debug(f"duckrun: no Delta stats for {path!r}: {exc}")
        return None
    if row is None:
        return None
    return {
        "num_rows": int(row[0]),
        "bytes": int(row[1]),
        "last_modified": int(row[2]) if row[2] is not None else None,
    }


def deleted_row_count(cur, dt) -> int:
    """Rows tombstoned by **deletion vectors** — still physically present in the parquet files, but
    logically gone. A parquet footer's ``num_rows`` counts them, so any row total read from the
    footers overstates the table by exactly this much (Fabric Warehouse and Spark write DVs on
    UPDATE/DELETE/MERGE; delta-rs does not).

    Only a table whose protocol declares the ``deletionVectors`` reader feature can have any, and
    that check is free — so tables without DVs (the overwhelming majority) pay nothing, and only
    those that can be affected pay for materialising the selection vectors. ``deletion_vectors()``
    yields one row per file that HAS a DV, with a keep-mask; the deleted count is the count of
    ``false`` in it, summed through the DuckDB cursor by replacement scan (no pyarrow)."""
    if "deletionVectors" not in (dt.protocol().reader_features or []):
        return 0
    deletion_vectors = dt.deletion_vectors()  # noqa: F841 - DuckDB replacement scan by name
    return int(cur.sql(
        "select coalesce(sum(len(selection_vector) "
        "  - coalesce(list_aggregate(selection_vector, 'sum'), 0)), 0)::bigint "
        "from deletion_vectors").fetchone()[0])


_DELTA_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName"


def _mapped_child_fields(dtype):
    """The named child fields of a Delta ``DataType`` — the recursion step of
    :func:`physical_to_logical`. Only a struct has fields (and so physical names); an array or a map
    is unwrapped to reach any struct inside it, and a primitive has none."""
    kind = getattr(dtype, "type", None)
    if kind == "struct":
        return list(dtype.fields)
    if kind == "array":
        return _mapped_child_fields(dtype.element_type)
    if kind == "map":
        return _mapped_child_fields(dtype.key_type) + _mapped_child_fields(dtype.value_type)
    return []


def physical_to_logical(dt) -> Dict[str, str]:
    """``{physical name: logical column name}`` for a Delta table with **column mapping** on — the
    translation the parquet footer and the log's per-column stats both need to be readable. Fabric
    Warehouse enables mapping on every table and Spark does whenever a table feature requires it, and
    then the footer and ``add.stats`` are keyed ``col-<guid>`` where the column name belongs;
    delta-rs never enables it, and then this is empty and those names already ARE the logical ones.
    (``add.partitionValues`` needs no translation — delta-rs resolves it back before
    ``get_add_actions`` flattens it.) Nested fields are walked too, because a parquet schema path
    names a nested column one level at a time and each level has its own physical name. Free: the
    schema is already in the snapshot the caller opened. Best-effort — anything unreadable yields
    ``{}``, i.e. the untranslated names."""
    out: Dict[str, str] = {}
    try:
        mode = str((dt.metadata().configuration or {}).get(
            "delta.columnMapping.mode", "")).strip().lower()
        if mode not in ("name", "id"):
            return {}
        pending = list(dt.schema().fields)
        while pending:
            f = pending.pop()
            physical = (f.metadata or {}).get(_DELTA_PHYSICAL_NAME_KEY)
            if physical:
                out[str(physical)] = f.name
            pending.extend(_mapped_child_fields(f.type))
    except Exception:  # best-effort: an unreadable mapping just leaves the physical names alone
        return {}
    return out


def delta_file_summary(cur, path: str, storage_options: Optional[Dict[str, str]] = None,
                       count_deleted: bool = True):
    """Active-file list (absolute paths) + total size + VORDER flag + deletion-vector row count for a
    Delta table — the Delta-log half of ``get_stats`` (the parquet footers are read separately by the
    caller). ``file_uris()`` gives the live files (tombstoned ones excluded) as DuckDB-readable paths
    (bare local paths / ``abfss://`` URIs); size comes from the ``add_actions`` replacement-scan as
    :func:`delta_stats` (no pyarrow). VORDER is read from the table metadata property (see below).
    ``deleted`` is what :func:`deleted_row_count` finds — the caller must subtract it from any row
    count taken off the parquet footers; ``count_deleted=False`` skips that read (returning ``0``)
    for callers that report no row count at all. The last element is :func:`physical_to_logical` —
    empty unless the table has column mapping on, in which case the parquet footers carry
    ``col-<guid>`` names the caller has to translate before showing them to anyone."""
    dt = _delta_table(path, storage_options)
    files = list(dt.file_uris())
    add_actions = dt.get_add_actions(flatten=True)  # noqa: F841 - DuckDB replacement scan by name
    size = int(cur.sql("select coalesce(sum(size_bytes), 0)::bigint from add_actions").fetchone()[0])
    # The Fabric writer records VORDER both as a per-file `add.tags` entry AND as the table property
    # `delta.parquet.vorder.enabled`. delta-rs `get_add_actions` does NOT surface per-file tags, so
    # read the property off the reconstructed metadata (survives checkpointing).
    config = dt.metadata().configuration or {}
    vorder = str(config.get("delta.parquet.vorder.enabled", "")).strip().lower() == "true"
    return (files, size, vorder, deleted_row_count(cur, dt) if count_deleted else 0,
            physical_to_logical(dt))


def _log_ndv_cap(cur, type_str: str, qmn: str, qmx: str):
    """Exact NDV upper bound from a **discrete** column's value range, read from the Delta log:
    ``bool → 2``; ``int/date → global_max − global_min + 1`` (a DATE casts to its day count, so the same
    span arithmetic works). ``None`` for continuous/string types — a float span or a truncated string
    min/max bounds nothing useful. Best-effort: any cast failure just yields ``None``."""
    t = type_str.upper()
    if t.startswith("BOOL"):
        return 2
    if not (t.startswith("DATE") or "INT" in t):
        return None
    try:
        v = cur.sql(
            f'select (max("{qmx}")::HUGEINT - min("{qmn}")::HUGEINT + 1) from add_actions '
            f'where "{qmn}" is not null and "{qmx}" is not null').fetchone()[0]
        return int(v) if v is not None and v > 0 else None
    except Exception:
        return None


def delta_column_stats(cur, path: str, cols, types, storage_options: Optional[Dict[str, str]] = None):
    """Per-column statistics read from the Delta **log** only (no data scan), for the sort-key profiler.
    ``get_add_actions(flatten=True)`` carries, per active file, ``num_records`` and — for the first
    ``dataSkippingNumIndexedCols`` (default 32) columns — ``null_count.<c>`` / ``min.<c>`` / ``max.<c>``.
    Aggregating those over the (small) file list gives, per column:

    - ``null_frac`` — table-wide null share (``S1``): a mostly-null column is a poor thing to organise by
      and shouldn't burn one of the few sort-key slots.
    - ``constancy`` — fraction of files with ``min == max`` (``S2``): the file-granularity clustering of
      the *current* layout. Reserved for later waves (it says nothing about a from-scratch re-sort).
    - ``ndv_cap`` — exact NDV upper bound for a discrete column (``S3``, ``max − min + 1``): lets the
      sample profiler tighten (and sanity-cap) its HLL estimate for free, ``None`` when the type gives no
      usable bound.

    A column past the indexed-column cap, or a statless writer, simply gets no entry — the caller falls
    back to the sample profile for it. ``cols`` are the LOGICAL names (they come off a ``delta_scan``),
    and a column-mapped table keys its ``add.stats`` by the PHYSICAL one, so each key is looked up
    logical-first then physical — otherwise every column of a Fabric Warehouse / Spark table misses and
    the profiler silently loses its null shares. Returns ``({c: {"null_frac", "constancy", "ndv_cap"}},
    num_files, total_rows)`` keyed by the logical name; ``({}, 0, 0)`` on ANY failure — a best-effort
    refinement, never a hard dependency."""
    try:
        dt = _delta_table(path, storage_options)
        add_actions = dt.get_add_actions(flatten=True)  # noqa: F841 - DuckDB replacement scan by name
        have = {d[0] for d in cur.sql("select * from add_actions limit 0").description}
        nfiles, total = cur.sql(
            "select count(*), coalesce(sum(num_records), 0)::bigint from add_actions").fetchone()
        nfiles, total = int(nfiles or 0), int(total or 0)
        phys = {logical: physical for physical, logical in physical_to_logical(dt).items()}
        out = {}
        for c in cols:
            k = c if f"min.{c}" in have else phys.get(c, c)
            nc, mn, mx = f"null_count.{k}", f"min.{k}", f"max.{k}"
            if nc not in have or mn not in have or mx not in have:
                continue
            qnc, qmn, qmx = (s.replace('"', '""') for s in (nc, mn, mx))
            nulls, const, withstats = cur.sql(
                f'select coalesce(sum("{qnc}"), 0)::bigint, '
                f'  coalesce(sum(case when "{qmn}" = "{qmx}" then 1 else 0 end), 0)::bigint, '
                f'  coalesce(sum(case when "{qmn}" is not null then 1 else 0 end), 0)::bigint '
                f'from add_actions').fetchone()
            withstats = int(withstats or 0)
            if not withstats:
                continue
            out[c] = {"null_frac": (int(nulls) / total) if total else 0.0,
                      "constancy": int(const) / withstats,
                      "ndv_cap": _log_ndv_cap(cur, types.get(c, ""), qmn, qmx)}
        return out, nfiles, total
    except Exception as exc:
        logger.debug(f"duckrun: no Delta column stats for {path!r}: {exc}")
        return {}, 0, 0


def auto_sort_cols(cur, source, *, partition_cols=None, label=("model", "sort_by=auto"),
                   full_rows=None):
    """Run the sort-key recommender over any FROM-able ``source`` and return ``(cols, lines)``:
    the recommended ORDER BY columns (``[]`` if nothing pays off) and the advisory lines for the
    CALLER to print/log. The one seam behind dbt's ``sort_by='auto'`` and the connection API's
    relation fallback for ``SORTED BY AUTO``. The write itself is NOT sized here — an AUTO write
    lands on the same fixed geometry (6M-row ceiling, 256 MB files) as every other write.

    ``source`` MUST be cheap to scan repeatedly — the recommender reads it several times. The
    CALLER is responsible for that: both surfaces materialize a PROFILING SUBSTRATE (a full local
    copy at/below ``sortkey.SUBSTRATE_CAP`` rows, a deterministic ``hash(row) % K`` subset above
    it — see ``sortkey.substrate_modulus``) and pass that in. When the substrate is partial, pass
    the full table's exact count as ``full_rows``: the profile then reports/suppresses per the v6
    contract. This function deliberately does NOT make its own copy — the deleted reservoir sample
    is the cautionary tale (see the ``sortkey`` module docstring).

    ``partition_cols`` keeps declared partition columns from burning a sort-key slot. No Delta-log
    stats are passed here (there is no table behind a staged relation), so null shares and NDV caps
    come out empty and the profile rests on the data alone."""
    desc = cur.sql(f"DESCRIBE SELECT * FROM {source}").fetchall()
    cols = [d[0] for d in desc]
    types = {d[0]: str(d[1]) for d in desc}
    if not cols:
        return [], []
    cost = {}
    rows, _, lines = sortkey.recommend_sort_key(
        cur, label[0], label[1], source, cols, types, list(partition_cols or []),
        profile_info=cost, total_rows=full_rows)
    # The profile is the expensive half of an AUTO write (measured: ~2/3 of a 591.7M-row build),
    # so its cost gets one INFO line here — the shared seam.
    if cost:
        of = (f" of {cost['total_rows']:,}" if cost["total_rows"] > cost["rows"] else "")
        logger.info(f"duckrun: sort profile of {label[1]}: {cost['scans']} scans over "
                    f"{cost['rows']:,}{of} rows in {cost['seconds']}s")
    # rows follow sortkey._SCHEMA: [1]=in_sort_key, [2]=sort_position, [3]=column.
    key = [r[3] for r in sorted((x for x in rows if x[1]), key=lambda x: x[2])]
    return key, lines


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
    return docs_from_dt(dt)


def docs_from_dt(dt):
    """(relation_description, {column: description}) from an already-opened ``DeltaTable`` — the
    handle-reusing core of :func:`read_delta_docs`, so discovery can serve the tombstone check and
    the docs read from ONE log open per relation. Best-effort throughout."""
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


def table_version(path: str, storage_options: Optional[Dict[str, str]] = None,
                  dt: Optional[DeltaTable] = None) -> int:
    """Current (HEAD) Delta version of the table at ``path``. The ``vB`` a caller captures before
    reading a source, to pin a later merge/replace to the same snapshot (single-snapshot MERGE
    semantics). ``dt`` reuses an already-opened handle instead of a fresh log open — but every
    version capture still goes THROUGH this function: it is the seam the correctness suite uses to
    inject a stale read (monkeypatching it to a past vB), so callers must not read ``dt.version()``
    directly."""
    return (dt if dt is not None else _delta_table(path, storage_options)).version()


def table_history(path: str, storage_options: Optional[Dict[str, str]] = None,
                  limit: Optional[int] = None) -> List[Dict]:
    """Delta commit history (delta_rs ``DeltaTable.history``) — newest first; each entry is a dict
    with ``version``, ``timestamp``, ``operation``, etc. ``limit`` caps how many commits are read."""
    return _delta_table(path, storage_options).history(limit)


def convert_to_delta(path: str, storage_options: Optional[Dict[str, str]] = None,
                     *, partition_by=None, mode: str = "error") -> None:
    """Write a Delta ``_delta_log`` over an existing parquet directory IN PLACE (delta-rs
    ``convert_to_deltalake``) — zero-copy, the parquet files are not rewritten. ``partition_by`` is a
    pyarrow ``Schema`` of the Hive-partition columns (None for an unpartitioned dir). ``mode='error'``
    (delta-rs default) raises if ``path`` is already a Delta table; ``'ignore'`` makes it a no-op."""
    kwargs: Dict = {"mode": mode}
    if storage_options:
        kwargs["storage_options"] = storage_options
    if partition_by is not None:
        kwargs["partition_by"] = partition_by
        kwargs["partition_strategy"] = "hive"
    convert_to_deltalake(path, **kwargs)


# Post-write maintenance gates (see _maintain). The compaction trigger is the same byte policy the
# Tier-0 safe button uses (compaction_debt): small files under half the target, enough of them AND
# enough small bytes to be worth a rewrite. Vacuum and cleanup are decoupled from it — each reclaims
# nothing on a fresh table, so running them on every microbatch write is pure store-listing cost.
_MAINT_CLEANUP_EVERY = 50                        # commits between metadata cleanups
_last_cleanup_version: Dict[str, int] = {}       # path -> version at its last cleanup (per process)

# The compact/vacuum decision thresholds live in policy.MaintenancePolicy now instead of being inlined
# at each write path. Built from the LIVE target size (read here, not frozen at import) so a
# reconfigured/patched target is honored — the thresholds themselves are fixed: >= 8 files under half
# the target AND >= 2x the target in small bytes, vacuum no more often than the safe 7-day retention.
def _policy() -> MaintenancePolicy:
    return MaintenancePolicy(target_file_size=_TARGET_FILE_SIZE)


def _last_vacuum_age_s(dt: DeltaTable) -> float:
    """Seconds since the last VACUUM commit (delta_rs logs VACUUM START/END to the history). No VACUUM
    in the recent window — a table never vacuumed, or one whose last vacuum aged out — is infinitely
    old, i.e. due."""
    for entry in dt.history(limit=200):
        if str(entry.get("operation", "")).startswith("VACUUM"):
            ts = entry.get("timestamp")
            return float("inf") if ts is None else (int(time.time() * 1000) - ts) / 1000.0
    return float("inf")


def _cleanup_due(path: str, dt: DeltaTable) -> bool:
    """True once the log has grown at least _MAINT_CLEANUP_EVERY commits since we last cleaned it.
    cleanup_metadata writes no commit, so the marker is per-process (a dict, not the log): it runs at
    a process's first maintenance of a table, then every _MAINT_CLEANUP_EVERY commits within that
    process — enough to keep the log bounded without listing it on every microbatch write."""
    return dt.version() - _last_cleanup_version.get(path, -_MAINT_CLEANUP_EVERY) >= _MAINT_CLEANUP_EVERY


def _maintain(cur, path: str, storage_options: Optional[Dict[str, str]] = None, *,
              target_file_size: Optional[int] = None,
              row_group_cap: Optional[int] = None) -> None:
    """Best-effort post-write upkeep shared by the append / merge / delete+insert paths: compact
    small files when the byte debt is worth it, then (only if a compaction happened) vacuum, and —
    on its own log-growth gate — clean up expired log entries. Without it a table written on every
    run fragments into small files and keeps old versions forever. (The overwrite path vacuums
    unconditionally instead and does not use this.)

    NEVER fatal. It runs *after* the data has already committed, so a lost maintenance commit (a
    compaction that races a concurrent writer) must not fail the model — the durable outcome the
    caller asked for already succeeded, and the byte trigger simply re-fires next run since the small
    files are still small. So CommitFailedError is caught and logged, not raised.

    The compaction trigger matches the Tier-0 safe button exactly (compaction_debt: at least 8 files
    under half the target AND at least 2x the target in small bytes). compact() reuses the same
    _writer_properties() read layout (the fixed 6M-row ceiling) and _TARGET_FILE_SIZE every file
    write uses, so maintenance (including the consolidation of files a lean MERGE left behind) keeps
    the uniform Direct Lake layout.

    The byte debt is measured with the DuckDB cursor ``cur`` (a replacement scan over the Delta log's
    add-actions); a caller with no cursor to lend — a bare engine write outside a session — simply
    gets no maintenance. Every production write path (the dbt plugin, the DataFrame writer, the raw-DML
    router, the table handles) wires one, so this only skips ad-hoc/direct engine calls.

    ``target_file_size`` / ``row_group_cap`` carry a model's EXPLICIT write geometry (the
    ``target_file_size_mb`` / ``max_row_group_size`` dbt configs) into maintenance: without them, the
    very next compaction would re-fold the model's files back into the global 256 MB / fixed-6M
    layout, silently undoing the config the write just honored. ``None`` = the defaults, unchanged."""
    if cur is None:
        return
    # NEVER fatal means EVERY exception, not just the lost-race CommitFailedError the policy
    # swallows: the table open, the debt scan, compact, vacuum and cleanup_metadata all run after
    # the data commit, and a transient store fault there (a 503, a token that expired mid-run, a
    # DeltaError) used to propagate and report the model FAILED although its rows had landed — and
    # for an append model the retry then appends the same rows again. Warn and move on.
    try:
        _maintain_inner(cur, path, storage_options, target_file_size, row_group_cap)
    except Exception as e:
        logger.warning(f"post-write maintenance skipped (data commit already succeeded): {e}")


def _maintain_inner(cur, path, storage_options, target_file_size, row_group_cap) -> None:
    dt = _delta_table(path, storage_options)
    debt = compaction_debt(cur, path, dt=dt, storage_options=storage_options)
    _target = target_file_size if target_file_size is not None else _TARGET_FILE_SIZE
    policy = MaintenancePolicy(target_file_size=_target)

    # An explicit per-model ceiling wins verbatim — it IS the declared layout; None keeps the fixed
    # _ROW_GROUP_SIZE ceiling, same as every write (no derived sizing, no log row count taken).
    _compact_rg = row_group_cap

    def _compact():
        dt.optimize.compact(target_size=_target,
                            writer_properties=_writer_properties(row_group_rows=_compact_rg))

    def _vacuum():  # past the retention-length gate only; a compaction just ran → compacted=True
        if policy.should_vacuum(compacted=True, last_vacuum_age_s=_last_vacuum_age_s(dt)):
            dt.vacuum(dry_run=False)

    # The policy owns the compact trigger and swallows a lost-race CommitFailedError (the data commit
    # already succeeded, so the byte trigger simply re-fires next run) — maintenance never fails the write.
    policy.run_maintenance(_compact, _vacuum, should=policy.should_compact(debt["small_sizes"]))

    # Metadata cleanup is on its OWN log-growth gate, independent of whether a compaction fired.
    try:
        if _cleanup_due(path, dt):
            dt.cleanup_metadata()
            _last_cleanup_version[path] = dt.version()
            # Bound the per-process marker dict (a long-lived notebook touching many tables would
            # grow it forever). Evicting the oldest entry only means that table's next maintenance
            # re-runs an idempotent cleanup_metadata a bit early — cheap and harmless. The pop
            # default absorbs two threads racing to evict the same key (the dict is module-global).
            while len(_last_cleanup_version) > 512:
                _last_cleanup_version.pop(next(iter(_last_cleanup_version)), None)
    except CommitFailedError as e:
        logger.warning(f"post-write metadata cleanup skipped (data commit already succeeded): {e}")


def write_delta(
    path: str,
    data,
    mode: str = "overwrite",
    *,
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    read_version: Optional[int] = None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """
    Materialize ``data`` (a DuckDB relation / Arrow C-stream) to Delta and maintain it.

      - overwrite: write, then vacuum (safe 7-day default) + cleanup_metadata
      - append:    write, then best-effort compact/vacuum/cleanup on the byte trigger (``_maintain``)
      - ignore:    write only if the table does not already exist

    ``merge_schema`` evolves the schema (adds columns); ``overwrite_schema`` replaces it wholesale
    (overwrite mode only — Delta's ``overwriteSchema``). They are mutually exclusive.

    ``read_version`` turns the write into a compare-and-swap: the fenced siblings
    ``append_if_unchanged`` / ``overwrite_if_unchanged`` route their writes through here (this is the
    ONE Delta write seam), so a fenced write is pinned to the caller's snapshot and fails loudly on a
    concurrent commit instead of clobbering it. ``None`` = the unfenced last-writer-wins write.

    Every write lands in the one read-layout profile (tuned writer properties + 256 MB files) —
    append included, with the same fixed 6M row-group ceiling everywhere. Nothing is derived from
    the result: no planner estimate, no count, no prior-log probe (the only self-sizing write is
    SORTED BY AUTO, whose profile already paid for an exact count).

    An EXPLICIT ``row_group_rows`` (the ``max_row_group_size`` dbt config) overrides the fixed
    ceiling verbatim and, with ``target_file_size`` (bytes), is carried into post-write maintenance
    so compaction preserves it.
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

    # Naive-timestamp coercion (issue #42) at the seam, so every surface writes the same relation.
    # A schema-replacing overwrite may retype freely; anything else must keep matching an existing
    # NTZ target (target-aware skip).
    data = coerce_naive_timestamps(
        data, path=path, storage_options=storage_options,
        retype_ok=(schema_mode == "overwrite"),
        timestamp_ntz=timestamp_ntz, existing_dt=existing_dt,
    )

    args = build_write_deltalake_args(
        path, data, mode,
        schema_mode=schema_mode,
        partition_by=partition_by,
        storage_options=storage_options,
        row_group_rows=row_group_rows,
        target_file_size=target_file_size,
    )
    if read_version is None:
        write_deltalake(**args)
    else:
        # Fenced (compare-and-swap) via the one _fenced_write primitive. Kept here — not in the
        # fenced wrappers — so this stays the single seam a streamed mid-write race is observable at.
        # duckrun.readVersion is recorded in commitInfo so the fence is OBSERVABLE: a fenced append
        # (the self-reading `insert into t select … from t`, or safeappend) carries it, a blind
        # last-writer-wins append does not — a concurrent writer / external reader can tell them
        # apart in the log.
        _fenced_write(args, path, storage_options, read_version,
                      refusal_prefix=f"{mode} refused", refusal_suffix=". Re-read and retry.",
                      record_read_version=True)

    if mode == "overwrite":
        # Post-commit housekeeping, never fatal (same contract as _maintain): the overwrite has
        # already landed, so a store fault while reclaiming old versions must not report it failed.
        try:
            dt = _delta_table(path, storage_options)
            # A fresh table (this overwrite created v0) has no prior versions — nothing for vacuum or
            # cleanup_metadata to reclaim, so skip both store-listing operations on a brand-new create.
            if dt.version() > 0:
                dt.vacuum(dry_run=False)  # safe default 168h retention (no concurrent reader broken)
                dt.cleanup_metadata()
        except Exception as e:
            logger.warning(f"post-overwrite vacuum/cleanup skipped (data commit already succeeded): {e}")
    else:  # append
        _maintain(cur, path, storage_options=storage_options,
                  target_file_size=target_file_size, row_group_cap=row_group_rows)


def create_empty_delta(
    path: str,
    schema,
    *,
    mode: str = "error",
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Create an EMPTY Delta table at ``path`` from an Arrow ``schema`` (no data files).

    Used by the connection API's bare ``CREATE TABLE (col defs)``: it logs a ``CREATE TABLE``
    operation rather than a ``WRITE``/``Overwrite``, which is what a create — not an overwrite —
    should record. ``mode`` follows delta-rs: ``error`` (fail if the table exists), ``overwrite``
    (replace an existing table or drop-tombstone), or ``ignore`` (no-op if it exists).
    """
    DeltaTable.create(path, schema, mode=mode, storage_options=storage_options,
                      configuration={"delta.checkpointInterval": str(CHECKPOINT_INTERVAL)})


def append_if_unchanged(
    path: str,
    data,
    *,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """
    Optimistic ("safe") append: append ``data`` only if the table version has not moved since
    we read it — otherwise refuse with ``CommitFailedError``.

    delta_rs has no native conditional / compare-and-swap commit. A plain append normally
    auto-rebases onto the latest version (appends are non-conflicting), so it can never fail on
    a concurrent write. We instead pin the write to the snapshot we read — a ``DeltaTable``
    loaded at ``read_version`` — and pass ``max_commit_retries=0`` so delta_rs does NOT rebase:
    if any commit landed since that snapshot, the append's target version is already taken and
    the commit fails. That is compare-and-swap on the table version. ``read_version`` is REQUIRED
    (no blind-HEAD path); the lazy read may see a newer version than ``read_version`` and that is
    fine — the commit simply fails, so nothing stale lands (delta_rs cannot pin an append's read).

    Dedup is NOT performed — that is the model SQL's job. This only guarantees the append is
    atomic with respect to the version it was computed against; on a conflict the caller should
    re-run the model against the new HEAD. After a successful append, run the same threshold-
    gated maintenance as the plain append path.
    """
    if read_version is None:
        raise ValueError(
            "append_if_unchanged requires read_version (the version the caller read). A safe "
            "append must be pinned to its snapshot — a brand-new table's first write goes through "
            "write_delta, not here."
        )
    # Route through the single write seam with the CAS pin; the append branch then runs the same
    # threshold-gated maintenance as a plain append.
    write_delta(
        path, data, "append",
        partition_by=partition_by,
        merge_schema=merge_schema,
        storage_options=storage_options,
        cur=cur,
        read_version=read_version,
        row_group_rows=row_group_rows,
        target_file_size=target_file_size,
        timestamp_ntz=timestamp_ntz,
        existing_dt=existing_dt,
    )


def overwrite_if_unchanged(
    path: str,
    data,
    *,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """Optimistic FULL-TABLE overwrite: replace every row with ``data`` only if the table version
    has not moved since we read it — otherwise refuse with ``CommitFailedError``. The overwrite
    sibling of :func:`append_if_unchanged`, for the read-whole-table -> recompute -> write-it-back
    pattern.

    Same compare-and-swap trick: pin to ``read_version`` and ``max_commit_retries=0`` so a concurrent
    commit fails the overwrite instead of clobbering it. (An overwrite, like an append, is
    non-conflicting to delta_rs's checker — it would otherwise just replace whatever HEAD is — so
    strict version CAS is the only way to make it fail-loud.) ``read_version`` is REQUIRED; a
    brand-new table's first write goes through ``write_delta``, not here. Then the same vacuum +
    metadata cleanup as the plain overwrite path.

    ``cur`` is the connection that produced ``data`` — forwarded so the post-write hooks behave
    exactly as on every other overwrite."""
    if read_version is None:
        raise ValueError(
            "overwrite_if_unchanged requires read_version (the version the caller read). A fenced "
            "overwrite must be pinned to its snapshot — a brand-new table goes through write_delta."
        )
    # Route through the single write seam with the CAS pin. write_delta(overwrite) then vacuums +
    # cleans up exactly as the plain overwrite path does, so the replaced version's files retire.
    write_delta(
        path, data, "overwrite",
        partition_by=partition_by,
        overwrite_schema=overwrite_schema,
        storage_options=storage_options,
        cur=cur,
        read_version=read_version,
        row_group_rows=row_group_rows,
        target_file_size=target_file_size,
        timestamp_ntz=timestamp_ntz,
        existing_dt=existing_dt,
    )


def replace_where(
    path: str,
    data,
    predicate: str,
    *,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """``replaceWhere`` / ``INSERT OVERWRITE`` as a SINGLE atomic Delta commit: atomically
    replace the rows matching ``predicate`` with ``data``. One commit, not a delete-then-append
    pair — so there is no torn-read window (a reader never sees the range emptied-but-not-refilled)
    and no half-applied failure state.

    ``predicate`` is a delta_rs/datafusion SQL expression. Keep it CAST-free: delta_rs can't
    serialize a CAST expression back to a string ("Unable to convert expression to string").

    A replaceWhere is a read-modify-write, so ``read_version`` is REQUIRED (no blind-HEAD path):
    the overwrite is pinned to that snapshot and committed with ``max_commit_retries=0``
    (compare-and-swap), so a concurrent writer that lands since ``vB`` fails the commit loudly
    instead of silently interleaving. Maintenance always runs at a fresh HEAD afterward (never
    pinned)."""
    if read_version is None:
        raise ValueError(
            "replace_where requires read_version (the version the caller read). A replaceWhere is "
            "a read-modify-write and must be pinned to its snapshot."
        )
    # No DERIVED row_group_rows — like every write now: a replaceWhere keeps the fixed
    # _ROW_GROUP_SIZE ceiling and lets the file roll pick the group.
    # An EXPLICIT row_group_rows (per-model config) is different in kind: a declared CEILING, not a
    # derived size — it applies to a slice exactly as it applies to an append.
    # A replaceWhere writes INTO an existing schema, never replaces it — target-aware skip applies.
    data = coerce_naive_timestamps(
        data, path=path, storage_options=storage_options,
        timestamp_ntz=timestamp_ntz, existing_dt=existing_dt,
    )
    args = build_write_deltalake_args(
        path, data, "overwrite", partition_by=partition_by, storage_options=storage_options,
        row_group_rows=row_group_rows, target_file_size=target_file_size,
    )
    args["predicate"] = predicate  # replaceWhere: overwrite ONLY the rows matching the predicate
    # Pin to the read snapshot and disable rebasing (CAS), so a concurrent commit since vB fails
    # this overwrite instead of landing on top of it.
    _fenced_write(args, path, storage_options, read_version,
                  refusal_prefix="replaceWhere", refusal_suffix="; replace refused. Re-run.")

    # Maintenance ALWAYS at a fresh HEAD — never the pinned snapshot (a stale file list would
    # compact/vacuum files live versions still reference and corrupt the table).
    _maintain(cur, path, storage_options=storage_options,
              target_file_size=target_file_size, row_group_cap=row_group_rows)


def replace_window(
    path: str,
    data,
    *,
    column: str,
    start: str,
    end: str,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """Microbatch window replace: atomically replace the rows in ``[start, end)`` on ``column``
    with ``data`` (the batch's rows) — the Delta-native equivalent of dbt's microbatch "delete the
    window, insert the batch", as ONE atomic commit (``replaceWhere``). ``start``/``end`` are
    naive ``YYYY-MM-DD HH:MM:SS`` strings (UTC batch bounds from dbt). Delegates to
    :func:`replace_where` with the window predicate; ``read_version`` pins/fences the commit."""
    # CAST-free window predicate — see replace_where. delta_rs coerces the string literals to the
    # column's type, so this works whether event_time is a DATE or a TIMESTAMP.
    _col = quote_ident(column)
    predicate = f"{_col} >= '{start}' AND {_col} < '{end}'"
    replace_where(
        path, data, predicate,
        read_version=read_version, partition_by=partition_by,
        storage_options=storage_options, cur=cur,
        row_group_rows=row_group_rows, target_file_size=target_file_size,
        timestamp_ntz=timestamp_ntz, existing_dt=existing_dt,
    )


def delete_rows(
    path: str,
    predicate: Optional[str] = None,
    *,
    read_version: Optional[int],
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    dt: Optional[DeltaTable] = None,
) -> None:
    """Delete rows matching ``predicate`` (a delta_rs/datafusion SQL expression), or every row
    when ``predicate`` is None. The Delta-native ``DELETE FROM`` for the connection API.
    ``dt`` reuses an already-opened handle (e.g. the statement's existence check) instead of a
    fresh log open.

    A delete is a read-modify-write, so it is pinned to ``read_version`` (the version the caller
    read) with ``load_as_version`` and committed under delta-rs native OCC — exactly like merge:
    delta-rs validates the operation over ``(read_version, HEAD]`` and fails loudly if a
    *conflicting* commit landed since that version (a non-conflicting one rebases). ``read_version``
    is REQUIRED (no blind-HEAD path). Then maintenance at a fresh HEAD."""
    if read_version is None:
        raise ValueError(
            "delete_rows requires read_version (the version the caller read). A delete is a "
            "read-modify-write and must be pinned to its snapshot — pass the version you read "
            "(engine.table_version(path, storage_options) captures the current HEAD)."
        )
    dt = dt if dt is not None else _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    try:
        dt.delete(predicate)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"delete: table '{path}' changed since version {read_version} "
            f"(a conflicting concurrent write committed); delete refused. Re-read and retry."
        ) from e
    _maintain(cur, path, storage_options=storage_options)


def update_rows(
    path: str,
    updates: Dict[str, str],
    predicate: Optional[str] = None,
    *,
    read_version: Optional[int],
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    dt: Optional[DeltaTable] = None,
) -> None:
    """Update ``{column: expression}`` for rows matching ``predicate`` (delta_rs/datafusion SQL),
    or every row when ``predicate`` is None. The Delta-native ``UPDATE`` for the connection API.

    Like :func:`delete_rows`, an update is a read-modify-write: pinned to ``read_version`` with
    ``load_as_version`` and committed under delta-rs native OCC over ``(read_version, HEAD]``
    (conflict → fail, like merge). ``read_version`` is REQUIRED. Then maintenance at a fresh HEAD.
    ``dt`` reuses an already-opened handle instead of a fresh log open."""
    if read_version is None:
        raise ValueError(
            "update_rows requires read_version (the version the caller read). An update is a "
            "read-modify-write and must be pinned to its snapshot — pass the version you read "
            "(engine.table_version(path, storage_options) captures the current HEAD)."
        )
    dt = dt if dt is not None else _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    # Validate SET targets against the real schema BEFORE dt.update(): delta_rs silently accepts an
    # unknown column, writing a no-op commit that advances the log while changing nothing. Fail loud
    # with no commit — the same guard the raw-SQL UPDATE path applies (SQL == DataFrame parity).
    target_cols = [f.name for f in dt.schema().fields]
    by_lower = {c.lower() for c in target_cols}
    unknown = [c for c in updates if str(c).strip('"').lower() not in by_lower]
    if unknown:
        raise ValueError(
            f"UPDATE on {path!r} sets unknown column(s) {unknown}; table columns are {target_cols}"
        )
    try:
        dt.update(updates=updates, predicate=predicate)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"update: table '{path}' changed since version {read_version} "
            f"(a conflicting concurrent write committed); update refused. Re-read and retry."
        ) from e
    _maintain(cur, path, storage_options=storage_options)


def vacuum(
    path: str,
    *,
    retention_hours: Optional[int] = None,
    dry_run: bool = False,
    enforce_retention_duration: bool = True,
    storage_options: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Remove data files no longer referenced and older than the retention window (delta_rs
    ``DeltaTable.vacuum``). Returns the list of file paths deleted (or that *would* be deleted when
    ``dry_run=True``). ``retention_hours=None`` uses the table's configured retention (delta_rs
    default 7 days); a value below that needs ``enforce_retention_duration=False``."""
    dt = _delta_table(path, storage_options)
    return dt.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=enforce_retention_duration,
    )


def optimize(
    path: str,
    *,
    target_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
) -> Dict:
    """Compact small files into larger ones (delta_rs ``optimize.compact``) and return the operation
    metrics. Reuses the one ``_writer_properties()`` read layout (the fixed 6M-row ceiling — same as
    every write; ``cur`` is accepted for API compatibility and unused). A lexicographic ``ORDER BY``
    at write time
    (``CREATE OR REPLACE TABLE t SORTED BY AUTO AS SELECT * FROM t``) is what a columnar reader wants;
    there is no z-order path — bit-interleaving destroys the run-length runs the in-memory reader
    relies on."""
    dt = _delta_table(path, storage_options)
    return dt.optimize.compact(target_size=target_size,
                               writer_properties=_writer_properties())


def compaction_debt(cur, path: str, *, dt: Optional[DeltaTable] = None,
                    target_size: int = _TARGET_FILE_SIZE,
                    storage_options: Optional[Dict[str, str]] = None) -> Dict:
    """Small-file debt for the Tier-0 maintenance button, read from the Delta **log** (no data scan).
    A file is 'small' if it is under **half** the target size; returns the count and total bytes of
    the small files and the distinct partitions they sit in (``col=value`` labels). Pure read — no
    commit. The caller applies the fire trigger (enough small files AND enough small bytes). Pass a
    pre-opened ``dt`` to avoid reopening the table (the post-write ``_maintain`` already holds one)."""
    dt = dt or _delta_table(path, storage_options)
    add_actions = dt.get_add_actions(flatten=True)  # noqa: F841 - DuckDB replacement scan by name
    have = [d[0] for d in cur.sql("select * from add_actions limit 0").description]
    pcols = [c for c in have if c.startswith("partition.")]
    psel = "".join(f', "{c}"' for c in pcols)
    rows = cur.sql(
        f"select size_bytes{psel} from add_actions where size_bytes < {int(target_size * 0.5)}"
    ).fetchall()
    parts = sorted({
        "/".join(f"{c.split('.', 1)[1]}={r[i + 1]}" for i, c in enumerate(pcols)) for r in rows
    }) if pcols else []
    small_sizes = [int(r[0]) for r in rows]
    return {"small_files": len(small_sizes), "small_bytes": sum(small_sizes),
            "small_sizes": small_sizes, "partitions": parts}


def restore_to_version(
    path: str,
    target,
    *,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Restore the table to an earlier state — ``target`` is a Delta ``version`` (int) or a
    ``datetime`` timestamp (delta_rs ``DeltaTable.restore``). This is a new commit on top of history
    — it does not rewrite the log — so it is itself revertible."""
    dt = _delta_table(path, storage_options)
    dt.restore(target)


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
    existing_columns: Optional[List[str]] = None,
    max_spill_size: Optional[int] = None,
    max_temp_directory_size: Optional[int] = None,
    streamed_exec: bool = False,
    source_materialized: bool = False,
    read_version: Optional[int] = None,
    delete_unmatched_by_source=None,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    partition_by: Optional[List[str]] = None,
    sort_by=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
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
    - merge_schema=True evolves the table schema for new columns, backing
      ``on_schema_change='append_new_columns'`` / ``'sync_all_columns'``. The evolution is
      DECOUPLED from the merge (see ``merge_delta_clauses``): the new columns are added as a
      metadata-only commit BEFORE the merge so existing rows read NULL, and the merger itself
      always runs ``merge_schema=False`` — never letting delta_rs back-fill a new column onto a
      matched row from the source.
    - max_spill_size caps the merge's in-memory pool (bytes); beyond it delta_rs spills the
      join to disk instead of OOMing. None -> default to ~60% of RAM (see _merge_spill_caps);
      pass 0 (or any falsy non-None) to disable the cap and run unbounded.
    - max_temp_directory_size caps the merge's ON-DISK spill (bytes). delta_rs/DataFusion otherwise
      hard-caps it at a flat 100 GB regardless of disk size, aborting a wide merge on a terabyte disk.
      None -> default to free space on the spill disk minus a min(20%, 8 GiB) reserve
      (_default_merge_temp_dir_size).
    - read_version (REQUIRED): pin the merge TARGET to this Delta version (the model's ``vB``).
      delta_rs then validates OCC over ``(vB, HEAD]`` — the exact window the model's pinned read of
      ``{{ this }}`` could not have seen — so the read and the commit share one snapshot
      (single-snapshot MERGE semantics). None is rejected: a merge always has an existing target, so
      the caller always read a version; merging against HEAD instead would reopen the read->write
      gap. NOTE: only the merge target is pinned; the post-merge maintenance below always reopens a
      fresh HEAD and must NEVER receive this version.
    - streamed_exec: delta_rs's flag for how it reads the source. Its default (True) STREAMS the
      source and so cannot compute source statistics, which means it cannot derive an early
      pruning predicate — it scans the *whole* target. We default it to False: collect the source
      so delta_rs uses its min/max to prune target files to the ones the source can actually
      touch. That's the right trade for the incremental pattern (small source, large target):
      collecting a small delta is cheap and the prune avoids a full-target scan. For a merge whose
      *source* is itself huge, pass streamed_exec=True (``merge_streamed_exec``) so it isn't
      materialized — at the cost of no pruning.
    - source_materialized: tell the merge that ``data`` reads a table the caller already
      materialized (the dbt plugin's temp-table staging), i.e. probing it is cheap and stable.
      Enables the empty-source short-circuit in ``merge_delta_clauses``: a zero-row source with no
      by-source clause skips the merge machinery (target open + pin, source collection, post-merge
      maintenance) outright. Never set it for a lazy relation — the probe would re-evaluate the
      model.
    - delete_unmatched_by_source: the "WHEN NOT MATCHED BY SOURCE THEN DELETE" form — also remove
      target rows the source doesn't carry. True deletes every unmatched target row (full sync); a
      string deletes only those matching that predicate; None (default, used by the dbt incremental
      strategies) adds no such clause.

    After the merge, run the same best-effort maintenance the append/delete+insert paths use
    (``_maintain``): compact small files when the byte debt is worth it, vacuum tombstoned old
    versions (safe 7-day default retention), and clean up expired log entries. Without this an
    incremental table that is merged on every run grows old files forever.
    """
    extra = (predicates if isinstance(predicates, (list, tuple)) else [predicates]) if predicates else None
    predicate = merge_on_predicate(unique_key, extra)

    # Build the fixed clause shape this convenience wrapper has always produced, then hand it to the
    # ordered clause-core. dbt merge_update_condition / merge_insert_condition gate which matched rows
    # update and which unmatched rows insert; delta_rs expresses these as per-clause predicates
    # (referencing target/source — the caller has already rewritten DBT_INTERNAL_DEST/SOURCE).
    clauses: List[dict] = []
    if insert_only:
        clauses.append({"clause": "not_matched", "action": "insert_all",
                        "predicate": insert_condition})
    else:
        if update_columns:
            # Quoted when needed: datafusion parses an unquoted name, so a column with a space
            # ("Total Amount") fails with "No field named source.Total"; a bare identifier stays
            # verbatim (the shape every caller and the clause tests expect).
            clauses.append({"clause": "matched", "action": "update",
                            "updates": {quote_ident_if_needed(c): f"source.{quote_ident_if_needed(c)}"
                                        for c in update_columns},
                            "predicate": update_condition})
        elif exclude_columns:
            clauses.append({"clause": "matched", "action": "update_all",
                            "except_cols": list(exclude_columns), "predicate": update_condition})
        else:
            clauses.append({"clause": "matched", "action": "update_all",
                            "predicate": update_condition})
        clauses.append({"clause": "not_matched", "action": "insert_all",
                        "predicate": insert_condition})
    # "WHEN NOT MATCHED BY SOURCE THEN DELETE": optionally remove target rows the source doesn't carry
    # (full sync). True = all unmatched; a string = only those matching the predicate. Default None
    # adds nothing (dbt incremental paths unaffected).
    if delete_unmatched_by_source:
        by_source_pred = (delete_unmatched_by_source
                          if isinstance(delete_unmatched_by_source, str) else None)
        clauses.append({"clause": "not_matched_by_source", "action": "delete",
                        "predicate": by_source_pred})

    merge_delta_clauses(
        path, data, predicate, clauses,
        read_version=read_version,
        merge_schema=merge_schema,
        existing_columns=existing_columns,
        streamed_exec=streamed_exec,
        source_materialized=source_materialized,
        max_spill_size=max_spill_size,
        max_temp_directory_size=max_temp_directory_size,
        storage_options=storage_options,
        cur=cur,
        # Only consulted when the clause list turns out to be insert-only and gets diverted to the
        # append path (see merge_delta_clauses); a delta_rs merge writes into the existing layout.
        partition_by=partition_by,
        sort_by=sort_by,
        row_group_rows=row_group_rows,
        target_file_size=target_file_size,
        timestamp_ntz=timestamp_ntz,
        existing_dt=existing_dt,
    )


# delta-rs TableMerger method per (clause, action) — the full surface duckrun's connection-API MERGE
# exposes. `*_all` take except_cols; `update`/`insert` take an `updates` map; `delete` takes neither.
def _apply_merge_clause(merger, c: dict):
    clause, action = c["clause"], c["action"]
    pred = c.get("predicate")
    if clause == "matched":
        if action == "update_all":
            return merger.when_matched_update_all(predicate=pred, except_cols=c.get("except_cols"))
        if action == "update":
            return merger.when_matched_update(updates=c["updates"], predicate=pred)
        if action == "delete":
            return merger.when_matched_delete(predicate=pred)
    elif clause == "not_matched":
        if action == "insert_all":
            return merger.when_not_matched_insert_all(predicate=pred, except_cols=c.get("except_cols"))
        if action == "insert":
            return merger.when_not_matched_insert(updates=c["updates"], predicate=pred)
    elif clause == "not_matched_by_source":
        if action == "update":
            return merger.when_not_matched_by_source_update(updates=c["updates"], predicate=pred)
        if action == "delete":
            return merger.when_not_matched_by_source_delete(predicate=pred)
    raise ValueError(f"unsupported merge clause/action: {clause}/{action}")


# Equality pair in a MERGE ON predicate, either order: `source.x = target.y` / `target.y = source.x`.
_MERGE_EQ_RE = re.compile(r'(?i)(source|target)\.("?)(\w+)\2\s*=\s*(source|target)\.("?)(\w+)\5')


def _merge_source_keys(predicate: str) -> List[str]:
    """The SOURCE-side columns of each ``target.col = source.col`` equality in a MERGE ON predicate.

    Used to enforce the keyed-merge cardinality rule (the source must be unique on the join key).
    Returns ``[]`` when the predicate isn't a plain AND-of-equalities — an ``OR`` (or a non-equality
    join) means per-key uniqueness no longer bounds how many source rows can match a target row, so
    we don't guess and skip the guard there (advanced merges with no dbt-strategy equivalent)."""
    if not predicate or re.search(r'(?i)\bor\b', predicate):
        return []
    keys: List[str] = []
    for m in _MERGE_EQ_RE.finditer(predicate):
        lside, lcol, rside, rcol = m.group(1).lower(), m.group(3), m.group(4).lower(), m.group(6)
        if lside == "source" and rside == "target":
            keys.append(lcol)
        elif lside == "target" and rside == "source":
            keys.append(rcol)
    seen: set = set()
    out: List[str] = []
    for k in keys:
        if k.lower() not in seen:
            seen.add(k.lower())
            out.append(k)
    return out


def _merge_spill_caps(max_spill_size, max_temp_directory_size, streamed_exec):
    """Resolve + log the merge's two spill caps: the in-memory ``max_spill_size`` (a share of the
    effective RAM limit) and the on-disk ``max_temp_directory_size`` (a share of the spill disk's
    free space, overriding delta_rs/DataFusion's flat 100 GB default). Returns the kwarg dicts to
    forward — empty when a cap is undetectable or opted out (0), preserving delta_rs defaults."""
    # Sample the effective limit ONCE so the cap we apply and the cap we log can't disagree:
    # free RAM is read live on every call, so two separate reads would drift on a busy box.
    eff_limit = _effective_mem_limit_bytes()
    if max_spill_size is None:
        max_spill_size = int(eff_limit * _MERGE_SPILL_FRACTION) if eff_limit else None
    # Only forward the kwarg when we have a positive cap: delta_rs builds a spilling session
    # only when max_spill_size is set, so omitting it preserves the prior unbounded behavior
    # (e.g. RAM undetectable, or caller explicitly passed 0 to opt out).
    spill_kwargs = {"max_spill_size": max_spill_size} if max_spill_size else {}

    if max_temp_directory_size is None:
        max_temp_directory_size = _default_merge_temp_dir_size()
    temp_dir_kwargs = (
        {"max_temp_directory_size": max_temp_directory_size} if max_temp_directory_size else {}
    )

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
    if temp_dir_kwargs:
        logger.info(
            f"merge disk spill cap: {max_temp_directory_size / 2**30:.2f} GiB "
            f"(free on {_merge_spill_dir()} minus a min(20%, 8 GiB) reserve) "
            f"— overrides delta_rs's 100 GB default"
        )
    else:
        logger.info("merge disk spill cap: delta_rs default (100 GB) — free disk space undetectable")
    logger.info(
        "merge target pruning: "
        + ("on (source stats derive an early filter)" if not streamed_exec
           else "off (streamed_exec=True — source streamed, whole target scanned)")
    )
    return spill_kwargs, temp_dir_kwargs


def assert_source_unique(data, keys: List[str]) -> None:
    """Raise unless ``data`` (a DuckDB relation) holds at most one row per ``keys`` tuple.

    THE keyed-merge cardinality rule, shared by every path that resolves a source row against a
    target row by key: delta_rs's merge (via :func:`_merge_cardinality_guard`) and the DuckDB
    insert-only anti-join. Spark/Snowflake/BigQuery raise on a duplicate-key source; delta_rs
    silently produces duplicate rows, so duckrun fails loud instead. One implementation so the
    query and the message cannot drift between the two paths.

    Best-effort on the probe itself: if the guard query cannot run, warn and proceed rather than
    failing a valid write."""
    if not keys or not hasattr(data, "query"):
        return
    keycols = ", ".join('"' + k.replace('"', '""') + '"' for k in keys)
    try:
        dup = data.query(
            "__merge_src",
            f"SELECT {keycols}, count(*) AS __n FROM __merge_src "
            f"GROUP BY {keycols} HAVING count(*) > 1 LIMIT 1",
        ).fetchone()
    except Exception as e:  # never let the guard ITSELF break a valid merge; surface and proceed
        logger.warning(f"merge duplicate-key guard could not run ({e!r}); proceeding")
        return
    if dup is None:
        return
    keyval = ", ".join(f"{k}={v!r}" for k, v in zip(keys, dup[:-1]))
    raise ValueError(
        f"MERGE source is not unique on the join key ({', '.join(keys)}): "
        f"{dup[-1]} rows for {keyval}. A keyed merge/insert cannot resolve duplicate "
        f"source keys — Spark, Snowflake and BigQuery raise the same error, and delta_rs "
        f"would silently produce duplicate rows. Deduplicate the source, e.g. "
        f"qualify row_number() over (partition by {keycols} order by <tiebreak>) = 1."
    )


def _merge_cardinality_guard(data, predicate: str, clauses: List[dict], streamed_exec: bool) -> None:
    """Cardinality guard — applied to EVERY merge path (the dbt materialization and the conn.sql
    MERGE INTO handler all land here), so a keyed upsert behaves identically across them. A
    keyed merge/insert cannot resolve two source rows for one target row: Spark/Snowflake/BigQuery
    raise, but delta_rs silently produces duplicate rows. So when the merge has an update/insert
    clause keyed on an equality predicate, require the source to be unique on that key. Skipped when
    streamed_exec is set (the caller has a huge source it explicitly does NOT want collected) or when
    the predicate isn't a plain AND-of-equalities (OR / non-equality / by-source-only — no key to check)."""
    has_upsert = any(
        c.get("clause") in ("matched", "not_matched")
        and c.get("action") in ("update", "update_all", "insert", "insert_all")
        for c in clauses
    )
    if not has_upsert or streamed_exec:
        return
    assert_source_unique(data, _merge_source_keys(predicate))


class AntiJoinUnsupported(Exception):
    """The insert-only anti-join could not be expressed in DuckDB for this merge — the caller should
    fall through to delta_rs. Raised only for a bind/parse failure of the generated SQL (a MERGE ``ON``
    predicate is DataFusion SQL, and a caller may legitimately use something DuckDB doesn't accept),
    never after anything has been committed."""


def _sql_literal(v):
    """``v`` as a DuckDB SQL literal, or None when it has no rendering we trust. Deliberately narrow:
    these are low-cardinality keys (month keys, region codes, dates), and a float or binary value is
    not worth round-tripping through text."""
    import datetime as _dt
    from decimal import Decimal as _Decimal

    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, _Decimal):
        return str(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, _dt.datetime):
        return "'" + v.isoformat(sep=" ") + "'"
    if isinstance(v, _dt.date):
        return "'" + v.isoformat() + "'"
    return None


def _distinct_literals(cur, source_name, q, col):
    """The source's DISTINCT non-null values of ``q`` as SQL literals, or None when there are too many
    (``_PART_PRUNE_MAX``, the same cap the delta_rs merge hint uses) or any value has no literal
    rendering we trust."""
    try:
        rows = cur.sql(
            f"SELECT DISTINCT {q} FROM {source_name} WHERE {q} IS NOT NULL "
            f"LIMIT {_PART_PRUNE_MAX + 1}"
        ).fetchall()
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(f"insert probe pruning: could not collect {col!r} values ({e!r}); skipping")
        return None
    vals = [r[0] for r in rows]
    if not vals or len(vals) > _PART_PRUNE_MAX:
        return None
    lits = [_sql_literal(v) for v in vals]
    return None if any(l is None for l in lits) else lits


def _range_literals(cur, source_name, q, col):
    """``(min, max)`` of ``q`` over the source as SQL literals, or None when the column is empty,
    all-NULL, or has no literal rendering we trust."""
    try:
        row = cur.sql(f"SELECT min({q}), max({q}) FROM {source_name}").fetchone()
    except Exception as e:  # pragma: no cover - defensive
        logger.warning(f"insert probe pruning: could not bound {col!r} ({e!r}); skipping")
        return None
    if not row or row[0] is None or row[1] is None:
        return None
    lo, hi = _sql_literal(row[0]), _sql_literal(row[1])
    return None if lo is None or hi is None else (lo, hi)


def probe_filters(cur, source_name, partition_by, join_keys) -> List[str]:
    """CONSTANT filters that let the insert-only anti-join's target probe skip files at plan time.

    For every column equality-joined to the source (from the merge ON predicate — ``unique_key`` plus
    any ``incremental_predicates`` entry like ``target.month_key = source.month_key``):

    * a PARTITION column with few enough distinct source values → ``"p" IN (v1, v2, …)``, the exact
      set, so the reader skips whole partition directories;
    * otherwise → ``"k" >= lo AND "k" <= hi`` from the source's min/max, so the reader skips files
      whose Delta stats put them outside the batch's key range. This is the same early filter
      delta_rs's merge derives from source statistics — the reason a delta_rs merge of a contiguous
      batch can be fast — reproduced here so the DuckDB probe is not left scanning the whole key
      column.

    Applied directly on the ``delta_scan`` inside the probe's derived table (hence unqualified — no
    alias is bound there yet), which is where the reader can push them down.

    RESULT-NEUTRAL: the EXISTS body already requires ``t.k = s.k``, so a target row whose ``k`` is
    outside the source's value set — or outside its min/max range, or NULL — could never have matched
    any source row. A NULL source key never matches either, so no NULL arm is needed. Only ever
    derived from a declared equality, never from ``partition_by`` alone, which would not be
    result-neutral. Best-effort throughout: any failure logs and skips that column's filter rather
    than breaking a valid insert."""
    if not join_keys:
        return []
    parts = {str(c).strip().strip('"').lower()
             for c in (partition_by if isinstance(partition_by, (list, tuple))
                       else [partition_by] if partition_by else [])}
    out = []
    for key in join_keys:
        c = str(key).strip().strip('"')
        q = '"' + c.replace('"', '""') + '"'
        # Partition column: prefer the exact value set (an IN list beats a range, and a source that
        # unions an old backfill with a current feed is BIMODAL, so min/max would smear the bound
        # across every partition in between).
        if c.lower() in parts:
            lits = _distinct_literals(cur, source_name, q, c)
            if lits:
                out.append(f"{q} IN ({', '.join(sorted(lits))})")
                continue
        rng = _range_literals(cur, source_name, q, c)
        if rng:
            lo, hi = rng
            out.append(f"{q} = {lo}" if lo == hi else f"{q} >= {lo} AND {q} <= {hi}")
    return out


def resolve_do_nothing(clauses: List[dict]) -> List[dict]:
    """Fold ``DO NOTHING`` clauses into the delta_rs surface, which has no skip action.

    A DO NOTHING clause's only observable effect is first-match-wins: for rows of its kind matching its
    predicate it does nothing AND stops any LATER clause of the SAME kind from firing on them. delta_rs
    already does nothing when no clause matches, so we drop the DO NOTHING clause and push its predicate
    as a ``(<pred>) IS NOT TRUE`` guard onto every later same-kind clause — an unconditional DO NOTHING
    claims all such rows, so those later clauses are dropped outright. Same row outcome, no skip action.
    An all-DO-NOTHING merge folds to zero clauses: a pure no-op the caller skips. ``IS NOT TRUE`` (not
    ``NOT``) keeps NULL predicates correct — a row DO NOTHING didn't claim still reaches later clauses.

    Lives HERE, at the shared merge seam, because both surfaces that can express the action reach it:
    a raw SQL ``WHEN … THEN DO NOTHING`` (``delta_dml._build_merge_clause``) and dbt's
    ``merge_clauses: {when_matched: [{action: do_nothing}]}`` (``delta_plugin._specs_from_merge_clauses``)
    — one fold, so they cannot resolve to two different merges. Non-mutating: callers keep their own
    spec dicts (only the copies carry the guard)."""
    out = [dict(c) for c in clauses]
    for idx, c in enumerate(out):
        if c.get("action") != "do_nothing" or c.get("_dead"):
            continue
        kind, pred = c["clause"], c.get("predicate")
        for later in out[idx + 1:]:
            if later["clause"] != kind or later.get("_dead"):
                continue
            if pred is None:
                later["_dead"] = True  # unconditional skip: no row of this kind reaches a later clause
            else:
                guard = f"({pred}) IS NOT TRUE"
                lp = later.get("predicate")
                later["predicate"] = f"({lp}) AND {guard}" if lp else guard
    return [c for c in out if c.get("action") != "do_nothing" and not c.get("_dead")]


def _insert_only_shape(clauses: List[dict]) -> bool:
    """True when ``clauses`` is EXACTLY one unconditional-shape ``WHEN NOT MATCHED THEN INSERT *`` —
    the only merge shape that removes no row and is therefore expressible as a plain append.
    ``except_cols`` is excluded: a partial insert list has no anti-join form here."""
    if len(clauses) != 1:
        return False
    c = clauses[0]
    return (c.get("clause") == "not_matched" and c.get("action") == "insert_all"
            and not c.get("except_cols"))


def insert_delta(
    path: str,
    cur,
    source_name: str,
    predicate: str,
    *,
    read_version: int,
    insert_condition: Optional[str] = None,
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    sort_by=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
) -> None:
    """Insert only the source rows that match no target row, as a PLAIN APPEND — the anti-join form of
    ``WHEN NOT MATCHED THEN INSERT *``.

    THE one implementation, shared by every surface that expresses this operation: dbt's
    ``incremental_strategy='insert'`` and a raw ``MERGE INTO … WHEN NOT MATCHED THEN INSERT *`` both
    reach it through :func:`merge_delta_clauses`, so the same operation cannot execute two different
    ways depending on how it was written.

    Insert-only never removes a row, so no target file is rewritten and the Delta commit carries
    ``add`` actions only. delta_rs's MERGE produces the same table, but plans a join against the whole
    pinned target: its cost scales with the target's partition span rather than the batch, and its
    join state is not fully spillable — the shape that gets a run OOM-killed on a large fact table.
    DuckDB's anti-join reads only the columns the predicate touches (parquet projection pushdown),
    prunes files via :func:`probe_filters`, and spills to disk when it must.

    ``predicate`` is the merge ON condition in delta_rs form (``target.``/``source.`` aliases); it is
    rewritten onto the ``t``/``s`` aliases the anti-join uses. Semantics match delta_rs exactly: a
    source row is unmatched when no target row satisfies the predicate, which is what ``NOT EXISTS``
    over the same condition expresses — including NULL keys, where ``t.k = s.k`` is NULL (not TRUE)
    so the row is inserted, as SQL ``IN`` does.

    ALWAYS fenced: the anti-join READS the target, so this is a read-modify-append and the commit is
    pinned to ``read_version`` (``append_if_unchanged``, CAS via ``max_commit_retries=0``) — a writer
    committing in between would make the anti-join stale and let a duplicate through. A batch that
    adds nothing writes NO commit at all; the Delta version does not move.

    Raises :class:`AntiJoinUnsupported` if the generated SQL will not bind (nothing has been committed
    at that point), so the caller can fall through to delta_rs.
    """
    from . import sqlscan  # local: sqlscan imports nothing from engine, but keep the module leaf-clean

    loc_sql = path.replace("'", "''")
    target_cols = list(cur.sql(
        f"SELECT * FROM delta_scan('{loc_sql}', version => {read_version}) LIMIT 0").columns)
    src_cols = list(cur.sql(f"SELECT * FROM {source_name} LIMIT 0").columns)
    smap = {c.lower(): c for c in src_cols}
    missing = [c for c in target_cols if c.lower() not in smap]
    if missing:
        raise ValueError(
            f"insert: the source does not supply target column(s) {sorted(missing)}; "
            "an insert-only merge must cover every target column."
        )
    # Project onto the TARGET's column list, in the target's declared order and spelling, so a
    # reordered source SELECT can never shift values between columns; columns the target does not have
    # trail only when the schema is being evolved.
    added = [c for c in src_cols if c.lower() not in {t.lower() for t in target_cols}]
    out_cols = [smap[c.lower()] for c in target_cols] + (added if merge_schema else [])
    proj = ", ".join(f's."{c}"' for c in out_cols)

    def _to_probe(expr):
        return sqlscan.rename_qualifier(
            sqlscan.rename_qualifier(expr, "target", "t"), "source", "s")

    probe = probe_filters(cur, source_name, partition_by, _merge_source_keys(predicate))
    where_t = (" WHERE " + " AND ".join(probe)) if probe else ""
    outer = ""
    if insert_condition:
        # `IS TRUE`, not bare truthiness: a NULL clause predicate must not insert, matching delta_rs's
        # filter semantics.
        outer = f" AND ({_to_probe(insert_condition)}) IS TRUE"

    tmp = tmp_name("ins", path)
    sql = (f'CREATE OR REPLACE TEMP TABLE "{tmp}" AS '
           f"SELECT {proj} FROM {source_name} s WHERE NOT EXISTS ("
           f"SELECT 1 FROM (SELECT * FROM delta_scan('{loc_sql}', version => {read_version})"
           f"{where_t}) t WHERE {_to_probe(predicate)})" + outer)
    try:
        cur.execute(sql)
    except Exception as e:
        # A bind/parse failure means the ON predicate (or the insert condition) is DataFusion SQL that
        # DuckDB will not take. Nothing has been committed — let the caller run delta_rs instead.
        raise AntiJoinUnsupported(str(e)) from e
    try:
        if cur.sql(f'SELECT 1 FROM "{tmp}" LIMIT 1').fetchone() is None:
            return  # nothing new — no commit, the version stays put
        order = ""
        if sort_by:
            cols = sort_by if isinstance(sort_by, (list, tuple)) else [sort_by]
            order = " ORDER BY " + ", ".join(quote_ident(c) for c in cols)
        append_if_unchanged(
            path, cur.sql(f'SELECT * FROM "{tmp}"{order}'),
            read_version=read_version,
            partition_by=partition_by,
            merge_schema=merge_schema,
            storage_options=storage_options,
            cur=cur,
            row_group_rows=row_group_rows,
            target_file_size=target_file_size,
            timestamp_ntz=timestamp_ntz,
        )
    finally:
        cur.execute(f'DROP TABLE IF EXISTS "{tmp}"')


def _merge_evolve_schema(path, data, storage_options, read_version: int, merge_schema: bool,
                         existing_columns: Optional[List[str]] = None) -> int:
    """Schema evolution, DECOUPLED from the merge (never pass merge_schema to the merger): delta_rs,
    evolving mid-merge, back-fills a newly added column onto matched rows from the source — which is
    wrong for a narrow matched-update (a snapshot's close-row updates ONLY dbt_valid_to, so the new
    column must read NULL on the already-closed version, not the current source value). Instead, when
    new columns are present, add them as a metadata-only commit FIRST — existing rows (including every
    closed SCD2 version) then read NULL — and merge with the schema already in place. This keeps the
    merger byte-identical whether or not evolution happened; the update clause is never
    schema-dependent. Returns the version the merge must pin to (vB, or vB+1 after the add-columns
    commit — so the full (vB, HEAD] OCC window stays covered: (vB, vB+1] by this CAS, (vB+1, HEAD]
    by the merge)."""
    if not merge_schema:
        return read_version
    # `existing_columns` is the list the caller read while deciding merge_schema (same immutable
    # snapshot — no write happens in between), so don't re-open the log for the identical answer.
    existing = {c.lower() for c in (existing_columns if existing_columns is not None
                                    else delta_columns(path, storage_options))}
    added = [c for c in data.columns if c.lower() not in existing]
    if not added:
        return read_version
    # Compare-and-swap add-columns pinned to read_version (same primitive as append_if_unchanged):
    # a zero-row append with schema_mode="merge" adds only the new columns (delta_rs derives their
    # types from the source's Arrow stream) and commits nothing else.
    evo_args = build_write_deltalake_args(
        path, data.limit(0), "append",
        schema_mode="merge",
        storage_options=storage_options,
    )
    _fenced_write(evo_args, path, storage_options, read_version,
                  refusal_prefix="schema evolution",
                  refusal_suffix="; the on_schema_change add-columns commit was "
                                 "refused before the merge. Re-read and retry.")
    return read_version + 1


def _merge_partition_prune(dt: DeltaTable, data, predicate: str, streamed_exec: bool) -> str:
    """Explicit partition-set pruning. delta_rs's auto early_filter (try_construct_early_filter) is
    fragile: it silently returns None — degrading to a FULL target scan — on multi-key predicates
    (delta-rs #3636) and whenever source min/max stats are unavailable. For each PARTITION column
    joined as `target.p = source.p`, collect the source's DISTINCT values and fold a CONSTANT
    `target.p IN (<vals>)` into the ON predicate. This is RESULT-NEUTRAL — any target row that can
    match `source.p` already carries one of those values — but hands delta_rs a plan-time literal it
    can push down to skip partition files deterministically, which is exactly what its own docs
    recommend doing by hand. IN (not BETWEEN min/max): a source that unions two feeds (e.g. an old
    backfill + a current stream) is BIMODAL, so min/max would smear the bound across the whole table
    while the true set is a handful of partitions. Numeric partition cols only, capped at
    _PART_PRUNE_MAX distinct values (beyond that the IN list stops helping and we let delta_rs be);
    skipped for by-source merges (streamed_exec, which can't prune anyway). Best-effort: never break
    a merge."""
    if streamed_exec or not hasattr(data, "query"):
        return predicate
    try:
        part_cols = list(dt.metadata().partition_columns or [])
    except Exception:
        part_cols = []
    conds = []
    for pcol in part_cols:
        if not re.search(rf'target\."?{re.escape(pcol)}"?\s*=\s*source\."?{re.escape(pcol)}"?',
                         predicate, re.I):
            continue
        q = '"' + pcol.replace('"', '""') + '"'
        try:
            rows = data.query(
                "__mp",
                f"SELECT DISTINCT {q} FROM __mp WHERE {q} IS NOT NULL LIMIT {_PART_PRUNE_MAX + 1}",
            ).fetchall()
        except Exception as e:
            logger.warning(f"merge partition-set pruning: could not collect {pcol!r} values "
                           f"({e!r}); skipping")
            continue
        vals = [r[0] for r in rows]
        if not vals or len(vals) > _PART_PRUNE_MAX:
            continue
        if any(v is None or isinstance(v, bool) or not isinstance(v, (int, float)) for v in vals):
            continue
        tq = "target." + q
        lst = ", ".join(str(v) for v in sorted(vals))
        conds.append(f"{tq} IN ({lst})")
        logger.info(f"merge partition-set pruning: injected {tq} IN ({lst})")
    if conds:
        return f"({predicate}) AND " + " AND ".join(conds)
    return predicate


def merge_delta_clauses(
    path: str,
    data,
    predicate: str,
    clauses: List[dict],
    *,
    read_version: Optional[int] = None,
    merge_schema: bool = False,
    existing_columns: Optional[List[str]] = None,
    streamed_exec: bool = False,
    source_materialized: bool = False,
    max_spill_size: Optional[int] = None,
    max_temp_directory_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    cur=None,
    partition_by: Optional[List[str]] = None,
    sort_by=None,
    row_group_rows: Optional[int] = None,
    target_file_size: Optional[int] = None,
    timestamp_ntz: Optional[bool] = None,
    existing_dt: Optional[DeltaTable] = None,
) -> None:
    """Run a MERGE described by an ORDERED list of clause dicts — the full delta-rs ``TableMerger``
    surface. Each clause is ``{"clause": "matched"|"not_matched"|"not_matched_by_source",
    "action": "update"|"update_all"|"delete"|"insert"|"insert_all", "predicate": str|None,
    "updates": {col: expr}|None, "except_cols": [..]|None}`` and is applied in order (delta-rs
    evaluates them top-to-bottom). ``predicate`` is the full ON condition, referencing the literal
    ``target``/``source`` aliases.

    This is the shared core for every merge path: ``merge_delta`` (dbt incremental — builds a fixed
    clause list) and the ``conn.sql`` / raw-SQL ``MERGE INTO`` handler. The
    spill cap, target pruning, the REQUIRED ``read_version`` snapshot pin (OCC over (vB, HEAD]), and
    the post-merge maintenance are identical for every clause shape, so the single-snapshot and
    concurrency-safety guarantees hold for all of them. See ``merge_delta`` for the parameter
    semantics (spill / streamed_exec / read_version / maintenance).

    THE ROUTING SEAM. An insert-only merge (exactly one unconditional-shape
    ``WHEN NOT MATCHED THEN INSERT *``) removes no row, so it is diverted to :func:`insert_delta` —
    a DuckDB anti-join committed as a plain append, which neither rewrites a file nor builds delta_rs's
    join state. The decision lives HERE, not in a caller, because every surface that can express the
    operation funnels through this function: a dbt ``incremental_strategy='insert'`` model and a raw
    ``MERGE INTO … WHEN NOT MATCHED THEN INSERT *`` must not execute two different ways.

    It falls through to delta_rs when the anti-join cannot apply:
      * any other clause shape (matched update/delete, by-source, partial ``except_cols`` insert);
      * ``streamed_exec=True`` — an explicit request for delta_rs's streaming source handling (and the
        shape where the cardinality guard is skipped), so it is also the documented way to force the
        delta_rs path;
      * no ``cur`` or a non-DuckDB ``data`` (e.g. a pyarrow Table) — the anti-join needs a DuckDB
        relation and a cursor to build it;
      * the generated SQL will not bind (``AntiJoinUnsupported``): a MERGE ``ON`` predicate is
        DataFusion SQL and may use something DuckDB does not accept. Nothing is committed before that
        point, so falling through is safe."""
    # A merge always has an existing target (a brand-new table is created, never merged into), so
    # the caller (the dbt materialization / conn.sql MERGE INTO) always pins the version it read.
    # read_version=None would silently merge against HEAD and reopen the read->write gap — refuse it.
    if read_version is None:
        raise ValueError(
            "merge_delta_clauses requires read_version (the version the caller read). A merge always "
            "has an existing target to pin to; None would merge against HEAD and break single-snapshot."
        )
    if not clauses:
        raise ValueError("merge has no clauses")

    # DO NOTHING has no delta_rs skip action — fold it into IS-NOT-TRUE guards on later same-kind
    # clauses (see resolve_do_nothing). Done HERE so raw SQL and dbt's merge_clauses resolve
    # identically, and BEFORE the cardinality guard so the guard reflects the clauses that will
    # actually run. A merge whose every clause is DO NOTHING folds to nothing: a no-op statement (the
    # same outcome DuckDB/Spark give), so commit nothing — the table's version does not move.
    clauses = resolve_do_nothing(clauses)
    if not clauses:
        logger.info("merge: every clause is DO NOTHING; nothing to do (no commit)")
        return

    # Naive-timestamp coercion (issue #42) BEFORE the decoupled schema evolution and the
    # insert-only divert, so an evolved new column is typed tz-aware (the zero-row
    # schema_mode="merge" append derives its type from this relation) and both branches see the
    # coerced source. A merge writes into an existing target, so the target-aware skip applies.
    data = coerce_naive_timestamps(
        data, path=path, storage_options=storage_options,
        timestamp_ntz=timestamp_ntz, existing_dt=existing_dt,
    )

    _merge_cardinality_guard(data, predicate, clauses, streamed_exec)
    effective_version = _merge_evolve_schema(path, data, storage_options, read_version,
                                             merge_schema, existing_columns=existing_columns)

    # Empty-source short-circuit (issue #61): a merge whose source has no rows touches no target
    # row, so skip the whole merge machinery — the target open + version pin, delta_rs's source
    # collection and join build, the merge gate, and the post-merge maintenance. (delta_rs itself
    # declines to COMMIT an empty merge, but only after paying all of that — remote round-trips an
    # unchanged snapshot re-run pays for nothing.) Only when the caller MATERIALIZED the source
    # (``source_materialized`` — the dbt plugin's temp-table staging): probing it is then one
    # LIMIT 1 off a local table, and the answer describes the same rows the merger would collect.
    # A lazy source is never probed — the probe could cost a full model evaluation, and a
    # nondeterministic model could answer "empty" for rows the merger would then see (the #14
    # hazard). Placed AFTER the schema evolution so on_schema_change='append_new_columns' still
    # lands a new column carried by a zero-row source, and skipped when a not_matched_by_source
    # clause exists — for those an empty source matches EVERY target row: work, not a no-op.
    if (source_materialized and hasattr(data, "limit")
            and not any(c.get("clause") == "not_matched_by_source" for c in clauses)
            and data.limit(1).fetchone() is None):
        logger.info("merge: source has no rows and no by-source clause; nothing to do (no commit)")
        return

    # Insert-only: divert to the DuckDB anti-join + plain append (see the docstring). Done AFTER the
    # cardinality guard and the decoupled schema evolution so both branches inherit them unchanged,
    # and pinned to the same effective_version the merger would have used.
    if (_insert_only_shape(clauses) and not streamed_exec
            and cur is not None and hasattr(data, "query")):
        src_view = tmp_name("msrc", path)
        data.create_view(src_view, replace=True)
        try:
            insert_delta(
                path, cur, '"' + src_view + '"', predicate,
                read_version=effective_version,
                insert_condition=clauses[0].get("predicate"),
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                sort_by=sort_by,
                row_group_rows=row_group_rows,
                target_file_size=target_file_size,
                # Coercion already resolved on `data` above — True stops the downstream append
                # from re-opening the target or double-warning (idempotent either way).
                timestamp_ntz=True,
            )
            return
        except AntiJoinUnsupported as e:
            logger.info(f"insert-only merge: DuckDB could not bind the anti-join ({e}); "
                        f"running it through delta_rs instead")
        finally:
            try:
                cur.execute(f'DROP VIEW IF EXISTS "{src_view}"')
            except Exception:  # pragma: no cover - the view is temp; a failed drop is harmless
                pass

    # One delta_rs merge at a time (see _MERGE_GATE): the spill caps are sized to the WHOLE
    # budget, not a 1/N share, so two merges running together would sum past it. The caps are
    # resolved AFTER the gate so free RAM/disk are sampled when this merge actually starts, not
    # while the previous one still holds its working set — and so a merge diverted to the
    # anti-join above never logs caps it doesn't use.
    if not _MERGE_GATE.acquire(blocking=False):
        logger.info("merge gate: waiting for an in-flight delta_rs merge to finish")
        _MERGE_GATE.acquire()
    try:
        spill_kwargs, temp_dir_kwargs = _merge_spill_caps(
            max_spill_size, max_temp_directory_size, streamed_exec
        )

        dt = _delta_table(path, storage_options)
        # Pin the target to the snapshot the model read (vB, or vB+1 after a decoupled add-columns
        # commit) so OCC validates (effective_version, HEAD] — one snapshot for both the read and
        # the commit.
        dt.load_as_version(effective_version)

        predicate = _merge_partition_prune(dt, data, predicate, streamed_exec)

        merger = dt.merge(
            source=data,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            merge_schema=False,
            streamed_exec=streamed_exec,
            **spill_kwargs,
            **temp_dir_kwargs,
        )
        for c in clauses:
            merger = _apply_merge_clause(merger, c)
        merger.execute()
    finally:
        _MERGE_GATE.release()

    # Same best-effort maintenance as the append / delete+insert paths: a merged-on-every-run
    # incremental table fragments into small files and leaves tombstoned old versions otherwise.
    # The merge itself writes with delta_rs defaults (deliberately — no writer_properties on the
    # OOM-prone path), so THIS is where a model's declared geometry reaches a merged table: the
    # post-merge compaction folds the lean merge files into it.
    _maintain(cur, path, storage_options=storage_options,
              target_file_size=target_file_size, row_group_cap=row_group_rows)
