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
from deltalake import CommitProperties, DeltaTable, convert_to_deltalake, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

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


# Normal-write row group: 4M rows. Bigger row groups give fewer, larger scan ranges at the cost of
# more write-time memory (arrow-rs buffers a full row group per open writer). Kept moderate on the hot
# write paths; the experimental optimize uses the larger segment below.
_MAX_ROW_GROUP_SIZE = 1_048_576 * 4
# Optimize (experimental sort-rewrite) row group: 8 × 1,048,576 = 8,388,608 rows = one large
# in-memory-reader segment. A read-layout pass wants full segments; the extra write-time memory is
# acceptable on an opt-in path.
_OPTIMIZE_ROW_GROUP_SIZE = 1_048_576 * 8
# Bounded-but-large dictionary page limit (256 MB). arrow-rs's ~1 MB default silently falls back
# to PLAIN mid column chunk on wide columns at multi-million rows/group, defeating the dictionary
# encoding an in-memory columnar reader transcodes into its hash encoding. 256 MB holds any dictionary worth having
# and caps the per-column write RAM; a column that overflows it was never a good dictionary candidate.
# Used ONLY by _tuned_writer_properties() (the experimental sort-rewrite), not by normal writes.
_DICT_PAGE_SIZE_LIMIT = 256 * 1024 * 1024
# Bigger data pages → fewer page headers and runs that survive page boundaries. Tuned-path only.
_DATA_PAGE_SIZE_LIMIT = 8 * 1024 * 1024
# Target file size (~1 GB) for the experimental sort-rewrite ONLY — a pure read-layout pass that wants
# fewer, fatter files for a columnar reader. NOT a normal-write default: normal append/overwrite/merge writes
# leave target_file_size unset (delta-rs default), so an incremental MERGE never has to rewrite whole
# 1 GB files. Applied only when build_write_deltalake_args is called with optimize_layout=True.
_TARGET_FILE_SIZE = 1024 * 1024 * 1024


def _writer_properties():
    # Plain, boring writer config for every normal write path (append/overwrite/if_unchanged),
    # compaction, and classic optimize: ZSTD + large row groups, nothing else.
    # The opinionated read-layout tuning lives in _tuned_writer_properties() and is reached only by the
    # opt-in experimental sort-rewrite. Fall back to ZSTD-only if a pinned wheel rejects a parameter.
    if WriterProperties is None:
        return None
    for kwargs in (
        dict(compression="ZSTD", max_row_group_size=_MAX_ROW_GROUP_SIZE),
        dict(compression="ZSTD"),
    ):
        try:
            return WriterProperties(**kwargs)
        except Exception:  # best-effort: any build rejection tries the next, simpler rung
            continue
    return None


def _tuned_writer_properties(plain_cols=None):
    # Opinionated Parquet config for the read layout: ZSTD(3), large row groups, a
    # bounded-but-large dictionary page limit so wide dictionaries stay dictionary-encoded, big data
    # pages, and chunk-level stats. Used ONLY by the experimental sort-rewrite (optimize_layout=True) —
    # deliberately kept off the hot write paths, where it made merges rewrite/spill fat files. Degrade
    # gracefully if the pinned wheel rejects a newer parameter (last rung: ZSTD-only).
    #
    # ``plain_cols`` are unique/near-unique columns whose dictionary buys nothing (every value distinct);
    # they get a per-column ColumnProperties(dictionary_enabled=False) so arrow-rs writes them PLAIN.
    if WriterProperties is None:
        return None
    col_props = None
    per_col = None
    if ColumnProperties is not None:
        try:
            col_props = ColumnProperties(dictionary_enabled=True, statistics_enabled="CHUNK")
        except Exception:  # best-effort: fall through to writer-level props without per-column knobs
            col_props = None
        if plain_cols:
            try:
                per_col = {c: ColumnProperties(dictionary_enabled=False) for c in plain_cols}
            except Exception:  # best-effort: a rejected per-column build just keeps the dictionary on
                per_col = None
    for kwargs in (
        dict(compression="ZSTD", compression_level=3,
             max_row_group_size=_OPTIMIZE_ROW_GROUP_SIZE,
             dictionary_page_size_limit=_DICT_PAGE_SIZE_LIMIT,
             data_page_size_limit=_DATA_PAGE_SIZE_LIMIT,
             statistics_truncate_length=64,
             default_column_properties=col_props,
             column_properties=per_col),
        dict(compression="ZSTD", compression_level=3, max_row_group_size=_OPTIMIZE_ROW_GROUP_SIZE),
        dict(compression="ZSTD"),
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
        row = cur.execute(
            "SELECT coalesce(sum(memory_usage_bytes), 0), "
            "coalesce(sum(temporary_storage_bytes), 0) FROM duckdb_memory()"
        ).fetchone()
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
# container (Fabric/k8s) physical RAM is the whole node, not our slice — so the default
# overcommits and the kernel OOM-kills us. 0.85 of the effective limit (which folds in available RAM,
# the only signal that reflects the container on Fabric where the cgroup is the unlimited root)
# leaves ~15% for the Parquet writer's row-group buffers, the Arrow batch in flight, and — the real
# reason for the margin — DuckDB overshooting its own *soft* memory_limit. Safe because the job owns
# the machine (effective folds in currently-free RAM, so the OS/other processes are already excluded);
# on a shared box, drop back toward 0.8 to leave slack for other tenants growing mid-run.
_WRITE_MEM_FRACTION = 0.85


def _default_merge_spill_size() -> Optional[int]:
    """delta_rs merge ``max_spill_size`` default: ~60% of the *effective* memory limit
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

    The DuckDB ``memory_limit`` is deliberately left ALONE here; it's set per model in ``store()``.
    The write path clamps it to ``_WRITE_MEM_FRACTION`` of the effective limit
    (``set_write_memory_limit``) — DuckDB has no competing delta_rs pool there so it gets the bulk,
    but still bounded so its host-physical-RAM default can't OOM-kill a container. The merge path
    tightens further to its 0.3 share (``set_merge_memory_limit``)."""
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
    optimize_layout: bool = False,
    plain_cols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build kwargs for ``write_deltalake`` (deltalake >= 1.2).

    ``optimize_layout`` selects the read layout — the tuned writer properties
    plus the ~1 GB ``target_file_size``. It is opt-in and used only by the experimental sort-rewrite;
    every normal write leaves it False and gets plain ZSTD + row groups with delta-rs's default file
    size (so an incremental MERGE never has to rewrite fat files). ``plain_cols`` (optimize_layout only)
    are unique columns written PLAIN — no dictionary."""
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
    if optimize_layout:
        args["target_file_size"] = _TARGET_FILE_SIZE
        wp = _tuned_writer_properties(plain_cols=plain_cols)
    else:
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


def delta_stats(cur, path: str, storage_options: Optional[Dict[str, str]] = None):
    """Cheap table statistics for ``dbt docs generate``, read from the Delta **log** (no data scan).

    ``DeltaTable.get_add_actions()`` carries per-file ``num_records`` / ``size_bytes`` /
    ``modification_time``; summing rows+bytes and taking the latest mtime gives the whole table's
    stats without opening any data file. Aggregation goes through the DuckDB cursor (``cur``) via a
    replacement scan over the arro3 table — no pyarrow dependency.

    Returns ``{"num_rows", "bytes", "last_modified"}`` (last_modified = epoch milliseconds), or
    ``None`` on ANY failure (a drop-tombstone, a missing table, an unreachable/credential-less remote
    store). Best-effort by design: a statless catalog is fine, but a docs build must never break.
    """
    try:
        add_actions = _delta_table(path, storage_options).get_add_actions()  # noqa: F841 (replacement scan)
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


def delta_file_summary(cur, path: str, storage_options: Optional[Dict[str, str]] = None):
    """Active-file list (absolute paths) + total size + VORDER flag for a Delta table — the Delta-log
    half of ``get_stats`` (the parquet footers are read separately by the caller). ``file_uris()``
    gives the live files (tombstoned ones excluded) as DuckDB-readable paths (bare local paths /
    ``abfss://`` URIs); size + VORDER come from the same ``add_actions`` replacement-scan as
    :func:`delta_stats` (no pyarrow)."""
    dt = _delta_table(path, storage_options)
    files = list(dt.file_uris())
    add_actions = dt.get_add_actions(flatten=True)  # noqa: F841 - DuckDB replacement scan by name
    size = int(cur.sql("select coalesce(sum(size_bytes), 0)::bigint from add_actions").fetchone()[0])
    # VORDER is a Fabric write tag; flatten=True surfaces it as a column named like "tags.VORDER".
    vcols = [d[0] for d in cur.sql("select * from add_actions limit 0").description]
    vcol = next((c for c in vcols if "vorder" in c.lower()), None)
    vorder = bool(vcol) and cur.sql(
        f'select count(*) from add_actions where "{vcol}" is not null').fetchone()[0] > 0
    return files, size, vorder


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
    reading a source, to pin a later merge/replace to the same snapshot (single-snapshot MERGE semantics)."""
    return _delta_table(path, storage_options).version()


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


def _maintain(dt: DeltaTable, compaction_threshold: int) -> None:
    """Threshold-gated upkeep shared by the append / merge / delete+insert paths: once the table
    has more than ``compaction_threshold`` files, compact small files, vacuum tombstoned old
    versions (safe 7-day default retention — no concurrent reader broken), and clean up expired
    log entries. Without it a table written on every run fragments into small files and keeps old
    versions forever. (The overwrite path vacuums unconditionally instead and does not use this.)

    compact() reuses the plain _writer_properties() (ZSTD + row groups) — the same config the write
    used. The read-layout tuning is not applied here; it belongs only to the opt-in
    experimental sort-rewrite. It also passes the same _TARGET_FILE_SIZE the write path targets, so
    maintenance keeps the few-fat-files layout instead of binning to delta-rs's smaller default."""
    if len(dt.file_uris()) > compaction_threshold:
        dt.optimize.compact(target_size=_TARGET_FILE_SIZE, writer_properties=_writer_properties())
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
    optimize_layout: bool = False,
    plain_cols: Optional[List[str]] = None,
) -> None:
    """
    Materialize ``data`` (a DuckDB relation / Arrow C-stream) to Delta and maintain it.

      - overwrite: write, then vacuum (safe 7-day default) + cleanup_metadata
      - append:    write, then compact/vacuum/cleanup if file count exceeds threshold
      - ignore:    write only if the table does not already exist

    ``merge_schema`` evolves the schema (adds columns); ``overwrite_schema`` replaces it wholesale
    (overwrite mode only — Delta's ``overwriteSchema``). They are mutually exclusive.

    ``optimize_layout`` opts into the read layout (tuned writer properties + ~1 GB files);
    only the experimental sort-rewrite sets it. Normal writes leave it False (plain ZSTD + row groups).
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
        optimize_layout=optimize_layout,
        plain_cols=plain_cols,
    )
    write_deltalake(**args)

    dt = _delta_table(path, storage_options)
    if mode == "overwrite":
        dt.vacuum(dry_run=False)  # safe default 168h retention (no concurrent reader broken)
        dt.cleanup_metadata()
    else:  # append
        _maintain(dt, compaction_threshold)


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
    DeltaTable.create(path, schema, mode=mode, storage_options=storage_options)


def append_if_unchanged(
    path: str,
    data,
    *,
    read_version: Optional[int],
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
    dt = _delta_table(path, storage_options)
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
            f"append_if_unchanged: table '{path}' changed since version {pinned} "
            f"(a concurrent write committed); append refused. Re-read and retry."
        ) from e

    _maintain(_delta_table(path, storage_options), compaction_threshold)


def overwrite_if_unchanged(
    path: str,
    data,
    *,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
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
    metadata cleanup as the plain overwrite path."""
    if read_version is None:
        raise ValueError(
            "overwrite_if_unchanged requires read_version (the version the caller read). A fenced "
            "overwrite must be pinned to its snapshot — a brand-new table goes through write_delta."
        )
    dt = _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    pinned = dt.version()

    schema_mode = "overwrite" if overwrite_schema else None
    args = build_write_deltalake_args(
        path, data, "overwrite",
        schema_mode=schema_mode,
        partition_by=partition_by,
        storage_options=storage_options,
    )
    args["table_or_uri"] = dt
    args.pop("storage_options", None)
    args["commit_properties"] = CommitProperties(max_commit_retries=0)
    try:
        write_deltalake(**args)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"overwrite_if_unchanged: table '{path}' changed since version {pinned} "
            f"(a concurrent write committed); overwrite refused. Re-read and retry."
        ) from e

    # Full overwrite → vacuum + cleanup like the plain overwrite path (not threshold-gated append
    # maintenance): the replaced version's files become tombstones to retire.
    dt = _delta_table(path, storage_options)
    dt.vacuum(dry_run=False)
    dt.cleanup_metadata()


def replace_where(
    path: str,
    data,
    predicate: str,
    *,
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
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
    args = build_write_deltalake_args(
        path, data, "overwrite", partition_by=partition_by, storage_options=storage_options
    )
    args["predicate"] = predicate  # replaceWhere: overwrite ONLY the rows matching the predicate
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
    read_version: Optional[int],
    partition_by: Optional[List[str]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Microbatch window replace: atomically replace the rows in ``[start, end)`` on ``column``
    with ``data`` (the batch's rows) — the Delta-native equivalent of dbt's microbatch "delete the
    window, insert the batch", as ONE atomic commit (``replaceWhere``). ``start``/``end`` are
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
    read_version: Optional[int],
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Delete rows matching ``predicate`` (a delta_rs/datafusion SQL expression), or every row
    when ``predicate`` is None. The Delta-native ``DELETE FROM`` for the connection API.

    A delete is a read-modify-write, so it is pinned to ``read_version`` (the version the caller
    read) with ``load_as_version`` and committed under delta-rs native OCC — exactly like merge:
    delta-rs validates the operation over ``(read_version, HEAD]`` and fails loudly if a
    *conflicting* commit landed since that version (a non-conflicting one rebases). ``read_version``
    is REQUIRED (no blind-HEAD path). Then maintenance at a fresh HEAD."""
    if read_version is None:
        raise ValueError(
            "delete_rows requires read_version (the version the caller read). A delete is a "
            "read-modify-write and must be pinned to its snapshot — pass the version you read "
            "(e.g. DeltaTable.forName(conn, name) captures it)."
        )
    dt = _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    try:
        dt.delete(predicate)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"delete: table '{path}' changed since version {read_version} "
            f"(a conflicting concurrent write committed); delete refused. Re-read and retry."
        ) from e
    _maintain(_delta_table(path, storage_options), compaction_threshold)


def update_rows(
    path: str,
    updates: Dict[str, str],
    predicate: Optional[str] = None,
    *,
    read_version: Optional[int],
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Update ``{column: expression}`` for rows matching ``predicate`` (delta_rs/datafusion SQL),
    or every row when ``predicate`` is None. The Delta-native ``UPDATE`` for the connection API.

    Like :func:`delete_rows`, an update is a read-modify-write: pinned to ``read_version`` with
    ``load_as_version`` and committed under delta-rs native OCC over ``(read_version, HEAD]``
    (conflict → fail, like merge). ``read_version`` is REQUIRED. Then maintenance at a fresh HEAD."""
    if read_version is None:
        raise ValueError(
            "update_rows requires read_version (the version the caller read). An update is a "
            "read-modify-write and must be pinned to its snapshot — pass the version you read "
            "(e.g. DeltaTable.forName(conn, name) captures it)."
        )
    dt = _delta_table(path, storage_options)
    dt.load_as_version(read_version)
    try:
        dt.update(updates=updates, predicate=predicate)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"update: table '{path}' changed since version {read_version} "
            f"(a conflicting concurrent write committed); update refused. Re-read and retry."
        ) from e
    _maintain(_delta_table(path, storage_options), compaction_threshold)


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
    zorder_by: Optional[List[str]] = None,
    target_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict:
    """Compact small files into larger ones (delta_rs ``DeltaTable.optimize``) and return the
    operation metrics. With ``zorder_by`` the files are Z-ordered on those columns
    (``optimize.z_order``); otherwise a plain bin-packing compaction (``optimize.compact``).

    Both rewrites reuse the plain ``_writer_properties()`` (ZSTD + row groups); the opinionated
    read-layout tuning belongs only to the experimental sort-rewrite. Note that ``z_order`` rewrites
    files in bit-interleaved order, which *destroys* long run-length runs — a lexicographic
    ``ORDER BY`` at write time is what a columnar reader wants; only reach for z-order when
    multi-dimensional file pruning matters more."""
    dt = _delta_table(path, storage_options)
    wp = _writer_properties()
    if zorder_by:
        logger.warning(
            "optimize.z_order bit-interleaves rows, which breaks the RLE runs that make an "
            "in-memory columnar reader transcode fast; prefer a lexicographic ORDER BY at write time.")
        return dt.optimize.z_order(zorder_by, target_size=target_size, writer_properties=wp)
    return dt.optimize.compact(target_size=target_size, writer_properties=wp)


def restore_to_version(
    path: str,
    version: int,
    *,
    storage_options: Optional[Dict[str, str]] = None,
) -> None:
    """Restore the table to an earlier Delta ``version`` (delta_rs ``DeltaTable.restore``). This is a
    new commit on top of history — it does not rewrite the log — so it is itself revertible."""
    dt = _delta_table(path, storage_options)
    dt.restore(version)


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
      join to disk instead of OOMing. None -> default to ~60% of RAM (_default_merge_spill_size);
      pass 0 (or any falsy non-None) to disable the cap and run unbounded.
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
    - delete_unmatched_by_source: the "WHEN NOT MATCHED BY SOURCE THEN DELETE" form — also remove
      target rows the source doesn't carry. True deletes every unmatched target row (full sync); a
      string deletes only those matching that predicate; None (default, used by the dbt incremental
      strategies) adds no such clause. Used by the connection-API ``df.merge`` full-sync option.

    After the merge, run the same threshold-gated maintenance the append/delete+insert paths
    use: when the file count exceeds ``compaction_threshold``, compact small files, vacuum
    tombstoned old versions (safe 7-day default retention), and clean up expired log entries.
    Without this an incremental table that is merged on every run grows old files forever.
    """
    keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
    # Quote the join keys: a unique_key that needs quoting (mixed case on a case-sensitive path, a
    # reserved word, spaces) would otherwise emit an invalid datafusion predicate. Strip any quotes
    # the user already put on, then double-quote and escape — datafusion accepts "…" identifiers.
    def _q(k):
        return '"' + str(k).strip().strip('"').replace('"', '""') + '"'
    conditions = [f"target.{_q(k)} = source.{_q(k)}" for k in keys]
    if predicates:
        extra = predicates if isinstance(predicates, (list, tuple)) else [predicates]
        conditions.extend(p for p in extra if p)
    predicate = " AND ".join(conditions)

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
            clauses.append({"clause": "matched", "action": "update",
                            "updates": {c: f"source.{c}" for c in update_columns},
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
        streamed_exec=streamed_exec,
        max_spill_size=max_spill_size,
        storage_options=storage_options,
        compaction_threshold=compaction_threshold,
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


def merge_delta_clauses(
    path: str,
    data,
    predicate: str,
    clauses: List[dict],
    *,
    read_version: Optional[int] = None,
    merge_schema: bool = False,
    streamed_exec: bool = False,
    max_spill_size: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    compaction_threshold: int = 100,
) -> None:
    """Run a MERGE described by an ORDERED list of clause dicts — the full delta-rs ``TableMerger``
    surface. Each clause is ``{"clause": "matched"|"not_matched"|"not_matched_by_source",
    "action": "update"|"update_all"|"delete"|"insert"|"insert_all", "predicate": str|None,
    "updates": {col: expr}|None, "except_cols": [..]|None}`` and is applied in order (delta-rs
    evaluates them top-to-bottom). ``predicate`` is the full ON condition, referencing the literal
    ``target``/``source`` aliases.

    This is the shared core for every merge path: ``merge_delta`` (dbt incremental — builds a fixed
    clause list), the raw-SQL MERGE handler, and the DataFrame ``DeltaTable.merge`` builder. The
    spill cap, target pruning, the REQUIRED ``read_version`` snapshot pin (OCC over (vB, HEAD]), and
    the post-merge maintenance are identical for every clause shape, so the single-snapshot and
    concurrency-safety guarantees hold for all of them. See ``merge_delta`` for the parameter
    semantics (spill / streamed_exec / read_version / maintenance)."""
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

    # A merge always has an existing target (a brand-new table is created, never merged into), so
    # the caller (the dbt materialization / DeltaTable.merge) always pins the version it read.
    # read_version=None would silently merge against HEAD and reopen the read->write gap — refuse it.
    if read_version is None:
        raise ValueError(
            "merge_delta_clauses requires read_version (the version the caller read). A merge always "
            "has an existing target to pin to; None would merge against HEAD and break single-snapshot."
        )
    if not clauses:
        raise ValueError("merge has no clauses")

    # Cardinality guard — applied to EVERY merge path (the dbt materialization, raw-SQL MERGE, and the
    # DataFrame DeltaTable.merge all land here), so a keyed upsert behaves identically across them. A
    # keyed merge/insert cannot resolve two source rows for one target row: Spark/Snowflake/BigQuery
    # raise, but delta_rs silently produces duplicate rows. So when the merge has an update/insert
    # clause keyed on an equality predicate, require the source to be unique on that key. Skipped when
    # streamed_exec is set (the caller has a huge source it explicitly does NOT want collected) or when
    # the predicate isn't a plain AND-of-equalities (OR / non-equality / by-source-only — no key to check).
    _has_upsert = any(
        c.get("clause") in ("matched", "not_matched")
        and c.get("action") in ("update", "update_all", "insert", "insert_all")
        for c in clauses
    )
    if _has_upsert and not streamed_exec and hasattr(data, "query"):
        src_keys = _merge_source_keys(predicate)
        if src_keys:
            keycols = ", ".join('"' + k.replace('"', '""') + '"' for k in src_keys)
            try:
                dup = data.query(
                    "__merge_src",
                    f"SELECT {keycols}, count(*) AS __n FROM __merge_src "
                    f"GROUP BY {keycols} HAVING count(*) > 1 LIMIT 1",
                ).fetchone()
            except Exception as e:  # never let the guard ITSELF break a valid merge; surface and proceed
                logger.warning(f"merge duplicate-key guard could not run ({e!r}); proceeding")
                dup = None
            if dup is not None:
                keyval = ", ".join(f"{k}={v!r}" for k, v in zip(src_keys, dup[:-1]))
                raise ValueError(
                    f"MERGE source is not unique on the join key ({', '.join(src_keys)}): "
                    f"{dup[-1]} rows for {keyval}. A keyed merge/insert cannot resolve duplicate "
                    f"source keys — Spark, Snowflake and BigQuery raise the same error, and delta_rs "
                    f"would silently produce duplicate rows. Deduplicate the source, e.g. "
                    f"qualify row_number() over (partition by {keycols} order by <tiebreak>) = 1."
                )

    dt = _delta_table(path, storage_options)
    # Pin the target to the snapshot the model read (vB) so OCC validates (vB, HEAD] — one snapshot
    # for both the read and the commit.
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
    for c in clauses:
        merger = _apply_merge_clause(merger, c)
    merger.execute()

    # Same threshold-gated maintenance as the append / delete+insert paths: a merged-on-every-run
    # incremental table fragments into small files and leaves tombstoned old versions otherwise.
    _maintain(_delta_table(path, storage_options), compaction_threshold)
