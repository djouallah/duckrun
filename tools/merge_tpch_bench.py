"""Generate TPCH ``lineitem`` with DuckDB and merge it through duckrun, to exercise the
merge memory cap / disk-spill end to end.

Two phases so the (memory-heavy) data generation runs *outside* any tight memory cgroup,
and only the merge runs under the limit:

    # 1. generate base Delta table + a source parquet (run unconstrained)
    python tools/merge_tpch_bench.py prepare --dir /tmp/m --sf 3

    # 2. merge the source into a fresh copy of the base, under whatever memory limit the
    #    process is launched with (e.g. systemd-run --scope -p MemoryMax=4G ...)
    python tools/merge_tpch_bench.py merge --dir /tmp/m --spill default   # cgroup-aware ~80%
    python tools/merge_tpch_bench.py merge --dir /tmp/m --spill 1073741824 # explicit bytes
    python tools/merge_tpch_bench.py merge --dir /tmp/m --spill off        # unbounded (OOMs)

``merge`` exits 0 only if the upsert completes and the row count matches. Under a tight
``MemoryMax`` the ``off`` run is expected to be OOM-killed (exit 137) while ``default`` /
explicit caps spill to disk and succeed — that contrast is the whole point.
"""
import argparse
import os
import shutil
import sys
import time

import duckdb

from deltalake import DeltaTable

from dbt.adapters.duckrun import engine

KEYS = ["l_orderkey", "l_linenumber"]
BASE = "base"          # the Delta table to merge into
SOURCE = "source.parquet"  # the rows to merge (every key, l_quantity bumped by 1)


def _peak_rss_mb():
    """Best-effort peak memory in MB: Linux VmHWM, else Windows PeakWorkingSetSize; None
    if neither is available."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    try:
        import ctypes
        from ctypes import wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (n, ctypes.c_size_t) for n in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")
            ]

        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        p = _PMC()
        p.cb = ctypes.sizeof(_PMC)
        if ctypes.windll.psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(p), p.cb):
            return p.PeakWorkingSetSize // (1024 * 1024)
    except Exception:
        pass
    return None


def prepare(args):
    os.makedirs(args.dir, exist_ok=True)
    base = os.path.join(args.dir, BASE)
    source = os.path.join(args.dir, SOURCE)

    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    t = time.time()
    con.execute(f"CALL dbgen(sf={args.sf})")
    n = con.execute("SELECT count(*) FROM lineitem").fetchone()[0]
    print(f"prepare: generated lineitem rows={n:,} sf={args.sf} in {time.time()-t:.1f}s")

    if os.path.exists(base):
        shutil.rmtree(base)
    engine.write_delta(base, con.sql("SELECT * FROM lineitem"), "overwrite")
    # Source: same keys, l_quantity + 1, so every row is a real UPDATE on match.
    con.execute(
        f"COPY (SELECT * REPLACE (l_quantity + 1 AS l_quantity) FROM lineitem) "
        f"TO '{source}' (FORMAT parquet)"
    )
    print(f"prepare: wrote base Delta ({len(DeltaTable(base).file_uris())} files) and source parquet")


def _parse_spill(s):
    if s == "off":
        return 0          # falsy non-None -> merge_delta omits the cap (unbounded)
    if s == "default":
        return None       # None -> merge_delta applies the cgroup-aware ~80% default
    return int(s)         # explicit byte count


def merge(args):
    base = os.path.join(args.dir, BASE)
    source = os.path.join(args.dir, SOURCE)
    work = os.path.join(args.dir, "work")
    if os.path.exists(work):
        shutil.rmtree(work)
    shutil.copytree(base, work)

    expected = DeltaTable(base).to_pyarrow_dataset().count_rows()
    spill = _parse_spill(args.spill)
    eff = engine._effective_mem_limit_bytes()
    dflt = engine._default_merge_spill_size()
    print(f"merge: effective_mem={None if eff is None else round(eff/1048576)}MB "
          f"default_cap={None if dflt is None else round(dflt/1048576)}MB "
          f"requested_spill={args.spill}")

    con = duckdb.connect()
    src = con.sql(f"SELECT * FROM read_parquet('{source}')")  # streamed, like duckrun
    t = time.time()
    engine.merge_delta(work, src, KEYS, max_spill_size=spill)
    dt = time.time() - t

    got = DeltaTable(work).to_pyarrow_dataset().count_rows()
    peak = _peak_rss_mb()
    print(f"merge: DONE in {dt:.1f}s rows={got:,} (expected {expected:,}) "
          f"peakRSS={peak}MB")
    if got != expected:
        print(f"merge: ROW COUNT MISMATCH {got} != {expected}", file=sys.stderr)
        sys.exit(2)
    print("merge: OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--dir", required=True)
    p.add_argument("--sf", type=float, default=1.0)
    p.set_defaults(func=prepare)
    m = sub.add_parser("merge")
    m.add_argument("--dir", required=True)
    m.add_argument("--spill", default="default", help="'default' | 'off' | <bytes>")
    m.set_defaults(func=merge)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
