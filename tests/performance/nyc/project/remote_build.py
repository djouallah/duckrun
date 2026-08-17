"""Run `dbt build --full-refresh` for the nyc benchmark project — INSIDE a Fabric Python notebook.

Shipped whole (this folder) by tests/performance/nyc/nyc_bench.py via duckrun.run_python, which
pip-installs duckrun from the commit under test, unpacks the project as cwd and executes this file
in a fresh interpreter on Fabric compute — data-local to OneLake, on the ~135 GiB work disk the
591.7M-row build's spill needs (measured peak: 111 GiB; a GitHub runner has ~66). stdout streams
back to the runner as `[remote]` lines; the one `NYC_BENCH_RESULT {json}` line at the end is what
nyc_bench.py parses for per-model timings and versions.

Config arrives via env (WAREHOUSE_PATH, FILES_PATH, DBT_SCHEMA, DUCKDB_* knobs) — never tokens:
duckrun self-acquires through the Fabric runtime.
"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time

_TICK_SECONDS = 30


def _meminfo(key):
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith(key + ":"):
                    return int(line.split()[1]) * 1024
    except Exception:
        return None
    return None


def _gib(n):
    return "?" if n is None else f"{n / 2**30:.1f}GiB"


def _spill_usage(path):
    total = files = 0
    try:
        for root, _dirs, names in os.walk(path):
            for name in names:
                try:
                    total += os.path.getsize(os.path.join(root, name))
                    files += 1
                except OSError:
                    pass
    except Exception:
        return 0, 0
    return total, files


def _ticker(stop, spill_dir):
    t0 = time.monotonic()
    while not stop.wait(_TICK_SECONDS):
        avail = _meminfo("MemAvailable")
        spill, nf = _spill_usage(spill_dir)
        try:
            free = shutil.disk_usage(os.path.dirname(spill_dir) or "/").free
        except Exception:
            free = None
        print(f"[nyc_build] t=+{int(time.monotonic() - t0)}s mem_avail={_gib(avail)} "
              f"spill={_gib(spill)}/{nf} files disk_free={_gib(free)}", flush=True)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)
    env = dict(os.environ)
    env.setdefault("DBT_PROFILES_DIR", here)
    # Spill MUST live beside the unpacked project: the run_python harness unpacks onto the
    # ~135 GiB work disk, while the container's /tmp is a ~19 GiB overlay — the first dispatch
    # defaulted there and the msrc CTAS filled it in 3 minutes (max_temp_directory_size is sized
    # from the temp dir's free space). The dbt_project.yml hook reads the same env var.
    spill_dir = env.setdefault("DUCKDB_TEMP_DIR", os.path.join(here, "duckdb_spill"))
    try:
        du = shutil.disk_usage(here)
        print(f"[nyc_build] spill={spill_dir} disk total={_gib(du.total)} free={_gib(du.free)}",
              flush=True)
    except Exception:
        pass

    stop = threading.Event()
    threading.Thread(target=_ticker, args=(stop, spill_dir), daemon=True).start()
    t0 = time.monotonic()
    proc = subprocess.run(["dbt", "build", "--full-refresh"], env=env)
    elapsed = time.monotonic() - t0
    stop.set()

    result = {"ok": proc.returncode == 0, "build_seconds": round(elapsed, 1), "models": {}}
    try:
        with open(os.path.join(here, "target", "run_results.json")) as fh:
            rr = json.load(fh)
        for node in rr.get("results", []):
            uid = node.get("unique_id", "")
            if uid.startswith("model."):
                result["models"][uid.rsplit(".", 1)[-1]] = round(node.get("execution_time", 0.0), 1)
    except Exception as exc:
        result["run_results_error"] = str(exc)
    try:
        import duckdb
        import deltalake
        from importlib.metadata import version
        result["versions"] = {"duckrun": version("duckrun"), "duckdb": duckdb.__version__,
                              "deltalake": deltalake.__version__}
    except Exception:
        pass
    print("NYC_BENCH_RESULT " + json.dumps(result), flush=True)
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
