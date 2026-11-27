"""The workload gate's dbt runs (see .github/workflows/workload_gate.yml).

Runs in bench's workload/ directory, its duckrun target: ``dbt run`` with process_limit
CREATE_FILES (the models are created), then with MERGE_FILES (one incremental merge of older,
unseen files: every row new). Each run's peak RSS (this process and its children) and peak spill
(DUCKDB_TEMP_DIR plus TMPDIR) are sampled. Fails (exit 1) on a dbt error, on fct_scada not holding
every file both runs folded, or on a peak RSS past --max-rss-gb.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time

import duckdb
import psutil


def _dir_bytes(path: str) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:  # a spill file removed while walking
                pass
    return total


class Peak:
    """Peak RSS of this process tree and peak size of the spill directories, while running."""

    def __init__(self, dirs):
        self.dirs, self.rss, self.spill = [d for d in dirs if d], 0, 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        me = psutil.Process()
        while not self._stop.wait(0.5):
            rss = me.memory_info().rss
            for child in me.children(recursive=True):
                try:
                    rss += child.memory_info().rss
                except psutil.Error:
                    pass
            self.rss = max(self.rss, rss)
            self.spill = max(self.spill, sum(_dir_bytes(d) for d in self.dirs))

    def __enter__(self) -> Peak:
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


def dbt_run(process_limit: int):
    """One ``dbt run`` of the duckrun target: (success, seconds, [(model, status, seconds)])."""
    from dbt.cli.main import dbtRunner

    os.environ["process_limit"] = str(process_limit)  # noqa: SIM112 - the models read it lower case
    t0 = time.perf_counter()
    res = dbtRunner().invoke(["run", "--target", "duckrun", "--profiles-dir", "."])
    nodes = [(r.node.name, str(r.status), round(r.execution_time, 1))
             for r in getattr(res.result, "results", None) or []]
    if res.exception:
        print(res.exception, flush=True)
    return bool(res.success), time.perf_counter() - t0, nodes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create-files", type=int, default=int(os.environ.get("CREATE_FILES", 30)))
    ap.add_argument("--merge-files", type=int, default=int(os.environ.get("MERGE_FILES", 365)))
    ap.add_argument("--max-rss-gb", type=float, default=14.0, help="what the runner can hold")
    args = ap.parse_args()

    failures, lines = [], []
    for label, limit in (("create", args.create_files), ("merge", args.merge_files)):
        with Peak([os.environ.get("DUCKDB_TEMP_DIR"), os.environ.get("TMPDIR")]) as peak:
            ok, seconds, nodes = dbt_run(limit)
        lines.append(f"| {label} ({limit} files) | {'ok' if ok else 'FAILED'} | {seconds:.0f} s "
                     f"| {peak.rss / 2**30:.2f} GiB | {peak.spill / 2**30:.2f} GiB |")
        lines += [f"| &nbsp;&nbsp;{name} | {status} | {secs} s | | |" for name, status, secs in nodes]
        print("\n".join(lines[-len(nodes) - 1:]), flush=True)
        if not ok:
            failures.append(f"{label}: dbt run failed")
            break
        if peak.rss > args.max_rss_gb * 2**30:
            failures.append(f"{label}: peak RSS {peak.rss / 2**30:.2f} GiB > {args.max_rss_gb} GiB")

    if not failures:
        table = os.path.join(os.environ["ONELAKE_TABLES_PATH"], "dbt_duckrun_landing", "fct_scada")
        con = duckdb.connect()
        files = con.sql(f"SELECT count(DISTINCT file) FROM delta_scan('{table}')").fetchall()[0][0]
        want = args.create_files + args.merge_files
        lines.append(f"| fct_scada files | {files} of {want} | | | |")
        if files != want:
            failures.append(f"fct_scada holds {files} daily file(s), expected {want}")

    report = ["### Workload gate: bench's dbt workload, duckrun target, memory_limit "
              f"{os.environ.get('DUCKDB_MEMORY_LIMIT')}", "",
              "| step | result | time | peak RSS | peak spill |", "|---|---|---|---|---|", *lines, "",
              "**PASS**" if not failures else "**FAIL**\n\n" + "\n".join(f"- {f}" for f in failures)]
    print("\n".join(report))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("\n".join(report) + "\n")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
