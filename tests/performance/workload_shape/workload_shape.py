"""The workload-shape gate: a real fact's shape, written under a real memory limit.

Every other gate writes narrow tables (fct_summary, 6 columns; lineitem, 16) under DuckDB's default
memory limit, 80% of the runner. 0.6.0's writer passed them all and then failed in production on
the AEMO fct_price, 130 columns under a 9 GB limit: DuckDB reserves each column's whole dictionary
when it starts a row group, so the cost grows with the COLUMNS, which no gate had many of.

This writes a fct_price-shaped table -- 131 columns: 60 low-cardinality DOUBLEs, 60
high-cardinality DOUBLEs, 8 low-cardinality strings, a file name, a timestamp, a unique id --
under ``--memory-limit`` (the limit bench's workload legs run with), through every write path:

    create (CREATE TABLE AS) -> append (INSERT) -> merge (update + insert)
      -> 10 small appends -> compaction (OPTIMIZE, which must rewrite them)

and fails (exit 1) when a step raises (out of memory included), the row count is wrong, a
low-cardinality column chunk is not dictionary-encoded (the Direct Lake layout), or the process
peak RSS passes ``--max-rss-gb`` (what the runner can hold).

    python tests/performance/workload_shape/workload_shape.py --dir /tmp/ws --rows 4000000
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import threading
import time

import deltalake

import duckrun

LOW, HIGH, STRINGS = 60, 60, 8


def select(lo: int, hi: int, bump: int = 0) -> str:
    """Rows [lo, hi) of the table; ``bump`` changes the low-cardinality values (a real update)."""
    cols = [f"(((i + {k} + {bump}) * 7) % 997)::DOUBLE / 100 AS low_{k:02d}" for k in range(LOW)]
    cols += [f"(hash(i, {k}) % 1000000007)::DOUBLE / 1000 AS high_{k:02d}" for k in range(HIGH)]
    cols += [f"'DUID_' || ((i + {k}) % 500) AS str_{k}" for k in range(STRINGS)]
    cols += [
        "'PUBLIC_DAILY_' || (20260101 + i // 200000) || '.CSV' AS file",
        "TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_minutes(i % 525600) AS SETTLEMENTDATE",
        "i AS id",
    ]
    return f"SELECT {', '.join(cols)} FROM range({lo}, {hi}) t(i)"


class PeakRss:
    """This process's peak resident memory (delta-rs and DuckDB both live in it)."""

    def __init__(self) -> None:
        import psutil

        self.proc, self.peak, self._stop = psutil.Process(), 0, threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self._stop.wait(0.25):
            self.peak = max(self.peak, self.proc.memory_info().rss)

    def __enter__(self) -> PeakRss:
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=None, help="warehouse root (default: a temp dir)")
    ap.add_argument("--rows", type=int, default=4_000_000, help="rows the create writes")
    ap.add_argument("--memory-limit", default="9GB", help="DuckDB memory_limit for the session")
    ap.add_argument("--max-rss-gb", type=float, default=14.0, help="peak RSS the runner can hold")
    args = ap.parse_args()

    root = args.dir or tempfile.mkdtemp(prefix="workload_shape_")
    os.makedirs(root, exist_ok=True)
    n = args.rows
    conn = duckrun.connect(root, schema="dbo", read_only=False)
    conn.sql(f"SET memory_limit='{args.memory_limit}'")

    steps = [
        ("create", f"CREATE OR REPLACE TABLE wide AS {select(0, n)}"),
        ("append", f"INSERT INTO wide {select(n, n + n // 4)}"),
        # Matches the last eighth of the create and all of the append, and inserts a quarter more.
        ("merge", f"MERGE INTO wide USING ({select(n - n // 8, n + n // 2, bump=1)}) s "
                  "ON wide.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"),
    ]
    # Ten small files after the merge, so OPTIMIZE has something to compact.
    small = n // 1000 or 1
    steps += [(f"small append {k}", f"INSERT INTO wide {select(n + n // 2 + k * small, n + n // 2 + (k + 1) * small)}")
              for k in range(10)]
    steps.append(("compaction", "OPTIMIZE wide"))
    expected = n + n // 2 + 10 * small
    files_before = None
    failures, lines = [], []
    with PeakRss() as rss:
        for name, sql in steps:
            if name == "compaction":
                files_before = len(deltalake.DeltaTable(f"{root}/dbo/wide").file_uris())
            t0 = time.perf_counter()
            try:
                conn.sql(sql)
                status = "ok"
            except Exception as e:  # noqa: BLE001 - every failure is a gate failure, reported
                status = f"FAILED: {type(e).__name__}: {str(e)[:300]}"
                failures.append(f"{name}: {status}")
            if not name.startswith("small append") or status != "ok":
                lines.append(f"| {name} | {status} | {time.perf_counter() - t0:.1f} s "
                             f"| {rss.peak / 2**30:.2f} GiB |")
                print(lines[-1], flush=True)
            if status != "ok":
                break

    if not failures:
        rows = conn.sql("SELECT count(*) FROM wide").fetchall()[0][0]
        if rows != expected:
            failures.append(f"rows: {rows:,}, expected {expected:,}")
        files = deltalake.DeltaTable(f"{root}/dbo/wide").file_uris()
        if files_before is not None and len(files) >= files_before:
            failures.append(f"compaction rewrote nothing: {files_before} files before, {len(files)} after")
        lines.append(f"| files | {files_before} before compaction, {len(files)} after | | |")
        low = [f"low_{k:02d}" for k in range(LOW)] + [f"str_{k}" for k in range(STRINGS)] + ["file"]
        plain = conn.sql(
            f"SELECT path_in_schema, count(*) FROM parquet_metadata({files!r}) "
            f"WHERE path_in_schema IN ({', '.join(repr(c) for c in low)}) "
            "AND encodings NOT LIKE '%DICTIONARY%' GROUP BY ALL ORDER BY 1").fetchall()
        if plain:
            failures.append(f"low-cardinality column chunks not dictionary-encoded: {plain[:10]}")
        high = conn.sql(
            f"SELECT avg((encodings LIKE '%DICTIONARY%')::INT) FROM parquet_metadata({files!r}) "
            "WHERE path_in_schema LIKE 'high_%'").fetchall()[0][0]
        lines.append(f"| dictionary | high-cardinality chunks dictionary-encoded: {high:.0%} | | |")
    if rss.peak > args.max_rss_gb * 2**30:
        failures.append(f"peak RSS {rss.peak / 2**30:.2f} GiB > {args.max_rss_gb} GiB")

    report = [f"### Workload shape: {LOW + HIGH + STRINGS + 3} columns, {n:,} rows, "
              f"memory_limit {args.memory_limit}", "", "| step | result | time | peak RSS |",
              "|---|---|---|---|", *lines, "",
              "**PASS**" if not failures else "**FAIL**\n\n" + "\n".join(f"- {f}" for f in failures)]
    print("\n".join(report))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("\n".join(report) + "\n")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
