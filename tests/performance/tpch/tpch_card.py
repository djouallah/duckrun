"""Run the duckrun TPC-H benchmark and render its scorecard (ingestion + 22 query timings).

Mirrors ``snapshot_pin_card.py``: runs the benchmark on a fresh local warehouse, writes the card to
``docs/tpch_card.md`` (for injection into ``docs/tpch.md``), to the GitHub Actions step summary, and
to the console. SF defaults to 1 for a quick local run; CI sets ``TPCH_SF`` (the cores job runs
SF=10). Exit 0 iff the run is well-formed (all 8 tables ingested + 22 queries timed), else 1 — so it
doubles as the CI guard (a broken read/write surface fails the job).

    TPCH_SF=10 python tests/performance/tpch/tpch_card.py

Run from the repo root (it writes docs/tpch_card.md relative to the cwd).
"""
import os
import sys
import tempfile
from datetime import datetime, timezone

import deltalake
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)                                      # test_tpch.py + tpch_summary.py (same dir)

from test_tpch import TPCH_TABLES, run_tpch_benchmark  # noqa: E402
from tpch_summary import render_card  # noqa: E402


def _run_ref() -> str:
    """A markdown link to this GitHub Actions run (or 'local' outside CI)."""
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        return "local"
    num = os.environ.get("GITHUB_RUN_NUMBER", run_id)
    url = (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
           f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{run_id}")
    return f"[#{num}]({url})"


def _write_history_row(timings: dict, ok: bool) -> None:
    """Write ONE markdown table row for this run to docs/tpch_history_row.md, which the workflow
    appends to the tracked docs/tpch-benchmark-history.md so every run leaves a permanent entry.
    Gitignored; the workflow is the only thing that commits it."""
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    commit = os.environ.get("GITHUB_SHA", "")[:7] or "local"
    ing_total = sum(r["dur"] for r in timings.get("ingestion", []))
    qry_total = sum(r["dur"] for r in timings.get("queries", []))
    row = (f"| {date} | {_run_ref()} | {commit} | {timings.get('duckdb', '?')} "
           f"| {timings.get('deltalake', '?')} | {timings['sf']} | {timings['cpu']} "
           f"| {ing_total:.1f}s | {qry_total:.1f}s | {'✅' if ok else '❌'} |\n")
    with open("docs/tpch_history_row.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(row)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    sf = int(os.environ.get("TPCH_SF", "1"))
    timings = {}
    with tempfile.TemporaryDirectory() as tmp:
        run_tpch_benchmark(sf=sf, base_path=os.path.join(tmp, "wh"), timings_out=timings)
    timings["duckdb"] = duckdb.__version__
    timings["deltalake"] = deltalake.__version__

    # Well-formed iff every table was ingested and all 22 queries ran in order.
    ok = (len(timings.get("ingestion", [])) == len(TPCH_TABLES)
          and [q["query"] for q in timings.get("queries", [])] == list(range(1, 23)))

    card = render_card(timings)
    print(card)
    with open("docs/tpch_card.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(card)
    _write_history_row(timings, ok)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(card)
    return 0 if ok else 1


if __name__ == "__main__":
    _code = main()
    # delta-rs (Tokio) / duckdb native runtimes can abort the interpreter during shutdown on Linux
    # even on success; exit hard with the already-computed result, like snapshot_pin_card.py.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_code)
