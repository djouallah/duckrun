"""Release gate over a writer_cold run: fail when a cold Direct Lake probe regressed.

Reads the run's report (``RUN_REPORT``, default ``run_report.json``) and ``gate_baseline.json``
next to this file. Every arm and probe the baseline lists must be in the report, and each probe's
``cold_ms`` must be at most ``factor`` x its baseline. A missing arm or probe fails too: a gate
that measured nothing must not pass."""
import json
import os
import sys
from pathlib import Path


def check(report: dict, baseline: dict) -> tuple:
    factor = float(baseline["factor"])
    cold = report.get("cold") or {}
    problems, lines = [], []
    for arm, probes in baseline["arms"].items():
        got = {q["query"]: q["cold_ms"] for q in (cold.get(arm) or {}).get("queries", [])}
        for probe, base in probes.items():
            limit = factor * float(base)
            ms = got.get(probe)
            if ms is None:
                problems.append(f"{arm}/{probe}: not measured")
                continue
            verdict = "ok" if ms <= limit else "REGRESSED"
            lines.append(f"| {arm} | {probe} | {ms:,.1f} | {base:,.1f} | {limit:,.1f} | {verdict} |")
            if ms > limit:
                problems.append(f"{arm}/{probe}: {ms:,.1f} ms > {limit:,.1f} ms "
                                f"({factor:g} x baseline {base:,.1f})")
    return problems, lines


def main() -> int:
    report = json.loads(Path(os.environ.get("RUN_REPORT", "run_report.json")).read_text())
    baseline = json.loads((Path(__file__).with_name("gate_baseline.json")).read_text())
    problems, lines = check(report, baseline)
    table = "\n".join(["### Direct Lake release gate", "",
                       "| arm | probe | cold ms | baseline | limit | verdict |",
                       "|---|---|---|---|---|---|", *lines, ""])
    print(table)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(table + "\n")
    for p in problems:
        print(f"::error::Direct Lake gate: {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
