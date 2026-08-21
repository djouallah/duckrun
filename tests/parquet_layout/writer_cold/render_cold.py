"""Render the cold comparison to the job summary — the deliverable of the whole workflow.

Two things it deliberately makes visible rather than hiding in a ratio:

* the CONTROL (probe_rowcount) alongside every query, because cold time is
  fixed-overhead + transcode and only the marginal figure is about the parquet;
* whether the CU number is settled, because an immediate read is a lower bound.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "aemo"))

rep_path = os.environ.get("RUN_REPORT", "run_report.json")
if not os.path.exists(rep_path):
    raise SystemExit(f"no run report at {rep_path} — nothing to render")
rep = json.load(open(rep_path, encoding="utf-8"))
cold = rep.get("cold") or {}
if not cold:
    raise SystemExit("run report has no cold results — the benchmark did not get that far")

variants = sorted(cold)
out = ["## Cold Direct Lake — one pass, nothing cached", ""]
out.append("Each query is the first touch of its column, so its time is that column's "
           "Delta→memory transcode plus fixed overhead. `probe_rowcount` is the ~zero-column "
           "control; `marginal` subtracts it.")
out.append("")
out.append("| query | " + " | ".join(f"{v} cold ms" for v in variants)
           + " | " + " | ".join(f"{v} marginal" for v in variants) + " |")
out.append("|---|" + "---|" * (2 * len(variants)))

names = [q["query"] for q in cold[variants[0]]["queries"]]
for n in names:
    cells, marg = [], []
    for v in variants:
        q = next((x for x in cold[v]["queries"] if x["query"] == n), None)
        cells.append(f"{q['cold_ms']:.1f}" if q else "—")
        marg.append(f"{q['marginal_ms']:.1f}" if q else "—")
    out.append(f"| {n} | " + " | ".join(cells) + " | " + " | ".join(marg) + " |")
out.append("| **total** | "
           + " | ".join(f"**{cold[v]['total_cold_ms']:.1f}**" for v in variants)
           + " | " + " | ".join("" for _ in variants) + " |")

cu = rep.get("cu")
if cu and cu.get("by_item"):
    state = "settled" if cu.get("settled") else "LOWER BOUND (CU accrues for ~70 min after the run)"
    out += ["", f"## CU by item — {state}", "",
            "| item | operation | CU-s |", "|---|---|---|"]
    for label, ops in sorted(cu["by_item"].items()):
        for op, val in sorted(ops.items(), key=lambda kv: -kv[1]):
            out.append(f"| {label} | {op} | {val:.1f} |")
    if not cu.get("settled"):
        out += ["", f"Re-read later with `settle_run={cu.get('run')}` to get the settled figure."]
else:
    out += ["", "_No CU recorded — the Capacity Metrics read did not run or found nothing._"]

text = "\n".join(out)
print("\n" + text, flush=True)
summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    with open(summary, "a", encoding="utf-8") as f:
        f.write(text + "\n\n")
