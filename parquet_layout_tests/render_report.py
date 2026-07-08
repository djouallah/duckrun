import json
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

_PARQUET_COLS = ["table", "rows", "files", "row_groups", "avg_row_group",
                 "size_mb", "vorder", "compression"]


def _write(md):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")
    print(md)


def _fmt(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return f"{v:,.1f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


def _parquet_table(tables):
    rows = []
    for name, t in tables.items():
        p = t.get("parquet")
        if not p:
            continue
        rows.append([name] + [p.get(c) for c in _PARQUET_COLS[1:]])
    if not rows:
        return
    out = ["## Table layout (Parquet)", "",
           "| " + " | ".join(_PARQUET_COLS) + " |",
           "|" + "|".join([":--"] + ["--:"] * 5 + [":--", ":--"]) + "|"]
    for r in rows:
        out.append("| " + " | ".join(_fmt(v) for v in r) + " |")
    _write("\n".join(out) + "\n")


def _cmp(base, model, tb, tm, key, title):
    out = [f"### {title}", "",
           f"| Query | {base} (ms) | {model} (ms) | base/model | Winner |",
           "|:--|--:|--:|--:|:--|"]
    bt = mt = 0.0
    for q in tm:
        if q not in tb:
            continue
        b, m = tb[q].get(key), tm[q].get(key)
        if b is None or m is None:
            continue
        bt += b
        mt += m
        sp = (b / m) if m else float("inf")
        win = "model" if m < b else ("base" if m > b else "tie")
        out.append(f"| `{q}` | {b:,.1f} | {m:,.1f} | {sp:.2f}× | {win} |")
    if not (bt or mt):
        return
    ov = (bt / mt) if mt else float("inf")
    win = "model" if mt < bt else ("base" if mt > bt else "tie")
    out.append(f"| **TOTAL** | **{bt:,.1f}** | **{mt:,.1f}** | **{ov:.2f}×** | **{win}** |")
    _write("\n".join(out) + "\n")


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to render")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    run = rep.get("run", {})
    _write(f"# Direct Lake query benchmark — `{run.get('sha')}` "
           f"(run {run.get('run_id')})\n")

    _parquet_table(rep.get("tables", {}))

    timings = rep.get("timings", {})
    base = next((m for m in timings if m.endswith("_optimized")),
                min(timings, key=len) if timings else None)
    if base:
        tb = timings[base]
        for model in timings:
            if model == base:
                continue
            tm = timings[model]
            _cmp(base, model, tb, tm, "cold_ms", f"{model} vs {base} — COLD")
            _cmp(base, model, tb, tm, "hot_avg_ms", f"{model} vs {base} — HOT")


if __name__ == "__main__":
    main()
