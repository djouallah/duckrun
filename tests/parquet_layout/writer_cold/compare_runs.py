"""Put N writer_cold runs side by side — the ms ledger and the MB ledger in one table.

Every arm of a sweep is a separate workflow run with its own `writer-cold-report` artifact, and
render_cold.py only ever sees ONE run (its `variants` are the arms *within* a run). Comparing an
order or row-group sweep therefore meant reading N job logs by eye, which is how a 42%-bytes /
2%-time result stays invisible.

Config comes from `tables.<t>.build` in each report — the APPLIED sort key, row-group ceiling and
file count, not the dispatch form. That distinction is load-bearing: a workflow input records what
was asked for, and on this harness the two have diverged before.

Usage:  python compare_runs.py 32681772436 32681270789 [...]
        python compare_runs.py --repo djouallah/duckrun <run_id>...

Needs `gh` on PATH and read access to the repo. Reports are cached under the system temp dir, so
re-running against the same ids is free.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

REPO = "djouallah/duckrun"
ARTIFACT = "writer-cold-report"


def fetch(run_id, repo):
    """Download (and cache) one run's run_report.json."""
    cache = os.path.join(tempfile.gettempdir(), "writer_cold_reports", str(run_id))
    path = os.path.join(cache, "run_report.json")
    if not os.path.exists(path):
        os.makedirs(cache, exist_ok=True)
        subprocess.run(["gh", "run", "download", str(run_id), "--repo", repo,
                        "-n", ARTIFACT, "-D", cache], check=True)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _table(rep):
    """The one built table in the report. Sweeps run a single arm, so this is unambiguous; with
    several arms the first is taken and the caller sees it in the header."""
    tables = rep.get("tables") or {}
    return (sorted(tables)[0], tables[sorted(tables)[0]]) if tables else (None, {})


def _label(rep):
    _, t = _table(rep)
    b = t.get("build") or {}
    sort = (b.get("sort") or "?").replace("sorted by ", "")
    rg = b.get("row_group_ceiling")
    return f"{sort} @{rg/1e6:.0f}M" if rg else sort


def main(argv):
    repo = REPO
    if "--repo" in argv:
        i = argv.index("--repo")
        repo = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    if not argv:
        raise SystemExit(__doc__)
    if not shutil.which("gh"):
        raise SystemExit("compare_runs: `gh` not on PATH — needed to download the run artifacts")

    reps = [(rid, fetch(rid, repo)) for rid in argv]
    labels = [_label(r) for _, r in reps]

    print(f"\n| | {' | '.join(labels)} |")
    print("|---|" + "---|" * len(reps))
    print(f"| run | {' | '.join(str(rid) for rid, _ in reps)} |")

    # --- geometry, straight from what was built ---
    for field, name, fmt in (("files", "files", str),
                             ("rows", "rows", lambda v: f"{v:,}"),
                             ("seconds", "build s", lambda v: f"{v}")):
        vals = []
        for _, rep in reps:
            b = (_table(rep)[1].get("build") or {})
            vals.append(fmt(b[field]) if b.get(field) is not None else "—")
        print(f"| {name} | {' | '.join(vals)} |")
    for field, name in (("row_groups", "row groups"), ("size_mb", "size MB")):
        vals = []
        for _, rep in reps:
            p = (_table(rep)[1].get("parquet") or {})
            vals.append(str(p[field]) if p.get(field) is not None else "—")
        print(f"| {name} | {' | '.join(vals)} |")

    # --- per-column marginal ms ---
    print(f"\n| probe (marginal ms) | {' | '.join(labels)} |")
    print("|---|" + "---|" * len(reps))
    names, seen = [], set()
    for _, rep in reps:
        for arm in (rep.get("cold") or {}).values():
            for q in arm.get("queries", []):
                if q["query"] not in seen:
                    seen.add(q["query"])
                    names.append(q["query"])
    totals = [0.0] * len(reps)
    for n in names:
        cells = []
        for i, (_, rep) in enumerate(reps):
            hit = None
            for arm in (rep.get("cold") or {}).values():
                hit = next((q for q in arm.get("queries", []) if q["query"] == n), None) or hit
            if hit is None:
                cells.append("—")
                continue
            cells.append(f"{hit['marginal_ms']:.1f}")
            totals[i] += hit["marginal_ms"]
        print(f"| {n} | {' | '.join(cells)} |")
    print(f"| **sum of marginals** | {' | '.join(f'**{t:.1f}**' for t in totals)} |")

    # --- per-column parquet MB, so bytes and time sit in the same picture ---
    per_col = []
    for _, rep in reps:
        chunks = (_table(rep)[1].get("parquet") or {}).get("column_chunks") or []
        agg = {}
        for c in chunks:
            col = c.get("column_name") or c.get("path_in_schema")
            agg[col] = agg.get(col, 0) + (c.get("total_compressed_size") or 0)
        per_col.append(agg)
    cols = sorted({c for a in per_col for c in a})
    if cols:
        print(f"\n| column (parquet MB) | {' | '.join(labels)} |")
        print("|---|" + "---|" * len(reps))
        for c in cols:
            cells = [f"{a[c]/1e6:.1f}" if c in a else "—" for a in per_col]
            print(f"| {c} | {' | '.join(cells)} |")
        print(f"| **total** | "
              + " | ".join(f"**{sum(a.values())/1e6:.1f}**" for a in per_col) + " |")
    else:
        print("\n_no column_chunks in these reports — they predate the parquet byte ledger step_")


if __name__ == "__main__":
    main(sys.argv[1:])
