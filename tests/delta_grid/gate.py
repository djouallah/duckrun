"""Release gate: duckrun's column of the Delta support grid must not lose a cell.

    python tests/delta_grid/gate.py <readings.json> <baseline.json>

``readings.json`` is what the grid's duckrun probe wrote ($RESULTS_FILE, rows of
``{key, question, outcome, detail}``); ``baseline.json`` is ``{"run": ..., "outcomes": {key:
outcome}}``. A key the baseline has ``supported`` that is anything else now, or that was not asked
at all, fails. A key that became ``supported`` and a key the baseline lacks are reported, not
failed: update the baseline on purpose, in its own commit, citing the run.
"""
import json
import sys


def main(readings_path: str, baseline_path: str) -> int:
    with open(readings_path, encoding="utf-8") as fh:
        rows = {r["key"]: r for r in json.load(fh)["rows"]}
    with open(baseline_path, encoding="utf-8") as fh:
        baseline = json.load(fh)["outcomes"]

    lost, gained = [], []
    for key, want in baseline.items():
        got = rows.get(key)
        if got is None:
            if want == "supported":
                lost.append(f"{key}: not asked (baseline: supported)")
            continue
        if want == "supported" and got["outcome"] != "supported":
            lost.append(f"{key}: {got['outcome']} - {got.get('detail', '')[:300]}")
        elif want != "supported" and got["outcome"] == "supported":
            gained.append(key)
    unknown = sorted(set(rows) - set(baseline))

    for key in gained:
        print(f"::notice::delta grid: {key} is supported now (baseline: {baseline[key]}); "
              f"update tests/delta_grid/baseline.json")
    for key in unknown:
        print(f"::notice::delta grid: {key} ({rows[key]['outcome']}) is not in the baseline")
    for line in lost:
        print(f"::error::delta grid regression: {line}")
    supported = sum(1 for r in rows.values() if r["outcome"] == "supported")
    print(f"delta grid: {supported}/{len(rows)} supported; {len(lost)} regression(s), "
          f"{len(gained)} newly supported, {len(unknown)} not in the baseline")
    return 1 if lost else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
