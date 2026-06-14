"""Assert a dbt ``catalog.json`` carries Delta table stats for at least one model (issue #3).

Each integration docs job builds real Delta tables, so its published catalog must show row count /
size / last-modified (duckrun reads these from the Delta log). This guards the exact regression #3
reported — a statless catalog — per project. Used by the docs-* jobs in integration.yml.

    python tests/tools/check_catalog_stats.py <catalog.json> <project-label>
"""
import json
import sys


def main(catalog_path: str, project: str) -> None:
    cat = json.load(open(catalog_path))
    statted = {u: n["stats"] for u, n in cat["nodes"].items()
               if n.get("stats", {}).get("has_stats", {}).get("value")}
    if not statted:
        sys.exit(f"{project}: no model reported Delta stats in {catalog_path}")
    for uid, s in statted.items():
        for k in ("num_rows", "bytes", "last_modified"):
            assert s.get(k, {}).get("include") is True, f"{project}:{uid} missing stat {k!r}"
    print(f"{project}: {len(statted)} model(s) with Delta stats")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: check_catalog_stats.py <catalog.json> <project-label>")
    main(sys.argv[1], sys.argv[2])
