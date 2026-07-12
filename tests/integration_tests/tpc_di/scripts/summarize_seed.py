"""Precompute the audit's source-side aggregates from the freshly generated seed on LOCAL
disk, and write them to _seed_summary.json next to the seed.

Why: the final audit (audit/validate.py) reconciles each warehouse table against a value
derived from the raw source files. Over OneLake those raw scans are the expensive part —
tens of millions of rows of pipe/CSV text read over abfss on every run. But right after
generation the same files sit on the runner's local disk, where the scan is cheap. So we
compute the raw-file-derived source values once here and upload the tiny JSON with the seed;
the audit then reads the JSON instead of re-scanning (see validate.RAW_SOURCE_CHECKS).

To guarantee the precomputed value equals what the audit would have computed, we run the
EXACT same source SQL — imported from validate.build_checks — just pointed at the local
staging dir with a plain DuckDB connection.

Usage:
    python summarize_seed.py --staging ./staging --sf 100   # writes ./staging/_seed_summary.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import duckdb

# Import the audit's check definitions so the source SQL is defined in exactly one place.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "audit"))
import validate  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", required=True, help="local seed dir (holds Batch1/2/3)")
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "0")))
    ap.add_argument("--out", default=None, help="output JSON (default <staging>/_seed_summary.json)")
    args = ap.parse_args()

    staging = os.path.abspath(args.staging).replace("\\", "/")
    out = args.out or os.path.join(staging, "_seed_summary.json")

    recon, _ = validate.build_checks(staging)
    con = duckdb.connect()

    source = {}
    print(f"  precomputing source aggregates from {staging}")
    for name, ssql, _tsql, _op, _desc in recon:
        if name not in validate.RAW_SOURCE_CHECKS:
            continue  # the rest depend on dbt-built tables; the audit computes them live
        val = con.execute(ssql).fetchone()[0]
        source[name] = int(val)
        print(f"    {name:<20} {val:>14,}")

    payload = {"scale_factor": args.sf, "source": source}
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {out}")


if __name__ == "__main__":
    main()
