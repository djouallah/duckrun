"""Smoke-audit a locally-built TPC-DI warehouse.

Reads every Delta table under <warehouse>/<schema> and checks that the expected
dimensional and fact tables exist and are non-empty, printing a row-count summary.
Exits non-zero if any required table is missing or empty.

This is a coverage/liveness gate, NOT the full TPC-DI Appendix-A audit (row-count
and business-rule reconciliation against DIGen's audit CSVs) — that is a documented
follow-up. See README.
"""
from __future__ import annotations

import argparse
import os
import sys

import duckdb

# Final tables the load must produce (case-insensitive match against the Delta
# directories duckrun writes as <warehouse>/<schema>/<identifier>).
REQUIRED = [
    "DimDate", "DimTime", "DimBroker", "DimCompany", "DimSecurity", "Financial",
    "DimCustomer", "DimAccount", "DimTrade", "Prospect",
    "FactCashBalances", "FactHoldings", "FactWatches", "FactMarketHistory",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True, help="root_path (Delta output root)")
    ap.add_argument("--schema", default="tpcdi")
    args = ap.parse_args()

    schema_dir = os.path.join(args.warehouse, args.schema)
    if not os.path.isdir(schema_dir):
        sys.exit(f"ERROR: no schema dir at {schema_dir}")

    present = {
        d.lower(): os.path.join(schema_dir, d)
        for d in os.listdir(schema_dir)
        if os.path.isdir(os.path.join(schema_dir, d))
    }

    con = duckdb.connect()
    con.execute("install delta; load delta;")

    failures = []
    print(f"\n  TPC-DI warehouse: {schema_dir}\n  {'table':<22} rows")
    print("  " + "-" * 34)
    for tbl in REQUIRED:
        path = present.get(tbl.lower())
        if path is None:
            print(f"  {tbl:<22} MISSING")
            failures.append(f"{tbl}: missing")
            continue
        try:
            n = con.execute(
                f"select count(*) from delta_scan('{path.replace(chr(92), '/')}')"
            ).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            print(f"  {tbl:<22} ERROR ({e})")
            failures.append(f"{tbl}: {e}")
            continue
        print(f"  {tbl:<22} {n:>12,}")
        if n == 0:
            failures.append(f"{tbl}: empty")
    print()

    if failures:
        print("AUDIT FAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("AUDIT PASSED: all required tables present and non-empty.")


if __name__ == "__main__":
    main()
