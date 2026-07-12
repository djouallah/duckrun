"""Smoke-audit a TPC-DI warehouse (local dir or OneLake lakehouse).

Reads every required Delta table under <warehouse>/<schema> and checks it exists and is
non-empty, printing a row-count summary. Exits non-zero if any required table is missing or
empty.

Audits through duckrun's ``connect()`` so it works identically over a local directory and over
an ``abfss://`` OneLake lakehouse (duckrun registers each Delta table as a queryable view and,
on OneLake, mints the storage secret from ``ONELAKE_TOKEN``).

This is a coverage/liveness gate, NOT the full TPC-DI Appendix-A audit (row-count and
business-rule reconciliation against DIGen's audit CSVs) — that is a documented follow-up.
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun

# Final tables the load must produce (duckrun registers them as views named by identifier).
REQUIRED = [
    "DimDate", "DimTime", "DimBroker", "DimCompany", "DimSecurity", "Financial",
    "DimCustomer", "DimAccount", "DimTrade", "Prospect",
    "FactCashBalances", "FactHoldings", "FactWatches", "FactMarketHistory",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True, help="root_path (Delta output root; local or abfss://)")
    ap.add_argument("--schema", default="tpcdi")
    args = ap.parse_args()

    # On OneLake, pass the bearer token through storage_options; local needs nothing.
    storage_options = None
    if args.warehouse.startswith("abfss://"):
        token = os.environ.get("ONELAKE_TOKEN", "")
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to audit an abfss:// warehouse")
        storage_options = {"bearer_token": token}

    conn = duckrun.connect(
        args.warehouse, storage_options=storage_options, schema=args.schema, read_only=True)

    failures = []
    print(f"\n  TPC-DI warehouse: {args.warehouse} (schema {args.schema})\n  {'table':<22} rows")
    print("  " + "-" * 34)
    for tbl in REQUIRED:
        try:
            n = conn.sql(f'SELECT count(*) FROM "{tbl}"').fetchone()[0]
        except Exception as e:  # noqa: BLE001 — a missing table view surfaces here
            print(f"  {tbl:<22} MISSING/ERROR")
            failures.append(f"{tbl}: {str(e).splitlines()[0]}")
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
