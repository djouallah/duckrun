"""Check whether a TPC-DI seed already exists in OneLake Files — a cheap existence check,
NO download. Used to skip the (slow) Java generator when a prior run already generated and
uploaded the seed for this scale factor. dbt then reads the data straight from OneLake.

Writes ``exists=true|false`` to $GITHUB_OUTPUT (and prints it). Reuses duckrun's
``list_files()`` (DFS REST listing over the connect() secret — no bytes transferred).

Env:
    WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables
    ONELAKE_TOKEN    OneLake storage bearer token (minted by the workflow)

Usage:
    python check_seed.py --prefix tpcdi/sf3
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default=os.environ.get("TPCDI_ONELAKE_PREFIX", "tpcdi"),
                    help="Files/<prefix> to probe (e.g. tpcdi/sf3)")
    args = ap.parse_args()

    warehouse = os.environ.get("WAREHOUSE_PATH", "")
    token = os.environ.get("ONELAKE_TOKEN", "")
    if not warehouse.startswith("abfss://"):
        sys.exit("ERROR: WAREHOUSE_PATH must be an abfss:// OneLake Tables path")
    if not token:
        sys.exit("ERROR: ONELAKE_TOKEN is empty — mint one before checking")

    conn = duckrun.connect(
        warehouse, storage_options={"bearer_token": token}, read_only=True)
    # Presence of Batch1 files is our "seed complete" marker (Batch1 is the historical load).
    try:
        exists = bool(conn.list_files(f"{args.prefix}/Batch1"))
    except Exception as e:  # noqa: BLE001 — a missing folder lists as empty on most backends
        print(f"  list_files({args.prefix}/Batch1) -> {e!r}; treating as absent", flush=True)
        exists = False

    result = "true" if exists else "false"
    print(f"seed Files/{args.prefix} exists: {result}", flush=True)
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"exists={result}\n")


if __name__ == "__main__":
    main()
