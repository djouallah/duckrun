"""Upload the generated TPC-DI batches into OneLake Files.

After generate_data.py has produced <staging>/Batch1|2|3, this lands the whole tree
in the lakehouse Files section at Files/<prefix>/… so a routine dbt run can read the
seed back over abfss:// without regenerating (the Java gen only has to prime it once).

Reuses duckrun's storage-neutral conn.copy() (COPY … FORMAT BLOB over the connect()
secret) — the same OneLake Files upload path the aemo/coffee jobs dogfood; no new
storage client, no obstore.

Env:
    WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables
    ONELAKE_TOKEN    OneLake storage bearer token (minted by the workflow)

Usage:
    python upload_to_onelake.py --staging ./staging --prefix tpcdi
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", default=os.environ.get("TPCDI_STAGING", "./staging"),
                    help="local dir holding Batch1/2/3 (the generate_data.py output)")
    ap.add_argument("--prefix", default=os.environ.get("TPCDI_ONELAKE_PREFIX", "tpcdi"),
                    help="Files/<prefix> destination under the lakehouse")
    args = ap.parse_args()

    warehouse = os.environ.get("WAREHOUSE_PATH", "")
    token = os.environ.get("ONELAKE_TOKEN", "")
    if not warehouse.startswith("abfss://"):
        sys.exit("ERROR: WAREHOUSE_PATH must be an abfss:// OneLake Tables path")
    if not token:
        sys.exit("ERROR: ONELAKE_TOKEN is empty — mint one before uploading")
    if not os.path.isdir(os.path.join(args.staging, "Batch1")):
        sys.exit(f"ERROR: {args.staging}/Batch1 not found — run generate_data.py first")

    conn = duckrun.connect(
        warehouse, storage_options={"bearer_token": token}, read_only=False)
    # overwrite=True so a re-prime refreshes the seed rather than skipping existing files.
    ok = conn.copy(args.staging, args.prefix, overwrite=True)
    if not ok:
        sys.exit("ERROR: conn.copy() to OneLake Files failed")
    print(f"  uploaded {args.staging} -> Files/{args.prefix}/", flush=True)


if __name__ == "__main__":
    main()
