"""Drive the full TPC-DI load on duckrun: generate -> dbt run -> validate.

The dbx dbt project (and this port) is single-pass: one `dbt run` reads Batch1
(historical) plus Batch2/3 (incremental CDC) together and produces the complete
SCD2 warehouse. Per-batch audit checkpoints are a harder variant left as a
follow-up (see README).

Local:
    python run_benchmark.py --sf 3
OneLake (Delta output to a Fabric Lakehouse):
    WAREHOUSE_PATH=abfss://.../Tables ONELAKE_TOKEN=... python run_benchmark.py --sf 3 --target onelake
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "3")))
    ap.add_argument("--target", choices=["local", "onelake"], default="local")
    ap.add_argument("--staging", default=os.environ.get("TPCDI_DIR",
                    os.path.join(PROJ, "staging")))
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    staging = os.path.abspath(args.staging)
    env = dict(os.environ)
    env["TPCDI_DIR"] = staging
    env["DBT_SCHEMA"] = env.get("DBT_SCHEMA", "tpcdi")
    if args.target == "local":
        env["WAREHOUSE_PATH"] = os.path.abspath(
            env.get("WAREHOUSE_PATH", os.path.join(PROJ, "warehouse")))
    elif not env.get("WAREHOUSE_PATH"):
        sys.exit("ERROR: --target onelake needs WAREHOUSE_PATH (abfss://.../Tables)")

    if not args.skip_generate:
        gen = [sys.executable, os.path.join(HERE, "generate_data.py"),
               "--sf", str(args.sf), "--out", staging]
        if args.force:
            gen.append("--force")
        print(">> generating data", flush=True)
        subprocess.run(gen, check=True, env=env)

    print(">> dbt run", flush=True)
    subprocess.run(
        ["dbt", "run", "--project-dir", PROJ, "--profiles-dir", PROJ],
        check=True, env=env,
    )

    if args.target == "local":
        print(">> validate", flush=True)
        subprocess.run(
            [sys.executable, os.path.join(PROJ, "audit", "validate.py"),
             "--warehouse", env["WAREHOUSE_PATH"], "--schema", env["DBT_SCHEMA"],
             "--source-dir", staging],
            check=True, env=env,
        )
    else:
        print(">> skipping local validate for onelake target (dbt run success is the gate)", flush=True)
    print(">> done.", flush=True)


if __name__ == "__main__":
    main()
