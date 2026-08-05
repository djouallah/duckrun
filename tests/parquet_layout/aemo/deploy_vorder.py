"""Deploy the benchmark semantic model(s) under tests/parquet_layout/aemo/ (aemo_electricity_auto_sort
+ aemo_electricity_vorder) via duckrun's own ``workspace.deploy()`` and refresh them.

``deploy()`` repoints each ``model.bim``'s OneLake workspace/lakehouse GUIDs at the target ws +
lakehouse, creates the semantic model, and refreshes it (a Direct Lake reframe, retried while OneLake
read permission propagates) — no Fabric CLI, no manual GUID swap, no bim mutation to restore. Auth is
duckrun's self-acquired OIDC tokens (Fabric control plane + Power BI). Args: --env (deploy_config.yml
section, default main), --delete (delete the benchmark models instead of deploying — the end-of-run
cleanup so throwaway models never accumulate in the workspace).

Models land in the ``duckrun`` workspace folder (deploy() creates it if absent; placement always
applies because the delete-first pass below means every deploy is a CREATE, never an in-place update).
"""
import argparse
from pathlib import Path

import yaml

import duckrun
from duckrun import auth, fabric_remote

HERE = Path(__file__).resolve().parent            # this script lives in tests/parquet_layout/aemo/
BENCH = HERE                                       # the *.SemanticModel folders live here

ap = argparse.ArgumentParser()
ap.add_argument("--env", default="main")
ap.add_argument("--delete", action="store_true",
                help="delete the benchmark semantic models instead of deploying")
args = ap.parse_args()

allc = yaml.safe_load((HERE / "deploy_config.yml").read_text())
cfg = {**allc.get("defaults", {}), **allc[args.env]}
WS_ID, LH_NAME = cfg["ws"], cfg["lakehouse_name"]

names = sorted(p.name.removesuffix(".SemanticModel") for p in BENCH.glob("*.SemanticModel"))
if not names:
    raise SystemExit(f"No *.SemanticModel found under {BENCH}")

ws = duckrun.workspace(WS_ID)
# Delete EVERY existing benchmark model FIRST — every item whose name matches, not one per name:
# duplicates do occur (XMLA then refuses to connect at all: "multiple datasets named ..."), and a
# name-keyed dict silently collapses them, leaving survivors that deploy() then updates in place.
# Fresh items also keep the Capacity Metrics CU lines unique per run (the app aggregates per item
# GUID), and folder placement only applies on a CREATE.
deleted = 0
for it in ws.list_items("semanticModels"):
    if it["displayName"] in names:
        fabric_remote._delete_item(auth.get_fabric_token(), ws.id, it["id"])
        print(f"deleted existing semantic model {it['displayName']} ({it['id']})", flush=True)
        deleted += 1

if args.delete:
    print(f"Benchmark semantic model(s) deleted: {deleted}")
    raise SystemExit(0)

for n in names:
    bim = BENCH / f"{n}.SemanticModel" / "model.bim"   # deploy() names the item, repoints, refreshes
    ws.deploy(str(bim), lakehouse=LH_NAME, name=n, overwrite=True, folder="duckrun")
    print(f"deployed + refreshed {n}", flush=True)

print("Benchmark semantic model(s) deployed + refreshed:", ", ".join(names))
