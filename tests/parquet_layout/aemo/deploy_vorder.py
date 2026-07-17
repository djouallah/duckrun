"""Deploy the benchmark semantic model(s) under tests/parquet_layout/aemo/ (aemo_electricity_auto_sort
+ aemo_electricity_vorder) via duckrun's own ``workspace.deploy()`` and refresh them.

``deploy()`` repoints each ``model.bim``'s OneLake workspace/lakehouse GUIDs at the target ws +
lakehouse, creates the semantic model, and refreshes it (a Direct Lake reframe, retried while OneLake
read permission propagates) — no Fabric CLI, no manual GUID swap, no bim mutation to restore. Auth is
duckrun's self-acquired OIDC tokens (Fabric control plane + Power BI). Args: --env (deploy_config.yml
section, default main).
"""
import argparse
from pathlib import Path

import yaml

import duckrun

HERE = Path(__file__).resolve().parent            # this script lives in tests/parquet_layout/aemo/
BENCH = HERE                                       # the *.SemanticModel folders live here

ap = argparse.ArgumentParser()
ap.add_argument("--env", default="main")
args = ap.parse_args()

allc = yaml.safe_load((HERE / "deploy_config.yml").read_text())
cfg = {**allc.get("defaults", {}), **allc[args.env]}
WS_ID, LH_NAME = cfg["ws"], cfg["lakehouse_name"]

names = sorted(p.name.removesuffix(".SemanticModel") for p in BENCH.glob("*.SemanticModel"))
if not names:
    raise SystemExit(f"No *.SemanticModel found under {BENCH}")

ws = duckrun.workspace(WS_ID)
for n in names:
    bim = BENCH / f"{n}.SemanticModel" / "model.bim"   # deploy() names the item, repoints, refreshes
    ws.deploy(str(bim), lakehouse=LH_NAME, name=n, overwrite=True)
    print(f"deployed + refreshed {n}", flush=True)

print("Benchmark semantic model(s) deployed + refreshed:", ", ".join(names))
