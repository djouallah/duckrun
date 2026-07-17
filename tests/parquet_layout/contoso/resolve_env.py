"""Resolve the benchmark's workspace + lakehouse into $GITHUB_ENV via duckrun — no az/fab.

Reads deploy_config.yml (--env) for the workspace GUID + lakehouse name, then uses duckrun's Fabric
control-plane handle (self-acquired OIDC token) to resolve the lakehouse id and the workspace display
name. Emits WS_ID / LH_ID / WS_NAME / ONELAKE_TABLES_PATH / PBI_WORKSPACE — the same variables the
old `az rest` step produced, so the build / stats / XMLA steps are unchanged. Args: --env
(deploy_config.yml section, default main).
"""
import argparse
import os
from pathlib import Path

import yaml

import duckrun

HERE = Path(__file__).resolve().parent

ap = argparse.ArgumentParser()
ap.add_argument("--env", default="main")
args = ap.parse_args()

cfg = yaml.safe_load((HERE / "deploy_config.yml").read_text())
e = {**cfg.get("defaults", {}), **cfg[args.env]}

ws = duckrun.workspace(e["ws"])
lh_id = ws.lakehouse_id(e["lakehouse_name"])
ws_name = ws.display_name

out = {
    "WS_ID": ws.id,
    "LH_ID": lh_id,
    "WS_NAME": ws_name,
    "ONELAKE_TABLES_PATH": f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables",
    "PBI_WORKSPACE": ws_name,
}
gh = os.environ.get("GITHUB_ENV")
if gh:
    with open(gh, "a", encoding="utf-8") as f:
        f.write("".join(f"{k}={v}\n" for k, v in out.items()))
for k, v in out.items():
    print(f"{k}={v}")
