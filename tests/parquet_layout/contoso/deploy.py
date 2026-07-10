"""Deploy the Contoso benchmark semantic models under tests/parquet_layout/contoso/
(contoso_auto_sort + contoso_vorder) and refresh them — used by the Contoso parquet-layout workflow.

GUID-swap the bim (dev -> target ws/lh), `fab deploy` scoped to ./contoso, then
refresh via the Power BI API. Assumes `fab` is installed and logged in (federated) by the workflow.
Args: --env (deploy_config.yml section, default main).
"""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent            # tests/parquet_layout/contoso/
root = HERE.parent                                # tests/parquet_layout/ — fab runs here; ./{REPO_SUBDIR} resolves back to HERE
BENCH = HERE                                       # the *.SemanticModel folders live here
REPO_SUBDIR = HERE.name                            # "contoso"

ap = argparse.ArgumentParser()
ap.add_argument("--env", default="main")
args = ap.parse_args()

allc = yaml.safe_load((HERE / "deploy_config.yml").read_text())
cfg = {**allc.get("defaults", {}), **allc[args.env]}
WS_ID, LH_NAME = cfg["ws"], cfg["lakehouse_name"]


def cap(c):
    return subprocess.run(c, capture_output=True, text=True, cwd=str(root))


def run(c):
    subprocess.run(c, check=True, cwd=str(root))


ws = json.loads(cap(["fab", "api", "-X", "get", f"workspaces/{WS_ID}"]).stdout)["text"]["displayName"]
LAKEHOUSE = f"{ws}.Workspace/{LH_NAME}.Lakehouse"
lh_id = cap(["fab", "get", LAKEHOUSE, "-q", "id"]).stdout.strip()
print(f"Workspace {ws} ({WS_ID}), lakehouse {LH_NAME} ({lh_id})")

names = sorted(p.name.removesuffix(".SemanticModel") for p in BENCH.glob("*.SemanticModel"))
if not names:
    raise SystemExit(f"No *.SemanticModel found under {BENCH}")

# GUID-swap each bim (dev -> target), remembering them so we can restore after deploy.
bims = []
for n in names:
    bim = BENCH / f"{n}.SemanticModel" / "model.bim"
    text = bim.read_text()
    m = re.search(r'onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})', text)
    if not m:
        raise SystemExit(f"No OneLake URL GUIDs in {bim}")
    bim.write_text(text.replace(m.group(1), WS_ID).replace(m.group(2), lh_id))
    bims.append(bim)

cfg_yml = root / "_bench_deploy_contoso.yml"
cfg_yml.write_text(
    f'core:\n  workspace: "{ws}"\n  repository_directory: "./{REPO_SUBDIR}"\n'
    '  item_types_in_scope:\n    - SemanticModel\n')
try:
    for attempt in range(1, 4):
        try:
            run(["fab", "deploy", "--config", cfg_yml.name, "-f"])
            break
        except subprocess.CalledProcessError:
            if attempt == 3:
                raise
            print(f"deploy attempt {attempt} failed (likely mid-refresh); waiting 45s...")
            time.sleep(45)
finally:
    cfg_yml.unlink(missing_ok=True)
    for bim in bims:
        subprocess.run(["git", "checkout", str(bim)], cwd=str(root))

# Refresh each (Direct Lake reframe) — 3x retry for OneLake security propagation.
for n in names:
    sm_id = cap(["fab", "get", f"{ws}.Workspace/{n}.SemanticModel", "-q", "id"]).stdout.strip()
    # Fail loud if the item isn't in the workspace after deploy (e.g. the .SemanticModel folder was
    # missing its .platform marker, so `fab deploy` published nothing) — otherwise we'd POST to
    # datasets/[NotFound] and print a false "deployed + refreshed".
    if not sm_id or sm_id == "[NotFound]":
        raise SystemExit(f"semantic model '{n}' not found in workspace '{ws}' after deploy "
                         f"(fab get id -> {sm_id!r}); check the .SemanticModel/.platform marker")
    for attempt in range(1, 4):
        try:
            run(["fab", "api", "-A", "powerbi", "-X", "post", f"groups/{WS_ID}/datasets/{sm_id}/refreshes"])
            break
        except subprocess.CalledProcessError:
            if attempt == 3:
                raise
            print(f"[{n}] refresh attempt {attempt} failed (OneLake security propagating); waiting 60s...")
            time.sleep(60)

print("Contoso benchmark semantic model(s) deployed + refreshed:", ", ".join(names))
