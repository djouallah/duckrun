"""Deploy a real-world Fabric project to Fabric via duckrun — the project cloned fresh, VERBATIM.

Mirrors the parity-test convention (tests/parity_tests/): the project under test —
github.com/djouallah/dbt_fabric_python_delta (the AEMO electricity pipeline: a full dbt project plus
a real SemanticModel / Notebook / VariableLibrary in Fabric source-control format) — is `git clone`d
here at runtime and nothing from it is vendored into duckrun. That repo ships its own `deploy.py`
built on the Fabric CLI (`fab`); this is the same deploy expressed entirely through the
`duckrun.workspace(...)` API instead.

The flow:
  1. clone the repo,
  2. build its dbt project on THIS runner through the duckrun adapter (writing Delta to OneLake) so
     the semantic model has tables to reframe onto,
  3. deploy the notebook, variable library, and Direct Lake semantic model with `ws.deploy(...)`
     (the `.bim`'s OneLake GUIDs are repointed at our lakehouse and the model is refreshed), and
  4. schedule the notebook.

The DataPipeline is deliberately skipped: it references the notebook by GUID and `ws.deploy` writes
pipeline JSON verbatim (no notebook-ref repoint), so we schedule the notebook directly instead.

Auth: nothing is passed here. Given only the OIDC federated identity
(`AZURE_CLIENT_ID`/`AZURE_TENANT_ID`), duckrun mints every token it needs — storage (the dbt write),
the Fabric control plane (deploy), and Power BI (the model refresh) — by exchanging a fresh GitHub
OIDC assertion, and picks the OneLake HTTP transport itself. That self-sufficiency is the point.

Run:  python tests/deploy_testing/dbt_fabric_python_delta/run_deploy.py   (CI-only — needs Fabric + OIDC)
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import duckrun

# --- config --------------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/djouallah/dbt_fabric_python_delta"
CLONE_DIR = Path("C:/tmp/dfpd") if os.name == "nt" else Path("/tmp/dfpd")
FABRIC_ITEMS = CLONE_DIR / "fabric_items"
DBT_DIR = CLONE_DIR / "dbt"
LH_NAME = "deploy_demo"                 # dedicated/isolated; the .bim is repointed by GUID, not name
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "mart")
DOWNLOAD_LIMIT = os.environ.get("DOWNLOAD_LIMIT", "2")   # bound the AEMO ingest so the demo stays fast


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=cwd, env=env)
    if r.returncode != 0:
        sys.exit(f"command failed ({r.returncode}): {' '.join(cmd)}")


def fabric_item(item_type, def_filename):
    """The (definition-file path, Fabric display name) for the single ``*.{item_type}`` item in the
    cloned repo. The name comes from the item's ``.platform`` — deploy keys the item name off it, not
    off the definition filename (which is a fixed ``model.bim`` / ``notebook-content.ipynb`` etc.)."""
    folder = next(FABRIC_ITEMS.glob(f"*.{item_type}"))
    name = json.loads((folder / ".platform").read_text())["metadata"]["displayName"]
    return folder / def_filename, name


def main():
    if not (CLONE_DIR / "dbt_project.yml").exists() and not DBT_DIR.exists():
        sh(["git", "clone", "--depth", "1", "--branch", "main", REPO_URL, str(CLONE_DIR)])

    ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
    lh_id = ws.create_lakehouse(LH_NAME)
    tables = f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables"
    files = f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Files"

    # 1. Build the AEMO dbt project on this runner via duckrun (parity-style): our profiles.yml here
    #    (self-acquire, no token) via --profiles-dir; the repo's macros/profile read these env vars.
    env = {**os.environ,
           "DBT_PROFILES_DIR": str(HERE),
           "ONELAKE_TABLES_PATH": tables,
           "FILES_PATH": files,
           "DBT_SCHEMA": DBT_SCHEMA}
    sh(["dbt", "run", "--no-partial-parse", "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(HERE), "--target-path", "target_duckrun",
        "--vars", json.dumps({"download_limit": DOWNLOAD_LIMIT, "process_limit": DOWNLOAD_LIMIT})],
       cwd=DBT_DIR, env=env)

    # 2. Deploy the Fabric items over the built tables — all by name, no `fab` CLI.
    nb_path, nb_name = fabric_item("Notebook", "notebook-content.ipynb")
    vl_path, vl_name = fabric_item("VariableLibrary", "variables.json")
    sm_path, sm_name = fabric_item("SemanticModel", "model.bim")

    ws.deploy(str(nb_path), name=nb_name, overwrite=True)
    ws.deploy(str(vl_path), name=vl_name, overwrite=True,
              variables={"workspace_id": ws.id, "lakehouse_name": LH_NAME})
    ws.deploy(str(sm_path), name=sm_name, lakehouse=LH_NAME, overwrite=True)  # repoint GUIDs + refresh

    # 3. Schedule the notebook (the pipeline is skipped — see module docstring).
    ws.schedule(nb_name, daily="06:00")
    print(f"\nDEPLOY: OK — {nb_name} / {vl_name} / {sm_name} deployed to {ws.name}/{LH_NAME}")


if __name__ == "__main__":
    main()
