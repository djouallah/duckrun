import json
import os
import subprocess
import sys
from pathlib import Path

import duckrun

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/djouallah/dbt_fabric_python_delta"
CLONE_DIR = Path("C:/tmp/dfpd") if os.name == "nt" else Path("/tmp/dfpd")
ITEMS = CLONE_DIR / "fabric_items"
DBT_DIR = CLONE_DIR / "dbt"
LH_NAME = "data"
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "mart")
DOWNLOAD_LIMIT = os.environ.get("DOWNLOAD_LIMIT", "2")


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}", flush=True)
    if subprocess.run(cmd, cwd=cwd, env=env).returncode:
        sys.exit(f"command failed: {' '.join(cmd)}")


def main():
    if not DBT_DIR.exists():
        sh(["git", "clone", "--depth", "1", "--branch", "main", REPO_URL, str(CLONE_DIR)])

    ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE"])
    lh_id = ws.create_lakehouse(LH_NAME)   # idempotent — created only if it doesn't exist
    tables = f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables"
    files = f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Files"

    # Stage the project to Files/dbt (the notebook runs dbt from there on Fabric), then build it once
    # here to populate the tables the semantic model reframes onto.
    duckrun.connect(tables).copy(str(DBT_DIR), "dbt")
    sh(["dbt", "run", "--no-partial-parse", "--project-dir", str(DBT_DIR), "--profiles-dir", str(HERE),
        "--target-path", "target_duckrun",
        "--vars", json.dumps({"download_limit": DOWNLOAD_LIMIT, "process_limit": DOWNLOAD_LIMIT})],
       cwd=DBT_DIR, env={**os.environ, "ONELAKE_TABLES_PATH": tables, "FILES_PATH": files,
                         "DBT_SCHEMA": DBT_SCHEMA})

    # Deploy the Fabric items by name (notebook first — the pipeline points at it by name).
    ws.deploy(str(ITEMS / "run.Notebook/notebook-content.ipynb"), name="run", overwrite=True)
    ws.deploy(str(ITEMS / "deploy_config.VariableLibrary/variables.json"), name="deploy_config",
              variables={"workspace_id": ws.id, "lakehouse_name": LH_NAME}, overwrite=True)
    ws.deploy(str(ITEMS / "aemo_electricity.SemanticModel/model.bim"), name="aemo_electricity",
              lakehouse=LH_NAME, overwrite=True)
    ws.deploy(str(ITEMS / "run_pipeline.DataPipeline/pipeline-content.json"), name="run_pipeline",
              notebook="run", overwrite=True)
    ws.schedule("run_pipeline", daily="06:00")
    print(f"DEPLOY OK — {ws.name}/{LH_NAME}")


if __name__ == "__main__":
    main()
