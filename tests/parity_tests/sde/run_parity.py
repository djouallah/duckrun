"""Compatibility test: run the upstream Start-Data-Engineering `type: duckdb` project VERBATIM on
duckrun. The bar is a green `dbt build`; dbt-duckdb is NOT built here — the point is that an
existing dbt-duckdb project runs as-is, with only the connection swapped.

This is the project whose `delete+insert` model duckrun used to need a fixture rewrite for; it now
runs VERBATIM. The repo is cloned fresh and run as-is — the only thing supplied from outside is the
connection: the external duckrun profile in this folder (`path` → the same ./dbt.duckdb the repo's
EL fills, so `sources` resolve; `root_path` → a Delta warehouse for the models). The repo's own EL
(extract_load_pipeline.py) runs first, exactly as upstream documents.

Run:  python tests/parity_tests/sde/run_parity.py
Exit: 0 = the project built green on duckrun, 1 = failure.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/josephmachado/simple_dbt_project"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
DUCKRUN_DIR = TMP / "sde_duckrun"    # sources from ./dbt.duckdb; models → Delta warehouse
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or str(TMP / "sde_duckrun_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (parity_sde) so
# each project is an isolated Fabric schema under the SAME Tables root (like the integration suite).
DUCKRUN_SCHEMA = os.environ.get("DBT_SCHEMA", "main")
_REMOTE = "://" in DUCKRUN_WH


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}  (cwd={cwd})")
    if subprocess.run(cmd, cwd=cwd, env=env).returncode != 0:
        sys.exit(f"command failed: {' '.join(cmd)}")


def fresh_clone(dest: Path):
    import shutil
    shutil.rmtree(dest, ignore_errors=True)
    sh(["git", "clone", "--depth", "1", REPO_URL, str(dest)])


def build(dest: Path, profiles_dir: str, env_extra: dict):
    """Run the repo's EL then `dbt build`, verbatim, with the given profile."""
    env = {**os.environ, **env_extra}
    sh([sys.executable, "extract_load_pipeline.py"], cwd=dest, env=env)   # raw.* → ./dbt.duckdb
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    sh(["dbt", "build", "--profiles-dir", profiles_dir], cwd=dest, env=env)


def main():
    fresh_clone(DUCKRUN_DIR)
    if not _REMOTE:
        import shutil
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": DUCKRUN_WH, "DBT_SCHEMA": DUCKRUN_SCHEMA})
    print("\nPARITY: PASS — sde ran VERBATIM on duckrun (EL + models + snapshot + its own tests green)")


if __name__ == "__main__":
    main()
