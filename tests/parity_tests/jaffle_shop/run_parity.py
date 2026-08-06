"""Compatibility test: run the unmodified jaffle_shop (`type: duckdb`) dbt project VERBATIM on
duckrun. The bar is a green `dbt build` — seeds, models and the project's own data tests —
materialized to a Delta warehouse (OneLake in CI, a local dir otherwise). dbt-duckdb is NOT built
here: the point is that an existing dbt-duckdb project runs as-is, with only the connection swapped.

Run:  python tests/parity_tests/jaffle_shop/run_parity.py
Exit: 0 = the project built green on duckrun, 1 = failure.

The project repo is cloned fresh and run VERBATIM. The only thing supplied from outside is the
connection: the duckrun profile in this folder (profiles.yml), passed via --profiles-dir — in dbt
the profile is connection config that lives outside the project, so the repo is never modified.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/dbt-labs/jaffle_shop_duckdb"
CLONE_DIR = Path("C:/tmp/js") if os.name == "nt" else Path("/tmp/js")
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or ("C:/tmp/js_wh" if os.name == "nt" else "/tmp/js_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (e.g.
# parity_jaffle) so each project is an isolated Fabric schema under the SAME Tables root — exactly
# how the integration suite isolates projects. Local default 'main'.
DUCKRUN_SCHEMA = os.environ.get("DBT_SCHEMA", "main")
_REMOTE = "://" in DUCKRUN_WH


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd, env=env)
    if r.returncode != 0:
        sys.exit(f"command failed ({r.returncode}): {' '.join(cmd)}")


def main():
    if not (CLONE_DIR / "dbt_project.yml").exists():
        sh(["git", "clone", "--depth", "1", REPO_URL, str(CLONE_DIR)])
    if not _REMOTE:
        import shutil
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    env = {**os.environ,
           "DBT_PROFILES_DIR": str(HERE),
           "WAREHOUSE_PATH": DUCKRUN_WH,
           "DBT_SCHEMA": DUCKRUN_SCHEMA}
    sh(["dbt", "build", "--no-partial-parse", "--target-path", "target_duckrun"],
       cwd=CLONE_DIR, env=env)
    print("\nPARITY: PASS — jaffle_shop ran VERBATIM on duckrun (its own tests green)")


if __name__ == "__main__":
    main()
