"""Compatibility test: run the upstream TechFlow Analytics (ameijin/dbt-example) `type: duckdb`
project VERBATIM on duckrun. The bar is a green `dbt build`; dbt-duckdb is NOT built here — the
point is that an existing dbt-duckdb project runs as-is, with only the connection swapped.

A SaaS analytics project: raw data is committed **parquet** read via dbt-duckdb `external_location`
sources (data/*.parquet), plus a few CSV seeds. It exercises native dbt `unit_tests:`, an incremental
model (fct_mrr_daily), two timestamp snapshots, dbt_expectations + dbt_project_evaluator, exposures
and a staging->intermediate->marts layering. The repo is cloned fresh and run VERBATIM; the only
thing supplied from outside is the connection (the external duckrun profile in this folder,
root_path -> a Delta warehouse).

Run:  python tests/parity_tests/techflow/run_parity.py
Exit: 0 = the project built green on duckrun, 1 = failure.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/ameijin/dbt-example"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
DUCKRUN_DIR = TMP / "techflow_duckrun"    # seeds + models -> Delta warehouse
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or str(TMP / "techflow_duckrun_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (parity_techflow)
# so each project is an isolated Fabric schema under the SAME Tables root (like the integration suite).
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


def build(dest: Path, profiles_dir: str, env_extra: dict, full_refresh: bool = False):
    """Run `dbt deps` + `dbt build` with the given profile (seeds + snapshots + models + tests + unit
    tests). The raw parquet is committed in the repo, so no extract/generate step.

    full_refresh: pass `--full-refresh` so incremental models rebuild from scratch. The OneLake
    store PERSISTS across CI runs — without it, fct_mrr_daily (left from a prior run) rebuilds
    incrementally, and its cumulative_mrr is a `sum() over(...)` INSIDE its `where date_day > max`
    incremental filter, so an incremental run can't see history outside the batch and resets to 0 —
    dbt-duckdb does the exact same thing (verified), a project-model quirk, not a duckrun bug.
    --full-refresh rebuilds via the normal Delta overwrite (a new version, history retained) — it
    never deletes the OneLake table, and it exercises rebuild-over-a-persistent-store every run."""
    env = {**os.environ, **env_extra}
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    cmd = ["dbt", "build", "--profiles-dir", profiles_dir]
    if full_refresh:
        cmd.append("--full-refresh")
    sh(cmd, cwd=dest, env=env)


def main():
    fresh_clone(DUCKRUN_DIR)
    # NB: the duckrun warehouse is NOT wiped — neither the local dir nor (especially) OneLake. The
    # whole point is to prove --full-refresh rebuilds correctly OVER a persistent store: the same
    # persist-then-overwrite path CI hits every run.
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": DUCKRUN_WH, "DBT_SCHEMA": DUCKRUN_SCHEMA},
          full_refresh=True)
    print("\nPARITY: PASS — techflow ran VERBATIM on duckrun (models + snapshots + unit tests green)")


if __name__ == "__main__":
    main()
