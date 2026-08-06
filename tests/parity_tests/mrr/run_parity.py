"""Compatibility test: run the upstream MRR (dbt-mrr-assignment) `type: duckdb` project VERBATIM
on duckrun. The bar is a green `dbt build`; dbt-duckdb is NOT built here — the point is that an
existing dbt-duckdb project runs as-is, with only the connection swapped.

It's a subscription-revenue model: raw CSV seeds -> staging/intermediate (views) -> mart TABLES
(`fct_mrr`, `fct_mrr_movements`) that amortize invoices into monthly recurring revenue and derive
MRR movements (new/expansion/contraction/reactivation/retained). The repo also ships native dbt
`unit_tests:` on the amortization model, singular tests, and an exposure — `dbt build` runs all of
them, so a green build means duckrun's unit-test/test path works too.

The repo is cloned fresh and run VERBATIM. The only thing supplied from outside is the connection:
the external duckrun profile in this folder (`root_path` -> a Delta warehouse; seeds are in-repo
CSVs, no sources).

Run:  python tests/parity_tests/mrr/run_parity.py
Exit: 0 = the project built green on duckrun, 1 = failure.
"""
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/Elkadev/dbt-mrr-assignment"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
DUCKRUN_DIR = TMP / "mrr_duckrun"    # seeds + models -> Delta warehouse
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or str(TMP / "mrr_duckrun_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (parity_mrr) so
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
    """Run `dbt deps` + `dbt build`, verbatim, with the given profile (seeds + models + tests)."""
    env = {**os.environ, **env_extra}
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    sh(["dbt", "build", "--profiles-dir", profiles_dir], cwd=dest, env=env)


def main():
    fresh_clone(DUCKRUN_DIR)
    if not _REMOTE:
        import shutil
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": DUCKRUN_WH, "DBT_SCHEMA": DUCKRUN_SCHEMA})
    print("\nPARITY: PASS — mrr ran VERBATIM on duckrun (its own tests + unit tests green)")


if __name__ == "__main__":
    main()
