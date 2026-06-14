"""End-to-end port of josephmachado/simple_dbt_project onto the duckrun adapter.

Drives REAL dbt (via subprocess, one process per phase) so the SCD2 snapshot and the
merge-incremental model behave as they do in production — separate processes, not the same-process
harness artifact that the conformance suite documents. Asserts the upstream qa_script.sh invariants:

  phase 1 (initial load):  customer_id=82 -> 1 snapshot row,  fct_clickstream = 100
  phase 2 (append new):    customer_id=82 -> 2 snapshot rows,  fct_clickstream = 110

The whole flow is one test: seed/build, validate, append, build, validate.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

PROJECT_DIR = Path(__file__).parent
DBT = [sys.executable, "-m", "dbt.cli.main"]


def _env(warehouse: Path) -> dict:
    env = dict(os.environ)
    env["DUCKRUN_WAREHOUSE"] = warehouse.as_posix()
    return env


def _run(cmd, env):
    """Run a command in the project dir; surface stdout/stderr on failure."""
    res = subprocess.run(cmd, cwd=PROJECT_DIR, env=env, capture_output=True, text=True)
    if res.returncode != 0:
        raise AssertionError(
            f"command failed ({res.returncode}): {' '.join(str(c) for c in cmd)}\n"
            f"--- stdout ---\n{res.stdout}\n--- stderr ---\n{res.stderr}"
        )
    return res


def _dbt(args, env):
    return _run(DBT + args + ["--project-dir", str(PROJECT_DIR),
                              "--profiles-dir", str(PROJECT_DIR)], env)


def _delta(warehouse: Path, schema: str, table: str) -> str:
    return (warehouse / schema / table).as_posix()


def _scalar(sql: str):
    return duckdb.sql(sql).fetchone()[0]


def _count(path: str) -> int:
    return _scalar(f"select count(*) from delta_scan('{path}')")


def test_sde_dbt_tutorial_end_to_end(tmp_path):
    wh = tmp_path / "wh"
    env = _env(wh)

    clickstream = _delta(wh, "main", "fct_clickstream")
    dim_customer = _delta(wh, "snapshots", "dim_customer")

    # ---- phase 1: initial EL -> deps/seed/build ------------------------------------------------
    _run([sys.executable, str(PROJECT_DIR / "el" / "extract_load_pipeline.py")], env)
    _dbt(["deps"], env)
    _dbt(["seed"], env)
    _dbt(["build"], env)

    assert _count(clickstream) == 100
    assert _scalar(f"select count(*) from delta_scan('{dim_customer}') where customer_id = '82'") == 1

    # ---- phase 2: append new data -> build (separate process) ----------------------------------
    _run([sys.executable, str(PROJECT_DIR / "el" / "load_new_data.py")], env)
    _dbt(["build"], env)

    assert _count(clickstream) == 110
    assert _scalar(f"select count(*) from delta_scan('{dim_customer}') where customer_id = '82'") == 2

    # ---- gold OBT built and joined through the SCD2 validity window -----------------------------
    obt = _delta(wh, "main", "orders_obt")
    assert _count(obt) > 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-s", "-v"]))
