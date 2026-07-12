"""End-to-end TPC-DI test: generate (DIGen) -> dbt run -> audit, at SF=3.

Heavy (needs a JDK + network for DIGen and the webbed extension), so it only runs
when TPCDI_RUN=1 is set and `java` is present — otherwise it is skipped. The CI
workflow (.github/workflows/tpc_di.yml) sets TPCDI_RUN and runs the same driver.
"""
import os
import shutil
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.skipif(
    os.environ.get("TPCDI_RUN") != "1" or shutil.which("java") is None,
    reason="set TPCDI_RUN=1 and install a JDK to run the full TPC-DI load",
)
def test_tpc_di_full_load(tmp_path):
    env = dict(os.environ)
    env["WAREHOUSE_PATH"] = str(tmp_path / "warehouse")
    env["TPCDI_DIR"] = str(tmp_path / "staging")
    subprocess.run(
        [sys.executable, os.path.join(HERE, "scripts", "run_benchmark.py"),
         "--sf", "3", "--target", "local", "--staging", env["TPCDI_DIR"]],
        check=True, env=env,
    )
