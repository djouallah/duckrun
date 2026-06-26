"""dbt NATIVE unit tests (the `unit_tests:` given/expect feature) running on the duckrun adapter.

A small, realistic jaffle-style project (project/) — seeds -> staging -> marts, including the
canonical ``dim_customers`` email-validation model — carries the unit tests in
project/models/marts/_unit_tests.yml. Each one supplies explicit input rows for the model's ``ref()``s
and asserts explicit output rows (deterministic by construction).

This harness drives the project in-process with dbtRunner against a fresh local-fs Delta warehouse:

  * ``dbt build``  — seeds load, every model materializes to REAL Delta via duckrun, and ALL data +
    unit tests run. Green proves the native unit-test feature works on duckrun AND the project builds
    end to end.
  * ``dbt test --select test_type:unit`` — runs just the unit tests, in isolation.
  * a read-back of the real ``dim_customers`` Delta table (built from the seeds, not mocked) — ties
    the deterministic rows to an actual duckrun materialization, not only the in-memory unit-test
    temp. The read-back goes through duckrun itself (DuckDB + ``delta_scan``), the same way a user
    reads a duckrun table — no need for the raw delta-rs library here.
"""
import os
from pathlib import Path

import duckrun

from dbt.cli.main import dbtRunner

PROJECT_DIR = str(Path(__file__).parent / "project")
SCHEMA = "main"


def _dbt(warehouse: str, *args: str) -> object:
    """One in-process dbt command against a local-fs warehouse (e.g. _dbt(wh, "build"))."""
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke([*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR])


def _wh(tmp_path) -> str:
    return (tmp_path / "wh").as_posix()           # forward slashes so delta paths are clean on Windows


def _unit_results(res) -> dict:
    """{unit_test_name: status} from a dbtRunnerResult (unit tests are 'unit_test' nodes)."""
    out = {}
    for r in (res.result or []):
        node = getattr(r, "node", None)
        if node is not None and getattr(node, "resource_type", "") == "unit_test":
            out[node.name] = str(r.status)
    return out


def test_build_passes_including_unit_tests(tmp_path):
    """`dbt build` seeds + materializes every model to real Delta (duckrun) AND runs every data and
    unit test — one green run proves the native unit_tests feature works on duckrun end to end."""
    assert _dbt(_wh(tmp_path), "build").success


def test_unit_tests_run_and_pass(tmp_path):
    """Run the unit tests in isolation and confirm all five executed and passed."""
    wh = _wh(tmp_path)
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run").success
    res = _dbt(wh, "test", "--select", "test_type:unit")
    assert res.success
    statuses = _unit_results(res)
    assert len(statuses) == 5, f"expected 5 unit tests, ran {len(statuses)}: {statuses}"
    assert all(s == "pass" for s in statuses.values()), statuses


def test_dim_customers_real_delta_rows(tmp_path):
    """Beyond the mocked unit tests: dim_customers materialized to REAL Delta from the seeds yields
    the deterministic validation we expect — alice valid, bob's domain unaccepted, carol malformed.
    Read back through duckrun (DuckDB + delta_scan), exactly how a user reads a duckrun table."""
    wh = _wh(tmp_path)
    assert _dbt(wh, "build").success
    con = duckrun.connect(wh, schema=SCHEMA, read_only=True)
    valid = dict(con.sql("select email, is_valid_email_address from dim_customers").fetchall())
    assert valid == {
        "alice@example.com": True,    # valid format + accepted domain (example.com)
        "bob@unknown.com": False,     # valid format, but unknown.com is not an accepted domain
        "carolgmail.com": False,      # malformed — no @
    }
