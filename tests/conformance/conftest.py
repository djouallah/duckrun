"""
Pytest config for the official dbt adapter conformance suite, run against duckrun.

The test classes in this directory subclass dbt Labs' `dbt-tests-adapter` base classes
(dbt.tests.adapter.*) — the same suite every certified adapter runs. Nothing here is
Spark-specific. Each test spins up a throwaway dbt project pointed at a `duckrun` target that
writes Delta tables to a local warehouse dir, so it needs no cloud credentials.
"""
from importlib import metadata

import pytest

# Pulls in dbt's standard functional-test fixtures (the `project` fixture, unique per-test
# schema, etc.). Requires dbt-tests-adapter to be installed.
pytest_plugins = ["dbt.tests.fixtures.project"]

# Raise the open-file limit where the platform supports it (mirrors dbt-duckdb's conftest;
# works around dbt-core #7316). No-op on Windows, which has no `resource` module.
try:
    import resource

    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (_hard, _hard))
except Exception:
    pass


def pytest_report_header():
    pkgs = ("dbt-core", "dbt-adapters", "dbt-tests-adapter", "dbt-duckdb", "duckdb", "deltalake")
    out = []
    for pkg in pkgs:
        try:
            out.append(f"{pkg}: {metadata.version(pkg)}")
        except metadata.PackageNotFoundError:
            pass
    return out


@pytest.fixture(scope="session")
def dbt_profile_target(tmp_path_factory):
    """Profile dbt writes to profiles.yml for every conformance test.

    duckrun materializes Delta tables under ``root_path/<schema>/<model>`` on the local
    filesystem and reads them back via ``delta_scan``. dbt supplies a unique schema per test,
    so one session-wide warehouse dir is enough; DuckDB itself stays in-memory.
    """
    root = tmp_path_factory.mktemp("duckrun_warehouse")
    return {
        "type": "duckrun",
        # Single-threaded, as is normal for dbt adapter conformance suites (e.g. dbt-iceberg
        # runs threads=1 too). duckrun's delta_rs write path is single-threaded today; making
        # it the adapter default is a planned follow-up.
        "threads": 1,
        "root_path": str(root).replace("\\", "/"),
    }
