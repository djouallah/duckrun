"""
Pytest config for the dbt adapter conformance suite, run against duckrun.

The test files in this directory are vendored from dbt-duckdb's own conformance suite
(`tests/functional/adapter/`) — the curated subclasses of dbt Labs' `dbt-tests-adapter`
base classes, complete with the adapter-specific overrides (expected catalogs, constraint
type maps, etc.) that a bare `class Test(Base): pass` would miss. We point them at a
`duckrun` target instead of `duckdb`, so the results show exactly where duckrun's Delta
layer diverges from vanilla dbt-duckdb. No cloud credentials needed.
"""
from importlib import metadata

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]

# Raise the open-file limit where supported (mirrors dbt-duckdb's conftest; dbt-core #7316).
# No-op on Windows, which has no `resource` module.
try:
    import resource

    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (_hard, _hard))
except Exception:
    pass


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="duckrun", type=str)


def pytest_configure(config):
    # Markers used by the vendored dbt-duckdb test files.
    config.addinivalue_line("markers", "skip_profile(*profiles): skip on the named profiles")
    config.addinivalue_line("markers", "requires_ducklake: needs the ducklake extension")


def pytest_report_header():
    out = []
    for pkg in ("dbt-core", "dbt-adapters", "dbt-tests-adapter", "dbt-duckdb", "duckdb", "deltalake"):
        try:
            out.append(f"{pkg}: {metadata.version(pkg)}")
        except metadata.PackageNotFoundError:
            pass
    return out


@pytest.fixture(scope="session")
def profile_type(request):
    return request.config.getoption("--profile")


@pytest.fixture(scope="session")
def dbt_profile_target(tmp_path_factory):
    # duckrun writes Delta tables under root_path/<schema>/<model> and reads them via
    # delta_scan. dbt supplies a unique schema per test; DuckDB stays in-memory. Single-
    # threaded — see README "Limitations".
    root = tmp_path_factory.mktemp("duckrun_warehouse")
    return {
        "type": "duckrun",
        "threads": 1,
        "root_path": str(root).replace("\\", "/"),
    }


@pytest.fixture(autouse=True, scope="class")
def skip_by_profile_type(profile_type, request):
    marker = request.node.get_closest_marker("skip_profile")
    if marker and profile_type in marker.args:
        pytest.skip(f"skipped on '{profile_type}' profile")


def pytest_collection_modifyitems(config, items):
    # ducklake isn't set up here; skip the few tests that need it.
    skip_ducklake = pytest.mark.skip(reason="ducklake extension not configured")
    for item in items:
        if "requires_ducklake" in item.keywords:
            item.add_marker(skip_ducklake)
