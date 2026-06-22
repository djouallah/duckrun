"""Local de-risk for the multi-catalog demo — network-free, always runs.

The live showcase (``demo_multicatalog.main``) binds three OneLake catalogs; this test exercises the
exact same ``run_multicatalog_demo`` code path against three **local** directories (a writable primary,
a read-only "warehouse", a writable scratch) and asserts the load-bearing invariants: the read-only
fence holds, the lookup lands under the scratch root, and the mart is written back under the primary
(aemo) root. Storage-neutrality means this local run is representative of the OneLake run — only the
secret/discovery backend differs.
"""
import importlib.util
import pathlib

import deltalake
import duckdb
import pytest

import duckrun
from dbt.adapters.duckrun import engine

# Load the sibling demo by file path so the test doesn't depend on tests/ being an importable package.
_spec = importlib.util.spec_from_file_location(
    "demo_multicatalog", pathlib.Path(__file__).with_name("demo_multicatalog.py"))
demo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(demo)
SCHEMA = demo.SCHEMA


def _seed(path, sql):
    con = duckdb.connect()
    engine.write_delta(path, con.sql(sql), mode="overwrite")
    con.close()


def test_multicatalog_local_derisk(tmp_path):
    aemo = str(tmp_path / "aemo")        # writable primary (stands in for the OneLake lakehouse)
    warehouse = str(tmp_path / "warehouse")  # read-only reference (stands in for the Fabric Warehouse)
    scratch = str(tmp_path / "scratch")  # writable local scratch catalog
    # pre-seed the read-only "warehouse" so the read step has a table to list + sample.
    _seed(warehouse + "/dbo/dim_region", "select 'A' as region, 'NSW' as state")

    conn = duckrun.connect(aemo, schema=SCHEMA, read_only=False)
    results = demo.run_multicatalog_demo(conn, warehouse, None, scratch, SCHEMA)
    by = {r["name"]: r for r in results}

    # every operation correct
    assert all(r["count_ok"] and r["values_ok"] for r in results), by
    # the read-only fence refused the warehouse write
    assert by["Read-only fence (warehouse)"]["values_ok"]
    # the lookup physically landed under the scratch root; the mart under the aemo root
    assert by["Write lookup (scratch)"]["values_ok"]
    assert by["Write mart back (aemo)"]["values_ok"]
    assert deltalake.DeltaTable.is_deltatable(f"{aemo}/{SCHEMA}/mart_region_demand")
    assert deltalake.DeltaTable.is_deltatable(f"{scratch}/dbo/region_lookup")
    # the mart joined lakehouse + scratch into one row per region
    assert conn.sql(f"select count(*) from {SCHEMA}.mart_region_demand").fetchone()[0] == 5
    conn.stop()


def test_multicatalog_demo_writes_html(tmp_path, monkeypatch):
    # the HTML report is emitted when DUCKRUN_MULTICAT_PAGE is set (what CI publishes as multicatalog.html).
    page = tmp_path / "report.html"
    monkeypatch.setenv("DUCKRUN_MULTICAT_PAGE", str(page))
    aemo, warehouse, scratch = (str(tmp_path / n) for n in ("aemo", "warehouse", "scratch"))
    _seed(warehouse + "/dbo/dim_region", "select 'A' as region, 'NSW' as state")
    conn = duckrun.connect(aemo, schema=SCHEMA, read_only=False)
    demo.run_multicatalog_demo(conn, warehouse, None, scratch, SCHEMA)
    conn.stop()
    body = page.read_text(encoding="utf-8")
    assert "multi-catalog" in body and "read-only" in body
    assert "mart_region_demand" in body
