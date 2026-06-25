"""Local de-risk for the multi-catalog demo — network-free, always runs.

The live showcase (``demo_multicatalog.main``) binds three OneLake catalogs; this runs the exact same
``run_multicatalog_demo`` code path against three **local** directories — a writable lakehouse (the
primary), a read-only "warehouse" seeded with the facts, and a writable local catalog — and asserts the
pipeline JOINs across all three (warehouse facts ⋈ lakehouse dim ⋈ local lookup) and writes the mart
back under the lakehouse root. Storage-neutrality means the local run is representative of OneLake.
"""
import importlib.util
import pathlib

import deltalake
import duckdb

import duckrun
from dbt.adapters.duckrun import engine

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
    lakehouse = str(tmp_path / "lakehouse")   # writable primary (the OneLake lakehouse stand-in)
    warehouse = str(tmp_path / "warehouse")   # read-only facts (the Fabric Warehouse stand-in)
    local = str(tmp_path / "local")           # writable local catalog

    # The lakehouse owns the DUID dimension; the warehouse owns the facts (both under `mart`).
    _seed(lakehouse + "/mart/dim_duid",
          "SELECT * FROM (VALUES ('D1','New South Wales','Black coal'),"
          "('D2','Victoria','Wind'),('D3','Queensland','Natural gas')) t(DUID, State, FuelSourceDescriptor)")
    _seed(warehouse + "/mart/fct_summary",
          "SELECT * FROM (VALUES (DATE '2024-01-01','D1',500.0,80.0),"
          "(DATE '2024-01-01','D2',200.0,75.0),(DATE '2024-01-01','D3',300.0,90.0)) t(date, DUID, mw, price)")

    conn = duckrun.connect(lakehouse, schema=demo.REMOTE_SCHEMA, read_only=False, name="lakehouse")
    demo.run_multicatalog_demo(conn, warehouse, local, SCHEMA)

    # the joined mart was written BACK under the lakehouse root (not the warehouse / local).
    assert deltalake.DeltaTable.is_deltatable(f"{lakehouse}/{SCHEMA}/mart_generation_by_state")
    assert deltalake.DeltaTable.is_deltatable(f"{local}/dbo/fuel_factors")
    out = {s: (mw, co2) for s, mw, co2 in conn.sql(
        f"SELECT State, total_mw, est_tonnes_co2 FROM {SCHEMA}.mart_generation_by_state").collect()}
    assert set(out) == {"New South Wales", "Victoria", "Queensland"}      # 3 states, joined via dim
    assert out["New South Wales"] == (500, 450.0)   # 500 MW black coal × 900 kg/MWh = 450 t
    assert out["Victoria"][1] == 0.0                # wind → zero-carbon (local factor)
    # the warehouse stayed read-only: the fence write left no 'nope' table behind.
    conn.catalog.setCurrentCatalog("warehouse")
    assert "nope" not in conn.catalog.listTables("mart")
    conn.stop()


def test_multicatalog_demo_writes_html(tmp_path, monkeypatch):
    # the HTML report is emitted when DUCKRUN_MULTICAT_PAGE is set (what CI publishes as multicatalog.html).
    page = tmp_path / "report.html"
    monkeypatch.setenv("DUCKRUN_MULTICAT_PAGE", str(page))
    lakehouse, warehouse, local = (str(tmp_path / n) for n in ("lakehouse", "warehouse", "local"))
    _seed(lakehouse + "/mart/dim_duid", "SELECT 'D1' AS DUID, 'Victoria' AS State, 'Wind' AS FuelSourceDescriptor")
    _seed(warehouse + "/mart/fct_summary", "SELECT DATE '2024-01-01' AS date, 'D1' AS DUID, 10.0 AS mw, 50.0 AS price")
    conn = duckrun.connect(lakehouse, schema=demo.REMOTE_SCHEMA, read_only=False, name="lakehouse")
    demo.run_multicatalog_demo(conn, warehouse, local, SCHEMA)
    conn.stop()
    body = page.read_text(encoding="utf-8")
    assert "catalog.schema.table" in body          # the key three-part naming visual
    assert "warehouse.mart.fct_summary" in body     # cross-catalog reference shown
    assert "mart_generation_by_state" in body       # the written-back mart
