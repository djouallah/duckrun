"""Local de-risk for the multi-lakehouse **dbt** demo — network-free, always runs.

The live showcase points the three catalogs at three OneLake Lakehouses; this runs the exact same
dbt project against three **local** folders (a Bronze / Silver / Gold medallion) and asserts:

  * each layer's Delta table lands under its OWN lakehouse root (Bronze / Silver / Gold), not the others;
  * cross-lakehouse ref() resolves (Silver reads Bronze; Gold joins Bronze ⋈ Silver);
  * cross-catalog is_incremental() works (a second run merges into the Bronze table found under the
    Bronze root, upserting rather than clobbering).

Storage-neutrality means this local run is representative of OneLake — the only difference is the roots.
"""
import os
from pathlib import Path

import duckrun
from dbt.cli.main import dbtRunner

PROJECT_DIR = str(Path(__file__).parent)


def _dbt(roots, *args):
    bronze, silver, gold = roots
    os.environ["LH_BRONZE_PATH"] = bronze
    os.environ["LH_SILVER_PATH"] = silver
    os.environ["LH_GOLD_PATH"] = gold
    return dbtRunner().invoke([*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR])


def _roots(tmp_path):
    return tuple((tmp_path / n).as_posix() for n in ("bronze", "silver", "gold"))


def test_medallion_across_three_lakehouses(tmp_path):
    bronze, silver, gold = roots = _roots(tmp_path)
    assert _dbt(roots, "run").success

    # Each layer lands under its own lakehouse root — and NOWHERE else.
    assert (Path(bronze) / "main" / "raw_generation" / "_delta_log").is_dir()
    assert (Path(silver) / "main" / "clean_generation" / "_delta_log").is_dir()
    assert (Path(gold) / "main" / "generation_by_fuel" / "_delta_log").is_dir()
    assert not (Path(silver) / "main" / "raw_generation").exists()
    assert not (Path(gold) / "main" / "clean_generation").exists()

    # Gold joined Bronze (raw MW) with Silver (carbon estimate) across three lakehouses.
    rows = {
        r[0]: (r[1], r[2]) for r in
        duckrun.connect(gold, schema="main", read_only=True)
        .sql("select fuel, total_mw, total_tonnes_co2 from generation_by_fuel order by fuel").fetchall()
    }
    assert rows["Black coal"] == (500.0, 450.0)   # 500 MW × 900 kg/MWh = 450 t
    assert rows["Wind"] == (200.0, 0.0)           # zero-carbon
    assert rows["Natural gas"] == (300.0, 147.0)  # 300 × 490 / 1000


def test_cross_catalog_incremental_merges(tmp_path):
    """A second run upserts into the Bronze table under the Bronze root (cross-catalog discovery)."""
    bronze, silver, gold = roots = _roots(tmp_path)
    assert _dbt(roots, "run").success
    first = dict(
        (r[0].strftime("%Y-%m-%d") + "|" + r[1], r[2]) for r in
        duckrun.connect(bronze, schema="main", read_only=True)
        .sql("select gen_date, duid, mw from raw_generation").fetchall()
    )
    assert first == {"2024-01-01|BW01": 500.0, "2024-01-01|WIND1": 200.0, "2024-01-01|GAS1": 300.0}

    assert _dbt(roots, "run").success  # incremental: adds the 2024-01-02 readings, keeps day 1
    second = dict(
        (r[0].strftime("%Y-%m-%d") + "|" + r[1], r[2]) for r in
        duckrun.connect(bronze, schema="main", read_only=True)
        .sql("select gen_date, duid, mw from raw_generation").fetchall()
    )
    assert second == {
        "2024-01-01|BW01": 500.0, "2024-01-01|WIND1": 200.0, "2024-01-01|GAS1": 300.0,
        "2024-01-02|BW01": 480.0, "2024-01-02|WIND1": 210.0, "2024-01-02|GAS1": 150.0,
    }, second
