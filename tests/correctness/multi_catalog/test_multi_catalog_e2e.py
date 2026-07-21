"""End-to-end multi-catalog (multi-Lakehouse) write/read through REAL dbt (issue #7).

One dbt project writes across TWO physical Delta roots — the default ("silver") catalog and a
declared ``catalogs: lh_bronze`` catalog — using the standard ``+database: lh_bronze`` model config.
Local folders stand in for two Fabric Lakehouses. The project:

  * ``bronze_raw``  — an incremental merge model in the ``lh_bronze`` catalog.
  * ``silver_clean`` — a table in the default catalog that ``ref()``s bronze_raw ACROSS catalogs.

Assertions prove all three legs of the feature: each model lands under its OWN root (isolation),
cross-catalog ``ref()`` resolves, and cross-catalog ``is_incremental()`` works (a second run merges
into the bronze table found under the bronze root instead of clobbering it).
"""
import json
import os
from pathlib import Path

import duckrun
import pytest

from dbt.cli.main import dbtRunner

PROJECT_DIR = str(Path(__file__).parent)


def _dbt(silver: str, bronze: str, *args: str, gold: str = None) -> object:
    os.environ["SILVER_PATH"] = silver
    os.environ["BRONZE_PATH"] = bronze
    # gold defaults next to silver so the profile's GOLD_PATH always resolves, even for the
    # (older) two-root tests that don't build the gold model.
    os.environ["GOLD_PATH"] = gold or (silver + "_gold")
    return dbtRunner().invoke([*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR])


def _roots(tmp_path):
    return (tmp_path / "silver").as_posix(), (tmp_path / "bronze").as_posix()


def test_models_land_under_their_own_catalog_roots(tmp_path):
    """Each model materializes under its declared catalog's root, and NOT the other's."""
    silver, bronze = _roots(tmp_path)
    assert _dbt(silver, bronze, "run").success

    # bronze_raw is physically under the bronze root; silver_clean under the silver root.
    assert (Path(bronze) / "main" / "bronze_raw" / "_delta_log").is_dir()
    assert (Path(silver) / "main" / "silver_clean" / "_delta_log").is_dir()
    # ...and crucially NOT crossed over into the other lakehouse.
    assert not (Path(silver) / "main" / "bronze_raw").exists()
    assert not (Path(bronze) / "main" / "silver_clean").exists()


def test_cross_catalog_ref_resolves(tmp_path):
    """silver_clean reads bronze_raw from a DIFFERENT lakehouse; its rows are bronze doubled."""
    silver, bronze = _roots(tmp_path)
    assert _dbt(silver, bronze, "run").success

    rows = dict(
        duckrun.connect(silver, schema="main", read_only=True)
        .sql("select id, double_amount from silver_clean order by id").fetchall()
    )
    assert rows == {1: 200, 2: 400}  # from bronze {1:100, 2:200}, doubled


def test_cross_catalog_incremental_merges(tmp_path):
    """A second run merges into the bronze table found under the bronze root (proving cross-catalog
    is_incremental discovery): id=1 updated, id=2 preserved, id=3 inserted. A discovery miss would
    overwrite and drop id=2."""
    silver, bronze = _roots(tmp_path)
    assert _dbt(silver, bronze, "run").success
    con = duckrun.connect(bronze, schema="main", read_only=True)
    first = dict(con.sql("select id, amount from bronze_raw order by id").fetchall())
    assert first == {1: 100, 2: 200}

    assert _dbt(silver, bronze, "run").success
    second = dict(
        duckrun.connect(bronze, schema="main", read_only=True)
        .sql("select id, amount from bronze_raw order by id").fetchall()
    )
    assert second == {1: 111, 2: 200, 3: 300}, second


def test_cross_catalog_join_across_three_lakehouses(tmp_path):
    """gold_report JOINs bronze_raw (lh_bronze) with silver_clean (default/silver) and lands in a
    THIRD lakehouse (lh_gold). Proves tables from three separate Delta roots are all visible and
    joinable in a single query, and the result lands under the gold root."""
    silver, bronze = _roots(tmp_path)
    gold = (tmp_path / "gold").as_posix()
    assert _dbt(silver, bronze, "run", gold=gold).success

    # gold_report is physically under the gold root only.
    assert (Path(gold) / "main" / "gold_report" / "_delta_log").is_dir()
    assert not (Path(silver) / "main" / "gold_report").exists()
    assert not (Path(bronze) / "main" / "gold_report").exists()

    # The join stitched bronze amounts to silver's doubled amounts, keyed by id.
    rows = dict(
        (r[0], (r[1], r[2])) for r in
        duckrun.connect(gold, schema="main", read_only=True)
        .sql("select id, bronze_amount, silver_double_amount from gold_report order by id").fetchall()
    )
    assert rows == {1: (100, 200), 2: (200, 400)}


def test_docs_generate_lists_both_catalogs(tmp_path):
    """dbt docs generate succeeds across catalogs and reports each model under its own database."""
    silver, bronze = _roots(tmp_path)
    assert _dbt(silver, bronze, "run").success
    assert _dbt(silver, bronze, "docs", "generate").success

    catalog = json.loads((Path(PROJECT_DIR) / "target" / "catalog.json").read_text())
    by_model = {n["metadata"]["name"]: n["metadata"]["database"] for n in catalog["nodes"].values()}
    # The aliased model is reported under its catalog; the default model under the default catalog.
    assert by_model["bronze_raw"] == "lh_bronze"
    assert by_model["silver_clean"] != "lh_bronze"


def test_seed_lands_in_native_catalog(tmp_path):
    """A seed with ``+database`` naming a natively-attached catalog (the ``format: iceberg`` shape)
    lands as a real native table via the temp-stage + CTAS path — dbt-core's default seed COPYs the
    CSV straight into the catalog table, which DuckDB's Iceberg writes reject (jaffle parity, first
    live run). A second run must drop + recreate, not error."""
    import duckdb

    silver, bronze = _roots(tmp_path)
    native = (tmp_path / "native.duckdb").as_posix()
    os.environ["NATIVE_DB"] = native
    try:
        assert _dbt(silver, bronze, "seed").success
        assert _dbt(silver, bronze, "seed").success   # re-run: rebuild, not "already exists"
    finally:
        os.environ.pop("NATIVE_DB", None)

    # The in-process dbtRunner's cached connection still holds the file. Cycling the profile back
    # to the ':memory:' attach makes dbt-duckdb rebuild (and release) it, so the readback can open
    # the file — a same-process harness artifact, not a duckrun behavior.
    import gc
    assert _dbt(silver, bronze, "seed").success
    gc.collect()

    con = duckdb.connect(native)
    try:
        rows = con.sql('select id, amount from main.native_seed order by id').fetchall()
    finally:
        con.close()
    assert rows == [(1, 10.5), (2, 20.0)]
    # ...and nothing leaked into the Delta roots — the seed belongs to the native catalog alone.
    assert not (Path(silver) / "main" / "native_seed").exists()
    assert not (Path(bronze) / "main" / "native_seed").exists()
