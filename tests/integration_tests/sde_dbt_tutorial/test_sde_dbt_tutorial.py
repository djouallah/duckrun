"""End-to-end port of josephmachado/simple_dbt_project onto the duckrun adapter, run against a
**persistent** Microsoft Fabric OneLake warehouse (abfss://) — the way the SCD2 snapshot and the
merge-incremental model actually behave in production.

Unlike a throwaway local run, the warehouse here is long-lived and shared across CI runs, so the
test does NOT assume an empty starting state. Instead it drives a *controlled* source change and
asserts the SCD2 invariants that must hold no matter how much history has accumulated:

  * a key always has exactly one CURRENT version (dbt_valid_to = the 9999-12-31 sentinel);
  * applying an update opens a new current version carrying the new attribute value, and adds
    exactly one row;
  * the previously-current version is CLOSED at the instant the new one opens — contiguous,
    non-overlapping validity windows;
  * a no-op rebuild adds nothing (snapshots are idempotent).

Real dbt runs in a fresh subprocess per phase, so {{ this }} / is_incremental() / the snapshot
MERGE resolve across processes exactly as in production.

Skips unless OneLake is configured (WAREHOUSE_PATH=abfss://…/Tables and ONELAKE_TOKEN).
"""
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckrun
import pytest

PROJECT_DIR = Path(__file__).parent
EL = PROJECT_DIR / "el"
DBT = [sys.executable, "-m", "dbt.cli.main"]

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")
ONELAKE_TOKEN = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")

pytestmark = pytest.mark.skipif(
    not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://") and ONELAKE_TOKEN),
    reason="OneLake not configured (set WAREHOUSE_PATH=abfss://…/Tables and ONELAKE_TOKEN)",
)

# Root is the lakehouse Tables path itself: for OneLake, duckrun treats the first segment after
# /Tables as the SCHEMA, so a nested project subfolder can't work. sde isolates from the aemo and
# coffee scenarios that share the lakehouse by sde_-prefixed schema names (sde_raw / sde_main /
# sde_snapshots) instead.
ROOT = WAREHOUSE_PATH


def _storage_options() -> dict:
    return {"bearer_token": ONELAKE_TOKEN}


def _env() -> dict:
    env = dict(os.environ)
    env["DUCKRUN_WAREHOUSE"] = ROOT
    return env


def _run(cmd, env):
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


def _scalar(con, sql):
    return con.sql(sql).fetchone()[0]


def assert_catalog_has_delta_stats(catalog_path):
    """`dbt docs generate` must publish row/byte/last-modified stats for Delta-backed models
    (issue #3). At least one model reports stats, and each reported stat is shaped right. Native
    views / drop-tombstones stay statless, so we assert "some node", not "every node"."""
    catalog = json.loads(Path(catalog_path).read_text())
    statted = {uid: n["stats"] for uid, n in catalog.get("nodes", {}).items()
               if n.get("stats", {}).get("has_stats", {}).get("value")}
    assert statted, f"no model reported Delta stats in {catalog_path}"
    for uid, s in statted.items():
        for k in ("num_rows", "bytes", "last_modified"):
            assert s.get(k, {}).get("include") is True, f"{uid} missing stat {k!r}"
        assert isinstance(s["num_rows"]["value"], int), f"{uid} num_rows not an int"
        assert isinstance(s["bytes"]["value"], int), f"{uid} bytes not an int"


def test_sde_dbt_tutorial_scd2_stateful():
    env = _env()
    con = duckrun.connect(ROOT, storage_options=_storage_options(), read_only=False)

    snap = f"{ROOT}/sde_snapshots/dim_customer"
    obt = f"{ROOT}/sde_main/orders_obt"
    base_csv = (PROJECT_DIR / "raw_data" / "customer.csv").as_posix()
    key = "82"

    # CURRENT version of a key = the open row, whose dbt_valid_to is the 9999-12-31 sentinel.
    # Cast to varchar everywhere: the year-9999 sentinel overflows pandas' ns timestamps, and
    # string equality is enough to test contiguity of the validity windows.
    def versions(c):
        return _scalar(c, f"select count(*) from delta_scan('{snap}') where customer_id = '{key}'")

    def current_count(c):
        return _scalar(c, f"""
            select count(*) from delta_scan('{snap}')
            where customer_id = '{key}' and cast(dbt_valid_to as varchar) like '9999-12-31%'
        """)

    def current_row(c):
        return _scalar(c, f"""
            select city || '|' || cast(dbt_valid_from as varchar) from delta_scan('{snap}')
            where customer_id = '{key}' and cast(dbt_valid_to as varchar) like '9999-12-31%'
        """)

    # ---- baseline: bring the warehouse to a known-built state (idempotent on a persistent store) --
    _run([sys.executable, str(EL / "extract_load_pipeline.py")], env)
    _dbt(["deps"], env)
    _dbt(["seed"], env)
    _dbt(["build"], env)

    # SCD2 invariant that holds regardless of accumulated history: exactly one current version.
    assert current_count(con) == 1, "a key must have exactly one open SCD2 version"
    versions_before = versions(con)

    # ---- the change: the source system updates customer 82 (new city + newer datetime_updated) ----
    # A unique marker + wall-clock timestamp guarantee the update is strictly newer than anything
    # already snapshotted, so the timestamp strategy opens a new version on a long-lived table.
    marker = "scd2-" + uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    updated = con.sql(f"""
        select customer_id, zipcode,
               case when customer_id = '{key}' then '{marker}' else city end as city,
               state_code, datetime_created,
               case when customer_id = '{key}' then '{now}' else datetime_updated end
                   as datetime_updated
        from read_csv_auto('{base_csv}', all_varchar = true, header = true)
    """)
    updated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("sde_raw.raw_customer")
    _dbt(["build"], env)

    # exactly one new version opened, carrying the new attribute, with the old version closed.
    assert versions(con) == versions_before + 1, "an update must add exactly one SCD2 version"
    assert current_count(con) == 1, "still exactly one open version after the update"

    city, new_from = current_row(con).split("|", 1)
    assert city == marker, "the open version must carry the updated attribute"

    # Contiguity: the previously-current row is closed exactly when the new one opens (no gap/overlap).
    closed_at_boundary = _scalar(con, f"""
        select count(*) from delta_scan('{snap}')
        where customer_id = '{key}' and cast(dbt_valid_to as varchar) = '{new_from}'
    """)
    assert closed_at_boundary == 1, "prior version must close exactly when the new one opens"

    # ---- idempotency: a no-op rebuild adds no version and leaves the open row intact --------------
    versions_after = versions(con)
    _dbt(["build"], env)
    assert versions(con) == versions_after, "a no-op rebuild must not open a new version"
    assert current_count(con) == 1
    assert current_row(con).split("|", 1)[0] == marker

    # ---- the gold OBT still builds and joins through the SCD2 validity window ---------------------
    assert _scalar(con, f"select count(*) from delta_scan('{obt}')") > 0

    # ---- dbt docs: the catalog carries Delta stats (issue #3), here over LIVE OneLake -------------
    # This is the remote path the issue flagged as empty; delta_stats reads the Delta log via
    # delta-rs with the OneLake bearer token, so the published catalog now has rows/size/last-modified.
    _dbt(["docs", "generate"], env)
    assert_catalog_has_delta_stats(PROJECT_DIR / "target" / "catalog.json")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-s", "-v"]))
