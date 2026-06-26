"""Parity test: run the upstream TechFlow Analytics (ameijin/dbt-example) dbt project on BOTH
dbt-duckdb and duckrun, unchanged, and assert duckrun's Delta output matches the duckdb output
table-by-table. dbt-duckdb is the oracle; a mismatch is a duckrun bug (fix it in duckrun, never in
the project).

A SaaS analytics project: raw data is committed **parquet** read via dbt-duckdb `external_location`
sources (data/*.parquet), plus a few CSV seeds. It exercises native dbt `unit_tests:`, an incremental
model (fct_mrr_daily), two timestamp snapshots, dbt_expectations + dbt_project_evaluator, exposures
and a staging->intermediate->marts layering. The repo is cloned fresh and run VERBATIM; the only
thing supplied from outside is the connection (oracle = the repo's own type:duckdb profile; duckrun =
the external profile in this folder, root_path -> a local Delta warehouse).

Run:  python parity_tests/techflow/run_parity.py
Exit: 0 = parity, 1 = build failure or mismatch.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/ameijin/dbt-example"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
ORACLE_DIR = TMP / "techflow_oracle"      # builds into its own ./dev.duckdb via the duckdb profile
DUCKRUN_DIR = TMP / "techflow_duckrun"    # seeds + models -> Delta warehouse
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or str(TMP / "techflow_duckrun_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (parity_techflow)
# so each project is an isolated Fabric schema under the SAME Tables root (like the integration suite).
DUCKRUN_SCHEMA = os.environ.get("DBT_SCHEMA", "main")
_REMOTE = "://" in DUCKRUN_WH

# Columns stamped from run wall-clock differ between two independent builds — compare on the rest.
# fct_mrr_daily stamps `loaded_at = current_timestamp`; the two snapshots carry the usual SCD2
# bookkeeping columns. Everything else is deterministic (data is committed parquet, no random()).
_SCD2 = {"dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to"}
EXCLUDE_COLS = {
    "fct_mrr_daily": {"loaded_at"},
    "subscription_pricing_snapshot": _SCD2,
    "user_plan_snapshot": _SCD2,
}

# Tables whose DOUBLE columns are compared rounded to cents instead of bit-exactly.
# fct_mrr_daily's MRR columns are built by a parallel `GROUP BY sum(<double>)` (int_daily_mrr_changes)
# feeding a running-`sum()` window (cumulative_mrr). DuckDB combines partial float sums in a
# thread-scheduling order that depends on the runner's core count, so two INDEPENDENT builds drift in
# the last ULPs (e.g. 24813.09999999998 vs 24813.10000000025). dbt-duckdb has the exact same behavior
# — it is NOT a duckrun divergence (it reproduces locally only when the two builds happen to combine
# identically). Every value here is genuine currency: measured residue from an exact cent is <= 3e-12
# and zero rows are >1e-4 off a cent. So comparing rounded to cents drops only the float noise — it
# changes no real value, has no boundary risk (values sit at cents, 0.005 from any rounding boundary),
# and any real divergence >= $0.01 still fails. Same approach as the MRR fct_mrr_movements parity.
# Non-float columns (keys, date_day, product, event_count) are always compared exactly.
ROUND_CENTS_TABLES = {"fct_mrr_daily"}
_FLOAT_TYPES = {"DOUBLE", "REAL", "FLOAT"}


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}  (cwd={cwd})")
    if subprocess.run(cmd, cwd=cwd, env=env).returncode != 0:
        sys.exit(f"command failed: {' '.join(cmd)}")


def fresh_clone(dest: Path):
    import shutil
    shutil.rmtree(dest, ignore_errors=True)
    sh(["git", "clone", "--depth", "1", REPO_URL, str(dest)])


def build(dest: Path, profiles_dir: str, env_extra: dict):
    """Run `dbt deps` + `dbt build`, verbatim, with the given profile (seeds + snapshots + models +
    tests + unit tests). The raw parquet is committed in the repo, so no extract/generate step."""
    env = {**os.environ, **env_extra}
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    sh(["dbt", "build", "--profiles-dir", profiles_dir], cwd=dest, env=env)


def _rows(con, select_sql):
    return sorted(con.execute(select_sql).fetchall(), key=lambda r: tuple(str(c) for c in r))


def _evaluator_relations() -> set:
    """(schema, table) of every model dbt_project_evaluator contributes, read from the oracle's
    manifest. dbt_project_evaluator is a LINTING package: it introspects the dbt graph and builds
    tables that describe the project *and its adapter* — each node's `database` catalog (duckdb's
    'dev' file vs duckrun's 'memory') and `materialized`, which the package itself hardcodes via
    `+materialized: "{{ 'table' if target.type in ['duckdb'] else 'view' }}"`. Since duckrun's
    target.type is 'duckrun' (it is its own adapter type, not 'duckdb'), those columns differ from
    dbt-duckdb by design — they cannot match across two adapters and are NOT the project's data. So
    these tables are skipped from the row diff. The build still runs the whole package GREEN on
    duckrun, and every table the project actually produces (marts, snapshots, the incremental model,
    seeds) is diffed in full."""
    manifest = json.loads((ORACLE_DIR / "target" / "manifest.json").read_text())
    return {(n["schema"], n.get("alias") or n["name"])
            for n in manifest["nodes"].values()
            if n.get("package_name") == "dbt_project_evaluator"}


def _duckrun_uri(schema, t):
    return f"{DUCKRUN_WH.rstrip('/')}/{schema}/{t}"


def _present(c, schema, t) -> bool:
    """Is there a duckrun Delta table here? Views aren't persisted, so they're absent. Local: a
    directory check. OneLake (abfss://): try delta_scan and treat any failure as 'absent'."""
    if _REMOTE:
        try:
            c.execute(f"select 1 from delta_scan('{_duckrun_uri(schema, t)}') limit 1")
            return True
        except Exception:
            return False
    return Path(DUCKRUN_WH, schema, t).is_dir()


def diff() -> bool:
    c = duckdb.connect()
    c.execute("install delta; load delta")
    if _REMOTE:  # OneLake: mint the Azure secret so delta_scan can read abfss:// (same path the adapter uses)
        from dbt.adapters.duckrun import secret
        secret.ensure_azure_secret(c, {"bearer_token": os.environ.get("ONELAKE_TOKEN", "")})
    c.execute(f"attach '{(ORACLE_DIR / 'dev.duckdb').as_posix()}' as o (read_only)")
    evaluator = _evaluator_relations()
    # Every base table the oracle persisted (skip views — duckrun has no durable view).
    tabs = c.execute(
        "select schema_name, table_name from duckdb_tables() where database_name='o' "
        "and schema_name not in ('information_schema') order by 1,2"
    ).fetchall()
    all_ok = True
    for schema, t in tabs:
        if (schema, t) in evaluator:
            print(f"{schema}.{t:36} SKIP (dbt_project_evaluator — linting pkg, hardcodes target.type in ['duckdb'])")
            continue
        # The oracle's default schema ('main') maps to the duckrun side's DUCKRUN_SCHEMA (per-project
        # on OneLake); custom schemas (marts/seeds/snapshots) are written as-is by both → map to self.
        dr_schema = DUCKRUN_SCHEMA if schema == "main" else schema
        if not _present(c, dr_schema, t):
            print(f"{schema}.{t:36} SKIP (not persisted by duckrun)")
            continue
        uri = _duckrun_uri(dr_schema, t)
        ocols = c.execute(
            "select column_name, data_type from duckdb_columns() where database_name='o' "
            f"and schema_name='{schema}' and table_name='{t}' order by column_index").fetchall()
        drop = EXCLUDE_COLS.get(t, set())
        round_cents = t in ROUND_CENTS_TABLES

        def _expr(col, dtype):
            q = '"' + col + '"'
            # Round only this table's float columns to cents (see ROUND_CENTS_TABLES); everything else
            # is compared bit-exactly. Applied identically to oracle and duckrun.
            if round_cents and (dtype or "").upper() in _FLOAT_TYPES:
                return f"round({q}, 2)"
            return q

        sel = ", ".join(_expr(col, dt) for col, dt in ocols if col not in drop)
        o_rows = _rows(c, f'select {sel} from o."{schema}"."{t}"')
        d_rows = _rows(c, f"select {sel} from delta_scan('{uri}')")
        ok = o_rows == d_rows
        all_ok = all_ok and ok
        bits = []
        if drop:
            bits.append(f"excl {sorted(drop)}")
        if round_cents:
            bits.append("floats rounded to cents")
        note = f" ({'; '.join(bits)})" if bits else ""
        print(f"{schema}.{t:36} oracle={len(o_rows):<5} duckrun={len(d_rows):<5} "
              f"-> {'MATCH' if ok else 'MISMATCH'}{note}")
    return all_ok


def main():
    fresh_clone(ORACLE_DIR)
    fresh_clone(DUCKRUN_DIR)
    import shutil
    if not _REMOTE:
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    # Oracle: the repo's OWN profile (type: duckdb, path: ./dev.duckdb) — zero external config.
    build(ORACLE_DIR, str(ORACLE_DIR), {})
    # duckrun: external profile here (type: duckrun, root_path -> Delta warehouse).
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": DUCKRUN_WH, "DBT_SCHEMA": DUCKRUN_SCHEMA})
    print("\n=== parity diff (duckrun Delta vs duckdb oracle) ===")
    ok = diff()
    print("\nPARITY:", "PASS — duckrun == dbt-duckdb on every persisted table" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
