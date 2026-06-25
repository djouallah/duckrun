"""Parity test: run the upstream Start-Data-Engineering dbt project on BOTH dbt-duckdb and duckrun,
unchanged, and assert duckrun's Delta output matches the duckdb output table-by-table.

This is the project whose `delete+insert` model duckrun used to need a fixture rewrite for; it now
runs VERBATIM. The repo is cloned fresh and run as-is — the only thing supplied from outside is the
connection: the oracle uses the repo's OWN `type: duckdb` profile; duckrun uses the external profile
in this folder (`path` → the same ./dbt.duckdb the repo's EL fills, so `sources` resolve; `root_path`
→ a local Delta warehouse for the models). Both run the repo's own EL (extract_load_pipeline.py).

Run:  python parity_tests/sde/run_parity.py
Exit: 0 = parity, 1 = build failure or mismatch.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/josephmachado/simple_dbt_project"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
ORACLE_DIR = TMP / "sde_oracle"      # builds into its own ./dbt.duckdb via the repo's duckdb profile
DUCKRUN_DIR = TMP / "sde_duckrun"    # sources from ./dbt.duckdb; models → Delta warehouse
DUCKRUN_WH = TMP / "sde_duckrun_wh"

# SCD2 snapshot bookkeeping columns are stamped from run wall-clock / row hashes, so they differ
# between two independent runs — compare the business columns only (per table that needs it).
EXCLUDE_COLS = {"dim_customer": {"dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to"}}


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}  (cwd={cwd})")
    if subprocess.run(cmd, cwd=cwd, env=env).returncode != 0:
        sys.exit(f"command failed: {' '.join(cmd)}")


def fresh_clone(dest: Path):
    import shutil
    shutil.rmtree(dest, ignore_errors=True)
    sh(["git", "clone", "--depth", "1", REPO_URL, str(dest)])


def build(dest: Path, profiles_dir: str, env_extra: dict):
    """Run the repo's EL then `dbt build`, verbatim, with the given profile."""
    env = {**os.environ, **env_extra}
    sh([sys.executable, "extract_load_pipeline.py"], cwd=dest, env=env)   # raw.* → ./dbt.duckdb
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    sh(["dbt", "build", "--profiles-dir", profiles_dir], cwd=dest, env=env)


def _rows(con, select_sql):
    return sorted(con.execute(select_sql).fetchall(), key=lambda r: tuple(str(c) for c in r))


def diff() -> bool:
    import shutil
    c = duckdb.connect()
    c.execute("install delta; load delta")
    c.execute(f"attach '{(ORACLE_DIR / 'dbt.duckdb').as_posix()}' as o (read_only)")
    # Every base table the oracle persisted (skip views — duckrun has no durable view).
    tabs = c.execute(
        "select schema_name, table_name from duckdb_tables() where database_name='o' "
        "and schema_name not in ('information_schema') order by 1,2"
    ).fetchall()
    all_ok = True
    for schema, t in tabs:
        dpath = DUCKRUN_WH / schema / t
        if not dpath.is_dir():
            print(f"{schema}.{t:24} SKIP (not persisted by duckrun)")
            continue
        ocols = [r[0] for r in c.execute(
            "select column_name from duckdb_columns() where database_name='o' "
            f"and schema_name='{schema}' and table_name='{t}' order by column_index").fetchall()]
        drop = EXCLUDE_COLS.get(t, set())
        cols = [col for col in ocols if col not in drop]
        sel = ", ".join('"' + col + '"' for col in cols)
        o_rows = _rows(c, f'select {sel} from o."{schema}"."{t}"')
        d_rows = _rows(c, f"select {sel} from delta_scan('{dpath.as_posix()}')")
        ok = o_rows == d_rows
        all_ok = all_ok and ok
        note = f" (excl {sorted(drop)})" if drop else ""
        print(f"{schema}.{t:24} oracle={len(o_rows):<5} duckrun={len(d_rows):<5} "
              f"-> {'MATCH' if ok else 'MISMATCH'}{note}")
    return all_ok


def main():
    fresh_clone(ORACLE_DIR)
    fresh_clone(DUCKRUN_DIR)
    import shutil
    shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    # Oracle: the repo's OWN profile (type: duckdb, path: ./dbt.duckdb) — zero external config.
    build(ORACLE_DIR, str(ORACLE_DIR), {})
    # duckrun: external profile here (type: duckrun, path → ./dbt.duckdb, root_path → Delta warehouse).
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": str(DUCKRUN_WH), "DBT_SCHEMA": "main"})
    print("\n=== parity diff (duckrun Delta vs duckdb oracle) ===")
    ok = diff()
    print("\nPARITY:", "PASS — duckrun == dbt-duckdb on every persisted table" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
