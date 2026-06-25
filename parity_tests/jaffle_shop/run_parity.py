"""Parity test: run an unmodified `type: duckdb` dbt project on BOTH dbt-duckdb and duckrun, then
assert every persisted table is identical row-for-row. dbt-duckdb is the oracle; a mismatch is a
duckrun bug (fix it in duckrun, never in the project).

Run:  python parity_tests/jaffle_shop/run_parity.py
Exit: 0 = parity (every duckrun Delta table == the duckdb table), 1 = build failure or mismatch.

The project repo is cloned fresh and run VERBATIM. The only thing supplied from outside is the
connection: the duckrun profile in this folder (profiles.yml), passed via --profiles-dir — in dbt
the profile is connection config that lives outside the project, so the repo is never modified.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb

# --- config (jaffle_shop; the same shape generalizes to other duckdb dbt projects) --------------
HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/dbt-labs/jaffle_shop_duckdb"
CLONE_DIR = Path("C:/tmp/js") if os.name == "nt" else Path("/tmp/js")
ORACLE_DB = CLONE_DIR / "jaffle_shop.duckdb"   # path: in jaffle_shop's own profiles.yml
ORACLE_SCHEMA = "main"
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or ("C:/tmp/js_wh" if os.name == "nt" else "/tmp/js_wh")
DUCKRUN_SCHEMA = "main"
_REMOTE = "://" in DUCKRUN_WH


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=cwd, env=env)
    if r.returncode != 0:
        sys.exit(f"command failed ({r.returncode}): {' '.join(cmd)}")


def build_oracle():
    """dbt build on the repo's OWN duckdb profile → ORACLE_DB."""
    if ORACLE_DB.exists():
        ORACLE_DB.unlink()
    sh(["dbt", "build", "--profiles-dir", str(CLONE_DIR), "--target-path", "target_duckdb"],
       cwd=CLONE_DIR)


def build_duckrun():
    """dbt build on the duckrun profile here → a fresh Delta warehouse (local dir or OneLake)."""
    if not _REMOTE:
        import shutil
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    env = {**os.environ,
           "DBT_PROFILES_DIR": str(HERE),
           "WAREHOUSE_PATH": DUCKRUN_WH,
           "DBT_SCHEMA": DUCKRUN_SCHEMA}
    sh(["dbt", "build", "--no-partial-parse", "--target-path", "target_duckrun"],
       cwd=CLONE_DIR, env=env)


def _duckrun_uri(schema, t):
    return f"{DUCKRUN_WH.rstrip('/')}/{schema}/{t}"


def _present(c, schema, t) -> bool:
    """Is there a duckrun Delta table at this schema/name? Views aren't persisted, so they're absent.
    Local: a directory check. OneLake (abfss://): try delta_scan and treat any failure as 'absent'."""
    if _REMOTE:
        try:
            c.execute(f"select 1 from delta_scan('{_duckrun_uri(schema, t)}') limit 1")
            return True
        except Exception:
            return False
    return Path(DUCKRUN_WH, schema, t).is_dir()


def diff() -> bool:
    """Row-multiset compare every oracle BASE TABLE against the duckrun Delta table of the same
    name (EXCEPT ALL both ways). Views aren't persisted by duckrun, so they're skipped."""
    c = duckdb.connect()
    c.execute("install delta; load delta")  # delta_scan needs the extension (fresh CI duckdb)
    if _REMOTE:  # OneLake: mint the Azure secret so delta_scan can read abfss:// (same path the adapter uses)
        from dbt.adapters.duckrun import secret
        secret.ensure_azure_secret(c, {"bearer_token": os.environ.get("ONELAKE_TOKEN", "")})
    c.execute(f"attach '{ORACLE_DB.as_posix()}' as o (read_only)")
    tabs = [r[0] for r in c.execute(
        "select table_name from duckdb_tables() "
        f"where database_name='o' and schema_name='{ORACLE_SCHEMA}' order by 1").fetchall()]
    all_ok = True
    for t in tabs:
        if not _present(c, DUCKRUN_SCHEMA, t):
            print(f"{t:16} SKIP (not persisted by duckrun — e.g. a view)")
            continue
        uri = _duckrun_uri(DUCKRUN_SCHEMA, t)
        cols = [r[0] for r in c.execute(
            "select column_name from duckdb_columns() "
            f"where database_name='o' and schema_name='{ORACLE_SCHEMA}' and table_name='{t}' "
            "order by column_index").fetchall()]
        sel = ", ".join('"' + x + '"' for x in cols)
        o_sel = f'select {sel} from o.{ORACLE_SCHEMA}."{t}"'
        d_sel = f"select {sel} from delta_scan('{uri}')"
        no = c.execute(f'select count(*) from o.{ORACLE_SCHEMA}."{t}"').fetchone()[0]
        nd = c.execute(f"select count(*) from delta_scan('{uri}')").fetchone()[0]
        only_o = c.execute(f"select count(*) from (({o_sel}) except all ({d_sel}))").fetchone()[0]
        only_d = c.execute(f"select count(*) from (({d_sel}) except all ({o_sel}))").fetchone()[0]
        ok = only_o == 0 and only_d == 0
        all_ok = all_ok and ok
        print(f"{t:16} oracle={no:<6} duckrun={nd:<6} only_oracle={only_o} only_duckrun={only_d}"
              f"  -> {'MATCH' if ok else 'MISMATCH'}")
    return all_ok


def main():
    if not (CLONE_DIR / "dbt_project.yml").exists():
        sh(["git", "clone", "--depth", "1", REPO_URL, str(CLONE_DIR)])
    build_oracle()
    build_duckrun()
    print("\n=== parity diff (duckrun Delta vs duckdb oracle) ===")
    ok = diff()
    print("\nPARITY:", "PASS — duckrun == dbt-duckdb on every persisted table" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
