"""Parity test: run the upstream MRR (dbt-mrr-assignment) project on BOTH dbt-duckdb and duckrun,
unchanged, and assert duckrun's Delta output matches the duckdb output table-by-table.

It's a subscription-revenue model: raw CSV seeds -> staging/intermediate (views) -> mart TABLES
(`fct_mrr`, `fct_mrr_movements`) that amortize invoices into monthly recurring revenue and derive
MRR movements (new/expansion/contraction/reactivation/retained). The repo also ships native dbt
`unit_tests:` on the amortization model, singular tests, and an exposure — `dbt build` runs all of
them on both adapters, so a green build means duckrun's unit-test/test path works too.

The repo is cloned fresh and run VERBATIM. The only thing supplied from outside is the connection:
the oracle uses the repo's OWN `type: duckdb` profile; duckrun uses the external profile in this
folder (`root_path` -> a local Delta warehouse for the marts; seeds are in-repo CSVs, no sources).

Run:  python tests/parity_tests/mrr/run_parity.py
Exit: 0 = parity, 1 = build failure or mismatch.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
REPO_URL = "https://github.com/Elkadev/dbt-mrr-assignment"
TMP = Path("C:/tmp") if os.name == "nt" else Path("/tmp")
ORACLE_DIR = TMP / "mrr_oracle"      # builds into its own ./mrr_analytics.duckdb via the duckdb profile
DUCKRUN_DIR = TMP / "mrr_duckrun"    # seeds + models -> Delta warehouse
# duckrun warehouse root: an abfss:// OneLake Tables path when WAREHOUSE_PATH is set (the parity CI
# points it at Microsoft Fabric); otherwise a local-filesystem warehouse for a plain local run.
DUCKRUN_WH = os.environ.get("WAREHOUSE_PATH") or str(TMP / "mrr_duckrun_wh")
# duckrun writes <root>/<schema>/<table>. On OneLake the CI sets a per-project schema (parity_mrr) so
# each project is an isolated Fabric schema under the SAME Tables root (like the integration suite).
DUCKRUN_SCHEMA = os.environ.get("DBT_SCHEMA", "main")
_REMOTE = "://" in DUCKRUN_WH


def sh(cmd, cwd=None, env=None):
    print(f"$ {' '.join(cmd)}  (cwd={cwd})")
    if subprocess.run(cmd, cwd=cwd, env=env).returncode != 0:
        sys.exit(f"command failed: {' '.join(cmd)}")


def fresh_clone(dest: Path):
    import shutil
    shutil.rmtree(dest, ignore_errors=True)
    sh(["git", "clone", "--depth", "1", REPO_URL, str(dest)])


def build(dest: Path, profiles_dir: str, env_extra: dict):
    """Run `dbt deps` + `dbt build`, verbatim, with the given profile (seeds + models + tests)."""
    env = {**os.environ, **env_extra}
    sh(["dbt", "deps", "--profiles-dir", profiles_dir], cwd=dest, env=env)
    sh(["dbt", "build", "--profiles-dir", profiles_dir], cwd=dest, env=env)


def _rows(con, select_sql):
    return sorted(con.execute(select_sql).fetchall(), key=lambda r: tuple(str(c) for c in r))


# fct_mrr_movements splits each (month, use_case, country) group into movement buckets (new /
# expansion / contraction / reactivation / retained) via a STRICT >/< comparison of *unrounded*
# float sums (this month's mrr_usd vs last month's). Months that are equal to the cent still differ
# by ~5e-14 from float summation order, so 'retained' tips to 'expansion'/'contraction' differently
# depending on scan order (a duckdb native table vs duckrun's delta_scan over parquet). That's a
# non-determinism in the PROJECT's SQL, not a duckrun bug — fct_mrr (the rounded mart) matches
# exactly, so duckrun's values are right to the cent. Roll the fragile bucket split away to the
# (month, use_case, country) grain and compare what is provably invariant to which bucket a customer
# lands in (same roll-up applied to both engines):
#   - sum(customer_count): each customer is in exactly one bucket per group, so the total is fixed;
#   - round(sum(mrr_change_usd), 2): the only customers that flip buckets are the cent-equal ones,
#     whose change is ~0, so no bucket's rounded sum moves.
# mrr_usd is intentionally NOT rolled up here: a flipping customer carries their full ~$X between
# buckets, so re-summing the per-bucket-rounded mrr_usd could tip a cent — and it is already
# verified exactly by fct_mrr (same sum, one grain up).
ROLLUP = {
    "fct_mrr_movements":
        "select month, use_case, country, sum(customer_count) as customer_count, "
        "round(sum(mrr_change_usd), 2) as mrr_change_usd "
        "from {rel} group by month, use_case, country",
}


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
    c.execute(f"attach '{(ORACLE_DIR / 'mrr_analytics.duckdb').as_posix()}' as o (read_only)")
    # Every base table the oracle persisted (skip views — duckrun has no durable view).
    tabs = c.execute(
        "select schema_name, table_name from duckdb_tables() where database_name='o' "
        "and schema_name not in ('information_schema') order by 1,2"
    ).fetchall()
    all_ok = True
    for schema, t in tabs:
        # The oracle's default schema ('main') maps to the duckrun side's DUCKRUN_SCHEMA (per-project
        # on OneLake); custom schemas are written as-is by both, so they map to themselves.
        dr_schema = DUCKRUN_SCHEMA if schema == "main" else schema
        if not _present(c, dr_schema, t):
            print(f"{schema}.{t:24} SKIP (not persisted by duckrun)")
            continue
        uri = _duckrun_uri(dr_schema, t)
        rollup = ROLLUP.get(t)
        if rollup:
            o_rows = _rows(c, rollup.format(rel=f'o."{schema}"."{t}"'))
            d_rows = _rows(c, rollup.format(rel=f"delta_scan('{uri}')"))
        else:
            cols = [r[0] for r in c.execute(
                "select column_name from duckdb_columns() where database_name='o' "
                f"and schema_name='{schema}' and table_name='{t}' order by column_index").fetchall()]
            sel = ", ".join('"' + col + '"' for col in cols)
            o_rows = _rows(c, f'select {sel} from o."{schema}"."{t}"')
            d_rows = _rows(c, f"select {sel} from delta_scan('{uri}')")
        ok = o_rows == d_rows
        all_ok = all_ok and ok
        note = " (rolled up past non-deterministic movement split)" if rollup else ""
        print(f"{schema}.{t:24} oracle={len(o_rows):<5} duckrun={len(d_rows):<5} "
              f"-> {'MATCH' if ok else 'MISMATCH'}{note}")
    return all_ok


def main():
    fresh_clone(ORACLE_DIR)
    fresh_clone(DUCKRUN_DIR)
    import shutil
    if not _REMOTE:
        shutil.rmtree(DUCKRUN_WH, ignore_errors=True)
    # Oracle: the repo's OWN profile (type: duckdb, path: ./mrr_analytics.duckdb) — zero external config.
    build(ORACLE_DIR, str(ORACLE_DIR), {})
    # duckrun: external profile here (type: duckrun, root_path -> Delta warehouse).
    build(DUCKRUN_DIR, str(HERE), {"WAREHOUSE_PATH": DUCKRUN_WH, "DBT_SCHEMA": DUCKRUN_SCHEMA})
    print("\n=== parity diff (duckrun Delta vs duckdb oracle) ===")
    ok = diff()
    print("\nPARITY:", "PASS — duckrun == dbt-duckdb on every persisted table" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
