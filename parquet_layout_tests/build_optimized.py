"""Build a SEPARATE mart.fct_summary_optimized table (duckrun sorted) from the base
mart.fct_summary — WITHOUT touching the base. The benchmark stays independent: it only ever
assumes the base `fct_summary` exists; the optimized (duckrun-clustered) and vorder (Fabric
Spark V-Order) copies are both derived from it, so base vs optimized vs vorder is apples-to-apples.

Env: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, OPT_SORT ('auto' for the sort-key recommender, or a
column list like 'date, time'; default 'auto'), FORCE_REBUILD ('true' to rebuild even if the
table already exists; default skip-if-exists to save time/capacity).
"""
import os

import duckrun

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = (os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true")

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/mart",
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]},
                      read_only=False)


def _exists():
    try:
        con.sql("select 1 from mart.fct_summary_optimized limit 1").fetchone()
        return True
    except Exception:
        return False


if not force and _exists():
    rows = con.sql("select count(*) from mart.fct_summary_optimized").fetchone()[0]
    print(f"mart.fct_summary_optimized already exists ({rows:,} rows) — skipping build "
          "(set rebuild=true to rebuild)", flush=True)
else:
    print(f"Building mart.fct_summary_optimized with '{clause}' (base untouched) ...", flush=True)
    con.sql(f"create or replace table mart.fct_summary_optimized {clause} "
            "as select * from mart.fct_summary")
    rows = con.sql("select count(*) from mart.fct_summary_optimized").fetchone()[0]
    print(f"done — mart.fct_summary_optimized built ({rows:,} rows), base left pristine", flush=True)
