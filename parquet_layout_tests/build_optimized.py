import os

import duckrun

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"],
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]},
                      read_only=False)
try:
    con.sql("create schema if not exists tests")
except Exception:
    con.con.execute("create schema if not exists tests")


def _exists():
    try:
        con.sql("select 1 from tests.fct_summary_optimized limit 1").fetchone()
        return True
    except Exception:
        return False


if not force and _exists():
    rows = con.sql("select count(*) from tests.fct_summary_optimized").fetchone()[0]
    print(f"tests.fct_summary_optimized already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
else:
    print(f"Building tests.fct_summary_optimized with '{clause}' ...", flush=True)
    con.sql(f"create or replace table tests.fct_summary_optimized {clause} "
            "as select * from mart.fct_summary")
    rows = con.sql("select count(*) from tests.fct_summary_optimized").fetchone()[0]
    print(f"done — tests.fct_summary_optimized built ({rows:,} rows)", flush=True)
