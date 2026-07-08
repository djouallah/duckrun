import os

import duckrun

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
        con.sql("select 1 from tests.summary_unsorted limit 1").fetchone()
        return True
    except Exception:
        return False


if not force and _exists():
    rows = con.sql("select count(*) from tests.summary_unsorted").fetchone()[0]
    print(f"tests.summary_unsorted already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
else:
    print("Building tests.summary_unsorted (shuffled) ...", flush=True)
    con.sql("create or replace table tests.summary_unsorted "
            "as select * from mart.fct_summary order by random()")
    rows = con.sql("select count(*) from tests.summary_unsorted").fetchone()[0]
    print(f"done — tests.summary_unsorted built ({rows:,} rows)", flush=True)
