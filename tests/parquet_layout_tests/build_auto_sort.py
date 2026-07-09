import os
import sys
import time

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
# Read the source mart.fct_summary DIRECTLY (its own independent read, separate from the Spark
# V-Order build's) with the same row cap. SORTED BY AUTO re-sorts regardless of input order.
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
_src = "mart.fct_summary" if N is None else f"(select * from mart.fct_summary limit {N})"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"],
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]},
                      read_only=False)
try:
    con.sql("create schema if not exists tests")
except Exception:
    con.con.execute("create schema if not exists tests")


def _exists():
    try:
        con.sql("select 1 from tests.fct_summary_auto_sort limit 1").fetchone()
        return True
    except Exception:
        return False


_t0 = time.perf_counter()
if not force and _exists():
    rows = con.sql("select count(*) from tests.fct_summary_auto_sort").fetchone()[0]
    print(f"tests.fct_summary_auto_sort already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
    status = "skipped"
else:
    print(f"Building tests.fct_summary_auto_sort with '{clause}' ...", flush=True)
    # Read mart.fct_summary directly (independent of the Spark V-Order build's read); SORTED BY AUTO
    # re-sorts regardless of the source's order.
    con.sql(f"create or replace table tests.fct_summary_auto_sort {clause} "
            f"as select * from {_src}")
    rows = con.sql("select count(*) from tests.fct_summary_auto_sort").fetchone()[0]
    print(f"done — tests.fct_summary_auto_sort built ({rows:,} rows)", flush=True)
    status = "rebuilt"

report.merge({"tables": {"fct_summary_auto_sort": {"build": {
    "engine": "delta_rs", "sort": clause, "vorder": False,
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
