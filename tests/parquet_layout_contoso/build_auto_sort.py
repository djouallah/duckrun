"""Build the duckrun-clustered layout copy of the Contoso Sales fact under test:
tests.sales_auto_sort = contoso.sales, written `sorted by auto` (current WriterProperties).
Mirror of the AEMO benchmark's build_auto_sort.py, retargeted to the Contoso base.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, OPT_SORT (default 'auto'), FORCE_REBUILD,
        BENCH_ROW_LIMIT (optional row cap on the shared base).
"""
import os
import sys
import time

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
# Read contoso.sales directly with the same row cap. SORTED BY AUTO re-sorts regardless of input order.
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
_src = "contoso.sales" if N is None else f"(select * from contoso.sales limit {N})"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"],
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]},
                      read_only=False)
try:
    con.sql("create schema if not exists tests")
except Exception:
    con.con.execute("create schema if not exists tests")


def _exists():
    try:
        con.sql("select 1 from tests.sales_auto_sort limit 1").fetchone()
        return True
    except Exception:
        return False


_t0 = time.perf_counter()
if not force and _exists():
    rows = con.sql("select count(*) from tests.sales_auto_sort").fetchone()[0]
    print(f"tests.sales_auto_sort already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
    status = "skipped"
else:
    print(f"Building tests.sales_auto_sort with '{clause}' ...", flush=True)
    con.sql(f"create or replace table tests.sales_auto_sort {clause} "
            f"as select * from {_src}")
    rows = con.sql("select count(*) from tests.sales_auto_sort").fetchone()[0]
    print(f"done — tests.sales_auto_sort built ({rows:,} rows)", flush=True)
    status = "rebuilt"

report.merge({"tables": {"sales_auto_sort": {"build": {
    "engine": "delta_rs", "sort": clause, "vorder": False,
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
