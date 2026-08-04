import os
import sys
import time

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
# OPT_RG: pin the row-group CEILING for this build (rows), e.g. 16000000 vs 8000000 — the harness
# spelling of the per-model `max_row_group_size` dbt config, for A/B-ing geometries in THIS CI.
# The CTAS goes through the connection API (no dbt config to carry it), so pin it at the one
# documented sizing seam every overwrite flows through: engine.rg_for. Same writer path
# (row_group_rows -> WriterProperties) the dbt config feeds; empty = the adaptive default.
_rg = os.environ.get("OPT_RG", "").strip()
OPT_RG = int(_rg) if _rg.isdigit() and int(_rg) > 0 else None
if OPT_RG is not None:
    from dbt.adapters.duckrun import engine as _engine
    _engine.rg_for = lambda est, floor=None, _v=OPT_RG: _v
    print(f"OPT_RG: row-group ceiling pinned to {OPT_RG:,} rows for this build", flush=True)
# Read the source mart.fct_summary DIRECTLY (its own independent read, separate from the Spark
# V-Order build's) with the same row cap. SORTED BY AUTO re-sorts regardless of input order.
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
_src = "mart.fct_summary" if N is None else f"(select * from mart.fct_summary limit {N})"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"], read_only=False)
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
    "row_group_ceiling": OPT_RG,   # None = adaptive (the default sizing)
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
