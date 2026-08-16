"""Build the duckrun-clustered layout copy of the Contoso Sales fact under test:
tests.sales_auto_sort = the raw generator sales.parquet, written `sorted by auto` (current
WriterProperties). Mirror of the AEMO benchmark's build_auto_sort.py, retargeted to the Contoso base.

Reads the RAW generator sales.parquet straight from the lakehouse Files section (uploaded by
build_base.py) — the identical input Spark's V-Order build reads — so neither engine's layout seeds
the other. SORTED BY AUTO re-sorts regardless of input order.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, OPT_SORT (default 'auto'), FORCE_REBUILD,
        BENCH_ROW_LIMIT (optional row cap on the shared base).
"""
import os
import sys
import time

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402
import build_base  # noqa: E402  — sales_files_urls: one source of truth for the Files path

sort = (os.environ.get("OPT_SORT") or "auto").strip()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
# Read the raw sales.parquet from Files (abfss) with the same row cap Spark applies.
_ABFSS_URL, _ = build_base.sales_files_urls()
_base = f"read_parquet('{_ABFSS_URL}')"
_src = _base if N is None else f"(select * from {_base} limit {N})"

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"], read_only=False)
con.sql("create schema if not exists tests")


def _exists():
    try:
        con.sql("select 1 from tests.sales_auto_sort limit 1").fetchone()
        return True
    except Exception:
        return False


def resolved_sort_key(body):
    """The COLUMNS `clause` resolves to — see the AEMO build_auto_sort.py this mirrors for why the
    report needs the key and not just the string "sorted by auto", and why this resolves the key
    separately instead of substituting an explicit `sorted by (<cols>)` into the CTAS (doing that
    would skip `_narrow_wide_decimals`, which is worth ~1 GB and a 10x cold cliff on exactly this
    benchmark's price columns)."""
    if sort.lower() != "auto":
        return [c.strip() for c in sort.split(",") if c.strip()]
    tbl = con._auto_sort_single_table(body)
    if tbl is not None:
        return con._auto_sort_cols_from_table(tbl)
    # A derived body is profiled from a STAGED copy: the picker reads its source once and every
    # pass after that is local. `_auto_sort_cols` therefore takes that table's NAME, not a
    # relation — mirror what `session._resolve_auto_sort` does around it.
    staged = "_bench_auto_src"
    con.con.execute(f"CREATE OR REPLACE TEMP TABLE {staged} AS {body}")
    try:
        return con._auto_sort_cols(staged)
    finally:
        con.con.execute(f"DROP TABLE IF EXISTS {staged}")


_t0 = time.perf_counter()
sort_key = None
if not force and _exists():
    rows = con.sql("select count(*) from tests.sales_auto_sort").fetchone()[0]
    print(f"tests.sales_auto_sort already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
    status = "skipped"   # nothing was written, so no key was chosen — report null, not a guess
else:
    body = f"select * from {_src}"
    sort_key = resolved_sort_key(body)
    print(f"sort key: {clause} -> {sort_key or '(no sort — nothing pays off)'}", flush=True)
    print(f"Building tests.sales_auto_sort with '{clause}' ...", flush=True)
    con.sql(f"create or replace table tests.sales_auto_sort {clause} as {body}")
    rows = con.sql("select count(*) from tests.sales_auto_sort").fetchone()[0]
    print(f"done — tests.sales_auto_sort built ({rows:,} rows)", flush=True)
    status = "rebuilt"

report.merge({"tables": {"sales_auto_sort": {"build": {
    "engine": "delta_rs", "sort": clause, "sort_key": sort_key, "vorder": False,
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
