import os

import duckrun

force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
# Row cap for the whole benchmark (default 142M ≈ full table). Every layout variant derives from the
# ONE limited base built here, so all three hold the SAME rows at any size — the comparison stays
# apples-to-apples when we shrink the table to probe small-table behaviour.
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


def _exists(t):
    try:
        con.sql(f"select 1 from tests.{t} limit 1").fetchone()
        return True
    except Exception:
        return False


if not force and _exists("summary_unsorted") and _exists("summary_sorted"):
    rows = con.sql("select count(*) from tests.summary_unsorted").fetchone()[0]
    print(f"tests.summary_unsorted/_sorted already exist ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
else:
    print(f"Building base tables (limit={N or 'none (full)'}) ...", flush=True)
    # summary_unsorted: the shuffled base of N rows.
    con.sql(f"create or replace table tests.summary_unsorted as select * from {_src} order by random()")
    # summary_sorted: the SAME rows in natural (date, time) order — the sorted-input base for V-Order.
    # date/time are DuckDB keywords → quote them.
    con.sql('create or replace table tests.summary_sorted '
            'as select * from tests.summary_unsorted order by "date", "time"')
    rows = con.sql("select count(*) from tests.summary_unsorted").fetchone()[0]
    print(f"done — tests.summary_unsorted + tests.summary_sorted built ({rows:,} rows each)", flush=True)
