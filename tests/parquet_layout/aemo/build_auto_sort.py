import os
import sys
import time

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402
import sort_key as sort_key_mod  # noqa: E402

# OPT_TABLE lets a caller aim this builder at its own slot (writer_ab.yml builds delta-rs and the
# DuckDB writer side by side); the default keeps parquet_layout.yml's behaviour unchanged.
TABLE = os.environ.get("OPT_TABLE") or "fct_summary_auto_sort"
QUALIFIED = f"tests.{TABLE}"

sort = sort_key_mod.requested()
clause = "sorted by auto" if sort.lower() == "auto" else f"sorted by ({sort})"
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
# OPT_RG: pin the row-group CEILING for this build (rows), e.g. 16000000 vs 6000000 — the harness
# spelling of the per-model `max_row_group_size` dbt config, for A/B-ing geometries in THIS CI.
# The CTAS goes through the connection API (no dbt config to carry it), so pin it at the one
# sizing seam every write reads at call time: engine._ROW_GROUP_SIZE (the module global behind
# _writer_properties). Same writer path (row_group_rows -> WriterProperties) the dbt config
# feeds; empty = the fixed 6M default.
_rg = os.environ.get("OPT_RG", "").strip()
OPT_RG = int(_rg) if _rg.isdigit() and int(_rg) > 0 else None
if OPT_RG is not None:
    from dbt.adapters.duckrun import engine as _engine
    _engine._ROW_GROUP_SIZE = OPT_RG
    print(f"OPT_RG: row-group ceiling pinned to {OPT_RG:,} rows for this build", flush=True)
# OPT_TFS_MB: pin the target parquet FILE size (MB) — the harness spelling of the
# `target_file_size_mb` model config, same seam (the module global every write reads at call
# time). 1024 with a ~765 MB table = a single file, matching the V-Order reference's file count.
# NOTE an OPT_RG past ~16M also raises arrow-rs's per-writer buffer (a full uncompressed row
# group); ~48M rows of this fact is ~2 GB on the runner — fine on 16 GB, mind it elsewhere.
_tfs = os.environ.get("OPT_TFS_MB", "").strip()
OPT_TFS_MB = int(_tfs) if _tfs.isdigit() and int(_tfs) > 0 else None
if OPT_TFS_MB is not None:
    from dbt.adapters.duckrun import engine as _engine
    _engine._TARGET_FILE_SIZE = OPT_TFS_MB * 1024 * 1024
    print(f"OPT_TFS_MB: target file size pinned to {OPT_TFS_MB} MB for this build", flush=True)
# OPT_PAGE_ROWS: pin the data-page ROW cap (default 1M since d0d23b4, see
# engine._DATA_PAGE_ROW_LIMIT) — the page-granularity A/B vs the V-Order reference, whose parquet-mr
# pages are ~1 MB / ~775k rows (~61 data pages per 47M-row chunk). At the old 20k cap duckrun made
# ~2,350 of them; at 1M the 1 MB byte limit binds instead and pages land at parquet-mr's shape.
# Lower it here to reproduce the old geometry. Same call-time module-global seam as OPT_TFS_MB.
_pr = os.environ.get("OPT_PAGE_ROWS", "").strip()
OPT_PAGE_ROWS = int(_pr) if _pr.isdigit() and int(_pr) > 0 else None
if OPT_PAGE_ROWS is not None:
    from dbt.adapters.duckrun import engine as _engine
    _engine._DATA_PAGE_ROW_LIMIT = OPT_PAGE_ROWS
    print(f"OPT_PAGE_ROWS: data-page row cap pinned to {OPT_PAGE_ROWS:,} rows for this build",
          flush=True)
# Read the source mart.fct_summary DIRECTLY (its own independent read, separate from the Spark
# V-Order build's) with the same row cap. SORTED BY AUTO re-sorts regardless of input order.
# Shared with the sort-key step so the key it shows is the key this builds.
_src = sort_key_mod.source_expr()
# OPT_COLUMNS narrows the table to a subset. The point is a MINIMAL reproducer: probing one column
# only transcodes that column, so a small table measures the same thing as the wide one for far
# less written bytes. Every sort column must stay in the projection — the `sorted by` clause sorts
# the CTAS result, so it cannot reference a column the select does not produce.
_COLUMNS = [c.strip() for c in (os.environ.get("OPT_COLUMNS") or "").split(",") if c.strip()]
_PROJECT = ", ".join(f'"{c}"' for c in _COLUMNS) if _COLUMNS else "*"

# This builder has no OPT_DERIVED. Failing loudly beats building a delta-rs table that is silently
# missing the derived columns the duckdb/pyarrow arms wrote — a cross-arm mismatch is precisely the
# failure writer_cold.yml already carries two warnings about (opt_compression and opt_page_bytes
# each reached one arm and not the other, and one of them invalidated a whole run unnoticed).
if (os.environ.get("OPT_DERIVED") or "").strip():
    raise SystemExit(
        "build_auto_sort: OPT_DERIVED is not supported by the delta-rs arm — it would build a table "
        "without the derived columns the other arms have, and the comparison would be silently "
        "unmatched. Run the type matrix with arms=duckdb (optionally +pyarrow), or add derivation "
        "here first.")

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"], read_only=False)
try:
    con.sql("create schema if not exists tests")
except Exception:
    con.con.execute("create schema if not exists tests")


def _exists():
    try:
        con.sql(f"select 1 from {QUALIFIED} limit 1").fetchone()
        return True
    except Exception:
        return False


_t0 = time.perf_counter()
sort_key = None
if not force and _exists():
    rows = con.sql(f"select count(*) from {QUALIFIED}").fetchone()[0]
    print(f"{QUALIFIED} already exists ({rows:,} rows) — skipping "
          "(rebuild=true to rebuild)", flush=True)
    status = "skipped"   # nothing was written, so no key was chosen — report null, not a guess
else:
    body = f"select {_PROJECT} from {_src}"
    sort_key = sort_key_mod.resolve(con, sort, body)
    print(f"sort key: {clause} -> {sort_key_mod.label(sort_key)}", flush=True)
    print(f"Building {QUALIFIED} with '{clause}' ...", flush=True)
    # Read mart.fct_summary directly (independent of the Spark V-Order build's read); SORTED BY AUTO
    # re-sorts regardless of the source's order.
    con.sql(f"create or replace table {QUALIFIED} {clause} as {body}")
    rows = con.sql(f"select count(*) from {QUALIFIED}").fetchone()[0]
    print(f"done — {QUALIFIED} built ({rows:,} rows)", flush=True)
    status = "rebuilt"

report.merge({"tables": {TABLE: {"build": {
    "engine": "delta_rs", "sort": clause, "sort_key": sort_key, "vorder": False,
    "row_group_ceiling": OPT_RG,     # None = the fixed 6M default
    "target_file_size_mb": OPT_TFS_MB,  # None = the 128 MB default
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
