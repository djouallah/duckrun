"""Resolve the sort key `SORTED BY AUTO` would choose — and, run as a script, just print it.

Split out of build_auto_sort.py so the workflow can show the key as its own cheap STEP. The key is
the single decision the whole layout comparison exists to interrogate, and reading it should not
cost a table rebuild, two semantic-model deploys and a DAX suite. As a step this is one profiling
pass over the source (a seeded 100k-row reservoir plus a few HLL scans), so it is affordable on
every dispatch even when `dax=false` turns the measurement half off.

Resolves with the same two calls `session._resolve_auto_sort` makes, so what is printed is what a
build would apply — sound because the profiling sample is seeded (#48); before that, two resolves of
one table could legitimately disagree and this script would have been reporting a coin flip.

It deliberately does NOT let a caller substitute an explicit `sorted by (<cols>)` for `auto` in the
CTAS: `_resolve_auto_sort` also runs `_narrow_wide_decimals` and returns early for any non-AUTO
sort, so pinning the columns would silently drop the wide-DECIMAL narrowing (~1 GB and a 10x cold
cliff on Contoso's price columns). Hence resolve-and-report rather than resolve-and-pin.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

TABLE = "fct_summary_auto_sort"


def source_expr():
    """The FROM expression the build reads — `mart.fct_summary`, capped by BENCH_ROW_LIMIT.

    A bare table name profiles from the Delta LOG (exact row width, and exact when the table fits
    the byte budget); the `limit` form is a subquery, so it falls back to sampling the result
    relation. Kept identical to build_auto_sort.py's so the key shown is the key built."""
    lim = (os.environ.get("BENCH_ROW_LIMIT") or "").strip()
    n = int(lim) if lim.isdigit() and int(lim) > 0 else None
    return "mart.fct_summary" if n is None else f"(select * from mart.fct_summary limit {n})"


def requested():
    """The dispatch's OPT_SORT: 'auto' (let the picker choose) or an explicit column list."""
    return (os.environ.get("OPT_SORT") or "auto").strip()


def resolve(con, sort, body):
    """The COLUMNS `sort` resolves to for `body`: the picker's choice when 'auto', else the
    caller's own list. `[]` means the picker found nothing worth sorting by."""
    if sort.lower() != "auto":
        return [c.strip() for c in sort.split(",") if c.strip()]
    tbl = con._auto_sort_single_table(body)
    return (con._auto_sort_cols_from_table(tbl) if tbl is not None
            else con._auto_sort_cols(con.con.sql(body)))


def label(key):
    """Human form for a resolved key, distinguishing 'declined' from 'not resolved'."""
    if key is None:
        return "(not resolved)"
    return ", ".join(key) if key else "(no sort — nothing pays off)"


def main():
    import duckrun
    sort = requested()
    body = f"select * from {source_expr()}"
    # NOT read_only: profiling materialises a `_rle_src` TEMP table, and this matches the
    # connection build_auto_sort.py profiles with, so the key shown is the key a build would apply.
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"], read_only=False)
    key = resolve(con, sort, body)
    line = f"{'SORTED BY AUTO' if sort.lower() == 'auto' else f'sorted by ({sort})'} -> {label(key)}"
    print(f"\nsort key: {line}", flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### Sort key\n\n`{TABLE}` — {line}\n\n")
    # Recorded even when no build runs, so a dax=false dispatch still leaves the decision on record.
    report.merge({"tables": {TABLE: {"build": {"sort_key_preview": key}}}})


if __name__ == "__main__":
    main()
