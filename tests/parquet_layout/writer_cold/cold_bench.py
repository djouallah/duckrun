"""Cold-only Direct Lake measurement: one pass, one query per column, nothing cached.

WHAT COLD MEANS HERE. The model was deployed minutes ago and has never been queried, so its column
store is empty. Each query below is the first and only touch of its column, which makes its duration
the Delta->memory transcode cost of that column plus fixed overhead. There is deliberately no
`clearValues` dehydrate: forcing a re-transcode before every query is not what a user does, and the
fresh-model approach is the only non-destructive way to guarantee an empty store. There is also no
warm or hot pass — the question is exclusively "is this parquet good to read cold".

Because every query must touch a column nothing has touched yet, ORDER MATTERS and the suite must
not reuse columns. `probe_rowcount` runs last as the ~zero-column control: subtract it to get the
marginal per-column cost.

`warm_up` is NOT a warm measurement — it is the security-propagation guard. A freshly deployed
Direct Lake model cannot read OneLake for ~5 min. It uses COUNTROWS only, which reads no column
data, so it does not warm any column the suite later measures.

Env: PBI_WORKSPACE (display name), ADOMD_DIR, MODEL (catalog), VARIANT (label), RUN_REPORT.
"""
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "aemo"))
import report  # noqa: E402

# One column per query, cheapest aggregate that still forces a full scan of that column.
# probe_rowcount LAST: it is the control, and it must not warm anything.
QUERIES = [
    ("probe_mw",       'EVALUATE ROW("v", SUM(fct_summary[mw]))'),
    ("probe_price",    'EVALUATE ROW("v", SUM(fct_summary[price]))'),
    ("probe_duid",     'EVALUATE ROW("v", DISTINCTCOUNT(fct_summary[DUID]))'),
    ("probe_date",     'EVALUATE ROW("v", MIN(fct_summary[date]))'),
    ("probe_time",     'EVALUATE ROW("v", MAX(fct_summary[time]))'),
    ("probe_rowcount", 'EVALUATE ROW("v", COUNTROWS(fct_summary))'),
]


def _load_adomd(adomd_dir):
    import clr  # pythonnet
    hits = glob.glob(os.path.join(adomd_dir, "**", "Microsoft.AnalysisServices.AdomdClient.dll"),
                     recursive=True)
    if not hits:
        sys.exit(f"ADOMD client DLL not found under {adomd_dir!r}")
    hits.sort(key=lambda p: ("netcore" not in p.lower() and "net6" not in p.lower(), len(p)))
    d = os.path.dirname(hits[0])
    if d not in sys.path:
        sys.path.append(d)
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    print(f"Loaded ADOMD from {hits[0]}")


def open_conn(workspace, model, token, tries=5, delay=15):
    """Open an XMLA connection, retrying transient drops (SocketException 10054)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    conn_str = (f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
                f"Initial Catalog={model};User ID=;Password={token};")
    last = None
    for i in range(1, tries + 1):
        try:
            conn = AdomdConnection(conn_str)
            conn.Open()
            return conn
        except Exception as e:
            last = e
            print(f"  open_conn {i}/{tries} failed ({str(e).splitlines()[0][:100]}); "
                  f"retrying in {delay}s...", flush=True)
            time.sleep(delay)
    raise last


# PROBE_COLUMNS restricts the suite to columns the table actually has, so a narrowed reproducer
# does not fail on a probe for a column that was never written. probe_rowcount always survives —
# it is the control every marginal figure is measured against.
# Case-folded: the probes are named for the column in lower case (probe_duid) while the column
# itself is DUID, and an exact match silently leaves nothing to run.
_WANT = {c.strip().casefold() for c in (os.environ.get("PROBE_COLUMNS") or "").split(",")
         if c.strip()}
if _WANT:
    QUERIES = [q for q in QUERIES
               if q[0] == "probe_rowcount" or q[0][len("probe_"):].casefold() in _WANT]
    if len(QUERIES) < 2:
        raise SystemExit(f"cold_bench: PROBE_COLUMNS {_WANT} left no column probe to run")


def run_query(conn, dax):
    """Execute dax, drain all rows, return (elapsed_ms, row_count)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    t0 = time.perf_counter()
    reader = AdomdCommand(dax, conn).ExecuteReader()
    rows = 0
    try:
        fc = reader.FieldCount
        while reader.Read():
            for i in range(fc):
                reader.GetValue(i)
            rows += 1
    finally:
        reader.Close()
    return (time.perf_counter() - t0) * 1000.0, rows


def warm_up(conn, model, tries=16, delay=30):
    """Security-propagation guard, NOT a warm pass: a fresh Direct Lake model cannot read OneLake
    for ~5 min. COUNTROWS reads no column data, so nothing the suite measures gets warmed."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    for i in range(1, tries + 1):
        try:
            tmsl = json.dumps({"refresh": {"type": "full", "objects": [{"database": model}]}})
            AdomdCommand(tmsl, conn).ExecuteNonQuery()
            run_query(conn, 'EVALUATE ROW("n", COUNTROWS(fct_summary))')
            print(f"  ready after {i} attempt(s)", flush=True)
            return True
        except Exception as e:
            print(f"  not ready {i}/{tries} ({str(e).splitlines()[0][:110]}); waiting {delay}s...",
                  flush=True)
            time.sleep(delay)
    return False


def main():
    workspace = os.environ["PBI_WORKSPACE"]
    model = os.environ["MODEL"]
    variant = os.environ.get("VARIANT") or model
    _load_adomd(os.environ["ADOMD_DIR"])

    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()
    conn = open_conn(workspace, model, token)
    try:
        if not warm_up(conn, model):
            raise SystemExit(f"{model} never became queryable — cannot measure cold")

        results = []
        print(f"\n=== COLD pass: {variant} ({model}) ===", flush=True)
        for name, dax in QUERIES:
            ms, rows = run_query(conn, dax)
            results.append({"query": name, "cold_ms": round(ms, 1), "rows": rows})
            print(f"  {name:<16} {ms:>9.1f} ms", flush=True)
    finally:
        conn.Close()

    base = next((r["cold_ms"] for r in results if r["query"] == "probe_rowcount"), 0.0)
    for r in results:
        r["marginal_ms"] = round(r["cold_ms"] - base, 1)
    total = round(sum(r["cold_ms"] for r in results), 1)
    print(f"  {'TOTAL':<16} {total:>9.1f} ms   (control={base} ms)", flush=True)

    report.merge({"cold": {variant: {
        "model": model, "queries": results, "total_cold_ms": total, "control_ms": base,
    }}})


if __name__ == "__main__":
    main()
