"""Benchmark two Contoso semantic models by running the SAME heavy DAX queries against each over
the XMLA endpoint and timing them — to compare the `_auto_sort` model (a duckrun-clustered copy of
the Sales fact) against the `_vorder` model (a Fabric Spark V-Order copy). Both derive from the same
pristine contoso.sales base, so this is apples-to-apples: only the Delta layout differs, which
changes how much the Direct Lake engine transcodes (cold) and scans (hot).

Adapted from the AEMO benchmark's xmla_compare.py (tests/parquet_layout_tests/) — same XMLA engine
and cold/hot methodology, retargeted to the Contoso star (Sales/OrderRows/Orders + dims) and its
measures (Sales Amount, Total Cost, Margin, Margin %, Total Quantity — ported from SQLBI's pbit).

COLD is forced per query by DEHYDRATING the model first (clearValues then full reframe). See the
AEMO README for the full rationale. Uses ADOMD.NET; run headless (windows-latest).

Env in:
  PBI_WORKSPACE  — workspace display name
  PBI_TOKEN      — AAD token for https://analysis.windows.net/powerbi/api
  ADOMD_DIR      — folder with Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — HOT repetitions per query per model (default 5)
  COLD_REPEATS   — cold dehydrate→query cycles per cold-tier query (default 3)
  BENCH_COLD     — "true"/"false": measure cold via dehydrate (default true)
Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import json
import os
import statistics
import sys
import time
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

# Tiered DAX suite over the Contoso star. Each entry is (tier, name, dax).
#   probe      — one Sales column, full scan, scalar. probe_<Col> names match the raw parquet column
#                so render_report can line cold cost up with size/clustering. probe_rowcount is the
#                ~zero-column control (subtract in the marginal-cost analysis). Measured cold AND hot.
#   composite  — real SQLBI DAX Patterns (daxpatterns.com): YTD, year-over-year, cumulative total,
#                ranking, new customers, related distinct count — embedded self-contained via DEFINE
#                MEASURE, plus two Orders/OrderRows queries. Heavy SE scans. Measured cold AND hot.
#   hot_only   — selectivity ladder on the Sales sort-key columns, HOT only. "{brand}" filled at
#                runtime with the top Brand by Sales Amount.
# All columns/measures exist in model.bim (Sales: Quantity/Net Price/Unit Cost/ProductKey/Order Date;
# measures Sales Amount/Total Cost/Margin/Margin %/Total Quantity; Orders[Total Orders];
# OrderRows[OrderRows Amount/Quantity]; dims Product/Store/Customer/Date).
QUERIES = [
    # --- Tier 1: per-column cold probes on the Sales fact ---
    ("probe", "probe_Quantity",    'EVALUATE ROW("x", SUM(Sales[Quantity]))'),
    ("probe", "probe_NetPrice",    'EVALUATE ROW("x", SUM(Sales[Net Price]))'),
    ("probe", "probe_UnitCost",    'EVALUATE ROW("x", SUM(Sales[Unit Cost]))'),
    ("probe", "probe_ProductKey",  'EVALUATE ROW("x", DISTINCTCOUNT(Sales[ProductKey]))'),
    # OrderKey (model column "Order Number") is the adversarial case for dictionary-everywhere: it is
    # near-unique, so its dictionary costs the most bytes for the least value — if the pure-dictionary
    # policy is wrong anywhere it is wrong here first (DISTINCTCOUNT forces the full column scan).
    ("probe", "probe_OrderKey",    'EVALUATE ROW("x", DISTINCTCOUNT(Sales[Order Number]))'),
    ("probe", "probe_OrderDate",   'EVALUATE ROW("x", COUNTROWS(VALUES(Sales[Order Date])))'),
    ("probe", "probe_rowcount",    'EVALUATE ROW("x", COUNTROWS(Sales))'),
    # --- Tier 2: SQLBI DAX Patterns (https://www.daxpatterns.com), embedded as self-contained
    #     DEFINE MEASURE queries so each runs verbatim against this model with NO extra model measures.
    #     These are real analyst workloads, not toy group-bys — every one is a heavy storage-engine
    #     scan of the Sales fact, which is exactly what the duckrun-clustered vs V-Order layout
    #     comparison needs to move. Each is credited to its pattern page.
    ("composite", "ytd",  # https://www.daxpatterns.com/standard-time-related-calculations/
     '''DEFINE
         MEASURE Sales[Sales YTD] = CALCULATE ( [Sales Amount], DATESYTD ( 'Date'[Date] ) )
     EVALUATE
         SUMMARIZECOLUMNS ( 'Date'[Year Month], "YTD", [Sales YTD] )'''),
    ("composite", "year_over_year",  # https://www.daxpatterns.com/comparing-different-time-periods/
     '''DEFINE
         MEASURE Sales[Sales PY]  = CALCULATE ( [Sales Amount], DATEADD ( 'Date'[Date], -1, YEAR ) )
         MEASURE Sales[Sales YOY] = [Sales Amount] - [Sales PY]
     EVALUATE
         SUMMARIZECOLUMNS ( 'Date'[Year], "Sales", [Sales Amount], "PY", [Sales PY], "YOY", [Sales YOY] )'''),
    ("composite", "cumulative_total",  # https://www.daxpatterns.com/cumulative-total/
     '''DEFINE
         MEASURE Sales[Sales RT] =
             VAR LastVisibleDate = MAX ( 'Date'[Date] )
             RETURN CALCULATE ( [Sales Amount], 'Date'[Date] <= LastVisibleDate, ALL ( 'Date' ) )
     EVALUATE
         SUMMARIZECOLUMNS ( 'Date'[Year Month], "RunningTotal", [Sales RT] )'''),
    ("composite", "ranking",  # https://www.daxpatterns.com/ranking/
     '''DEFINE
         MEASURE Sales[Product Rank] = RANKX ( ALLSELECTED ( 'Product'[Product Name] ), [Sales Amount] )
     EVALUATE
         TOPN ( 100,
             SUMMARIZECOLUMNS ( 'Product'[Product Name], "Sales", [Sales Amount], "Rank", [Product Rank] ),
             [Sales], DESC )'''),
    ("composite", "new_customers",  # https://www.daxpatterns.com/new-and-returning-customers/
     '''DEFINE
         MEASURE Sales[First Purchase] = CALCULATE ( MIN ( Sales[Order Date] ), REMOVEFILTERS ( 'Date' ) )
         MEASURE Sales[New Customers] =
             VAR FirstDates  = ADDCOLUMNS ( VALUES ( Sales[CustomerKey] ), "@First", [First Purchase] )
             VAR MinVisible  = MIN ( 'Date'[Date] )
             VAR MaxVisible  = MAX ( 'Date'[Date] )
             RETURN COUNTROWS ( FILTER ( FirstDates, [@First] >= MinVisible && [@First] <= MaxVisible ) )
     EVALUATE
         SUMMARIZECOLUMNS ( 'Date'[Year Month], "NewCustomers", [New Customers] )'''),
    ("composite", "related_distinct_count",  # https://www.daxpatterns.com/related-distinct-count/
     '''EVALUATE
         SUMMARIZECOLUMNS ( 'Date'[Year],
             "Distinct Products",  DISTINCTCOUNT ( Sales[ProductKey] ),
             "Distinct Customers", DISTINCTCOUNT ( Sales[CustomerKey] ),
             "Distinct Stores",    DISTINCTCOUNT ( Sales[StoreKey] ) )'''),
    # --- Tier 2 (cont.): exercise the Orders + OrderRows facts too (the patterns above are Sales-only) ---
    ("composite", "orderrows_amount_x_year",
     '''EVALUATE SUMMARIZECOLUMNS ( 'Date'[Year],
         "ORAmount", [OrderRows Amount], "ORQty", [OrderRows Quantity] )'''),
    ("composite", "orders_count_x_country",
     '''EVALUATE SUMMARIZECOLUMNS ( 'Date'[Year], Store[Country], "Orders", [Total Orders] )'''),
    # --- Tier 3: hot-only selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(Sales, Sales[Quantity] * Sales[Net Price]), '
     '\'Date\'[Year] = 2015))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(Sales, Sales[Quantity] * Sales[Net Price]), '
     '\'Date\'[Year] = 2015, \'Date\'[Month Number] = 6))'),
    ("hot_only", "sel_1brand",
     'EVALUATE ROW("r", CALCULATE(SUMX(Sales, Sales[Quantity] * Sales[Net Price]), '
     'Product[Brand] = "{brand}"))'),
    ("hot_only", "sel_1brand_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(Sales, Sales[Quantity] * Sales[Net Price]), '
     'Product[Brand] = "{brand}", \'Date\'[Year] = 2015, \'Date\'[Month Number] = 6))'),
]

COLD_TIERS = ("probe", "composite")   # tiers that get the dehydrate→query cold path


def resolve_queries(top_brand):
    """Fill the "{brand}" placeholder in the hot_only ladder with the actual top Brand. If none was
    resolved, drop the brand-dependent ladder queries rather than run a broken filter."""
    out = []
    for tier, name, dax in QUERIES:
        if "{brand}" in dax:
            if not top_brand:
                continue
            dax = dax.replace("{brand}", top_brand)
        out.append((tier, name, dax))
    return out


def _load_adomd(adomd_dir: str):
    """Make Microsoft.AnalysisServices.AdomdClient importable via pythonnet."""
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


def open_conn(workspace: str, model: str, token: str, tries=5, delay=15):
    """Open an XMLA connection, retrying transient drops (SocketException 10054 under throttling)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    conn_str = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
        f"Initial Catalog={model};User ID=;Password={token};"
    )
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


def _refresh(conn, model, kind):
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    tmsl = json.dumps({"refresh": {"type": kind, "objects": [{"database": model}]}})
    AdomdCommand(tmsl, conn).ExecuteNonQuery()


def dehydrate_model(conn, model):
    """Evict all column data (clearValues) then reframe (full = metadata only on Direct Lake),
    leaving the model cold — the next query pays the full Delta->memory transcode cost."""
    for kind in ("clearValues", "full"):
        _refresh(conn, model, kind)


def warm_up(conn, model, tries=16, delay=30):
    """A freshly-deployed Direct Lake model can't read its OneLake source until security propagates.
    Reframe (full) + probe a trivial query, looping until it actually reads data. Returns True once
    queryable."""
    probe = 'EVALUATE ROW("n", COUNTROWS(Sales))'
    for i in range(1, tries + 1):
        try:
            _refresh(conn, model, "full")   # (re)frame Direct Lake against the current Delta
            run_query(conn, probe)          # confirm it can actually transcode/read the data
            print(f"  warm-up: queryable after {i} attempt(s)", flush=True)
            return True
        except Exception as e:
            print(f"  warm-up {i}/{tries}: not ready ({str(e).splitlines()[0][:110]}); "
                  f"waiting {delay}s...", flush=True)
            time.sleep(delay)
    print("  warm-up: model never became queryable — skipping it", flush=True)
    return False


def run_query(conn, dax: str):
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


def run_scalar(conn, dax):
    """Execute dax and return the first cell of the first row (or None)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    reader = AdomdCommand(dax, conn).ExecuteReader()
    try:
        if reader.Read() and reader.FieldCount:
            return reader.GetValue(0)
    finally:
        reader.Close()
    return None


def top_brand(conn):
    """The Brand with the largest Sales Amount — fills the hot_only selectivity ladder. Same data
    across layouts, so resolve once on the base model and reuse."""
    v = run_scalar(conn,
                   'EVALUATE TOPN(1, SUMMARIZECOLUMNS(Product[Brand], "m", [Sales Amount]), '
                   '[m], DESC)')
    return None if v is None else str(v)


def tie(b, m, b_spread_pct, m_spread_pct):
    """A per-query winner is a TIE when the relative gap is smaller than the larger of the two cold
    spreads — i.e. inside the measurement noise. Returns "base", "model", or "tie"."""
    if not b or not m:
        return "tie" if b == m else ("model" if (m or 0) < (b or 0) else "base")
    rel = abs(b - m) / max(b, m)
    noise = max((b_spread_pct or 0.0), (m_spread_pct or 0.0)) / 100.0
    if rel < noise:
        return "tie"
    return "model" if m < b else ("base" if m > b else "tie")


def bench_model(workspace, model, token, runs, want_cold, cold_repeats, queries):
    print(f"\n=== Benchmarking {model} (runs={runs}, cold={want_cold}) ===")
    conn = open_conn(workspace, model, token)
    if not warm_up(conn, model):
        conn.Close()
        return None, False
    can_cold = want_cold
    if want_cold:
        try:
            dehydrate_model(conn, model)
            print("  dehydrate: OK (per-query cold timing enabled)")
        except Exception as e:
            can_cold = False
            print(f"  dehydrate: unavailable ({str(e).splitlines()[0][:120]}) — hot timing only")
    results = {}
    try:
        for tier, name, dax in queries:
            do_cold = can_cold and tier in COLD_TIERS
            rowcount = None
            res = {"tier": tier}
            cold = []
            if do_cold:
                for _ in range(cold_repeats):
                    dehydrate_model(conn, model)
                    t, rows = run_query(conn, dax)
                    cold.append(t)
                    rowcount = rows
                res["cold_ms_all"] = cold
                res["cold_median_ms"] = statistics.median(cold)
                res["cold_min_ms"] = min(cold)
                lo, hi, med = min(cold), max(cold), statistics.median(cold)
                res["cold_spread_pct"] = 100.0 * (hi - lo) / med if med else 0.0
            hot_times = []
            for _ in range(runs):
                t, rows = run_query(conn, dax)
                hot_times.append(t)
                rowcount = rows
            hot = hot_times[2:] or hot_times[1:] or hot_times[:1]
            res["all_hot_ms"] = hot_times
            res["hot_avg_ms"] = sum(hot) / len(hot)      # continuity only — NOT used for verdicts
            res["hot_median_ms"] = statistics.median(hot)
            hlo, hhi, hmed = min(hot), max(hot), statistics.median(hot)
            res["hot_spread_pct"] = 100.0 * (hhi - hlo) / hmed if hmed else 0.0
            if tier == "hot_only":
                res["first_touch_ms"] = hot_times[0]     # first run = the data-skipping measurement
            res["rows"] = rowcount
            results[name] = res
            print(f"  [{tier}] {name}  (rows={rowcount})")
            if do_cold:
                cold_str = ", ".join(f"{c:.1f}" for c in cold)
                print(f"      cold x{cold_repeats}: [{cold_str}]  median={res['cold_median_ms']:.1f}ms"
                      f"  spread={res['cold_spread_pct']:.1f}%")
            hot_str = ", ".join(f"{h:.1f}" for h in hot_times)
            print(f"      hot   x{runs}: [{hot_str}]  median={res['hot_median_ms']:.1f}ms"
                  f"  spread={res['hot_spread_pct']:.1f}%")
    finally:
        conn.Close()
    return results, can_cold


def discover_models():
    here = Path(__file__).resolve().parent  # this script lives alongside the *.SemanticModel folders
    names = sorted(p.name.removesuffix(".SemanticModel")
                   for p in here.glob("*.SemanticModel"))
    if len(names) < 2:
        sys.exit(f"Need at least 2 benchmark semantic models, found {len(names)}: {names}")
    # Reference = auto_sort (duckrun); challenger = vorder. Ratio reads base/vorder.
    base = next((n for n in names if n.endswith("_auto_sort")), min(names, key=len))
    return base, [n for n in names if n != base]


def _write_timings(model, res):
    report.merge({"timings": {model: res}})


def _render_console(title, headers, rows, aligns, sep_before_last=False):
    """A boxed, aligned unicode table to stdout."""
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(str(c)))
    line = lambda l, m, rt: l + m.join("─" * (w + 2) for w in widths) + rt
    def frow(cells):
        parts = []
        for i, c in enumerate(cells):
            c = str(c)
            parts.append(" " + (c.rjust(widths[i]) if aligns[i] == "r" else c.ljust(widths[i])) + " ")
        return "│" + "│".join(parts) + "│"
    print(f"\n{title}")
    print(line("┌", "┬", "┐"))
    print(frow(headers))
    print(line("├", "┼", "┤"))
    body = rows[:-1] if sep_before_last else rows
    for r in body:
        print(frow(r))
    if sep_before_last:
        print(line("├", "┼", "┤"))
        print(frow(rows[-1]))
    print(line("└", "┴", "┘"))


def compare_table(title, base, model, base_res, opt_res, key, spread_key=None):
    base_tot = opt_tot = 0.0
    wins = 0
    counted = 0
    rows = []
    for name in base_res:  # base_res preserves query order; only queries present in BOTH, with key
        if name not in opt_res:
            continue
        b = base_res[name].get(key)
        o = opt_res[name].get(key)
        if b is None or o is None:
            continue
        bs = base_res[name].get(spread_key) if spread_key else None
        os_ = opt_res[name].get(spread_key) if spread_key else None
        w = tie(b, o, bs, os_)                 # "base" / "model" / "tie"
        base_tot += b
        opt_tot += o
        counted += 1
        wins += 1 if w == "model" else 0
        speedup = (b / o) if o else float("inf")
        rows.append((name, b, o, speedup, w))
    if not counted:
        return
    overall = (base_tot / opt_tot) if opt_tot else float("inf")
    total_w = "model" if opt_tot < base_tot else ("base" if opt_tot > base_tot else "tie")
    mshort = model.rsplit("_", 1)[-1]  # e.g. contoso_vorder -> "vorder"
    winner_lbl = {"model": model, "base": base, "tie": "tie"}
    factor = overall if overall >= 1 else (1.0 / overall if overall else 0.0)
    headline = (f"{winner_lbl[total_w]} is {factor:.2f}× faster overall"
                f" — {mshort} wins {wins}/{counted}")

    mark = {"model": f"{mshort} ✔", "base": "base ✔", "tie": "tie"}
    disp = [(n, f"{b:,.1f}", f"{o:,.1f}", f"{s:.2f}×", mark[w]) for (n, b, o, s, w) in rows]
    disp.append(("TOTAL", f"{base_tot:,.1f}", f"{opt_tot:,.1f}", f"{overall:.2f}×", mark[total_w]))
    _render_console(title, ("query", f"{base} (ms)", f"{model} (ms)", f"base/{mshort}", "winner"),
                    disp, ("l", "r", "r", "r", "l"), sep_before_last=True)
    print(f"  → {headline}")


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    token = os.environ["PBI_TOKEN"].strip()
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    runs = int(os.environ.get("BENCH_RUNS", "5"))
    cold_repeats = int(os.environ.get("COLD_REPEATS", "3"))
    want_cold = (os.environ.get("BENCH_COLD", "true").strip().lower() != "false")
    gap = int(os.environ.get("BENCH_GAP_SECONDS", "300"))  # idle gap between models (>CU smoothing)

    _load_adomd(adomd_dir)
    base, others = discover_models()
    print(f"Workspace : {workspace}")
    print(f"Base model: {base}")
    print(f"Compare   : {', '.join(others)}")
    print(f"Runs (hot): {runs}   Cold repeats: {cold_repeats}")

    # Resolve the top Brand once on the base model (same data across layouts) to fill the ladder.
    tb = None
    try:
        c = open_conn(workspace, base, token)
        if warm_up(c, base):
            tb = top_brand(c)
        c.Close()
    except Exception as e:
        print(f"  top Brand resolve failed ({str(e).splitlines()[0][:100]}) — dropping brand ladder.")
    print(f"Top Brand : {tb}")
    queries = resolve_queries(tb)

    base_res, base_cold = bench_model(workspace, base, token, runs, want_cold, cold_repeats, queries)
    if base_res is None:
        sys.exit(f"Base model {base!r} never became queryable — cannot benchmark.")
    _write_timings(base, base_res)

    for model in others:
        if gap:
            print(f"\n⏳ Idle gap: sleeping {gap}s before {model} so the Fabric capacity chart "
                  f"shows a clean base → gap → {model} separation...", flush=True)
            time.sleep(gap)
        try:
            opt_res, opt_cold = bench_model(workspace, model, token, runs, want_cold,
                                            cold_repeats, queries)
        except Exception as e:
            print(f"  {model}: benchmark failed ({str(e).splitlines()[0][:120]}) — skipping.",
                  flush=True)
            continue
        if opt_res is None:
            print(f"  {model} never became queryable — skipping its comparison.", flush=True)
            continue
        _write_timings(model, opt_res)
        if base_cold and opt_cold:
            compare_table(f"{model} vs {base}  —  COLD (median of {cold_repeats} dehydrate cycles)",
                          base, model, base_res, opt_res, "cold_median_ms", "cold_spread_pct")
        compare_table(f"{model} vs {base}  —  HOT (median of hot runs, dropping run1/run2 warm)",
                      base, model, base_res, opt_res, "hot_median_ms", "hot_spread_pct")


if __name__ == "__main__":
    main()
