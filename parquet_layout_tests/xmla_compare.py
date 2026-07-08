"""Benchmark two semantic models by running the SAME heavy DAX queries against each
over the XMLA endpoint and timing them — to compare the `_optimized` model (a duckrun-
clustered copy of the fact) against the `_vorder` model (a Fabric Spark V-Order copy).
Both are derived from the same pristine base fct_summary, so this is apples-to-apples.

NOT a correctness check — both models read the same data, so the numbers are identical
by construction. What differs is the Delta layout, which changes how much the Direct Lake
engine has to transcode (cold) and scan (hot). We measure both as query wall-clock.

COLD is forced per query by DEHYDRATING the model first: a TMSL `clearValues` refresh evicts
all transcoded column data from memory, then a `full` refresh reframes (on Direct Lake that's
metadata only — no transcode), so the next query pays the full cold Delta→memory cost. We
dehydrate before EACH query because the queries share the big fact columns (mw/price/DUID/
date/time) — without a per-query dehydrate only the first query would be cold.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless (GitHub Actions, windows-latest) — see .github/workflows/parquet_layout.yml.

Env in:
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — AAD access token for https://analysis.windows.net/powerbi/api
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — HOT repetitions per query per model (default 5); run1/run2 dropped as warm.
  COLD_REPEATS   — cold dehydrate→query cycles per cold-tier query (default 3); we report the
                   median + spread over these, so a single cold sample no longer decides a winner.
  BENCH_COLD     — "true"/"false": measure cold via dehydrate (default true). Falls back to
                   hot-only automatically if the token can't run the refresh (needs write).

Cold is a black box probed only by wall-clock: dehydrate (clearValues+full) forces a full
Delta→memory transcode on the next query, so COLD_REPEATS cycles give a small distribution
instead of an n=1 point. Queries are tiered — see QUERIES below.

Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import json
import os
import statistics
import sys
import time
from pathlib import Path

# Windows CI console defaults to cp1252, which can't encode the emoji/glyphs — force UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

# Tiered DAX suite. Each entry is (tier, name, dax). Adding a query = adding a tuple.
#   probe      — one column, full scan, scalar result. Cold time ≈ that column's transcode cost +
#                fixed overhead; probe_rowcount is the ~zero-column control (subtract it in P3 to
#                get the marginal per-column cost). Measured cold (COLD_REPEATS×) AND hot.
#   composite  — realistic multi-column workloads, also measured cold AND hot.
#   hot_only   — selectivity ladder on the sort-key column, measured HOT only (segment/row-group
#                elimination is only visible once resident — cold is dominated by full-column
#                transcode). "{duid}" is filled at runtime with the top DUID by MWh.
# All columns/measures exist in model.bim (fct_summary: date,time,DUID,mw,price,cutoff;
# measures Total MW, Total MWh, Avg Price, Generator Count; dim_duid, dim_calendar).
QUERIES = [
    # --- Tier 1: per-column cold probes ---
    ("probe", "probe_mw",       'EVALUATE ROW("x", SUM(fct_summary[mw]))'),
    ("probe", "probe_price",    'EVALUATE ROW("x", SUM(fct_summary[price]))'),
    ("probe", "probe_duid",     'EVALUATE ROW("x", DISTINCTCOUNT(fct_summary[DUID]))'),
    ("probe", "probe_date",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[date])))'),
    ("probe", "probe_time",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[time])))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(fct_summary))'),
    # --- Tier 2: composite workloads (the original 7) ---
    ("composite", "region_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MWh", [Total MWh], "AvgP", [Avg Price], "Gens", [Generator Count])'),
    ("composite", "fuel_x_region",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[FuelSourceDescriptor], dim_duid[Region], '
     '"MWh", [Total MWh], "MW", [Total MW])'),
    ("composite", "timeofday_x_region",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[time], dim_duid[Region], '
     '"MWh", [Total MWh], "AvgP", [Avg Price])'),
    ("composite", "duid_x_month",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], dim_calendar[month], '
     '"MWh", [Total MWh])'),
    ("composite", "filtered_nsw_2024_by_duid",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_summary[DUID], "MWh", [Total MWh], "AvgP", [Avg Price]), '
     'dim_duid[Region] = "NSW1", dim_calendar[year] = 2024)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"RevenueProxy", SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     '"DistinctDUID", DISTINCTCOUNT(fct_summary[DUID]), '
     '"Rows", COUNTROWS(fct_summary))'),
    ("composite", "topn_duid_by_mwh",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], '
     '"MWh", [Total MWh]), [MWh], DESC)'),
    # --- Tier 2 (cont.): column-width at fixed shape (cold scaling with touched columns) ---
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh], "b", [Avg Price], '
     '"c", [Total MW], "d", [Generator Count])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh])'),
    # --- Tier 3: hot-only selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
    ("hot_only", "sel_1duid",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = "{duid}"))'),
    ("hot_only", "sel_1duid_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = "{duid}", dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
]

COLD_TIERS = ("probe", "composite")   # tiers that get the dehydrate→query cold path


def resolve_queries(top_duid):
    """Fill the "{duid}" placeholder in the hot_only ladder with the actual top DUID. If no top
    DUID could be resolved, drop the DUID-dependent ladder queries rather than run a broken filter."""
    out = []
    for tier, name, dax in QUERIES:
        if "{duid}" in dax:
            if not top_duid:
                continue
            dax = dax.replace("{duid}", top_duid)
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
    """Open an XMLA connection, retrying transient drops. The XMLA endpoint can forcibly close an
    idle connection (SocketException 10054) — especially after the idle gap or under capacity
    throttling — so one blip shouldn't kill the run."""
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
    """A freshly-deployed Direct Lake model can't read its OneLake source until security
    propagates — the first refresh/query fails with 'source tables ... do not exist or access
    was denied'. Reframe (full) + probe a trivial query, looping until it actually reads data
    (or we give up). Returns True once queryable."""
    probe = 'EVALUATE ROW("n", COUNTROWS(fct_summary))'
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


def top_duid(conn):
    """The DUID with the largest Total MWh — used to fill the hot_only selectivity ladder.
    Same underlying data across layouts, so resolve once on the base model and reuse."""
    v = run_scalar(conn,
                   'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_summary[DUID], "m", [Total MWh]), '
                   '[m], DESC)')
    return None if v is None else str(v)


def tie(b, m, b_spread_pct, m_spread_pct):
    """A per-query winner is a TIE when the relative gap between the two times is smaller than
    the larger of their cold spreads — i.e. the difference is inside the measurement noise.
    Returns "base", "model", or "tie". b_spread_pct/m_spread_pct may be None (hot: no spread)."""
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
            # COLD: dehydrate → query, COLD_REPEATS times, so cold is a small distribution (median +
            # spread) rather than an n=1 point. Each cycle pays the full Delta→memory transcode.
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
            # HOT: run WITHOUT dehydrating; drop run1/run2 as the warm transition, average the rest.
            hot_times = []
            for _ in range(runs):
                t, rows = run_query(conn, dax)
                hot_times.append(t)
                rowcount = rows
            hot = hot_times[2:] or hot_times[1:] or hot_times[:1]
            res["all_hot_ms"] = hot_times
            res["hot_avg_ms"] = sum(hot) / len(hot)
            res["rows"] = rowcount
            results[name] = res
            # Console trace.
            print(f"  [{tier}] {name}  (rows={rowcount})")
            if do_cold:
                cold_str = ", ".join(f"{c:.1f}" for c in cold)
                print(f"      cold x{cold_repeats}: [{cold_str}]  median={res['cold_median_ms']:.1f}ms"
                      f"  spread={res['cold_spread_pct']:.1f}%")
            hot_str = ", ".join(f"{h:.1f}" for h in hot_times)
            print(f"      hot   x{runs}: [{hot_str}]  hot_avg={res['hot_avg_ms']:.1f}ms")
    finally:
        conn.Close()
    return results, can_cold


def discover_models():
    here = Path(__file__).resolve().parent  # this script lives alongside the *.SemanticModel folders
    # Benchmark ONLY the experiment's derived models: optimized (duckrun-clustered) vs vorder
    # (Fabric Spark V-Order). Both are built from the same pristine base fct_summary, so this is
    # apples-to-apples.
    names = sorted(p.name.removesuffix(".SemanticModel")
                   for p in here.glob("*.SemanticModel"))
    if len(names) < 2:
        sys.exit(f"Need at least 2 benchmark semantic models, found {len(names)}: {names}")
    # Reference = optimized (duckrun); challenger = vorder. Ratio reads base/vorder.
    base = next((n for n in names if n.endswith("_optimized")), min(names, key=len))
    return base, [n for n in names if n != base]


def _write_timings(model, res):
    # res is already keyed by query with the final report keys (tier, rows, cold_ms_all,
    # cold_median_ms, cold_min_ms, cold_spread_pct, all_hot_ms, hot_avg_ms) — merge as-is.
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
    mshort = model.rsplit("_", 1)[-1]  # e.g. aemo_electricity_vorder -> "vorder"
    winner_lbl = {"model": model, "base": base, "tie": "tie"}
    factor = overall if overall >= 1 else (1.0 / overall if overall else 0.0)
    headline = (f"{winner_lbl[total_w]} is {factor:.2f}× faster overall"
                f" — {mshort} wins {wins}/{counted}")

    # ---- boxed console table ----
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

    # Resolve the top DUID once on the base model (same data across layouts) to fill the ladder.
    td = None
    try:
        c = open_conn(workspace, base, token)
        if warm_up(c, base):
            td = top_duid(c)
        c.Close()
    except Exception as e:
        print(f"  top DUID resolve failed ({str(e).splitlines()[0][:100]}) — dropping DUID ladder.")
    print(f"Top DUID  : {td}")
    queries = resolve_queries(td)

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
        compare_table(f"{model} vs {base}  —  HOT (avg of hot runs, dropping run1/run2 warm)",
                      base, model, base_res, opt_res, "hot_avg_ms")


if __name__ == "__main__":
    main()
