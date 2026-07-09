"""Render run_report.json to the GitHub job summary AND compute the derived `analysis` block,
which is merged back into the same file. Pure post-processing — everything here is recomputable
offline from the raw values already in run_report.json (cold samples, hot runs, per-column-chunk
parquet_metadata). Layout in, wall-clock out; no engine internals.

Env in: RUN_REPORT (the one JSON), GITHUB_STEP_SUMMARY (optional; also prints to stdout).
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

_PROBE_COLS = ["mw", "price", "duid", "date", "time"]     # probe_<col> minus probe_rowcount
_FILTER_COLS = ["date", "time", "DUID"]                   # clustering candidates
_RG_BIG = 1 << 24                                          # 16,777,216 rows — Direct Lake segment cap
# hot_only ladder query -> the fact column(s) its predicate prunes on (year/month filter via
# dim_calendar reaches the fact `date`; DUID filter is direct).
_LADDER_COLS = {"sel_1yr": ["date"], "sel_1mo": ["date"],
                "sel_1duid": ["DUID"], "sel_1duid_1mo": ["DUID", "date"]}


def _write(md):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")
    print(md)


def _fmt(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return f"{v:,.1f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


def _table(model):
    """Physical table name for a semantic-model name: fct_summary + the model's suffix."""
    return "fct_summary" + model.removeprefix("aemo_electricity")


# Display labels. Physical names stay as-is (the base-detection keys off "_optimized"); these are
# presentation only. "optimized" is renamed to "auto_sort" because it presumes the conclusion —
# the layout is duckrun's SORTED BY AUTO, nothing more. "vorder" already reads cleanly, so it maps
# to itself. Keys below must match the physical table/model name tokens.
_LABELS = {"optimized": "auto_sort"}


def _short(model):
    s = model.removeprefix("aemo_electricity_").removeprefix("fct_summary_") or model
    return _LABELS.get(s, s)


# ---------------------------------------------------------------------------- derived analysis

def _bounds(chunk):
    """(min, max) for a column-chunk. DuckDB puts current stats in stats_min_value/stats_max_value
    (string columns like DUID have ONLY these — stats_min/stats_max are null); fall back to the
    legacy stats_min/stats_max for anything that only has those."""
    lo = chunk.get("stats_min_value")
    if lo is None:
        lo = chunk.get("stats_min")
    hi = chunk.get("stats_max_value")
    if hi is None:
        hi = chunk.get("stats_max")
    return lo, hi


def _coerce_intervals(chunks, col):
    """[(min, max)] for column `col` across its column-chunks, coerced to a comparable type
    (float if every bound parses as a number, else str — strings compare lexicographically)."""
    raw = []
    for c in chunks:
        if c.get("path_in_schema") != col:
            continue
        lo, hi = _bounds(c)
        if lo is None or hi is None:
            continue
        raw.append((lo, hi))
    if not raw:
        return []
    flat = [v for pair in raw for v in pair]
    try:
        [float(v) for v in flat]
        return [(float(a), float(b)) for a, b in raw]
    except (TypeError, ValueError):
        return [(str(a), str(b)) for a, b in raw]


def _overlap_score(intervals):
    """0 = perfectly clustered (no two row groups overlap on this column), ~1 = fully interleaved.
    Mean over row groups of the fraction of OTHER row groups whose [min,max] overlaps this one."""
    n = len(intervals)
    if n < 2:
        return 0.0
    tot = 0.0
    for i, (a0, a1) in enumerate(intervals):
        c = sum(1 for j, (b0, b1) in enumerate(intervals)
                if j != i and a0 <= b1 and b0 <= a1)
        tot += c / (n - 1)
    return round(tot / n, 4)


def _tie(b, m, b_spread_pct, m_spread_pct):
    """Winner of base vs model with the noise/tie rule (same logic as xmla_compare.tie)."""
    if not b or not m:
        return "tie" if b == m else ("model" if (m or 0) < (b or 0) else "base")
    rel = abs(b - m) / max(b, m)
    noise = max((b_spread_pct or 0.0), (m_spread_pct or 0.0)) / 100.0
    if rel < noise:
        return "tie"
    return "model" if m < b else ("base" if m > b else "tie")


def _agg_verdict(base_t, model_t, key, spread_key):
    """Aggregate base-vs-model verdict for one metric, tie rule applied per query. If no query
    separates the two outside its spread, the verdict is 'no measurable difference' — that is a
    finding, not a win."""
    bt = mt = 0.0
    per = []
    for q, mv in model_t.items():
        bv = base_t.get(q, {})
        b, x = bv.get(key), mv.get(key)
        if b is None or x is None:
            continue
        per.append(_tie(b, x, bv.get(spread_key) if spread_key else None,
                        mv.get(spread_key) if spread_key else None))
        bt += b
        mt += x
    if not per:
        return None
    wins, losses, ties = per.count("model"), per.count("base"), per.count("tie")
    ratio = (bt / mt) if mt else float("inf")
    if wins == 0 and losses == 0:
        verdict, text = "tie", "no measurable difference (all deltas within spread)"
    else:
        verdict = "model" if mt < bt else "base"
        fac = ratio if ratio >= 1 else (1 / ratio if ratio else 0)
        text = f"{verdict} {fac:.2f}× faster overall (W/L/T {wins}/{losses}/{ties})"
    return {"base_total_ms": round(bt, 1), "model_total_ms": round(mt, 1),
            "ratio": round(ratio, 3), "wins": wins, "losses": losses, "ties": ties,
            "verdict": verdict, "text": text}


def compute_analysis(rep):
    timings = rep.get("timings", {})
    tables = rep.get("tables", {})
    models = list(timings)
    analysis = {"cold_column_cost": {}, "cold_vs_geometry": {}, "size_attribution": {},
                "clustering_scores": {}, "skipping": {}, "verdicts": []}

    # cold_column_cost: probe_<col>.cold_median - probe_rowcount.cold_median (marginal transcode).
    for m in models:
        base = timings[m].get("probe_rowcount", {}).get("cold_median_ms")
        if base is None:
            continue
        row = {}
        for col in _PROBE_COLS:
            v = timings[m].get(f"probe_{col}", {}).get("cold_median_ms")
            if v is not None:
                row[col] = round(v - base, 1)
        analysis["cold_column_cost"][m] = {"rowcount_overhead_ms": round(base, 1), "columns": row}

    # cold_vs_geometry: cold total (all cold-tier medians) alongside open geometry from parquet.
    for m in models:
        cold_total = sum(q["cold_median_ms"] for q in timings[m].values()
                         if q.get("cold_median_ms") is not None)
        p = tables.get(_table(m), {}).get("parquet", {})
        rgd = p.get("row_group_detail") or []
        analysis["cold_vs_geometry"][m] = {
            "cold_total_ms": round(cold_total, 1),
            "row_groups": p.get("row_groups"),
            "avg_row_group": p.get("avg_row_group"),
            "row_groups_over_2p24": sum(1 for r in rgd if (r.get("rows") or 0) > _RG_BIG),
        }

    # size_attribution: compressed bytes per column, per model (from raw column_chunks).
    for m in models:
        chunks = tables.get(_table(m), {}).get("parquet", {}).get("column_chunks") or []
        by_col = {}
        for c in chunks:
            col = c.get("path_in_schema")
            by_col[col] = by_col.get(col, 0) + (c.get("total_compressed_size") or 0)
        if by_col:
            analysis["size_attribution"][m] = {k: by_col[k] for k in sorted(by_col)}

    # clustering_scores: overlap metric per (table, filter column). Uses stats_min_value/
    # stats_max_value so string columns (DUID) are compared, not silently scored 0.0.
    for m in models:
        chunks = tables.get(_table(m), {}).get("parquet", {}).get("column_chunks") or []
        if not chunks:
            continue
        scores = {col: _overlap_score(_coerce_intervals(chunks, col)) for col in _FILTER_COLS}
        analysis["clustering_scores"][_table(m)] = scores
        # Sanity: 0.0 over >1 row group with stats is almost always the string-compare failure mode.
        for col, s in scores.items():
            n = sum(1 for c in chunks
                    if c.get("path_in_schema") == col and _bounds(c)[0] is not None)
            if s == 0.0 and n > 1:
                print(f"WARN clustering {_table(m)}.{col} = 0.0 over {n} row groups with stats — "
                      "verify the min/max comparison (string columns need stats_*_value)",
                      flush=True)

    # skipping: pair each ladder query's first_touch_ms with the clustering of its filtered
    # column(s) — this is the data-skipping evidence.
    for q, cols in _LADDER_COLS.items():
        entry = {"filter_cols": cols, "models": {}}
        present = False
        for m in models:
            ft = timings[m].get(q, {}).get("first_touch_ms")
            if ft is not None:
                present = True
            sc = analysis["clustering_scores"].get(_table(m), {})
            entry["models"][m] = {"first_touch_ms": ft,
                                  "clustering": {c: sc.get(c) for c in cols}}
        if present:
            analysis["skipping"][q] = entry

    # verdicts: structured, base vs each challenger, medians + tie rule (never a mean).
    base = next((x for x in models if x.endswith("_optimized")), min(models, key=len) if models else None)
    if base:
        for m in models:
            if m == base:
                continue
            for metric, key, sk in (("COLD", "cold_median_ms", "cold_spread_pct"),
                                    ("HOT", "hot_median_ms", "hot_spread_pct")):
                agg = _agg_verdict(timings[base], timings[m], key, sk)
                if agg:
                    agg.update({"metric": metric, "base": _short(base), "model": _short(m)})
                    analysis["verdicts"].append(agg)
    return analysis


# ---------------------------------------------------------------------------- rendering

# Build intent per layout for the `sort` column when a cycle reused prebuilt tables and recorded no
# build metadata. Keyed by physical suffix; the label must never contradict the layout's name.
_SORT_INTENT = {"optimized": "SORTED BY AUTO", "vorder": "vorder"}
_LAYOUT_COLS = ["table", "writer", "sort", "rows", "files", "row groups", "avg RG rows",
                "size MB", "compression", "cold total (ms)", "hot total (ms)"]


def _sort_label(rep, t):
    """The `sort` cell. Prefer build metadata (the actual columns AUTO picked); else the layout's
    definitional intent, so auto_sort never shows an empty sort against its own name."""
    b = rep.get("tables", {}).get(t, {}).get("build", {})
    if b.get("sort"):
        return b["sort"]
    suf = t.removeprefix("fct_summary_")
    if suf == "optimized":
        opt = (rep.get("run", {}).get("inputs", {}) or {}).get("opt_sort")
        return f"sorted by ({opt})" if (opt and str(opt).lower() != "auto") else "SORTED BY AUTO"
    return _SORT_INTENT.get(suf, "—")


def _int(v):
    return "" if v is None else f"{v:,.0f}"


def _layout_table(rep, analysis):
    """One row per layout: file geometry + aggregate cold/hot wall-clock, ✔ on the faster layout per
    column (tie rule). Merges the former parquet-layout, layout-matrix, and TL;DR tables into one.
    Assumes a single challenger (the current benchmark shape: auto_sort vs vorder)."""
    tables = rep.get("tables", {})
    models = list(rep.get("timings", {}))
    base = next((m for m in models if m.endswith("_optimized")),
                min(models, key=len) if models else None)
    order = ([base] + sorted(m for m in models if m != base)) if base else models
    chal = next((m for m in order if m != base), None)

    # cold/hot totals + ✔ winners, from the verdicts computed above. Blank if no challenger.
    by = {}
    for v in analysis.get("verdicts", []):
        by.setdefault(v["model"], {})[v["metric"]] = v
    ref = by.get(_short(chal), {}) if chal else {}
    totals = {}
    if base and chal:
        totals[_short(base)] = (ref.get("COLD", {}).get("base_total_ms"),
                                ref.get("HOT", {}).get("base_total_ms"))
        totals[_short(chal)] = (ref.get("COLD", {}).get("model_total_ms"),
                                ref.get("HOT", {}).get("model_total_ms"))

    def _winner(metric):
        v = ref.get(metric)
        if not v or v["verdict"] == "tie":
            return None
        return _short(base) if v["verdict"] == "base" else _short(chal)

    cold_w, hot_w = _winner("COLD"), _winner("HOT")

    body = []
    for m in order:
        t = _table(m)
        p = tables.get(t, {}).get("parquet")
        if not p:
            continue
        b = tables.get(t, {}).get("build", {})
        name = _short(m)
        engine = b.get("engine") or ("spark" if p.get("vorder") else "delta_rs")
        c, h = totals.get(name, (None, None))
        cc = f"{_int(c)} ✔" if name == cold_w else _int(c)
        hc = f"{_int(h)} ✔" if name == hot_w else _int(h)
        body.append([name, engine, _sort_label(rep, t), _int(p.get("rows")),
                     _fmt(p.get("files")), _fmt(p.get("row_groups")), _int(p.get("avg_row_group")),
                     _fmt(p.get("size_mb")), _fmt(p.get("compression")), cc, hc])
    if not body:
        return
    out = ["## Table layout (Parquet)", "",
           "| " + " | ".join(_LAYOUT_COLS) + " |",
           "|:--|:--|:--|--:|--:|--:|--:|--:|:--|--:|--:|"]
    for r in body:
        out.append("| " + " | ".join(r) + " |")
    _write("\n".join(out) + "\n")


def _sidebyside(title, timings, base, others, key, spread_key):
    """One table: rows = queries, columns = base + each challenger, best model per row (tie rule)."""
    models = [base] + others
    labels = [_short(m) for m in models]
    header = "| Query | " + " | ".join(f"{l} (ms)" for l in labels) + " | best |"
    sep = "|:--|" + "--:|" * len(models) + ":--|"
    out = [f"### {title}", "", header, sep]
    any_row = False
    for q in timings[base]:
        vals = {m: timings[m].get(q, {}).get(key) for m in models}
        if any(vals[m] is None for m in models):
            continue
        any_row = True
        spreads = {m: (timings[m].get(q, {}).get(spread_key) if spread_key else None) for m in models}
        ranked = sorted(models, key=lambda m: vals[m])
        best, second = ranked[0], ranked[1] if len(ranked) > 1 else ranked[0]
        w = _tie(vals[second], vals[best], spreads[second], spreads[best])
        best_lbl = "tie" if w == "tie" else _short(best)
        cells = " | ".join(f"{vals[m]:,.1f}" for m in models)
        out.append(f"| `{q}` | {cells} | {best_lbl} |")
    if any_row:
        _write("\n".join(out) + "\n")


def _cold_cost_table(cc):
    if not cc:
        return
    models = list(cc)
    out = ["### Marginal cold column cost (probe_col − probe_rowcount, ms)", "",
           "| column | " + " | ".join(_short(m) for m in models) + " |",
           "|:--|" + "--:|" * len(models)]
    for col in _PROBE_COLS:
        cells = " | ".join(_fmt(cc[m]["columns"].get(col)) for m in models)
        out.append(f"| `{col}` | {cells} |")
    out.append("| _rowcount overhead_ | "
               + " | ".join(_fmt(cc[m]["rowcount_overhead_ms"]) for m in models) + " |")
    _write("\n".join(out) + "\n")


def _verdicts(vs):
    if not vs:
        return
    lines = [f"- **{v['metric']}** {v['model']} vs {v['base']}: {v['text']}" for v in vs]
    _write("### Verdicts (medians, tie rule applied)\n\n" + "\n".join(lines) + "\n")


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to render")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    analysis = compute_analysis(rep)
    report.merge({"analysis": analysis})   # derived block lands in the same one file

    run = rep.get("run", {})
    _write(f"# Direct Lake query benchmark — `{run.get('sha')}` "
           f"(run {run.get('run_id')})\n")

    _layout_table(rep, analysis)

    timings = rep.get("timings", {})
    base = next((m for m in timings if m.endswith("_optimized")),
                min(timings, key=len) if timings else None)
    if base:
        others = [m for m in timings if m != base]
        _sidebyside("COLD (median of dehydrate cycles)", timings, base, others,
                    "cold_median_ms", "cold_spread_pct")
        _sidebyside("HOT (median of steady-state runs)", timings, base, others,
                    "hot_median_ms", "hot_spread_pct")
    _cold_cost_table(analysis.get("cold_column_cost", {}))
    _verdicts(analysis.get("verdicts", []))


if __name__ == "__main__":
    main()
