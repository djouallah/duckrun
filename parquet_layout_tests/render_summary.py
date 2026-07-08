"""SUMMARY.md — a specialist findings report for a storage/query-engine reader. Reads ONLY
run_report.json and recomputes every number from it (nothing hardcoded); writes SUMMARY.md and
appends the same content to $GITHUB_STEP_SUMMARY. Black-box constraint holds: DAX wall-clock and
Parquet metadata only. Medians only — never a mean in any comparison or verdict; ties render as `=`.

Env in: RUN_REPORT (the one JSON), GITHUB_STEP_SUMMARY (optional).
"""
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_report as rr  # noqa: E402  (pure helpers: compute_analysis, _bounds, _table, consts)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

MB = 1024 * 1024
OUT = []


def w(line=""):
    OUT.append(line)


def lbl(model):
    return model.removeprefix("aemo_electricity_").replace("vorder_base_", "vorder_")


def _order(models):
    """optimized first, then challengers by name (sorted before notsorted)."""
    base = [m for m in models if m.endswith("_optimized")]
    return base + sorted(m for m in models if not m.endswith("_optimized"))


def _ms(v):
    return "—" if v is None else f"{v:,.0f}"


def _mb(v):
    return "—" if v is None else f"{v:,.1f}"


def _ratio(v):
    return "—" if v is None else f"{v:.2f}"


def _rg_over(parquet):
    return sum(1 for r in (parquet.get("row_group_detail") or []) if (r.get("rows") or 0) > rr._RG_BIG)


def _size_of(sz_model, col):
    """Compressed bytes for a probe column, matching the parquet column name case-insensitively
    (probe_duid ↔ parquet 'DUID')."""
    if col in sz_model:
        return sz_model[col]
    for k in sz_model:
        if k.lower() == col.lower():
            return sz_model[k]
    return None


def _compression(chunks):
    """per column: compressed MB, distinct encodings, dictionary-page MB (data_page_offset −
    dictionary_page_offset, summed over row groups)."""
    out = {}
    for c in chunks:
        col = c.get("path_in_schema")
        d = out.setdefault(col, {"comp": 0, "enc": set(), "dict": 0})
        d["comp"] += c.get("total_compressed_size") or 0
        if c.get("encodings"):
            for e in str(c["encodings"]).split(","):
                d["enc"].add(e.strip())
        dpo, dp = c.get("dictionary_page_offset"), c.get("data_page_offset")
        if dpo is not None and dp is not None and dp > dpo:
            d["dict"] += dp - dpo
    return out


# ------------------------------------------------------------------------------------- sections

def s1_header(rep):
    run = rep.get("run", {})
    inp = run.get("inputs", {})
    rows = max((t.get("parquet", {}).get("rows") or 0) for t in rep.get("tables", {}).values() or [{}])
    w("# Specialist findings — Direct Lake layout benchmark")
    w()
    w(f"- run `{run.get('run_id')}` · sha `{run.get('sha')}` · {run.get('date')}")
    w(f"- duckrun `{run.get('duckrun_version')}` · writer `{run.get('writer_profile')}`")
    w(f"- inputs: cold_repeats={inp.get('cold_repeats')} · runs={inp.get('runs')} · "
      f"gap_seconds={inp.get('gap_seconds')}")
    w()
    w(f"{len(rep.get('tables', {}))} layouts of the same {rows/1e6:,.1f}M-row table; DAX over XMLA "
      f"against Direct Lake; {inp.get('cold_repeats')} cold cycles (dehydrate→query) per query, "
      f"medians reported.")
    w()


def s2_layout(rep, tables_order):
    w("## 2. Layout matrix")
    w()
    w("| table | writer | sort | vorder | files | row groups | avg RG rows | RGs > 2²⁴ | MB |")
    w("|:--|:--|:--|:--|--:|--:|--:|--:|--:|")
    for t in tables_order:
        p = rep["tables"][t].get("parquet", {})
        b = rep["tables"][t].get("build", {})
        engine = b.get("engine") or ("spark" if p.get("vorder") else "duckdb")
        sort = b.get("sort") or "—"
        w(f"| {t.removeprefix('fct_summary_')} | {engine} | {sort} | "
          f"{'yes' if p.get('vorder') else 'no'} | {p.get('files')} | {p.get('row_groups')} | "
          f"{_ms(p.get('avg_row_group'))} | {_rg_over(p)} | {_mb(p.get('size_mb'))} |")
    w()


def s3_verdicts(analysis, base_lbl):
    vs = analysis.get("verdicts", [])
    by = {}
    for v in vs:
        by.setdefault(v["model"], {})[v["metric"]] = v
    w("## 3. Headline verdicts (medians, tie rule)")
    w()
    w("| pair | COLD base/model | HOT base/model | verdict |")
    w("|:--|--:|--:|:--|")
    for model, mv in by.items():
        c, h = mv.get("COLD"), mv.get("HOT")
        cr = "=" if (c and c["verdict"] == "tie") else _ratio(c["ratio"]) if c else "—"
        hr = "=" if (h and h["verdict"] == "tie") else _ratio(h["ratio"]) if h else "—"
        parts = []
        if c:
            parts.append("COLD " + (c["text"] if c["verdict"] != "tie" else "no measurable difference"))
        if h:
            parts.append("HOT " + (h["text"] if h["verdict"] != "tie"
                                   else "no measurable difference (all deltas within spread)"))
        w(f"| {model} vs {base_lbl} | {cr} | {hr} | {'; '.join(parts)} |")
    w()


def s4_cold_decomp(analysis, models):
    cc = analysis.get("cold_column_cost", {})
    sz = analysis.get("size_attribution", {})
    if not cc:
        return
    w("## 4. Cold decomposition (marginal cost per column)")
    w()
    hdr = "| column |" + "".join(f" {lbl(m)} ms | {lbl(m)} ms/MB |" for m in models)
    w(hdr)
    w("|:--|" + "--:|--:|" * len(models))
    cost_by_col = {}  # col -> [cost per model] for the observations below
    for col in rr._PROBE_COLS:
        cells = []
        for m in models:
            cost = cc.get(m, {}).get("columns", {}).get(col)
            comp_mb = (_size_of(sz.get(m, {}), col) or 0) / MB
            perbyte = (cost / comp_mb) if (cost is not None and comp_mb) else None
            cells.append(f" {_ms(cost)} | {_ratio(perbyte)} |")
            cost_by_col.setdefault(col, []).append(cost)
        w(f"| {col} |" + "".join(cells))
    ov = " · ".join(f"{lbl(m)} {_ms(cc.get(m, {}).get('rowcount_overhead_ms'))}" for m in models)
    w()
    w(f"_rowcount overhead (ms): {ov}._")
    w()
    # auto observations: CV of cost across layouts per column
    cv = {}
    floor = {}
    for col, vals in cost_by_col.items():
        v = [x for x in vals if x is not None]
        if len(v) >= 2 and statistics.mean(v):
            cv[col] = statistics.pstdev(v) / statistics.mean(v)
        if v:
            floor[col] = statistics.mean(v)
    if cv:
        med_cv = statistics.median(cv.values())
        hi = sorted((c for c, x in cv.items() if x >= med_cv), key=lambda c: -cv[c])
        lo = [c for c, x in cv.items() if x < med_cv]
        cheapest = min(floor, key=floor.get) if floor else None
        w(f"- sort-sensitive (high cross-layout cost variance): "
          f"{', '.join(f'{c} (CV {cv[c]:.2f})' for c in hi) or 'none'}.")
        w(f"- layout-invariant (low variance): {', '.join(lo) or 'none'}.")
        if cheapest:
            w(f"- irreducible floor: {cheapest} (~{floor[cheapest]:,.0f} ms across layouts).")
        w()


def s5_compression(rep, models):
    w("## 5. Compression attribution")
    w()
    w("| column | model | comp MB | encodings | dict MB |")
    w("|:--|:--|--:|:--|--:|")
    dict_by_col = {}  # col -> {model: dict_mb}
    for m in models:
        chunks = rep["tables"][rr._table(m)].get("parquet", {}).get("column_chunks") or []
        comp = _compression(chunks)
        for col in sorted(comp):
            d = comp[col]
            dmb = d["dict"] / MB
            dict_by_col.setdefault(col, {})[m] = dmb
            w(f"| {col} | {lbl(m)} | {_mb(d['comp']/MB)} | {', '.join(sorted(d['enc']))} | "
              f"{_mb(dmb)} |")
    w()
    for col, dd in dict_by_col.items():
        vals = {k: v for k, v in dd.items() if v}
        if len(vals) >= 2 and max(vals.values()) > 3 * min(vals.values()):
            hi = max(vals, key=vals.get)
            lo = min(vals, key=vals.get)
            w(f"- ⚠ {col} dictionary: {lbl(hi)} {vals[hi]:,.1f} MB vs {lbl(lo)} {vals[lo]:,.1f} MB "
              f"(>3×).")
    w()


def s6_skipping(analysis, models):
    sk = analysis.get("skipping", {})
    if not sk:
        return
    w("## 6. Data skipping (first touch vs clustering)")
    w()
    w("| ladder query | filter | " + " | ".join(f"{lbl(m)} score" for m in models)
      + " | " + " | ".join(f"{lbl(m)} ft ms" for m in models) + " |")
    w("|:--|:--|" + "--:|" * (2 * len(models)))
    for q, e in sk.items():
        cols = e["filter_cols"]
        scores, fts = [], []
        for m in models:
            mm = e["models"].get(m, {})
            sc = mm.get("clustering", {})
            s = min((sc[c] for c in cols if sc.get(c) is not None), default=None)
            scores.append(s)
            fts.append(mm.get("first_touch_ms"))
        srow = " | ".join(_ratio(s) for s in scores)
        frow = " | ".join(_ms(f) for f in fts)
        w(f"| {q} | {','.join(cols)} | {srow} | {frow} |")
        fv = [f for f in fts if f is not None]
        sv = [s for s in scores if s is not None]
        note = "inconclusive"
        if fv and sv:
            ratio = max(fv) / min(fv) if min(fv) else 1.0
            if ratio > 1.5 and (max(sv) - min(sv)) > 0.3:
                note = f"skipping observed ({ratio:.1f}× first-touch spread)"
            elif ratio <= 1.5 and min(sv) > 0.8:
                note = "no skipping anywhere (first-touch flat, all interleaved)"
        w(f"  - {q}: {note}.")
    w()


def s7_caveats(rep, analysis):
    w("## 7. Caveats")
    w()
    noisy = []
    for m, qs in rep.get("timings", {}).items():
        for q, d in qs.items():
            for which, key in (("cold", "cold_spread_pct"), ("hot", "hot_spread_pct")):
                sp = d.get(key)
                if sp is not None and sp > 25:
                    noisy.append(f"{lbl(m)}/{q} {which} spread {sp:.0f}%")
    if noisy:
        w(f"- high spread (>25%) — verdicts touching these are low-confidence: {'; '.join(noisy)}.")
    else:
        w("- no query exceeded 25% cold/hot spread.")
    inp = rep.get("run", {}).get("inputs", {})
    w(f"- n = {inp.get('cold_repeats')} per cold median; capacity is shared across tenants, so "
      "absolute ms drift between models is expected — read ratios, not absolutes.")
    w("- single dataset and shape (numeric-heavy fact, one modest-cardinality string column, "
      "DUID); conclusions do not generalize to high-cardinality-string or wide-dimension tables.")
    w()


def s8_pointers(rep):
    w("## 8. Raw")
    w()
    w("- artifact `run-report`: `run_report.json` (this summary is `SUMMARY.md` beside it).")
    w("- cross-run: `duckdb -c \"SELECT run.run_id, * FROM read_json('reports/*.json')\"`.")
    w("- every number above recomputes from run_report.json (timings.*, tables.*.parquet, analysis.*).")
    w()


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to summarize")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    analysis = rep.get("analysis") or rr.compute_analysis(rep)
    models = _order(list(rep.get("timings", {})))
    tables_order = [rr._table(m) for m in models if rr._table(m) in rep.get("tables", {})]
    base = next((m for m in models if m.endswith("_optimized")), models[0] if models else None)

    s1_header(rep)
    s2_layout(rep, tables_order)
    if base:
        s3_verdicts(analysis, lbl(base))
    s4_cold_decomp(analysis, models)
    s5_compression(rep, models)
    s6_skipping(analysis, models)
    s7_caveats(rep, analysis)
    s8_pointers(rep)

    text = "\n".join(OUT) + "\n"
    with open("SUMMARY.md", "w", encoding="utf-8") as f:
        f.write(text)
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(text)
    print(text)


if __name__ == "__main__":
    main()
