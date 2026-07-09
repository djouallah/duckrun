"""Specialist findings report for a storage/query-engine reader. Reads ONLY run_report.json and
recomputes every number from it (nothing hardcoded); appends to the CI job summary
($GITHUB_STEP_SUMMARY) and prints to stdout — no file artifact. Black-box constraint holds: DAX
wall-clock and Parquet metadata only. Medians only — never a mean in any comparison or verdict;
ties render as `=`.

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
    return rr._short(model)  # layout label: physical name minus its aemo_electricity_/fct_summary_ prefix


def _noisy_cols(rep, thresh=25.0):
    """Probe columns whose cold spread exceeds `thresh`% in ANY layout — measurement is too noisy
    to quote (n = cold_repeats over shared capacity). These are excluded from headline sentences."""
    tim = rep.get("timings", {})
    noisy = set()
    for m in tim:
        for col in rr._PROBE_COLS:
            sp = tim[m].get(f"probe_{col}", {}).get("cold_spread_pct")
            if sp is not None and sp > thresh:
                noisy.add(col)
    return noisy


def _verdict_line(v, base_lbl, chal_lbl):
    """One metric's verdict, stated with explicit model names (never 'base'/'model')."""
    if not v:
        return None
    if v["verdict"] == "tie":
        return f"{v['metric']}: no measurable difference (all deltas within spread)"
    fac = v["ratio"] if v["ratio"] >= 1 else (1 / v["ratio"] if v["ratio"] else 0)
    if v["verdict"] == "base":
        winner, loser, wc, lc = base_lbl, chal_lbl, v["losses"], v["wins"]
    else:
        winner, loser, wc, lc = chal_lbl, base_lbl, v["wins"], v["losses"]
    return (f"{v['metric']}: {winner} {fac:.2f}× faster "
            f"({winner} wins {wc}, {loser} wins {lc}, ties {v['ties']})")


def _verdict_row(by, base_lbl, m):
    chal = lbl(m)
    mv = by.get(rr._short(m), {})
    c, h = mv.get("COLD"), mv.get("HOT")
    cr = "=" if (c and c["verdict"] == "tie") else (_ratio(c["ratio"]) if c else "—")
    hr = "=" if (h and h["verdict"] == "tie") else (_ratio(h["ratio"]) if h else "—")
    parts = [p for p in (_verdict_line(c, base_lbl, chal), _verdict_line(h, base_lbl, chal)) if p]
    w(f"| {chal} vs {base_lbl} | {cr} | {hr} | {'; '.join(parts)} |")


def _order(models):
    """auto_sort (base) first, then challenger(s) by name."""
    base = [m for m in models if m.endswith("_auto_sort")]
    return base + sorted(m for m in models if not m.endswith("_auto_sort"))


def _ms(v):
    return "—" if v is None else f"{v:,.0f}"


def _mb(v):
    return "—" if v is None else f"{v:,.1f}"


def _ratio(v):
    return "—" if v is None else f"{v:.2f}"


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
      f"row_limit={inp.get('row_limit')} · gap_seconds={inp.get('gap_seconds')}")
    w()
    w(f"{len(rep.get('tables', {}))} layouts of the same {rows/1e6:,.1f}M-row table; DAX over XMLA "
      f"against Direct Lake; {inp.get('cold_repeats')} cold cycles (dehydrate→query) per query, "
      f"medians reported.")
    w()


def s3_verdicts(rep, analysis, base, models):
    by = {}
    for v in analysis.get("verdicts", []):
        by.setdefault(v["model"], {})[v["metric"]] = v
    base_lbl = lbl(base)
    headline = [m for m in models if m != base]
    noisy = sorted(_noisy_cols(rep))

    w("## 2. Headline verdict (medians, tie rule)")
    w()
    w(f"Ratio column is `{base_lbl} ÷ challenger` (< 1 ⇒ {base_lbl} faster).")
    w()
    w(f"| pair | COLD {base_lbl}÷chal | HOT {base_lbl}÷chal | verdict |")
    w("|:--|--:|--:|:--|")
    for m in headline:
        _verdict_row(by, base_lbl, m)
    w()
    if noisy:
        cr = rep.get("run", {}).get("inputs", {}).get("cold_repeats")
        agg = []
        for m in headline:
            c = by.get(rr._short(m), {}).get("COLD")
            if c and c["verdict"] != "tie":
                tot = c["wins"] + c["losses"] + c["ties"]
                who = base_lbl if c["verdict"] == "base" else lbl(m)
                cnt = c["losses"] if c["verdict"] == "base" else c["wins"]
                agg.append(f"{who} wins {cnt}/{tot} vs {lbl(m)}")
        w(f"- ⚠ {len(noisy)} probe columns exceed 25% cold spread (n={cr}, shared capacity; see §6). "
          f"The headline rests on the aggregate, not any single column: "
          f"{'; '.join(agg) or 'see table'} (per-query cold median, tie rule).")
        w()


def s4_cold_decomp(rep, analysis, models):
    cc = analysis.get("cold_column_cost", {})
    sz = analysis.get("size_attribution", {})
    if not cc:
        return
    noisy = _noisy_cols(rep)
    w("## 3. Cold decomposition (marginal cost per column)")
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
        # sort-sensitivity is only quotable for low-noise columns; noisy ones (spread>25%) have
        # cost variance confounded with measurement noise, so name them separately.
        hi = sorted((c for c, x in cv.items() if x >= med_cv and c not in noisy), key=lambda c: -cv[c])
        lo = [c for c, x in cv.items() if x < med_cv and c not in noisy]
        # The cheapest columns overall (candidate "floor"). If any is non-quotable, the irreducible
        # transcode floor isn't measurable at this n — naming a noisy cheap column the floor while
        # its own medians sit below it is the contradiction we refuse to print.
        cheap = sorted(floor, key=floor.get)[:2]
        cheap_noisy = [c for c in cheap if c in noisy]
        w(f"- sort-sensitive (high cross-layout variance, low noise): "
          f"{', '.join(f'{c} (CV {cv[c]:.2f})' for c in hi) or 'none clearly separable at this n'}.")
        w(f"- layout-invariant (low variance, low noise): "
          f"{', '.join(lo) or 'none clearly separable at this n'}.")
        if noisy:
            w(f"- non-quotable (cold spread >25%): {', '.join(sorted(noisy))}.")
        if cheap_noisy:
            w(f"- irreducible floor: not measurable at this n — the cheapest columns "
              f"({', '.join(cheap_noisy)}) are non-quotable; anchor conclusions on the aggregate "
              "cold win (§2) and the data-skipping evidence (§5), not per-column costs.")
        elif cheap:
            w(f"- irreducible floor (stable): {cheap[0]} (~{floor[cheap[0]]:,.0f} ms across layouts).")
        w()


def s5_compression(rep, models):
    w("## 4. Compression attribution")
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
    w("## 5. Data skipping (first touch vs clustering)")
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
    w("## 6. Caveats")
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
    w("## 7. Raw")
    w()
    w("- artifact `run-report`: `run_report.json` (these findings are in the CI job summary).")
    w("- cross-run: `duckdb -c \"SELECT run.run_id, * FROM read_json('reports/*.json')\"`.")
    w("- every number above recomputes from run_report.json (timings.*, tables.*.parquet, analysis.*).")
    w()


def verify_verdicts(rep, analysis):
    """Orientation guard: the verdict winner must agree with the per-query cold-median majority
    over the SAME queries the verdict aggregates — a disagreement there is a true ratio inversion
    and is fatal. The summed marginal PROBE cost is a second view over a DIFFERENT (probe-only)
    query subset; probes and composites can legitimately point different ways, so a disagreement
    there is a non-fatal note, not a build failure. Returns (errors, notes)."""
    tim = rep.get("timings", {})
    cc = analysis.get("cold_column_cost", {})
    base = next((m for m in tim if m.endswith("_auto_sort")), None)
    if not base or base not in cc:
        return [], []
    def _cost(m):
        cols = cc.get(m, {}).get("columns")
        return (sum(cols.values()) + cc[m]["rowcount_overhead_ms"]) if cols else None
    base_cost = _cost(base)
    vmap = {v["model"]: v for v in analysis.get("verdicts", []) if v["metric"] == "COLD"}
    errs, notes = [], []
    for m in tim:
        if m == base or m not in cc:
            continue
        v = vmap.get(rr._short(m))
        if not v or v["verdict"] == "tie":
            continue
        verdict_winner = base if v["verdict"] == "base" else m
        # per-query cold-median majority — same query set as the verdict; the orientation invariant.
        bw = mw = 0
        for q, d in tim[m].items():
            b, x = tim[base].get(q, {}).get("cold_median_ms"), d.get("cold_median_ms")
            if b is None or x is None:
                continue
            bw += b < x
            mw += x < b
        median_winner = base if bw > mw else (m if mw > bw else None)
        if median_winner and verdict_winner != median_winner:      # FATAL: real inversion
            errs.append(f"{lbl(m)}: verdict says {lbl(verdict_winner)} but per-query cold-median "
                        f"majority says {lbl(median_winner)}")
            continue
        # summed marginal probe cost — different subset, advisory only.
        mcost = _cost(m)
        cost_winner = (base if base_cost < mcost else m) if (base_cost and mcost) else None
        if cost_winner and verdict_winner != cost_winner:
            notes.append(f"{lbl(m)}: full-query verdict favours {lbl(verdict_winner)} while the "
                         f"probe-only marginal cost favours {lbl(cost_winner)} — probes and "
                         f"composites diverge (not an inversion)")
    return errs, notes


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to summarize")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    analysis = rep.get("analysis") or rr.compute_analysis(rep)
    models = _order(list(rep.get("timings", {})))
    base = next((m for m in models if m.endswith("_auto_sort")), models[0] if models else None)

    s1_header(rep)
    if base:
        s3_verdicts(rep, analysis, base, models)
    s4_cold_decomp(rep, analysis, models)
    s5_compression(rep, models)
    s6_skipping(analysis, models)
    s7_caveats(rep, analysis)
    s8_pointers(rep)

    text = "\n".join(OUT) + "\n"
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(text)
    print(text)

    # Direction guard — the findings are already in the job summary. A genuine verdict inversion
    # (verdict disagrees with the same-query median majority) is fatal; a probe-vs-composite
    # divergence is only a warning.
    errs, notes = verify_verdicts(rep, analysis)
    for n in notes:
        print(f"::warning::{n}")
    if errs:
        for e in errs:
            print(f"::error::verdict direction inversion — {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
