"""Regression guards for the Direct Lake benchmark verdict layer (parquet_layout_tests).

The verdict layer once read a base/model ratio with the wrong orientation, so a lower total (the
FASTER layout) could be reported as the loser. These tests pin the direction so it can't recur:
the winner is always the lower-total layout, ratios are oriented base÷model, and the summed-cost
winner must equal the verdict winner. Pure functions only — no Fabric, no XMLA.
"""
import os
import sys

import pytest

_PL = os.path.join(os.path.dirname(__file__), "..", "..", "parquet_layout_tests")
sys.path.insert(0, os.path.abspath(_PL))

import render_report as rr          # noqa: E402
import render_summary as rs         # noqa: E402


def _cold(median, spread=5.0):
    return {"tier": "composite", "cold_median_ms": median, "cold_spread_pct": spread,
            "hot_median_ms": median, "hot_spread_pct": spread}


def test_ratio_orientation_base_slower_loses():
    """base is the SLOWER layout (higher total) -> the verdict must say base loses (model wins)."""
    base_t = {"q1": _cold(200), "q2": _cold(200)}      # base total 400 (slow)
    model_t = {"q1": _cold(100), "q2": _cold(100)}     # model total 200 (fast)
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["verdict"] == "model"                     # faster (lower total) wins
    assert v["ratio"] == pytest.approx(2.0)            # base/model = 400/200
    assert v["wins"] == 2 and v["losses"] == 0


def test_ratio_orientation_base_faster_wins():
    base_t = {"q1": _cold(100), "q2": _cold(100)}      # base fast
    model_t = {"q1": _cold(200), "q2": _cold(200)}     # model slow
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["verdict"] == "base"
    assert v["ratio"] == pytest.approx(0.5)            # base/model = 100/200 < 1 => base faster


def test_within_spread_is_a_tie_not_a_win():
    base_t = {"q1": _cold(100, spread=30)}             # 30% spread
    model_t = {"q1": _cold(104, spread=30)}            # 4% apart << 30% noise
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["ties"] == 1 and v["wins"] == 0 and v["losses"] == 0
    assert v["verdict"] == "tie"


def test_verify_verdicts_passes_when_consistent():
    """Direction guard is silent when the verdict agrees with the per-query median majority."""
    rep = {"timings": {
        "aemo_electricity_auto_sort": {"probe_rowcount": _cold(100),
                                       "q1": _cold(100), "q2": _cold(100)},
        "aemo_electricity_vorder": {"probe_rowcount": _cold(100),
                                    "q1": _cold(200), "q2": _cold(200)}}}
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_verdicts(rep, analysis)     # auto_sort faster everywhere, consistent
    assert errs == [] and notes == []


def test_verify_verdicts_flags_a_true_inversion():
    """Fatal only when the verdict disagrees with the SAME-query median majority."""
    rep = {"timings": {
        "aemo_electricity_auto_sort": {"probe_rowcount": _cold(100),
                                       "q1": _cold(100), "q2": _cold(100)},
        "aemo_electricity_vorder": {"probe_rowcount": _cold(100),
                                    "q1": _cold(300), "q2": _cold(300)}}}
    analysis = rr.compute_analysis(rep)
    # auto_sort is clearly faster; corrupt the verdict to claim the challenger won.
    for v in analysis["verdicts"]:
        if v["metric"] == "COLD":
            v["verdict"] = "model"
    errs, notes = rs.verify_verdicts(rep, analysis)
    assert errs and "verdict says vorder" in errs[0]


def test_probe_vs_composite_divergence_is_a_note_not_fatal():
    """When the verdict agrees with the median majority but the probe-only cost points the other
    way (composites and probes diverge), it must be a non-fatal note — not a build failure. This
    is the 6M-row-group regression case that false-failed the CI."""
    # Composites favour the challenger (so verdict + median majority = challenger), but the single
    # probe column favours the base — the two subsets legitimately disagree.
    rep = {"timings": {
        "aemo_electricity_auto_sort": {
            "probe_rowcount": _cold(100), "probe_mw": _cold(200),   # base cheaper on the probe
            "c1": _cold(500), "c2": _cold(500), "c3": _cold(500)},  # base slower on composites
        "aemo_electricity_vorder": {
            "probe_rowcount": _cold(100), "probe_mw": _cold(400),   # challenger dearer on the probe
            "c1": _cold(300), "c2": _cold(300), "c3": _cold(300)}}} # challenger faster on composites
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_verdicts(rep, analysis)
    assert errs == []                                   # not fatal
    assert notes and "diverge" in notes[0]              # surfaced as a note


def test_sort_label_never_contradicts_the_name():
    """auto_sort must not render an empty sort cell — the name asserts a sort, so the Summary table
    must show one even when the build was gated and recorded no metadata."""
    rep = {"tables": {"fct_summary_auto_sort": {"parquet": {}}},
           "run": {"inputs": {"opt_sort": "auto"}}}
    s = rr._sort_label(rep, "fct_summary_auto_sort")
    assert s not in ("", "—", None) and "AUTO" in s


def test_sort_label_prefers_recorded_build_metadata():
    rep = {"tables": {"fct_summary_auto_sort": {"build": {"sort": "sorted by (date, time)"}}}}
    assert rr._sort_label(rep, "fct_summary_auto_sort") == "sorted by (date, time)"


def test_sort_label_explicit_columns_from_inputs():
    rep = {"tables": {"fct_summary_auto_sort": {"parquet": {}}},
           "run": {"inputs": {"opt_sort": "region, date"}}}
    assert rr._sort_label(rep, "fct_summary_auto_sort") == "sorted by (region, date)"


def test_floor_uses_only_quotable_columns():
    """A cheap-but-noisy column (spread>25%) must not be named the irreducible floor."""
    noisy = {"tier": "probe", "cold_median_ms": 50, "cold_spread_pct": 110,
             "hot_median_ms": 50, "hot_spread_pct": 110}     # date-like: cheap + very noisy
    stable = {"tier": "probe", "cold_median_ms": 300, "cold_spread_pct": 5,
              "hot_median_ms": 300, "hot_spread_pct": 5}
    rep = {"timings": {"aemo_electricity_auto_sort": {
        "probe_rowcount": stable, "probe_date": noisy, "probe_mw": stable}}}
    assert "date" in rs._noisy_cols(rep)                # flagged non-quotable
    assert "mw" not in rs._noisy_cols(rep)
