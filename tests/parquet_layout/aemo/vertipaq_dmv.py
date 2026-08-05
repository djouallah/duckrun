"""VertiPaq DMV readout — what the Direct Lake transcoder ACTUALLY did with each column.

The parquet footers say what we wrote; wall-clock says what it cost; this says WHY: the resident
encoding the engine chose. For each benchmark model, hydrate the fact fully, then dump the two
storage DMVs over the same ADOMD connection the benchmark uses:

  DISCOVER_STORAGE_TABLE_COLUMNS         — per column: encoding (HASH/VALUE), dictionary size
  DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS — per column-segment: rows, used bytes, compression
                                           type (the VertiPaq encoding actually picked), bit width

The question this exists to answer (2026-08-05): sorting DUID third (`date, time, DUID`) shrank
the parquet 17% but made relationship-heavy hot queries slower — is DUID's RESIDENT shape
different under that sort (e.g. an RLE pick with ~unit-length runs) vs the unsorted reference?
Compare the same column across the two models and the madness gets a method, one way or the other.

Env in: PBI_WORKSPACE, ADOMD_DIR (PBI_TOKEN self-acquired via duckrun when absent), RUN_REPORT
(optional — merged under `vertipaq:` when set). Prints per-model tables + the cross-model DUID
comparison, and appends to GITHUB_STEP_SUMMARY.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import report  # noqa: E402
import xmla_compare as xc  # noqa: E402  — reuses _load_adomd/open_conn/warm_up/discover_models

FACT = "fct_summary"
# One query that touches EVERY fact column, so all segments are resident before the DMVs read.
HYDRATE = ('EVALUATE ROW("r", COUNTROWS(fct_summary), "d", DISTINCTCOUNT(fct_summary[DUID]), '
           '"dt", DISTINCTCOUNT(fct_summary[date]), "t", DISTINCTCOUNT(fct_summary[time]), '
           '"m", SUM(fct_summary[mw]), "p", SUM(fct_summary[price]), '
           '"c", MAX(fct_summary[cutoff]))')


def dmv_rows(conn, sql):
    """Run a DMV query and return (columns, rows) with real values (xc.run_query drains but
    discards). DMV restrictions are limited server-side, so callers filter client-side."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    reader = AdomdCommand(sql, conn).ExecuteReader()
    cols = [reader.GetName(i) for i in range(reader.FieldCount)]
    rows = []
    while reader.Read():
        rows.append([reader.GetValue(i) for i in range(reader.FieldCount)])
    reader.Close()
    return cols, rows


def _pick(cols, row, name, default=None):
    try:
        return row[cols.index(name)]
    except (ValueError, IndexError):
        return default


def _norm(ident):
    """DMV identifiers carry an internal id suffix — 'DUID (114)' -> 'DUID'."""
    return re.sub(r" \(\d+\)$", "", ident)


def read_model(workspace, model, token):
    conn = xc.open_conn(workspace, model, token)
    try:
        if not xc.warm_up(conn, model):
            return None
        xc.run_query(conn, HYDRATE)   # make every fact column resident
        ccols, crows = dmv_rows(conn, "SELECT * FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS")
        scols, srows = dmv_rows(conn, "SELECT * FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS")
    finally:
        conn.Close()

    out = {}
    for row in crows:
        # the table name lives in DIMENSION_NAME (VertiPaq Analyzer's key); TABLE_ID is the
        # storage-object id ('H$…'/'R$…' prefixes for shadow tables — keep only the data table)
        tname = str(_pick(ccols, row, "DIMENSION_NAME", ""))
        tid = str(_pick(ccols, row, "TABLE_ID", ""))
        if tname != FACT or tid.startswith(("H$", "R$", "U$")):
            continue
        col = _norm(str(_pick(ccols, row, "COLUMN_ID", "")))
        if col.startswith("RowNumber"):
            continue
        out[col] = {
            "encoding": {1: "HASH", 2: "VALUE"}.get(_pick(ccols, row, "COLUMN_ENCODING"),
                                                    str(_pick(ccols, row, "COLUMN_ENCODING"))),
            "dictionary_mb": round((_pick(ccols, row, "DICTIONARY_SIZE") or 0) / 1e6, 2),
            "segments": [],
        }
    if not out:  # filter matched nothing — dump reality so the next fix is fact-based
        print(f"  DMV columns: {ccols}", flush=True)
        for r in crows[:6]:
            print(f"  DMV row sample: {r}", flush=True)
    for row in srows:
        tname = str(_pick(scols, row, "DIMENSION_NAME", ""))
        tid = str(_pick(scols, row, "TABLE_ID", ""))
        if tname != FACT or tid.startswith(("H$", "R$", "U$")):
            continue
        col = _norm(str(_pick(scols, row, "COLUMN_ID", "")))
        if col not in out:
            continue
        out[col]["segments"].append({
            "rows": int(_pick(scols, row, "RECORDS_COUNT") or 0),
            "used_mb": round((_pick(scols, row, "USED_SIZE") or 0) / 1e6, 2),
            "compression": str(_pick(scols, row, "COMPRESSION_TYPE", "")),
            "bits": int(_pick(scols, row, "BITS_COUNT") or 0),
        })
    return out


def render(model, data, out):
    out.append(f"\n### {model} — resident fact columns (VertiPaq DMVs)")
    out.append("| column | encoding | dict MB | segs | resident MB | bits/seg | compression/seg |")
    out.append("|---|---|---|---|---|---|---|")
    rendered = 0
    for col in ("date", "time", "DUID", "mw", "price", "cutoff"):
        d = data.get(col)
        if not d:
            continue
        segs = d["segments"]
        used = round(sum(s["used_mb"] for s in segs), 1)
        bits = "/".join(str(s["bits"]) for s in segs)
        comp = "/".join(s["compression"] for s in segs)
        out.append(f"| {col} | {d['encoding']} | {d['dictionary_mb']} | {len(segs)} "
                   f"| {used} | {bits} | {comp} |")
        rendered += 1
    if not rendered:  # never render silently-empty tables again; show what the DMV actually keyed
        out.append(f"| _no column matched; DMV keys were: {sorted(data)[:8]}_ | | | | | | |")


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()
    xc._load_adomd(os.environ.get("ADOMD_DIR", "."))
    base, others = xc.discover_models()
    lines = ["## VertiPaq resident-encoding readout"]
    merged = {}
    for model in [base] + others:
        print(f"Reading DMVs from {model}...", flush=True)
        data = read_model(workspace, model, token)
        if data is None:
            print(f"  {model}: never became queryable — skipped", flush=True)
            continue
        merged[model] = data
        render(model, data, lines)
    text = "\n".join(lines) + "\n"
    print(text)
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(text)
    if os.environ.get("RUN_REPORT"):
        report.merge({"vertipaq": merged})


if __name__ == "__main__":
    main()
