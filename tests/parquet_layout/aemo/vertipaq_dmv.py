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
(optional — merged under `vertipaq:` when set). DMV_MODELS (comma list) names the models to read
instead of discovering the aemo pair from disk; DMV_COLUMNS (comma list) replaces the wide-fact
hydrate/render columns for models that pruned the fact (writer_cold narrows to `date, time, DUID`
— the default HYDRATE would error on the missing columns). Prints per-model tables + the
cross-model DUID comparison, and appends to GITHUB_STEP_SUMMARY.
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
DEFAULT_COLUMNS = ("date", "time", "DUID", "mw", "price", "cutoff",
                   # OPT_DERIVED type-matrix columns (writer_cold): present only when that run
                   # derived them, which is why DMV_COLUMNS overrides this list per run.
                   "year_int", "duid_id", "price_dec", "price_dbl")
COLUMNS = tuple(c.strip() for c in (os.environ.get("DMV_COLUMNS") or "").split(",")
                if c.strip()) or DEFAULT_COLUMNS
# One query that touches EVERY fact column, so all segments are resident before the DMVs read.
# DISTINCTCOUNT is type-agnostic, so the same shape works for any column list.
HYDRATE = ('EVALUATE ROW("r", COUNTROWS(fct_summary), '
           + ", ".join(f'"c{i}", DISTINCTCOUNT(fct_summary[{c}])'
                       for i, c in enumerate(COLUMNS)) + ")")


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


def _key(ident):
    """Match key. The model spells the derived columns with SPACES where the parquet and
    DMV_COLUMNS use underscores — the DMV returns 'duid id' for `duid_id` — so run 32681270789
    reported all four OPT_DERIVED columns as `MISSING from the DMV` while listing the same four as
    unrequested extras. They were there the whole time under a different spelling. Fold the two
    apart-ness's that are purely cosmetic (separator, case) and nothing else."""
    return _norm(ident).replace(" ", "_").casefold()


def read_model(workspace, model, token):
    conn = xc.open_conn(workspace, model, token)
    try:
        if not xc.warm_up(conn, model):
            return None
        # Hydrate per column, not in one ROW(): a single DAX statement is all-or-nothing, so one
        # bad column (a type DISTINCTCOUNT rejects, a name that never bound) takes the whole
        # hydrate down and every column then reads as "not resident" for the wrong reason. Per
        # column, a failure is attributed to the column that caused it and the rest still load.
        for c in COLUMNS:
            try:
                xc.run_query(conn, f'EVALUATE ROW("v", DISTINCTCOUNT(fct_summary[{c}]))')
            except Exception as e:
                print(f"  hydrate FAILED for {c}: {str(e).splitlines()[0][:140]}", flush=True)
        ccols, crows = dmv_rows(conn, "SELECT * FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS")
        scols, srows = dmv_rows(conn, "SELECT * FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS")
    finally:
        conn.Close()

    # Print the DMV's own field names ONCE, always. On run 32681270789 every column came back with
    # USED_SIZE / DICTIONARY_SIZE / RECORDS_COUNT = 0 while BITS_COUNT and COMPRESSION_TYPE were
    # populated and varying — so the rows are real and correctly matched, and only the SIZE fields
    # are absent or spelled differently here. The dump below is how that gets settled from evidence
    # instead of guessed; the old one only fired when NO column matched, so it never printed.
    print(f"  column  DMV fields: {ccols}", flush=True)
    print(f"  segment DMV fields: {scols}", flush=True)

    out = {}
    for row in crows:
        # the table name lives in DIMENSION_NAME (VertiPaq Analyzer's key); TABLE_ID is the
        # storage-object id ('H$…'/'R$…' prefixes for shadow tables — keep only the data table)
        tname = str(_pick(ccols, row, "DIMENSION_NAME", ""))
        tid = str(_pick(ccols, row, "TABLE_ID", ""))
        if tname != FACT or tid.startswith(("H$", "R$", "U$")):
            continue
        raw = _norm(str(_pick(ccols, row, "COLUMN_ID", "")))
        if raw.startswith("RowNumber"):
            continue
        out[_key(raw)] = {
            "name": raw,  # the DMV's own spelling, so the readout can show what it really returned
            "encoding": {1: "HASH", 2: "VALUE"}.get(_pick(ccols, row, "COLUMN_ENCODING"),
                                                    str(_pick(ccols, row, "COLUMN_ENCODING"))),
            "dictionary_mb": round((_pick(ccols, row, "DICTIONARY_SIZE") or 0) / 1e6, 2),
            # Direct Lake columns are PAGEABLE: the DMV describes every column whether or not its
            # data is in memory, and a non-resident dictionary reports SIZE 0. Without these two
            # flags a 0 is unreadable — it could mean "empty" or "not loaded" — which is what made
            # the all-zero readout on run 32681270789 impossible to interpret.
            "dictionary_resident": bool(_pick(ccols, row, "DICTIONARY_ISRESIDENT")),
            "dictionary_pageable": bool(_pick(ccols, row, "DICTIONARY_ISPAGEABLE")),
            "segments": [],
        }
    if not out:  # filter matched nothing — dump reality so the next fix is fact-based
        for r in crows[:6]:
            print(f"  DMV row sample: {r}", flush=True)
    for row in srows:
        tname = str(_pick(scols, row, "DIMENSION_NAME", ""))
        tid = str(_pick(scols, row, "TABLE_ID", ""))
        if tname != FACT or tid.startswith(("H$", "R$", "U$")):
            continue
        col = _key(str(_pick(scols, row, "COLUMN_ID", "")))
        if col not in out:
            continue
        out[col]["segments"].append({
            "rows": int(_pick(scols, row, "RECORDS_COUNT") or 0),
            "used_mb": round((_pick(scols, row, "USED_SIZE") or 0) / 1e6, 2),
            "allocated_mb": round((_pick(scols, row, "ALLOCATED_SIZE") or 0) / 1e6, 2),
            "compression": str(_pick(scols, row, "COMPRESSION_TYPE", "")),
            "bits": int(_pick(scols, row, "BITS_COUNT") or 0),
            # Same reason as the dictionary flags above. VERTIPAQ_STATE names what the engine has
            # actually done with the segment, which is the difference between "the transcode chose
            # this" and "nothing has been transcoded yet".
            "state": str(_pick(scols, row, "VERTIPAQ_STATE", "")),
            "resident": bool(_pick(scols, row, "ISRESIDENT")),
            "pageable": bool(_pick(scols, row, "ISPAGEABLE")),
        })
    return out


def _runs(values):
    """'21x3/32x21' rather than 145 slash-separated numbers. At one row group the old spelling was
    readable; at 145 it is a 700-character cell that hides the thing it is meant to show."""
    out, prev, n = [], object(), 0
    for v in list(values) + [object()]:
        if v == prev:
            n += 1
            continue
        if n:
            out.append(f"{prev}x{n}" if n > 1 else f"{prev}")
        prev, n = v, 1
    return "/".join(out)


def render(model, data, out):
    out.append(f"\n### {model} — resident fact columns (VertiPaq DMVs)")
    out.append("| column | encoding | dict MB | dict res | segs | resident | used MB | alloc MB "
               "| bits/seg | compression/seg | state |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")
    # Show EVERY column, in this order: the ones asked for (so the table reads in schema order),
    # then anything the DMV returned that was not asked for. A requested column with no DMV row
    # gets a visible "not in DMV" row rather than vanishing.
    #
    # It used to `continue` past a missing column, which silently rendered a 5-row table for a
    # 9-column model and looked exactly like a complete one. A readout whose whole job is "what
    # did the transcode actually do" must never quietly drop a column.
    want = {_key(c): c for c in COLUMNS}
    extras = [c for c in sorted(data) if c not in want]
    for col in list(want) + extras:
        d = data.get(col)
        label = want.get(col) or (d or {}).get("name") or col
        if not d:
            out.append(f"| {label} | _not in DMV_ | | | | | | | | | |")
            continue
        segs = d["segments"]
        used = round(sum(s["used_mb"] for s in segs), 1)
        alloc = round(sum(s["allocated_mb"] for s in segs), 1)
        res = sum(1 for s in segs if s["resident"])
        out.append(f"| {label}{' *' if col in extras else ''} | {d['encoding']} "
                   f"| {d['dictionary_mb']} | {'Y' if d['dictionary_resident'] else 'n'} "
                   f"| {len(segs)} | {res}/{len(segs)} | {used} | {alloc} "
                   f"| {_runs(s['bits'] for s in segs)} | {_runs(s['compression'] for s in segs)} "
                   f"| {_runs(s['state'] for s in segs)} |")
    missing = [c for k, c in want.items() if k not in data]
    if missing or extras:
        out.append("")
        out.append(f"_requested but absent from the DMV: {missing or 'none'}; "
                   f"present but not requested (\\*): {extras or 'none'}_")
    print(f"  DMV returned {len(data)} fact column(s): {sorted(data)}", flush=True)
    if missing:
        print(f"  MISSING from the DMV: {missing}", flush=True)


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()
    xc._load_adomd(os.environ.get("ADOMD_DIR", "."))
    named = [m.strip() for m in (os.environ.get("DMV_MODELS") or "").split(",") if m.strip()]
    base, others = (named[0], named[1:]) if named else xc.discover_models()
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
