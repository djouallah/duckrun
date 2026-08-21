"""Read capacity-unit consumption per Fabric item from the Capacity Metrics app.

WHY THIS IS FIDDLY, up front:

* CU keeps accruing for roughly 70 minutes after the work finishes — ingestion lags ~6 min and
  smoothing spreads a burst over 5-64 min. A read taken right after a run is therefore a genuine
  LOWER BOUND, not the final bill. `read` records it as such; `settle` re-reads later and keeps
  max(old, new), so re-reads are idempotent and only ever correct upward.
* Operation name is the ONLY thing separating compute from storage when they share an item, so
  everything is grouped by (item, operation) and never collapsed to a single number per item.
* The metrics model's fact table is DirectQuery (live), but its `Items` dimension is import-mode
  and lags, so a just-deleted item may have CU rows with no name. We join on GUID and carry our own
  labels — never on display name.
* Deleted items keep their CU rows indefinitely. Tearing down the benchmark costs no measurement.
* Retention is 14 days: any window floor is clamped to now-14d or the query returns nothing useful.
* Column names differ across app versions, so they are resolved from INFO.VIEW.COLUMNS() rather
  than hardcoded.
* One capacity per query — filtering to several returns an opaque "Internal Error".

Usage:  cu.py read    --items '{"deltars":"<guid>",...}' --since-iso <utc> [--label <run id>]
        cu.py settle  [--run <run id>]        # re-read a past run from the ledger, keep the max

Env: METRICS_WORKSPACE_ID, METRICS_MODEL_ID, CAPACITY_ID, ADOMD_DIR, RUN_REPORT.
"""
import argparse
import datetime as dt
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cold_bench  # noqa: E402  — reuse _load_adomd / open_conn / run_query
import report  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.path.join(HERE, "cu_history.json")
RETENTION_DAYS = 14
FACT = "Metrics By Item Operation And Hour"


def _rows(conn, dax):
    """Execute dax and return a list of tuples (all columns), unlike cold_bench.run_query which
    only times and drains."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    reader = AdomdCommand(dax, conn).ExecuteReader()
    out = []
    try:
        fc = reader.FieldCount
        while reader.Read():
            out.append(tuple(reader.GetValue(i) for i in range(fc)))
    finally:
        reader.Close()
    return out


def _resolve_columns(conn):
    """Find the real column names in this version of the app. Getting the date column wrong is the
    dangerous failure: a 'Datetime' vs 'Date' mixup silently widens the window instead of erroring."""
    rows = _rows(conn, 'EVALUATE SELECTCOLUMNS(INFO.VIEW.COLUMNS(), "t", [Table], "c", [Name])')
    cols = [(str(t), str(c)) for t, c in rows]
    fact_cols = [c for t, c in cols if t == FACT]
    if not fact_cols:
        raise SystemExit(f"'{FACT}' not found in the metrics model — is METRICS_MODEL_ID correct? "
                         f"tables seen: {sorted({t for t, _ in cols})}")

    def pick(*candidates, required=True):
        for want in candidates:
            for c in fact_cols:
                if c.lower() == want.lower():
                    return c
        for want in candidates:                      # substring fallback
            for c in fact_cols:
                if want.lower() in c.lower():
                    return c
        if required:
            raise SystemExit(f"no column like {candidates} in '{FACT}'; have: {sorted(fact_cols)}")
        return None

    return {
        "item": pick("ItemId", "Item Id", "ItemGuid"),
        "operation": pick("OperationName", "Operation Name", "Operation"),
        "cu": pick("sum_CU", "CU", "Total CU", "CU (s)"),
        "date": pick("Date", "Datetime", "DateTime"),
        "duration": pick("Duration (s)", "sum_duration", "Duration", required=False),
        "capacity": pick("CapacityId", "Capacity Id", required=False),
    }


def _query(conn, cols, capacity_id, since):
    since = max(since, dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RETENTION_DAYS))
    stamp = since.strftime("%Y-%m-%dT%H:%M:%S")
    filters = [f"'{FACT}'[{cols['date']}] >= DATEVALUE(\"{since.strftime('%Y-%m-%d')}\")"]
    if cols["capacity"]:
        filters.append(f"'{FACT}'[{cols['capacity']}] = \"{capacity_id}\"")
    measures = [f'"cu", SUM(\'{FACT}\'[{cols["cu"]}])']
    if cols["duration"]:
        measures.append(f'"dur", SUM(\'{FACT}\'[{cols["duration"]}])')
    dax = (f"EVALUATE SUMMARIZECOLUMNS("
           f"'{FACT}'[{cols['item']}], '{FACT}'[{cols['operation']}], "
           + ", ".join(f"FILTER(ALL('{FACT}'), {f})" for f in filters) + ", "
           + ", ".join(measures) + ")")
    print(f"  window from {stamp}Z, capacity {capacity_id[:8]}...", flush=True)
    return _rows(conn, dax)


_GUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def _names(ws, model, token):
    """XMLA addresses a workspace and a dataset by DISPLAY NAME, not GUID — `Data Source=…/myorg/
    {workspace}` and `Initial Catalog={dataset}`. The secrets hold GUIDs (stable, unambiguous), so
    resolve them here; anything that isn't a GUID is passed through as an already-correct name."""
    import requests
    api = "https://api.powerbi.com/v1.0/myorg"
    h = {"Authorization": f"Bearer {token}"}
    ws_name = ws
    if _GUID.match(ws):
        r = requests.get(f"{api}/groups/{ws}", headers=h, timeout=60)
        r.raise_for_status()
        ws_name = r.json()["name"]
    model_name = model
    if _GUID.match(model):
        r = requests.get(f"{api}/groups/{ws}/datasets/{model}", headers=h, timeout=60)
        r.raise_for_status()
        model_name = r.json()["name"]
    print(f"  metrics model: {model_name!r} in workspace {ws_name!r}", flush=True)
    return ws_name, model_name


def _collect(items, since):
    cold_bench._load_adomd(os.environ["ADOMD_DIR"])
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()
    ws_name, model_name = _names(os.environ["METRICS_WORKSPACE_ID"],
                                 os.environ["METRICS_MODEL_ID"], token)
    conn = cold_bench.open_conn(ws_name, model_name, token)
    try:
        cols = _resolve_columns(conn)
        print(f"  resolved columns: {cols}", flush=True)
        rows = _query(conn, cols, os.environ["CAPACITY_ID"], since)
    finally:
        conn.Close()

    wanted = {str(v).lower(): k for k, v in items.items()}
    out = {k: {} for k in items}
    for r in rows:
        guid = str(r[0]).lower()
        label = wanted.get(guid)
        if label is None:
            continue
        op = str(r[1])
        cu = float(r[2] or 0)
        out[label][op] = max(out[label].get(op, 0.0), cu)
    return out


def _load_ledger():
    if os.path.exists(LEDGER):
        with open(LEDGER, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_ledger(led):
    with open(LEDGER, "w", encoding="utf-8") as f:
        json.dump(led, f, indent=2, sort_keys=True)


def _merge_max(old, new):
    """CU only ever grows as smoothing lands, so a re-read takes the max. This makes `settle`
    idempotent and self-correcting for an undercounted first read."""
    merged = dict(old or {})
    for op, cu in (new or {}).items():
        merged[op] = max(float(merged.get(op, 0.0)), float(cu))
    return merged


def _print(per_item, settled):
    # NB single-line f-string expressions only — CI is Python 3.11, where a multi-line expression
    # inside the braces is a SyntaxError (PEP 701 relaxed that in 3.12, which is what runs locally).
    note = "settled" if settled else "LOWER BOUND — CU accrues for ~70 min after the run"
    print(f"\n## CU by item and operation ({note})", flush=True)
    for label, ops in sorted(per_item.items()):
        total = sum(ops.values())
        print(f"  {label:<10} total {total:>10.1f} CU-s", flush=True)
        for op, cu in sorted(ops.items(), key=lambda kv: -kv[1]):
            print(f"      {op:<34} {cu:>10.1f}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["read", "settle"])
    ap.add_argument("--items", help='JSON {"label": "item-guid"}')
    ap.add_argument("--since-iso", help="UTC start of the run window")
    ap.add_argument("--label", help="run id used as the ledger key")
    ap.add_argument("--run", help="settle: ledger key to re-read (default: every unsettled run)")
    a = ap.parse_args()

    led = _load_ledger()
    if a.cmd == "read":
        items = json.loads(a.items)
        since = dt.datetime.fromisoformat(a.since_iso.replace("Z", "+00:00"))
        per_item = _collect(items, since)
        key = a.label or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        entry = led.get(key, {})
        entry["items"] = items
        entry["since"] = since.isoformat()
        entry["settled"] = False
        entry["cu"] = {k: _merge_max(entry.get("cu", {}).get(k), v) for k, v in per_item.items()}
        led[key] = entry
        _save_ledger(led)
        _print(per_item, settled=False)
        report.merge({"cu": {"run": key, "settled": False, "by_item": per_item}})
        return

    targets = [a.run] if a.run else [k for k, v in led.items() if not v.get("settled")]
    if not targets:
        print("nothing to settle", flush=True)
        return
    for key in targets:
        entry = led.get(key)
        if not entry:
            raise SystemExit(f"no ledger entry {key!r}; have: {sorted(led)}")
        since = dt.datetime.fromisoformat(entry["since"])
        per_item = _collect(entry["items"], since)
        entry["cu"] = {k: _merge_max(entry.get("cu", {}).get(k), v) for k, v in per_item.items()}
        # Only call it settled once we are past the ~70 min smoothing tail.
        age = dt.datetime.now(dt.timezone.utc) - since
        entry["settled"] = age > dt.timedelta(minutes=75)
        led[key] = entry
        print(f"\n{key} (age {age}):", flush=True)
        _print(entry["cu"], settled=entry["settled"])
    _save_ledger(led)


if __name__ == "__main__":
    main()
