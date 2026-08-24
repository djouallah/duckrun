import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

from duckrun import auth  # noqa: E402
TOKEN = auth.get_fabric_token()  # self-acquire the Fabric control-plane token (Livy) from OIDC
BASE = (f"https://api.fabric.microsoft.com/v1/workspaces/{os.environ['WS_ID']}"
        f"/lakehouses/{os.environ['LH_ID']}/livyapi/versions/2023-12-01")
# The V-Order reference reads the pristine mart.fct_summary and depends on NO duckrun knob, so it
# never changes between runs — its OWN rebuild flag, not the shared FORCE_REBUILD, which the
# auto_sort geometry experiments flip on every dispatch (that used to drag a ~3-minute byte-identical
# Spark rebuild along each time). Rebuild it only when the source/row_limit actually changed.
FORCE = os.environ.get("FORCE_REBUILD_VORDER", "false").strip().lower() == "true"

# Read the source mart.fct_summary DIRECTLY (never a duckrun-written intermediate) so the V-Order
# input is never influenced by duckrun's write layout. The row cap is applied here, independently
# of the duckrun auto_sort build's own read.
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
# The cap is a CUTOFF on the leading sort column, exactly as build_duckdb_native computes it, NOT
# `LIMIT N`. LIMIT takes an arbitrary N rows, so the reference would hold a different row SET than
# the arms it is supposed to anchor — and a baseline measured on different data anchors nothing.
# Same expression on both sides means the same cutoff value and therefore the same rows.
_lead = (os.environ.get("OPT_SORT") or "date").split(",")[0].strip() or "date"
SOURCE = "mart.fct_summary" if N is None else (
    f"(SELECT * FROM mart.fct_summary WHERE `{_lead}` <= "
    f"(SELECT max(k) FROM (SELECT `{_lead}` AS k FROM mart.fct_summary ORDER BY k LIMIT {N})))")

# The output table name is a knob because the row-set contract is baked into the table: the
# historical `fct_summary_vorder` was built under LIMIT semantics, and skip-if-exists would keep
# serving those rows forever. A different contract gets a different name rather than a rebuild flag
# nobody remembers to set — and nothing existing is overwritten.
VARIANT = (os.environ.get("VORDER_TABLE") or "fct_summary_vorder").strip()
if not VARIANT.startswith("fct_summary_"):
    raise SystemExit(f"build_spark_variant: VORDER_TABLE must start with 'fct_summary_', "
                     f"got {VARIANT!r} — downstream get_stats globs on that prefix")
_V = VARIANT[len("fct_summary_"):]
VARIANTS = {_V: SOURCE}
# Human-readable sort provenance for the build metadata / summary layout matrix.
SORTS = {_V: "source order"}


def _record_build(variant, seconds, status):
    report.merge({"tables": {f"fct_summary_{variant}": {"build": {
        "engine": "spark", "sort": SORTS[variant], "vorder": True,
        "seconds": (round(seconds, 1) if seconds is not None else None),
        "status": status}}}})


def _spark_code(variant, source):
    return (
        'spark.sql("CREATE SCHEMA IF NOT EXISTS tests")\n'
        'spark.conf.set("spark.sql.parquet.vorder.default", "true")\n'
        f'(spark.sql("SELECT * FROM {source}")\n'
        '      .write.mode("overwrite").format("delta")\n'
        '      .option("parquet.vorder.enabled", "true")\n'
        f'      .saveAsTable("tests.fct_summary_{variant}"))\n'
        f'print("WRITE_OK tests.fct_summary_{variant} rows=" '
        f'+ str(spark.read.table("tests.fct_summary_{variant}").count()))\n'
    )


def _table_exists(qualified):
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"])
    try:
        con.sql(f"select 1 from {qualified} limit 1").fetchone()
        return True
    except Exception:
        return False


def _req(method, path, body=None):
    url = path if path.startswith("http") else f"{BASE}/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            txt = r.read().decode()
            return json.loads(txt) if txt.strip() else {}
    except urllib.error.HTTPError as e:
        sys.exit(f"Livy {method} {url} -> HTTP {e.code}: {e.read().decode()[:800]}")


def _poll_state(path, label, ok, bad, timeout, interval):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        st = _req("GET", path).get("state", "?")
        if st != last:
            print(f"  {label}: {st}", flush=True)
            last = st
        if st in ok:
            return st
        if st in bad:
            sys.exit(f"{label} entered terminal state '{st}'")
        time.sleep(interval)
    sys.exit(f"{label} timed out after {timeout}s")


def _run_statement(sid, code):
    stid = _req("POST", f"sessions/{sid}/statements", {"kind": "pyspark", "code": code})["id"]
    deadline = time.time() + 1800
    last = None
    while time.time() < deadline:
        s = _req("GET", f"sessions/{sid}/statements/{stid}")
        st = s.get("state", "?")
        if st != last:
            print(f"  statement {stid}: {st}", flush=True)
            last = st
        if st == "available":
            out = s.get("output", {}) or {}
            if out.get("status") == "error":
                tb = "\n".join(out.get("traceback", []) or [])
                sys.exit(f"Spark error {out.get('ename')}: {out.get('evalue')}\n{tb}")
            print(out.get("data", {}).get("text/plain", ""), flush=True)
            return
        if st in ("error", "cancelled", "cancelling"):
            sys.exit(f"statement entered '{st}'")
        time.sleep(10)
    sys.exit("statement timed out after 1800s")


def main():
    todo = {}
    for v, src in VARIANTS.items():
        out = f"tests.fct_summary_{v}"
        if not FORCE and _table_exists(out):
            print(f"{out} already exists — skipping (set rebuild=true to rebuild).", flush=True)
            _record_build(v, None, "skipped")
            continue
        # Probe the TABLE, not the source expression: the expression now carries the cutoff
        # subquery, and running it here just to answer "does the source exist" would make DuckDB
        # sort the whole 143M-row fact over OneLake for nothing.
        if not _table_exists("mart.fct_summary"):
            print("source mart.fct_summary not found — skipping.", flush=True)
            continue
        todo[v] = src
    if not todo:
        return
    print("Creating Livy session...", flush=True)
    sid = _req("POST", "sessions",
               {"name": "ci-spark-variants",
                "conf": {"spark.sql.parquet.vorder.default": "true"}})["id"]
    print(f"session id = {sid}", flush=True)
    try:
        _poll_state(f"sessions/{sid}", "session", {"idle"},
                    {"error", "dead", "killed", "shutting_down"}, timeout=900, interval=15)
        for v, src in todo.items():
            print(f"Building tests.fct_summary_{v} (V-Order, source {src})...", flush=True)
            t0 = time.perf_counter()
            _run_statement(sid, _spark_code(v, src))
            _record_build(v, time.perf_counter() - t0, "rebuilt")
    finally:
        print(f"Deleting session {sid}...", flush=True)
        try:
            _req("DELETE", f"sessions/{sid}")
        except SystemExit:
            pass


if __name__ == "__main__":
    main()
