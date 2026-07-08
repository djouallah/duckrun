import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

TOKEN = os.environ["FABRIC_TOKEN"]
BASE = (f"https://api.fabric.microsoft.com/v1/workspaces/{os.environ['WS_ID']}"
        f"/lakehouses/{os.environ['LH_ID']}/livyapi/versions/2023-12-01")
FORCE = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"

# Both derive from the shared limited base so all layouts hold the SAME rows: sorted reads the
# natural-order twin, notsorted reads the shuffled one.
VARIANTS = {"vorder_base_sorted": "tests.summary_sorted",
            "vorder_base_notsorted": "tests.summary_unsorted"}
# Human-readable sort provenance for the build metadata / summary layout matrix.
SORTS = {"vorder_base_sorted": "source order (sorted base)",
         "vorder_base_notsorted": "shuffled source"}


def _record_build(variant, seconds, status):
    report.merge({"tables": {f"fct_summary_{variant}": {"build": {
        "engine": "spark", "sort": SORTS[variant], "vorder": True,
        "seconds": (round(seconds, 1) if seconds is not None else None),
        "status": status}}}})


def _spark_code(variant, source):
    return (
        'spark.sql("CREATE SCHEMA IF NOT EXISTS tests")\n'
        'spark.conf.set("spark.sql.parquet.vorder.default", "true")\n'
        f'(spark.read.table("{source}")\n'
        '      .write.mode("overwrite").format("delta")\n'
        '      .option("parquet.vorder.enabled", "true")\n'
        f'      .saveAsTable("tests.fct_summary_{variant}"))\n'
        f'print("WRITE_OK tests.fct_summary_{variant} rows=" '
        f'+ str(spark.read.table("tests.fct_summary_{variant}").count()))\n'
    )


def _table_exists(qualified):
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"],
                          storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})
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
        if not _table_exists(src):
            print(f"source {src} not found — skipping {v}.", flush=True)
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
