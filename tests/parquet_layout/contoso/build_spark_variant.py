"""Build the Fabric Spark V-Order reference copy of the Contoso Sales fact:
tests.sales_vorder = the raw generator sales.parquet, written with V-Order enabled (Spark-only) via
the Livy API. Mirror of the AEMO benchmark's build_spark_variant.py, retargeted to the Contoso base.

Spark reads the RAW generator ``sales.parquet`` straight from the lakehouse **Files** section (uploaded
byte-verbatim by build_base.py) — NOT a duckrun-written Delta table. V-Order preserves input row order,
so reading a duckrun-clustered Delta would seed Spark's clustering; reading the pristine parquet keeps
the comparison fair: Spark and duckrun both start from the identical raw input and only their own
layout engine differs.

Env in: FABRIC_TOKEN, WS_ID, LH_ID, ONELAKE_TABLES_PATH, ONELAKE_TOKEN, FORCE_REBUILD,
        BENCH_ROW_LIMIT (optional).
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402
import build_base  # noqa: E402  — sales_files_urls / _files_exists: one source of truth for the path

TOKEN = os.environ["FABRIC_TOKEN"]
BASE = (f"https://api.fabric.microsoft.com/v1/workspaces/{os.environ['WS_ID']}"
        f"/lakehouses/{os.environ['LH_ID']}/livyapi/versions/2023-12-01")
FORCE = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"

_ABFSS_URL, _STORE_ROOT = build_base.sales_files_urls()
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None
# Spark reads the raw parquet from Files; .limit(N) applies the optional row cap.
READER = f'spark.read.parquet("{_ABFSS_URL}")' + (f".limit({N})" if N else "")
SOURCE_DESC = build_base.SALES_FILES_REL + (f" (limit {N})" if N else "")

VARIANTS = {"vorder": READER}
SORTS = {"vorder": "source order"}


def _record_build(variant, seconds, status):
    report.merge({"tables": {f"sales_{variant}": {"build": {
        "engine": "spark", "sort": SORTS[variant], "vorder": True,
        "seconds": (round(seconds, 1) if seconds is not None else None),
        "status": status}}}})


def _spark_code(variant, reader):
    return (
        'spark.sql("CREATE SCHEMA IF NOT EXISTS tests")\n'
        'spark.conf.set("spark.sql.parquet.vorder.default", "true")\n'
        f'({reader}\n'
        '      .write.mode("overwrite").format("delta")\n'
        '      .option("parquet.vorder.enabled", "true")\n'
        '      .option("overwriteSchema", "true")\n'   # a rebuild replaces schema too (raw parquet
        f'      .saveAsTable("tests.sales_{variant}"))\n'   # types differ from the old duckrun Delta)
        f'print("WRITE_OK tests.sales_{variant} rows=" '
        f'+ str(spark.read.table("tests.sales_{variant}").count()))\n'
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
    src_ok = build_base._files_exists(_STORE_ROOT, os.environ.get("ONELAKE_TOKEN"))
    todo = {}
    for v, reader in VARIANTS.items():
        out = f"tests.sales_{v}"
        if not FORCE and _table_exists(out):
            print(f"{out} already exists — skipping (set rebuild=true to rebuild).", flush=True)
            _record_build(v, None, "skipped")
            continue
        if not src_ok:
            print(f"source {SOURCE_DESC} not found in Files — skipping {v}.", flush=True)
            continue
        todo[v] = reader
    if not todo:
        return
    print("Creating Livy session...", flush=True)
    sid = _req("POST", "sessions",
               {"name": "ci-spark-contoso",
                "conf": {"spark.sql.parquet.vorder.default": "true"}})["id"]
    print(f"session id = {sid}", flush=True)
    try:
        _poll_state(f"sessions/{sid}", "session", {"idle"},
                    {"error", "dead", "killed", "shutting_down"}, timeout=900, interval=15)
        for v, reader in todo.items():
            print(f"Building tests.sales_{v} (V-Order, source {SOURCE_DESC})...", flush=True)
            t0 = time.perf_counter()
            _run_statement(sid, _spark_code(v, reader))
            _record_build(v, time.perf_counter() - t0, "rebuilt")
    finally:
        print(f"Deleting session {sid}...", flush=True)
        try:
            _req("DELETE", f"sessions/{sid}")
        except SystemExit:
            pass


if __name__ == "__main__":
    main()
