"""Build mart.fct_summary_vorder on **Fabric Spark** via the Lakehouse Livy API.

V-Order is a Fabric-Spark-only write path — it cannot be produced on the CI runner (a plain
Spark / delta-rs write yields vorder=false). So instead of running Spark here, we open a Livy
SESSION against the lakehouse, submit a tiny PySpark CTAS with V-Order enabled as an inline
statement, wait for it, and tear the session down. The core run.Notebook stays pure-Python
(duckrun) — this is the only Spark in the repo, and it runs remotely on Fabric.

Env in:
  FABRIC_TOKEN — bearer token for https://api.fabric.microsoft.com (minted in the workflow via az)
  WS_ID, LH_ID — workspace + lakehouse GUIDs (exported by pipeline Phase 1)

Exits non-zero on any Livy/Spark error.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

TOKEN = os.environ["FABRIC_TOKEN"]
BASE = (f"https://api.fabric.microsoft.com/v1/workspaces/{os.environ['WS_ID']}"
        f"/lakehouses/{os.environ['LH_ID']}/livyapi/versions/2023-12-01")
FORCE = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"

# One Spark cell: enable V-Order, then overwrite mart.fct_summary_vorder from the delta-rs base.
# The Livy session is created under the lakehouse, so mart.* resolves to its schema-enabled tables.
SPARK_CODE = '''
spark.conf.set("spark.sql.parquet.vorder.default", "true")
(spark.read.table("mart.fct_summary")
      .write.mode("overwrite").format("delta")
      .option("parquet.vorder.enabled", "true")
      .saveAsTable("mart.fct_summary_vorder"))
print("VORDER_WRITE_OK rows=" + str(spark.read.table("mart.fct_summary_vorder").count()))
'''


def vorder_exists():
    """Cheap LOCAL existence check over OneLake (duckrun/duckdb) — so we can skip spinning up a
    Livy Spark session entirely when the table is already there. NO Spark involved."""
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/mart",
                          storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})
    try:
        con.sql("select 1 from mart.fct_summary_vorder limit 1").fetchone()
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


def main():
    if not FORCE and vorder_exists():
        print("mart.fct_summary_vorder already exists — skipping Livy/Spark entirely "
              "(set rebuild=true to rebuild).", flush=True)
        return
    print("Creating Livy session...", flush=True)
    sid = _req("POST", "sessions",
               {"name": "ci-vorder",
                "conf": {"spark.sql.parquet.vorder.default": "true"}})["id"]
    print(f"session id = {sid}", flush=True)
    try:
        _poll_state(f"sessions/{sid}", "session", {"idle"},
                    {"error", "dead", "killed", "shutting_down"}, timeout=900, interval=15)

        print("Submitting V-Order write statement...", flush=True)
        stid = _req("POST", f"sessions/{sid}/statements",
                    {"kind": "pyspark", "code": SPARK_CODE})["id"]

        deadline = time.time() + 1800
        last = None
        while time.time() < deadline:
            s = _req("GET", f"sessions/{sid}/statements/{stid}")
            st = s.get("state", "?")
            if st != last:
                print(f"  statement: {st}", flush=True)
                last = st
            if st == "available":
                out = s.get("output", {}) or {}
                if out.get("status") == "error":
                    tb = "\n".join(out.get("traceback", []) or [])
                    sys.exit(f"Spark error {out.get('ename')}: {out.get('evalue')}\n{tb}")
                print(out.get("data", {}).get("text/plain", ""), flush=True)
                print("V-Order table build complete.", flush=True)
                return
            if st in ("error", "cancelled", "cancelling"):
                sys.exit(f"statement entered '{st}'")
            time.sleep(10)
        sys.exit("statement timed out after 1800s")
    finally:
        print(f"Deleting session {sid}...", flush=True)
        try:
            _req("DELETE", f"sessions/{sid}")
        except SystemExit:
            pass


if __name__ == "__main__":
    main()
