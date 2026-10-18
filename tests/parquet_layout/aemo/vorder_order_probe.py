"""Does V-Order PERMUTE rows, or only re-encode them? Feed Spark a table whose physical order is
exactly known and diff what comes back.

Row-group footers can't answer this (min/max/ndv are order-blind), and the layout benchmark's
overlap score only sees BETWEEN-row-group clustering. This probe pins the input order with an
explicit ``rid`` (row_number over the sort that produced the physical order), then:

  seed   — ONE local parquet file via DuckDB COPY (never delta-rs: its multi-batch writes can
           interleave row groups), rid-ascending by construction, uploaded to Files/probe/.
  plain  — Spark reads the seed, ``.coalesce(1)``, writes Delta with V-Order OFF. The control:
           whatever reordering IT shows is Spark's read/write plumbing, not V-Order.
  vorder — byte-identical pipeline with V-Order ON. Only the option differs.

Then DuckDB reads each result's live parquet in PHYSICAL order (file_row_number) and reports, per
row group: rid descents/breaks (0 = input order survived), rid span, and measured value-runs per
column (the quantity RLE actually sees — compare vorder vs plain to see what a permutation, if
any, optimised). The Delta log's own vorder flag guards the control: Fabric enables V-Order by
default on some runtimes, so a ``plain`` table that comes back flagged vorder=True aborts the
probe rather than reporting a contaminated A/B.

``PROBE_SEED_ORDER`` picks what the known order IS: ``sorted`` (date,time,DUID — the layout a
tuned fact already has) or ``shuffle`` (ORDER BY hash of the key — the worst case, maximum
incentive for V-Order to reorder). ``rid`` is stamped AFTER that ordering, so the seed is
rid-ascending physically either way and descent detection works identically; the shuffle names
get their own ``_shuffle`` seed/table so both experiments coexist.

Env in: ONELAKE_TABLES_PATH, WS_ID, LH_ID (resolve_env.py), PROBE_ROWS (default 20M),
PROBE_SEED_ORDER (sorted|shuffle, default sorted), FORCE_REBUILD. Writes only Files/probe/ and
tests.order_probe* — never touches mart.*; tables are left in place (skip-if-exists), like
every other harness table.
"""
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request

import duckrun
from duckrun import auth
from dbt.adapters.duckrun import engine

TABLES = os.environ["ONELAKE_TABLES_PATH"].rstrip("/")
assert TABLES.endswith("/Tables"), f"expected a /Tables path, got {TABLES}"
FILES_ROOT = TABLES[: -len("/Tables")] + "/Files"
SEED_ORDER = os.environ.get("PROBE_SEED_ORDER", "sorted").strip().lower()
assert SEED_ORDER in ("sorted", "shuffle"), f"PROBE_SEED_ORDER must be sorted|shuffle, got {SEED_ORDER}"
# hash() of the unique key = a deterministic shuffle: scrambled for every column, reproducible.
RID_OVER = ('"date", "time", "DUID"' if SEED_ORDER == "sorted"
            else 'hash("date", "time", "DUID")')
SUFFIX = "" if SEED_ORDER == "sorted" else "_shuffle"
SEED_REMOTE = f"{FILES_ROOT}/probe/order_probe{SUFFIX}.parquet"
FORCE = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
_rows = os.environ.get("PROBE_ROWS", "").strip()
N = int(_rows) if _rows.isdigit() and int(_rows) > 0 else 20_000_000

PROBE_COLS = ["date", "time", "DUID", "mw", "price"]
VARIANTS = {"plain": "false", "vorder": "true"}   # table suffix -> parquet.vorder.enabled


# ---------------------------------------------------------------- Livy (same shape as
# build_spark_variant.py; copied, not imported — that module mints its token at import time)
def _req(method, path, body=None):
    base = (f"https://api.fabric.microsoft.com/v1/workspaces/{os.environ['WS_ID']}"
            f"/lakehouses/{os.environ['LH_ID']}/livyapi/versions/2023-12-01")
    url = path if path.startswith("http") else f"{base}/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {auth.get_fabric_token()}",
        "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            txt = r.read().decode()
            return json.loads(txt) if txt.strip() else {}
    except urllib.error.HTTPError as e:
        sys.exit(f"Livy {method} {url} -> HTTP {e.code}: {e.read().decode()[:800]}")


def _poll_session(sid):
    deadline = time.time() + 900
    last = None
    while time.time() < deadline:
        st = _req("GET", f"sessions/{sid}").get("state", "?")
        if st != last:
            print(f"  session: {st}", flush=True)
            last = st
        if st == "idle":
            return
        if st in ("error", "dead", "killed", "shutting_down"):
            sys.exit(f"session entered terminal state '{st}'")
        time.sleep(15)
    sys.exit("session timed out after 900s")


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


def _spark_code(variant, enabled):
    # Session default is pinned OFF at creation; the per-write option is the ONLY toggle, so the
    # two writes differ in exactly one boolean. coalesce(1) = a single task, pinning Spark's own
    # plumbing order so the plain control is a real noise floor.
    tbl = f"tests.order_probe{SUFFIX}_{variant}"
    return (
        'spark.sql("CREATE SCHEMA IF NOT EXISTS tests")\n'
        f'df = spark.read.parquet("{SEED_REMOTE}").coalesce(1)\n'
        f'(df.write.mode("overwrite").format("delta")\n'
        f'   .option("parquet.vorder.enabled", "{enabled}")\n'
        f'   .saveAsTable("{tbl}"))\n'
        f'print("WRITE_OK {tbl} rows=" + str(spark.read.table("{tbl}").count()))\n'
    )


# ---------------------------------------------------------------- probe SQL
def _order_metrics(con, files, label):
    """Physical-order metrics per (file, row group) for a list of parquet files: rid descents /
    sequence breaks (against the seed's rid-ascending contract), rid span, and per-column value
    runs — measured in file_row_number order, the order a parquet reader (and the Direct Lake
    transcode) consumes. DUID-major descents are computed too, to characterise a permutation if
    one shows up. The lag window spans the whole file, so a run crossing a row-group boundary is
    charged to the later group — same convention for every table, so deltas stay comparable."""
    lit = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    runs = ", ".join(
        f"SUM(c_{c}) AS runs_{c}" for c in [x.lower() for x in PROBE_COLS])
    changes = ", ".join(
        f'CASE WHEN "{c}" IS DISTINCT FROM lag("{c}") OVER w THEN 1 ELSE 0 END AS c_{c.lower()}'
        for c in PROBE_COLS)
    rows = con.sql(f"""
        WITH meta AS (
            SELECT file_name, row_group_id, MAX(row_group_num_rows) AS n
            FROM parquet_metadata({lit}) GROUP BY 1, 2),
        b AS (
            SELECT file_name, row_group_id, n,
                   SUM(n) OVER (PARTITION BY file_name ORDER BY row_group_id
                                ROWS UNBOUNDED PRECEDING) - n AS lo
            FROM meta),
        d AS (
            SELECT filename, file_row_number AS frn, rid,
                   rid - lag(rid) OVER w AS drid,
                   CASE WHEN "DUID" < lag("DUID") OVER w THEN 1 ELSE 0 END AS desc_duid,
                   {changes}
            FROM parquet_scan({lit}, file_row_number=1, filename=1)
            WINDOW w AS (PARTITION BY filename ORDER BY file_row_number))
        SELECT b.file_name, b.row_group_id, b.n AS rows,
               COUNT(*) FILTER (WHERE drid < 0)                    AS rid_descents,
               COUNT(*) FILTER (WHERE drid IS NOT NULL AND drid <> 1) AS rid_breaks,
               SUM(desc_duid)                                      AS duid_descents,
               MIN(rid) AS rid_min, MAX(rid) AS rid_max,
               {runs}
        FROM d JOIN b ON d.filename = b.file_name AND d.frn >= b.lo AND d.frn < b.lo + b.n
        GROUP BY b.file_name, b.row_group_id, b.n
        ORDER BY b.file_name, b.row_group_id""").fetchall()
    cols = (["file", "rg", "rows", "rid_descents", "rid_breaks", "duid_descents",
             "rid_min", "rid_max"] + [f"runs_{c.lower()}" for c in PROBE_COLS])
    out = [dict(zip(cols, r)) for r in rows]
    print(f"\n[{label}] {len(files)} file(s), {len(out)} row group(s):", flush=True)
    for r in out:
        print("  " + " ".join(f"{k}={r[k]:,}" if isinstance(r[k], int) else f"{k}={r[k]}"
                              for k in cols[1:]), flush=True)
    return out


def _table_files(con, table):
    files, _, vorder, _, _ = engine.delta_file_summary(
        con.con, f"{TABLES}/tests/{table}", con.storage_options)
    return files, vorder


def _exists(con, table):
    try:
        con.sql(f"select 1 from tests.{table} limit 1").fetchone()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------- verdict
def _tot(metrics, key):
    return sum(r[key] for r in metrics)


def _verdict(seed, plain, vorder):
    lines = [f"## V-Order reordering probe — seed order: {SEED_ORDER}", "",
             f"seed: {sum(r['rows'] for r in seed):,} rows, rid strictly ascending "
             f"(descents={_tot(seed, 'rid_descents')})", ""]
    hdr = ["table", "row groups", "rid descents", "rid breaks", "DUID descents"] + \
          [f"runs {c}" for c in PROBE_COLS]
    rows = []
    for label, m in (("seed (input)", seed), ("plain (control)", plain), ("vorder", vorder)):
        rows.append([label, str(len(m)), f"{_tot(m, 'rid_descents'):,}",
                     f"{_tot(m, 'rid_breaks'):,}", f"{_tot(m, 'duid_descents'):,}"]
                    + [f"{_tot(m, 'runs_' + c.lower()):,}" for c in PROBE_COLS])
    lines.append("| " + " | ".join(hdr) + " |")
    lines.append("|" + "---|" * len(hdr))
    lines += ["| " + " | ".join(r) + " |" for r in rows]
    lines.append("")

    pd, vd = _tot(plain, "rid_descents"), _tot(vorder, "rid_descents")
    if pd > 0:
        lines.append(f"**Inconclusive:** the plain control itself reordered rows "
                     f"({pd:,} descents) — Spark's plumbing is not order-preserving here, so "
                     f"V-Order's contribution can't be isolated from this A/B.")
    elif vd == 0:
        lines.append("**Verdict: NO reordering.** V-Order preserved the input row order exactly "
                     "(0 descents, control clean) — its wins are encoding-side "
                     "(dictionary-everything), not a row permutation.")
    else:
        # rid spans per row group tell global vs local: disjoint spans = rows stayed in their
        # row group and were permuted inside it; overlapping spans = a wider shuffle.
        spans = sorted((r["rid_min"], r["rid_max"]) for r in vorder)
        local = all(a[1] < b[0] for a, b in zip(spans, spans[1:]))
        scope = ("within row groups only (rid spans stay disjoint)" if local
                 else "across row groups (rid spans overlap)")
        lines.append(f"**Verdict: REORDERED.** {vd:,} rid descents (control 0), scope: {scope}. "
                     f"Run-count deltas vs plain show what the permutation optimised.")
    text = "\n".join(lines)
    print("\n" + text, flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")


def main():
    con = duckrun.connect(TABLES)

    # 1) seed — build locally (single COPY file, explicit ORDER BY ⇒ rid-ascending bytes),
    # verify, upload. Skip when the remote seed already exists unless rebuild.
    seed_ok = False
    if not FORCE:
        try:
            con.sql(f"SELECT COUNT(*) FROM parquet_metadata('{SEED_REMOTE}')").fetchone()
            seed_ok = True
            print(f"seed {SEED_REMOTE} already exists — skipping build", flush=True)
        except Exception:
            pass
    if not seed_ok:
        scratch = tempfile.mkdtemp(prefix="order_probe_")
        local = os.path.join(scratch, f"order_probe{SUFFIX}.parquet").replace("\\", "/")
        print(f"Building seed ({N:,} rows) locally...", flush=True)
        con.con.execute(f"""
            COPY (SELECT row_number() OVER (ORDER BY {RID_OVER}) AS rid,
                         "date", "time", "DUID", mw, price
                  FROM (SELECT * FROM mart.fct_summary LIMIT {N})
                  ORDER BY rid)
            TO '{local}' (FORMAT PARQUET, COMPRESSION SNAPPY)""")
        seed_local = _order_metrics(con, [local], "seed-local")
        if _tot(seed_local, "rid_descents") or _tot(seed_local, "rid_breaks"):
            sys.exit("seed file is not rid-ascending — aborting (COPY order contract broken?)")
        con.copy(scratch, "probe", [".parquet"], overwrite=True)
        shutil.rmtree(scratch, ignore_errors=True)
        print(f"seed uploaded to {SEED_REMOTE}", flush=True)

    # 2) the two Spark writes — one Livy session, V-Order default pinned OFF, per-write option
    # is the only difference between the variants.
    todo = {v: e for v, e in VARIANTS.items()
            if FORCE or not _exists(con, f"order_probe{SUFFIX}_{v}")}
    for v in VARIANTS:
        if v not in todo:
            print(f"tests.order_probe{SUFFIX}_{v} already exists — skipping (rebuild=true to rebuild)",
                  flush=True)
    if todo:
        print("Creating Livy session (vorder default OFF)...", flush=True)
        sid = _req("POST", "sessions",
                   {"name": "ci-order-probe",
                    "conf": {"spark.sql.parquet.vorder.default": "false"}})["id"]
        print(f"session id = {sid}", flush=True)
        try:
            _poll_session(sid)
            for v, enabled in todo.items():
                print(f"Building tests.order_probe{SUFFIX}_{v} (vorder={enabled})...", flush=True)
                _run_statement(sid, _spark_code(v, enabled))
        finally:
            print(f"Deleting session {sid}...", flush=True)
            try:
                _req("DELETE", f"sessions/{sid}")
            except SystemExit:
                pass

    # 3) probe — physical-order metrics for the seed and both results, then the verdict. The
    # Delta log's vorder flag guards the A/B before any row is read.
    plain_files, plain_vorder = _table_files(con, f"order_probe{SUFFIX}_plain")
    vorder_files, vorder_vorder = _table_files(con, f"order_probe{SUFFIX}_vorder")
    print(f"\nDelta-log vorder flags: plain={plain_vorder} vorder={vorder_vorder}", flush=True)
    if plain_vorder:
        sys.exit(f"CONTROL CONTAMINATED: tests.order_probe{SUFFIX}_plain is flagged vorder=True in its "
                 "Delta log — the runtime's V-Order default won over the per-write option. "
                 "Fix the session/write config; this A/B proves nothing as-is.")
    if not vorder_vorder:
        print(f"WARN: tests.order_probe{SUFFIX}_vorder is NOT flagged vorder=True — the option may not "
              "have taken; treat a 'no reordering' verdict with suspicion.", flush=True)
    seed = _order_metrics(con, [SEED_REMOTE], "seed")
    plain = _order_metrics(con, plain_files, "plain")
    vorder = _order_metrics(con, vorder_files, "vorder")
    _verdict(seed, plain, vorder)


if __name__ == "__main__":
    main()
