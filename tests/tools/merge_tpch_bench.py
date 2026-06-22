"""Rigorous MERGE smoke/stress test on a big fact table — run entirely through the dbt path.

Generate one big TPCH ``lineitem`` (default SF=20, ~120M rows) with ``tpchgen-cli`` as parquet. A dbt
DAG then builds a CHAIN of Delta tables: the first model STREAMS the parquet ``source`` into its Delta
table, and everything downstream is ``ref`` to Delta tables duckrun writes — no manual convert, and
nothing ever materializes the whole fact in RAM:

  generate_data (python: tpchgen -> lineitem parquet; returns a tiny marker)
    -> mixed_upsert        (seeds by streaming the parquet source; then ~1% sample:
                            ~80% UPDATE existing keys + ~20% INSERT key-shifted)
    -> insert_only         (~5% sample key-shifted past max, future shipdate -> all INSERT)
    -> update_only         (~5% sample existing keys, 100% match -> row count unchanged)
    -> idempotent_remerge  (re-merge unchanged rows -> nothing changes)
    -> append_only         (~5% sample appended -- no target scan/join)
    -> safeappend_only     (~5% sample via safeappend -- version-guarded append)
    -> overwrite_all       (~5% sample, table overwrite -- replaces the whole table)

Each op is an incremental model: its first ``dbt run`` seeds the table from ``ref(previous)`` (the
chain), and a second ``dbt run`` applies the op (the merge runs through the materialization's
read-pin + the plugin's spill cap). Batches are sampled in-model with deterministic markers
(l_quantity = -1 / -2, l_shipdate = 2035) so the runner can verify the effect by querying the table.

    python tests/tools/merge_tpch_bench.py --dir /tmp/m --sf 20
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

import duckdb
import deltalake

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "integration_tests", "merge_spill"))
SCHEMA = "mart"
MARK_UPSERT = "-1.0"
MARK_UPDATE = "-2.0"
FUTURE_SHIPDATE = "2035-06-15"
# Full-surface scenarios (local-only): CDC tombstone/update markers + insert shipdate, full-sync
# update marker, and the expression-update marker.
MARK_CDC_DELETE = "-7.0"
MARK_CDC_UPDATE = "-8.0"
CDC_INSERT_SHIPDATE = "2036-01-01"
MARK_SYNC = "-5.0"
MARK_EXPR = "-9.0"


def _peak_rss_mb():
    """Best-effort peak memory in MB: Linux VmHWM, else Windows PeakWorkingSetSize; None if neither."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    try:
        import ctypes
        from ctypes import wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (n, ctypes.c_size_t) for n in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")
            ]

        k32 = ctypes.windll.kernel32
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        p = _PMC()
        p.cb = ctypes.sizeof(_PMC)
        if ctypes.windll.psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(p), p.cb):
            return p.PeakWorkingSetSize // (1024 * 1024)
    except Exception:
        pass
    return None


def _child_rss_peak_mb(pid):
    """Peak RSS (MB) of another process via /proc/<pid>/status VmHWM (the kernel's own high-water,
    falling back to the current VmRSS). None off Linux or once the process is gone. The whole merge
    runs in the spawned `dbt run` child, so this — not _peak_rss_mb() on the harness — is what
    measures the merge's real memory."""
    try:
        with open(f"/proc/{pid}/status") as fh:
            hwm = rss = None
            for line in fh:
                if line.startswith("VmHWM:"):
                    hwm = int(line.split()[1]) // 1024
                elif line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) // 1024
            return hwm if hwm is not None else rss
    except (OSError, ValueError, IndexError):
        return None


class Bench:
    def __init__(self, args):
        self.args = args
        # --warehouse abfss://… points the whole chain at OneLake (the small-N integration job);
        # the default is the local <dir>/warehouse the heavy SF=20 gate uses. Schema comes from
        # DBT_SCHEMA so the OneLake job can write into an isolated schema (no aemo collision).
        self.remote = bool(args.warehouse and args.warehouse.startswith("abfss://"))
        self.root = args.warehouse if args.warehouse else os.path.join(args.dir, "warehouse")
        self.schema = os.environ.get("DBT_SCHEMA", SCHEMA)
        self.con = duckdb.connect()
        self.con.execute("INSTALL delta; LOAD delta;")
        from dbt.adapters.duckrun import engine
        self.engine = engine
        engine.configure_duckdb_session(self.con)
        if self.remote:
            # The verification queries below delta_scan abfss://… directly, so this connection
            # needs the same Azure secret + transport the adapter mints at connection-open.
            from dbt.adapters.duckrun import secret
            secret.ensure_azure_secret(self.con, {"bearer_token": os.environ.get("ONELAKE_TOKEN", "")})
        self.peak_mb = None  # child-process peak RSS of the most recent dbt() call
        self.env = dict(os.environ)
        self.env["WAREHOUSE_PATH"] = self.root
        self.env["DBT_SCHEMA"] = self.schema
        # Turn on the adapter's in-process memory profiler so each child `dbt run` logs the
        # DuckDB-vs-delta_rs(+Arrow) split for its write/merge (see engine.mem_profile).
        self.env["DUCKRUN_MEM_PROFILE"] = "1"

    def path(self, model):
        return os.path.join(self.root, self.schema, model).replace(os.sep, "/")

    def count(self, model):
        return self.con.execute(f"SELECT count(*) FROM delta_scan('{self.path(model)}')").fetchone()[0]

    def q(self, sql):
        return self.con.execute(sql).fetchone()[0]

    def dbt(self, model, full_refresh=False):
        """One `dbt run --select <model>`; returns elapsed seconds and stashes the child process's
        peak RSS on self.peak_mb. Raises on non-zero exit.

        The merge runs in this spawned child (DuckDB + delta_rs both load into the dbt process), so
        we poll the CHILD's /proc RSS while it runs — the harness's own RSS never sees the merge.
        With DUCKRUN_MEM_PROFILE set in the env, the child also logs the DuckDB-vs-delta_rs split.

        ``full_refresh`` forces a fresh (re)create of the table — used to seed each chain model on
        OneLake, where tables persist across CI runs (local runs start from an rmtree'd warehouse).
        """
        cmd = ["dbt", "run", "--select", model, "--target", self.args.target,
               "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
        if full_refresh:
            cmd.append("--full-refresh")
        if self.args.spill_size is not None:
            cmd += ["--vars", f"{{merge_max_spill_size: {self.args.spill_size}}}"]
        t = time.time()
        proc = subprocess.Popen(cmd, env=self.env)
        peak = 0
        while proc.poll() is None:
            cur = _child_rss_peak_mb(proc.pid)
            if cur is not None and cur > peak:
                peak = cur
            time.sleep(0.05)
        self.peak_mb = peak or None
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        return time.time() - t

    def seed(self, model):
        """First run: seed this op's table from ref(previous) — no op applied yet. On OneLake the
        table may survive from a prior CI run, so force a full-refresh to recreate it fresh (locally
        the warehouse was rmtree'd at the start, so a plain run already creates it)."""
        self.dbt(model, full_refresh=self.remote)

    # -- per-op scenarios: seed already done; run the increment and verify by querying the table --

    def mixed(self, model):
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM delta_scan('{self.path(model)}')")
        dt = self.dbt(model)
        after = self.count(model)
        p = self.path(model)
        batch = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_UPSERT}")
        ins = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_UPSERT} AND l_orderkey > {base_max}")
        return {"name": "Mixed upsert", "src": batch, "upd": batch - ins, "ins": ins,
                "before": before, "after": after, "expected": before + ins, "dt": dt,
                "count_ok": after == before + ins, "verify_ok": (batch - ins) > 0 and ins > 0,
                "peak_mb": self.peak_mb,
                "verify": "existing keys updated (l_quantity=-1), key-shifted rows inserted"}

    def insert_only(self, model):
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM delta_scan('{self.path(model)}')")
        dt = self.dbt(model)
        after = self.count(model)
        p = self.path(model)
        ins = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_shipdate = DATE '{FUTURE_SHIPDATE}'")
        future_past_max = self.q(
            f"SELECT count(*) FROM delta_scan('{p}') WHERE l_shipdate = DATE '{FUTURE_SHIPDATE}' AND l_orderkey > {base_max}")
        return {"name": "Insert-only (future shipdate)", "src": ins, "upd": 0, "ins": ins,
                "before": before, "after": after, "expected": before + ins, "dt": dt,
                "count_ok": after == before + ins, "verify_ok": ins > 0 and future_past_max == ins,
                "peak_mb": self.peak_mb,
                "verify": f"{ins:,} rows carry the 2035 shipdate, all key-shifted (== inserts)"}

    def update_only(self, model):
        before = self.count(model)
        dt = self.dbt(model)
        after = self.count(model)
        marked = self.q(f"SELECT count(*) FROM delta_scan('{self.path(model)}') WHERE l_quantity = {MARK_UPDATE}")
        return {"name": "Update-only (100% match)", "src": marked, "upd": marked, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": marked > 0,
                "peak_mb": self.peak_mb,
                "verify": "row count unchanged; sampled rows carry l_quantity=-2"}

    def idempotent(self, model):
        # Seed already merged one unchanged batch; run it AGAIN — must change nothing.
        before = self.count(model)
        dt = self.dbt(model)
        after = self.count(model)
        return {"name": "Idempotent re-merge", "src": 0, "upd": 0, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": after == before,
                "peak_mb": self.peak_mb,
                "verify": "re-merging unchanged rows changed nothing"}

    def appendish(self, model, label):
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM delta_scan('{self.path(model)}')")
        dt = self.dbt(model)
        after = self.count(model)
        appended = self.q(f"SELECT count(*) FROM delta_scan('{self.path(model)}') WHERE l_orderkey > {base_max}")
        return {"name": f"{label} (no merge)", "src": appended, "upd": 0, "ins": appended,
                "before": before, "after": after, "expected": before + appended, "dt": dt,
                "count_ok": after == before + appended and appended > 0, "verify_ok": appended > 0,
                "peak_mb": self.peak_mb,
                "verify": f"appended {appended:,} key-shifted rows — no target scan"}

    def overwrite(self, model, prev_model):
        before = self.count(prev_model)  # the table this op conceptually replaces (chain tail)
        dt = self.dbt(model)
        after = self.count(model)
        return {"name": "Overwrite (no merge)", "src": after, "upd": 0, "ins": after,
                "before": before, "after": after, "expected": after, "dt": dt,
                "count_ok": 0 < after < before, "verify_ok": 0 < after < before,
                "peak_mb": self.peak_mb,
                "verify": f"table replaced with the {after:,}-row batch (no target scan)"}

    # -- full-surface MERGE scenarios (delta_rs's complete clause set via dbt merge_clauses /
    #    merge_update_set_expressions) — LOCAL stress only; the OneLake job skips them to stay fast --

    def cdc(self, model):
        """CDC change-set in ONE merge: matched DELETE (tombstones) + matched UPDATE + NOT MATCHED
        INSERT. Deletes are a deterministic 1% slice counted BEFORE the merge; inserts/updates carry
        markers counted after, so the net count closes (after == before + inserts - deletes)."""
        p = self.path(model)
        before = self.count(model)
        deletes = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_orderkey % 100 = 7")
        dt = self.dbt(model)
        after = self.count(model)
        inserts = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_shipdate = DATE '{CDC_INSERT_SHIPDATE}'")
        updates = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_CDC_UPDATE}")
        # the delete clause must have removed the tombstoned slice (none left, and none survived as
        # a stale -7 marker — that would mean the UPDATE clause caught them instead of DELETE).
        tombstones_left = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_CDC_DELETE}")
        slice_left = self.q(
            f"SELECT count(*) FROM delta_scan('{p}') WHERE l_orderkey % 100 = 7 "
            f"AND l_shipdate <> DATE '{CDC_INSERT_SHIPDATE}'")
        expected = before + inserts - deletes
        return {"name": "CDC merge (delete+update+insert)", "src": deletes + updates + inserts,
                "upd": updates, "ins": inserts, "before": before, "after": after,
                "expected": expected, "dt": dt, "count_ok": after == expected,
                "verify_ok": deletes > 0 and inserts > 0 and updates > 0
                             and tombstones_left == 0 and slice_left == 0,
                "peak_mb": self.peak_mb,
                "verify": f"deleted {deletes:,} tombstoned (slice gone, none updated), "
                          f"updated {updates:,} (-8), inserted {inserts:,} (2036 shipdate)"}

    def full_sync(self, model):
        """Full-dimension sync: matched UPDATE + WHEN NOT MATCHED BY SOURCE DELETE. The source is
        ~50% of the table (every odd orderkey), streamed (merge_streamed_exec) so the big source
        isn't materialized whole. Even orderkeys are 'departed' → by-source DELETE; odd → UPDATE."""
        p = self.path(model)
        before = self.count(model)
        departed = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_orderkey % 2 = 0")
        dt = self.dbt(model)
        after = self.count(model)
        updated = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_SYNC}")
        departed_left = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_orderkey % 2 = 0")
        expected = before - departed
        return {"name": "Full sync (update + by-source delete)", "src": before - departed,
                "upd": updated, "ins": 0, "before": before, "after": after,
                "expected": expected, "dt": dt, "count_ok": after == expected,
                "verify_ok": departed > 0 and departed_left == 0 and updated == after,
                "peak_mb": self.peak_mb,
                "verify": f"by-source-deleted {departed:,} departed keys; updated all {updated:,} survivors (-5)"}

    def expr_update(self, model):
        """100%-match update via merge_update_set_expressions: sets l_quantity = -9 and re-derives
        l_returnflag with a CASE over the source (arbitrary-expression path). Count unchanged."""
        p = self.path(model)
        before = self.count(model)
        dt = self.dbt(model)
        after = self.count(model)
        marked = self.q(f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_EXPR}")
        # every expression-updated row must also carry the CASE-derived flag ('H' or 'L') — proving
        # the expression ran, not a plain copy (lineitem's real flags are A/N/R).
        bad_case = self.q(
            f"SELECT count(*) FROM delta_scan('{p}') WHERE l_quantity = {MARK_EXPR} "
            f"AND l_returnflag NOT IN ('H', 'L')")
        return {"name": "Expression update (set_expressions + CASE)", "src": marked, "upd": marked,
                "ins": 0, "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": marked > 0 and bad_case == 0,
                "peak_mb": self.peak_mb,
                "verify": f"updated {marked:,} rows via expressions (l_quantity=-9, l_returnflag=CASE H/L)"}


def run(args):
    # Local runs start from a clean warehouse; the OneLake job can't rmtree a remote store, so it
    # relies on the per-seed --full-refresh (Bench.seed) to recreate each table fresh instead.
    if not (args.warehouse and args.warehouse.startswith("abfss://")):
        if os.path.exists(os.path.join(args.dir, "warehouse")):
            shutil.rmtree(os.path.join(args.dir, "warehouse"))

    b = Bench(args)
    # The generate_data python model runs tpchgen-cli (in the DAG) to write the lineitem PARQUET; the
    # runner only tells it the scale factor and a scratch dir. It returns a tiny marker — the data is
    # read by the `tpch.lineitem` parquet source (sources.yml), which mixed_upsert streams from. This
    # keeps generation and the Delta write separate: nothing collects the whole fact into RAM.
    b.env["MERGE_SPILL_SF"] = str(args.sf)
    b.env["MERGE_SPILL_GEN"] = os.path.join(args.dir, "gen")

    print(f"== generate base: generate_data runs tpchgen SF={args.sf} -> lineitem parquet source ==",
          flush=True)
    b.dbt("generate_data")
    gen_glob = os.path.join(b.env["MERGE_SPILL_GEN"], "lineitem", "*.parquet").replace(os.sep, "/")
    target_rows = b.q(f"SELECT count(*) FROM read_parquet('{gen_glob}')")
    eff = b.engine._effective_mem_limit_bytes()
    cap = b.engine._default_merge_spill_size()
    print(f"== base lineitem: {target_rows:,} rows | effective_mem="
          f"{None if eff is None else round(eff/1048576)}MB merge_cap="
          f"{None if cap is None else round(cap/1048576)}MB ==", flush=True)

    setup = {"duckdb_ver": duckdb.__version__, "deltalake_ver": deltalake.__version__,
             "sf": args.sf, "target_rows": target_rows,
             "eff_mb": None if eff is None else round(eff/1048576),
             "cap_mb": None if cap is None else round(cap/1048576)}

    # Chain: seed each op from the prior (ref), then apply the op.
    results = []
    b.seed("mixed_upsert");        results.append(b.mixed("mixed_upsert"))
    b.seed("insert_only");         results.append(b.insert_only("insert_only"))
    b.seed("update_only");         results.append(b.update_only("update_only"))
    b.seed("idempotent_remerge")
    b.dbt("idempotent_remerge")    # first increment (set-up); the reported run is the re-merge
    results.append(b.idempotent("idempotent_remerge"))
    # Full-surface MERGE scenarios — the complete delta_rs clause set (matched delete + multi-clause,
    # by-source delete, arbitrary/CASE update expressions) at scale, each seeded from a big (sampled)
    # base. LOCAL stress only: skipped on the OneLake integration run (remote), which keeps the small
    # path-smoke job fast and is the wrong place for these heavy, whole-target merges.
    if not b.remote:
        b.seed("cdc_merge");       results.append(b.cdc("cdc_merge"))
        b.seed("full_sync");       results.append(b.full_sync("full_sync"))
        b.seed("expr_update");     results.append(b.expr_update("expr_update"))
    b.seed("append_only");         results.append(b.appendish("append_only", "Append"))
    b.seed("safeappend_only");     results.append(b.appendish("safeappend_only", "Safeappend"))
    results.append(b.overwrite("overwrite_all", "safeappend_only"))

    ok = True
    for r in results:
        status = "OK" if (r["count_ok"] and r["verify_ok"]) else "FAIL"
        ok = ok and r["count_ok"] and r["verify_ok"]
        print(f"== [{status}] {r['name']}: {r['src']:,} rows ({r['upd']:,} upd + {r['ins']:,} ins) "
              f"in {r['dt']:.1f}s; rows {r['before']:,} -> {r['after']:,} (expected {r['expected']:,}); "
              f"{r['verify']} ==", flush=True)
        if not r["count_ok"]:
            print(f"   FAIL: row count {r['after']:,} != expected {r['expected']:,}", file=sys.stderr)
        if not r["verify_ok"]:
            print(f"   FAIL: value check failed ({r['verify']})", file=sys.stderr)

    final_rows = b.count("safeappend_only")
    # The merges run in the spawned `dbt run` children, so the headline peak is the worst child RSS
    # across the ops — not _peak_rss_mb() on this harness, which only ever runs verification queries.
    child_peaks = [r["peak_mb"] for r in results if r.get("peak_mb")]
    peak = max(child_peaks) if child_peaks else _peak_rss_mb()
    peak_s = f"{peak}MB" if peak is not None else "n/a (child /proc sampling is Linux-only)"
    print(f"== done: peak child RSS={peak_s} (chain tail grew to {final_rows:,} rows) ==", flush=True)
    _write_summary(setup, results, final_rows, peak, ok)
    if not ok:
        sys.exit(2)
    print("OK: duckrun ran the chain through the dbt path and survived.")


def _write_summary(setup, results, final_rows, peak, all_ok):
    card = _build_card(setup, results, final_rows, peak, all_ok)
    with open("docs/merge_card.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(card + "\n")
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(card + "\n")


def _build_card(setup, results, final_rows, peak, all_ok) -> str:
    peak_s = f"{peak:,} MB" if peak is not None else "n/a"
    L = ["## 🔀 Incremental MERGE test — duckrun on Delta Lake (via dbt)", ""]
    L.append("**What this checks:** that duckrun MERGEs incremental batches into a large Delta "
             "*fact* table **through the dbt path** — a chain of `ref`-ed incremental models (the "
             "materialization's read-pin + the plugin's spill cap) — applying UPDATEs and INSERTs "
             "correctly without being OOM-killed, and how the same shape compares against a plain "
             "`append` / `safeappend` / `overwrite` (which never scan the target).")
    L += ["", "### Setup (the inputs)", "| | |", "|---|---|"]
    L.append(f"| Engine | duckrun &middot; DuckDB {setup['duckdb_ver']} &middot; delta_rs {setup['deltalake_ver']} |")
    L.append(f"| Target fact table | TPCH `lineitem`, scale factor **{setup['sf']}** → **{setup['target_rows']:,} rows** |")
    L.append("| Primary key (merge on) | `(l_orderkey, l_linenumber)` |")
    L.append(f"| Effective memory | {setup['eff_mb']} MB (runner RAM, no artificial limit) |")
    L.append(f"| Merge spill cap | {setup['cap_mb']} MB — delta_rs `max_spill_size` |")
    L += ["", "### The operations (a chain — each builds on the previous via `ref`)"]
    L.append("1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE, ~20% key-shifted → INSERT.")
    L.append("2. **Insert-only (~5% sample):** key-shifted past max key, future `l_shipdate` (2035) → all INSERT.")
    L.append("3. **Update-only (~5% sample):** existing keys, no shift → 100% match; row count unchanged.")
    L.append("4. **Idempotent re-merge:** re-merge unchanged rows → nothing changes.")
    L.append("5. **CDC merge (full clause set):** one MERGE that DELETEs a tombstoned slice, UPDATEs a "
             "sample, and INSERTs key-shifted rows — matched-delete + matched-update + not-matched-insert "
             "via `merge_clauses`.")
    L.append("6. **Full sync (by-source delete):** matched rows UPDATEd, keys a ~50% (streamed) source "
             "no longer carries DELETEd via `WHEN NOT MATCHED BY SOURCE` — the heaviest shape "
             "(whole-target anti-join); `merge_streamed_exec` keeps the big source from materializing.")
    L.append("7. **Expression update:** a 100%-match UPDATE whose SET is an arbitrary expression + `CASE` "
             "(`merge_update_set_expressions`), not a plain column copy.")
    L.append("8. **Append (no merge):** the batch appended — no target scan/join (far cheaper).")
    L.append("9. **Safeappend (no merge):** same cheap append, version-guarded against concurrent writers.")
    L.append("10. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.")
    L.append("")
    L.append("_Operations 5–7 exercise delta-rs's full MERGE clause set and run on the LOCAL stress gate "
             "only; the OneLake path-smoke job skips them._")
    L += [""]

    def _m(n):
        return f"{n/1_000_000:.1f}M"

    L += ["### Results (row counts in millions; peak RSS is the `dbt run` child's, per op)",
          "| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Peak RSS | Time |",
          "|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|---:|"]
    for r in results:
        peak = f"{r['peak_mb']:,} MB" if r.get("peak_mb") else "n/a"
        L.append(f"| {r['name']} | {_m(r['src'])} | {_m(r['upd'])} | {_m(r['ins'])} | {_m(r['before'])} | "
                 f"{_m(r['after'])} | {_m(r['expected'])} | {'✅' if r['count_ok'] else '❌'} | "
                 f"{'✅' if r['verify_ok'] else '❌'} | {peak} | {r['dt']:.1f}s |")
    L += [""]
    verdict = (f"**Result: ✅ all operations correct.** The chain tail reached **{final_rows:,} rows**, "
               f"peak memory **{peak_s}** — duckrun stayed within the runner's RAM and every "
               f"update/insert landed through the dbt path."
               if all_ok else
               "**Result: ❌ one or more operations were wrong** — see the ❌ cells above and the step log.")
    L.append(verdict)
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--sf", type=float, default=20.0, help="target fact table scale factor")
    ap.add_argument("--warehouse", default=None,
                    help="Delta warehouse root. An abfss://… path runs the whole chain against "
                         "OneLake (the small-N integration job); default is the local <dir>/warehouse "
                         "the heavy SF=20 gate uses.")
    ap.add_argument("--target", default="dev",
                    help="dbt target/profile output (use 'onelake' with --warehouse abfss://…)")
    ap.add_argument("--spill-size", type=int, default=None, dest="spill_size",
                    help="merge_max_spill_size override (bytes); default uses duckrun's cgroup-aware cap")
    ap.set_defaults(func=run)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
