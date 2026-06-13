"""Rigorous MERGE smoke/stress test on a big fact table — run entirely through the dbt path.

Generate one big TPCH ``lineitem`` (default SF=20, ~120M rows) with ``tpchgen-cli`` as parquet. A dbt
DAG then builds a CHAIN of Delta tables, each ``ref``-ing the previous (the first model reads the
parquet ``source``; everything downstream is ``ref`` to Delta tables duckrun writes — no manual
convert):

  lineitem (source parquet -> Delta base)
    -> mixed_upsert        (~1% sample: ~80% UPDATE existing keys + ~20% INSERT key-shifted)
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


class Bench:
    def __init__(self, args):
        self.args = args
        self.root = os.path.join(args.dir, "warehouse")
        self.con = duckdb.connect()
        self.con.execute("INSTALL delta; LOAD delta;")
        from dbt.adapters.duckrun import engine
        self.engine = engine
        engine.configure_duckdb_session(self.con)
        self.env = dict(os.environ)
        self.env["WAREHOUSE_PATH"] = self.root
        self.env["DBT_SCHEMA"] = SCHEMA

    def path(self, model):
        return os.path.join(self.root, SCHEMA, model).replace(os.sep, "/")

    def count(self, model):
        return self.con.execute(f"SELECT count(*) FROM delta_scan('{self.path(model)}')").fetchone()[0]

    def q(self, sql):
        return self.con.execute(sql).fetchone()[0]

    def dbt(self, model):
        """One `dbt run --select <model>`; returns elapsed seconds. Raises on non-zero exit."""
        cmd = ["dbt", "run", "--select", model,
               "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
        if self.args.spill_size is not None:
            cmd += ["--vars", f"{{merge_max_spill_size: {self.args.spill_size}}}"]
        t = time.time()
        subprocess.run(cmd, check=True, env=self.env)
        return time.time() - t

    def seed(self, model):
        """First run: seed this op's table from ref(previous) — no op applied yet."""
        self.dbt(model)

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
                "verify": f"{ins:,} rows carry the 2035 shipdate, all key-shifted (== inserts)"}

    def update_only(self, model):
        before = self.count(model)
        dt = self.dbt(model)
        after = self.count(model)
        marked = self.q(f"SELECT count(*) FROM delta_scan('{self.path(model)}') WHERE l_quantity = {MARK_UPDATE}")
        return {"name": "Update-only (100% match)", "src": marked, "upd": marked, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": marked > 0,
                "verify": "row count unchanged; sampled rows carry l_quantity=-2"}

    def idempotent(self, model):
        # Seed already merged one unchanged batch; run it AGAIN — must change nothing.
        before = self.count(model)
        dt = self.dbt(model)
        after = self.count(model)
        return {"name": "Idempotent re-merge", "src": 0, "upd": 0, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": after == before,
                "verify": "re-merging unchanged rows changed nothing"}

    def appendish(self, model, label):
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM delta_scan('{self.path(model)}')")
        dt = self.dbt(model)
        after = self.count(model)
        appended = self.q(f"SELECT count(*) FROM delta_scan('{self.path(model)}') WHERE l_orderkey > {base_max}")
        return {"name": f"{label} (no merge)", "src": appended, "upd": 0, "ins": appended,
                "before": before, "after": after, "expected": before + appended, "dt": dt,
                "count_ok": after == before + appended and appended > 0, "verify_ok": appended > 0,
                "verify": f"appended {appended:,} key-shifted rows — no target scan"}

    def overwrite(self, model, prev_model):
        before = self.count(prev_model)  # the table this op conceptually replaces (chain tail)
        dt = self.dbt(model)
        after = self.count(model)
        return {"name": "Overwrite (no merge)", "src": after, "upd": 0, "ins": after,
                "before": before, "after": after, "expected": after, "dt": dt,
                "count_ok": 0 < after < before, "verify_ok": 0 < after < before,
                "verify": f"table replaced with the {after:,}-row batch (no target scan)"}


def run(args):
    if os.path.exists(os.path.join(args.dir, "warehouse")):
        shutil.rmtree(os.path.join(args.dir, "warehouse"))

    b = Bench(args)
    # The lineitem python model generates the data with tpchgen-cli itself (in the DAG); the runner
    # only tells it the scale factor and a scratch dir to write the parquet into.
    b.env["MERGE_SPILL_SF"] = str(args.sf)
    b.env["MERGE_SPILL_GEN"] = os.path.join(args.dir, "gen")

    print(f"== build base: lineitem python model generates SF={args.sf} via tpchgen, writes Delta ==",
          flush=True)
    b.dbt("lineitem")
    target_rows = b.count("lineitem")
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
    peak = _peak_rss_mb()
    print(f"== done: peakRSS={peak}MB (chain tail grew to {final_rows:,} rows) ==", flush=True)
    _write_summary(setup, results, final_rows, peak, ok)
    if not ok:
        sys.exit(2)
    print("OK: duckrun ran the chain through the dbt path and survived.")


def _write_summary(setup, results, final_rows, peak, all_ok):
    card = _build_card(setup, results, final_rows, peak, all_ok)
    with open("merge_card.md", "w", encoding="utf-8", newline="\n") as fh:
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
    L.append("5. **Append (no merge):** the batch appended — no target scan/join (far cheaper).")
    L.append("6. **Safeappend (no merge):** same cheap append, version-guarded against concurrent writers.")
    L.append("7. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.")
    L += [""]

    def _m(n):
        return f"{n/1_000_000:.1f}M"

    L += ["### Results (row counts in millions)",
          "| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Time |",
          "|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|"]
    for r in results:
        L.append(f"| {r['name']} | {_m(r['src'])} | {_m(r['upd'])} | {_m(r['ins'])} | {_m(r['before'])} | "
                 f"{_m(r['after'])} | {_m(r['expected'])} | {'✅' if r['count_ok'] else '❌'} | "
                 f"{'✅' if r['verify_ok'] else '❌'} | {r['dt']:.1f}s |")
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
    ap.add_argument("--spill-size", type=int, default=None, dest="spill_size",
                    help="merge_max_spill_size override (bytes); default uses duckrun's cgroup-aware cap")
    ap.set_defaults(func=run)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
