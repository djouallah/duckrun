"""Rigorous MERGE smoke/stress test on a big fact table — run entirely through the connection API.

Generate one big TPCH ``lineitem`` (default SF=10, ~60M rows) with ``tpchgen-cli`` as parquet. A
single ``duckrun.connect()`` session then builds a CHAIN of Delta tables with ``conn.sql(...)``: the
first table STREAMS the parquet into Delta, and everything downstream is seeded from the Delta table
the previous step wrote — no dbt, no manual convert, and nothing ever materializes the whole fact in
RAM:

  generate (tpchgen -> lineitem parquet)
    -> mixed_upsert        (seeds by streaming the parquet; then ~1% sample:
                            ~80% UPDATE existing keys + ~20% INSERT key-shifted)
    -> insert_only         (~5% sample key-shifted past max, future shipdate -> all INSERT)
    -> update_only         (~5% sample existing keys, 100% match -> row count unchanged)
    -> idempotent_remerge  (re-merge unchanged rows -> nothing changes)
    -> cdc_merge           (one MERGE: matched DELETE + matched UPDATE + NOT MATCHED INSERT)   [local]
    -> full_sync           (matched UPDATE + WHEN NOT MATCHED BY SOURCE DELETE)                [local]
    -> expr_update         (100%-match UPDATE via arbitrary SET expressions + CASE)            [local]
    -> append_only         (~5% sample appended -- no target scan/join)
    -> append_if_unchanged_only     (~5% sample appended -- plain INSERT; the version-guard verb was
                                     removed, a read-modify-append on the same table is auto-fenced)
    -> overwrite_all       (~5% sample, table overwrite -- replaces the whole table)

Each op's SQL lives in ``performance_test/merge_spill/sql/<op>.sql`` (plain DuckDB, no Jinja):
it builds the batch into a ``_batch`` TEMP table and applies the op via ``conn.sql(...)`` (the merge
runs through delta_rs's spill cap + the per-merge DuckDB memory pin — the same engine core the dbt
adapter uses). Batches are sampled with deterministic markers (l_quantity = -1 / -2, l_shipdate =
2035) so the runner can verify the effect by querying the table.

    python tests/performance/merge_tpch_bench.py --dir /tmp/m --sf 10
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import deltalake
import duckrun

# merge_tpch_bench lives in tests/performance/ but merge_spill is a top-level performance_test
# project, so resolve from the repo root (parents[2]: performance -> tests -> repo).
SQL_DIR = Path(__file__).resolve().parents[2] / "performance_test" / "merge_spill" / "sql"
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

# Each op (except mixed_upsert, seeded from parquet) is seeded from a prior table: (source, sample%).
SEEDS = {
    "insert_only": ("mixed_upsert", None),
    "update_only": ("insert_only", None),
    "idempotent_remerge": ("update_only", None),
    "cdc_merge": ("mixed_upsert", 50),
    "full_sync": ("mixed_upsert", 50),
    "expr_update": ("mixed_upsert", 50),
    "append_only": ("idempotent_remerge", None),
    "append_if_unchanged_only": ("append_only", None),
}


def _split_statements(sql_text):
    """Split a .sql file into individual statements for one-per-conn.sql() execution. Comments are
    stripped first so a ';' inside a `--`/`/* */` comment can't split a statement mid-sentence (the
    benchmark SQL has no ';' inside string literals)."""
    sql_text = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.S)
    sql_text = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def _self_rss_mb():
    """Current resident set size of THIS process in MB (Linux ``/proc/self/status`` ``VmRSS``);
    None off Linux. The merge now runs in-process, so the per-op peak is sampled off self."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        return None
    return None


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


class _RssSampler:
    """Polls this process's VmRSS in a background thread for the duration of an op and records the
    peak — the in-process replacement for the old per-child /proc sampling. Linux-only (peak is None
    off Linux); the merge runs synchronously in conn.sql, so the sampler runs alongside it."""

    def __init__(self, interval=0.05):
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self.peak = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self):
        peak = 0
        while not self._stop.is_set():
            cur = _self_rss_mb()
            if cur is not None and cur > peak:
                peak = cur
            time.sleep(self.interval)
        self.peak = peak or None

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        return False


class Bench:
    def __init__(self, args):
        self.args = args
        # --warehouse abfss://… points the whole chain at OneLake (the small-N integration job);
        # the default is the local <dir>/warehouse the heavy SF=10 gate uses. Schema comes from
        # DBT_SCHEMA so the OneLake job can write into an isolated schema (no aemo collision).
        self.remote = bool(args.warehouse and args.warehouse.startswith("abfss://"))
        self.root = args.warehouse if args.warehouse else os.path.join(args.dir, "warehouse")
        self.schema = os.environ.get("DBT_SCHEMA", SCHEMA)
        self.gen = os.path.join(args.dir, "gen")
        # Turn on the adapter's in-process memory profiler so each merge logs the DuckDB-vs-delta_rs
        # (+Arrow) split for its write/merge (see engine.mem_profile).
        os.environ["DUCKRUN_MEM_PROFILE"] = "1"
        storage_options = {"bearer_token": os.environ.get("ONELAKE_TOKEN", "")} if self.remote else None
        if not self.remote:
            os.makedirs(self.root, exist_ok=True)
        # One writable session for the whole chain — the ops AND the verification reads. On OneLake
        # connect() mints the Azure secret, so the abfss:// reads below work with no extra setup.
        self.conn = duckrun.connect(self.root, storage_options=storage_options, read_only=False)
        from dbt.adapters.duckrun import engine
        self.engine = engine
        self.peak_mb = None  # per-op peak RSS of the most recent run_op()

    def t(self, model):
        return f"{self.schema}.{model}"

    def q(self, sql):
        return self.conn.sql(sql).fetchone()[0]

    def count(self, model):
        return self.q(f"SELECT count(*) FROM {self.t(model)}")

    def generate(self):
        """Generate the big TPCH lineitem as PARQUET with tpchgen-cli — the only thing this needs is
        the scale factor and a scratch dir. The mixed_upsert seed then streams it straight into Delta;
        nothing collects the whole fact into RAM."""
        shutil.rmtree(self.gen, ignore_errors=True)
        os.makedirs(self.gen, exist_ok=True)
        parts = max(1, round(float(self.args.sf) / 2))
        subprocess.run(
            ["tpchgen-cli", "-s", str(self.args.sf), "--tables", "lineitem",
             "--parts", str(parts), "--format", "parquet", "--output-dir", self.gen],
            check=True,
        )

    def gen_glob(self):
        return os.path.join(self.gen, "lineitem", "*.parquet").replace(os.sep, "/")

    def seed(self, model):
        """Seed this op's table — no op applied yet. mixed_upsert streams the generated parquet into
        Delta; every other table is CREATE-OR-REPLACE'd from the prior table (a fresh seed each run,
        local and on OneLake where tables persist across CI runs)."""
        if model == "mixed_upsert":
            self.conn.sql(
                f"CREATE OR REPLACE TABLE {self.t('mixed_upsert')} AS "
                f"SELECT * FROM read_parquet('{self.gen_glob()}')")
            return
        source, sample = SEEDS[model]
        samp = f" USING SAMPLE {sample} PERCENT (bernoulli)" if sample else ""
        self.conn.sql(
            f"CREATE OR REPLACE TABLE {self.t(model)} AS SELECT * FROM {self.t(source)}{samp}")

    def run_op(self, model):
        """Apply one op: run every statement in sql/<model>.sql through conn.sql (the batch TEMP-table
        build + the MERGE / INSERT / overwrite), timing it and sampling this process's peak RSS. The
        merge runs in-process via delta_rs (spill cap + per-merge DuckDB pin), so the sampler — not a
        child /proc poll — is what measures the merge's memory. Returns elapsed seconds."""
        sql_text = (SQL_DIR / f"{model}.sql").read_text(encoding="utf-8").replace("{schema}", self.schema)
        statements = _split_statements(sql_text)
        t = time.time()
        with _RssSampler() as sampler:
            for stmt in statements:
                self.conn.sql(stmt)
            if model == "append_if_unchanged_only":
                # The append_if_unchanged VERB is gone; _batch is new data (not a read of the target),
                # so this is a plain, unfenced append. (A read-modify-append on the same table would be
                # auto-fenced — see delta_dml._reads_target.)
                self.conn.sql(f"INSERT INTO {self.t('append_if_unchanged_only')} SELECT * FROM _batch")
            if model == "full_sync":
                # A by-source MERGE (WHEN NOT MATCHED BY SOURCE) auto-streams its big ~50% source in the
                # router (collecting it whole builds a non-spillable hash → OOM). _src is built by
                # sql/full_sync.sql above.
                self.conn.sql(
                    f"MERGE INTO {self.t('full_sync')} USING _src "
                    "ON target.l_orderkey = source.l_orderkey "
                    "AND target.l_linenumber = source.l_linenumber "
                    "WHEN MATCHED THEN UPDATE SET * "
                    "WHEN NOT MATCHED BY SOURCE THEN DELETE")
        self.peak_mb = sampler.peak
        return time.time() - t

    # -- per-op scenarios: seed already done; run the increment and verify by querying the table --

    def mixed(self, model):
        p = self.t(model)
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM {p}")
        dt = self.run_op(model)
        after = self.count(model)
        batch = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_UPSERT}")
        ins = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_UPSERT} AND l_orderkey > {base_max}")
        return {"name": "Mixed upsert", "src": batch, "upd": batch - ins, "ins": ins,
                "before": before, "after": after, "expected": before + ins, "dt": dt,
                "count_ok": after == before + ins, "verify_ok": (batch - ins) > 0 and ins > 0,
                "peak_mb": self.peak_mb,
                "verify": "existing keys updated (l_quantity=-1), key-shifted rows inserted"}

    def insert_only(self, model):
        p = self.t(model)
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM {p}")
        dt = self.run_op(model)
        after = self.count(model)
        ins = self.q(f"SELECT count(*) FROM {p} WHERE l_shipdate = DATE '{FUTURE_SHIPDATE}'")
        future_past_max = self.q(
            f"SELECT count(*) FROM {p} WHERE l_shipdate = DATE '{FUTURE_SHIPDATE}' AND l_orderkey > {base_max}")
        return {"name": "Insert-only (future shipdate)", "src": ins, "upd": 0, "ins": ins,
                "before": before, "after": after, "expected": before + ins, "dt": dt,
                "count_ok": after == before + ins, "verify_ok": ins > 0 and future_past_max == ins,
                "peak_mb": self.peak_mb,
                "verify": f"{ins:,} rows carry the 2035 shipdate, all key-shifted (== inserts)"}

    def update_only(self, model):
        before = self.count(model)
        dt = self.run_op(model)
        after = self.count(model)
        marked = self.q(f"SELECT count(*) FROM {self.t(model)} WHERE l_quantity = {MARK_UPDATE}")
        return {"name": "Update-only (100% match)", "src": marked, "upd": marked, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": marked > 0,
                "peak_mb": self.peak_mb,
                "verify": "row count unchanged; sampled rows carry l_quantity=-2"}

    def idempotent(self, model):
        # Seed already merged one unchanged batch; run it AGAIN — must change nothing.
        before = self.count(model)
        dt = self.run_op(model)
        after = self.count(model)
        return {"name": "Idempotent re-merge", "src": 0, "upd": 0, "ins": 0,
                "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": after == before,
                "peak_mb": self.peak_mb,
                "verify": "re-merging unchanged rows changed nothing"}

    def appendish(self, model, label):
        p = self.t(model)
        before, base_max = self.count(model), self.q(f"SELECT max(l_orderkey) FROM {p}")
        dt = self.run_op(model)
        after = self.count(model)
        appended = self.q(f"SELECT count(*) FROM {p} WHERE l_orderkey > {base_max}")
        return {"name": f"{label} (no merge)", "src": appended, "upd": 0, "ins": appended,
                "before": before, "after": after, "expected": before + appended, "dt": dt,
                "count_ok": after == before + appended and appended > 0, "verify_ok": appended > 0,
                "peak_mb": self.peak_mb,
                "verify": f"appended {appended:,} key-shifted rows — no target scan"}

    def overwrite(self, model, prev_model):
        before = self.count(prev_model)  # the table this op conceptually replaces (chain tail)
        dt = self.run_op(model)
        after = self.count(model)
        return {"name": "Overwrite (no merge)", "src": after, "upd": 0, "ins": after,
                "before": before, "after": after, "expected": after, "dt": dt,
                "count_ok": 0 < after < before, "verify_ok": 0 < after < before,
                "peak_mb": self.peak_mb,
                "verify": f"table replaced with the {after:,}-row batch (no target scan)"}

    # -- full-surface MERGE scenarios (delta_rs's complete clause set via raw conn.sql MERGE) —
    #    LOCAL stress only; the OneLake job skips them to stay fast --

    def cdc(self, model):
        """CDC change-set in ONE merge: matched DELETE (tombstones) + matched UPDATE + NOT MATCHED
        INSERT. Deletes are a deterministic 1% slice counted BEFORE the merge; inserts/updates carry
        markers counted after, so the net count closes (after == before + inserts - deletes)."""
        p = self.t(model)
        before = self.count(model)
        deletes = self.q(f"SELECT count(*) FROM {p} WHERE l_orderkey % 100 = 7")
        dt = self.run_op(model)
        after = self.count(model)
        inserts = self.q(f"SELECT count(*) FROM {p} WHERE l_shipdate = DATE '{CDC_INSERT_SHIPDATE}'")
        updates = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_CDC_UPDATE}")
        # the delete clause must have removed the tombstoned slice (none left, and none survived as
        # a stale -7 marker — that would mean the UPDATE clause caught them instead of DELETE).
        tombstones_left = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_CDC_DELETE}")
        slice_left = self.q(
            f"SELECT count(*) FROM {p} WHERE l_orderkey % 100 = 7 "
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
        ~50% of the table (every odd orderkey), left as an inline subquery so the big source isn't
        materialized whole. Even orderkeys are 'departed' → by-source DELETE; odd → UPDATE."""
        p = self.t(model)
        before = self.count(model)
        departed = self.q(f"SELECT count(*) FROM {p} WHERE l_orderkey % 2 = 0")
        dt = self.run_op(model)
        after = self.count(model)
        updated = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_SYNC}")
        departed_left = self.q(f"SELECT count(*) FROM {p} WHERE l_orderkey % 2 = 0")
        expected = before - departed
        return {"name": "Full sync (update + by-source delete)", "src": before - departed,
                "upd": updated, "ins": 0, "before": before, "after": after,
                "expected": expected, "dt": dt, "count_ok": after == expected,
                "verify_ok": departed > 0 and departed_left == 0 and updated == after,
                "peak_mb": self.peak_mb,
                "verify": f"by-source-deleted {departed:,} departed keys; updated all {updated:,} survivors (-5)"}

    def expr_update(self, model):
        """100%-match update via arbitrary SET expressions: sets l_quantity = -9 and re-derives
        l_returnflag with a CASE over the source (arbitrary-expression path). Count unchanged."""
        p = self.t(model)
        before = self.count(model)
        dt = self.run_op(model)
        after = self.count(model)
        marked = self.q(f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_EXPR}")
        # every expression-updated row must also carry the CASE-derived flag ('H' or 'L') — proving
        # the expression ran, not a plain copy (lineitem's real flags are A/N/R).
        bad_case = self.q(
            f"SELECT count(*) FROM {p} WHERE l_quantity = {MARK_EXPR} "
            f"AND l_returnflag NOT IN ('H', 'L')")
        return {"name": "Expression update (set expressions + CASE)", "src": marked, "upd": marked,
                "ins": 0, "before": before, "after": after, "expected": before, "dt": dt,
                "count_ok": after == before, "verify_ok": marked > 0 and bad_case == 0,
                "peak_mb": self.peak_mb,
                "verify": f"updated {marked:,} rows via expressions (l_quantity=-9, l_returnflag=CASE H/L)"}


def run(args):
    # Local runs start from a clean warehouse; the OneLake job can't rmtree a remote store, so it
    # relies on each seed's CREATE OR REPLACE TABLE to recreate the table fresh instead.
    if not (args.warehouse and args.warehouse.startswith("abfss://")):
        if os.path.exists(os.path.join(args.dir, "warehouse")):
            shutil.rmtree(os.path.join(args.dir, "warehouse"))

    b = Bench(args)
    print(f"== generate base: tpchgen SF={args.sf} -> lineitem parquet ==", flush=True)
    b.generate()
    target_rows = b.q(f"SELECT count(*) FROM read_parquet('{b.gen_glob()}')")
    eff = b.engine._effective_mem_limit_bytes()
    cap = b.engine._default_merge_spill_size()
    print(f"== base lineitem: {target_rows:,} rows | effective_mem="
          f"{None if eff is None else round(eff/1048576)}MB merge_cap="
          f"{None if cap is None else round(cap/1048576)}MB ==", flush=True)

    setup = {"duckdb_ver": duckdb.__version__, "deltalake_ver": deltalake.__version__,
             "sf": args.sf, "target_rows": target_rows,
             "eff_mb": None if eff is None else round(eff/1048576),
             "cap_mb": None if cap is None else round(cap/1048576)}

    # Chain: seed each op from the prior, then apply the op.
    results = []
    b.seed("mixed_upsert");        results.append(b.mixed("mixed_upsert"))
    b.seed("insert_only");         results.append(b.insert_only("insert_only"))
    b.seed("update_only");         results.append(b.update_only("update_only"))
    b.seed("idempotent_remerge")
    b.run_op("idempotent_remerge")  # first increment (set-up); the reported run is the re-merge
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
    b.seed("append_if_unchanged_only");     results.append(b.appendish("append_if_unchanged_only", "Append #2 (plain)"))
    results.append(b.overwrite("overwrite_all", "append_if_unchanged_only"))

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

    final_rows = b.count("append_if_unchanged_only")
    op_peaks = [r["peak_mb"] for r in results if r.get("peak_mb")]
    peak = max(op_peaks) if op_peaks else _peak_rss_mb()
    peak_s = f"{peak}MB" if peak is not None else "n/a (/proc RSS sampling is Linux-only)"
    print(f"== done: peak RSS={peak_s} (chain tail grew to {final_rows:,} rows) ==", flush=True)
    _write_summary(setup, results, final_rows, peak, ok)
    if not ok:
        sys.exit(2)
    print("OK: duckrun ran the chain through the connection API and survived.")


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
    L = ["## 🔀 Incremental MERGE test — duckrun on Delta Lake (via the connection API)", ""]
    L.append("**What this checks:** that duckrun MERGEs incremental batches into a large Delta "
             "*fact* table **through the connection API** — a chain of `conn.sql(...)` MERGEs (the "
             "delta_rs spill cap + the per-merge DuckDB memory pin) — applying UPDATEs and INSERTs "
             "correctly without being OOM-killed, and how the same shape compares against a plain "
             "`append` / `append_if_unchanged` / `overwrite` (which never scan the target).")
    L += ["", "### Setup (the inputs)", "| | |", "|---|---|"]
    L.append(f"| Engine | duckrun &middot; DuckDB {setup['duckdb_ver']} &middot; delta_rs {setup['deltalake_ver']} |")
    L.append(f"| Target fact table | TPCH `lineitem`, scale factor **{setup['sf']}** → **{setup['target_rows']:,} rows** |")
    L.append("| Primary key (merge on) | `(l_orderkey, l_linenumber)` |")
    L.append(f"| Effective memory | {setup['eff_mb']} MB (runner RAM, no artificial limit) |")
    L.append(f"| Merge spill cap | {setup['cap_mb']} MB — delta_rs `max_spill_size` |")
    L += ["", "### The operations (a chain — each builds on the previous)"]
    L.append("1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE, ~20% key-shifted → INSERT.")
    L.append("2. **Insert-only (~5% sample):** key-shifted past max key, future `l_shipdate` (2035) → all INSERT.")
    L.append("3. **Update-only (~5% sample):** existing keys, no shift → 100% match; row count unchanged.")
    L.append("4. **Idempotent re-merge:** re-merge unchanged rows → nothing changes.")
    L.append("5. **CDC merge (full clause set):** one MERGE that DELETEs a tombstoned slice, UPDATEs a "
             "sample, and INSERTs key-shifted rows — `WHEN MATCHED … THEN DELETE` + `WHEN MATCHED THEN "
             "UPDATE SET *` + `WHEN NOT MATCHED THEN INSERT *`.")
    L.append("6. **Full sync (by-source delete):** matched rows UPDATEd, keys a ~50% (inline-subquery) "
             "source no longer carries DELETEd via `WHEN NOT MATCHED BY SOURCE` — the heaviest shape "
             "(whole-target anti-join).")
    L.append("7. **Expression update:** a 100%-match UPDATE whose SET is an arbitrary expression + `CASE` "
             "over the source, not a plain column copy.")
    L.append("8. **Append (no merge):** the batch appended — no target scan/join (far cheaper).")
    L.append("9. **Append #2 (no merge):** a second plain append of new data (the version-guard verb was "
             "removed; a read-modify-append on the SAME table is auto-fenced instead).")
    L.append("10. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.")
    L.append("")
    L.append("_Operations 5–7 exercise delta-rs's full MERGE clause set and run on the LOCAL stress gate "
             "only; the OneLake path-smoke job skips them._")
    L += [""]

    def _m(n):
        return f"{n/1_000_000:.1f}M"

    L += ["### Results (row counts in millions; peak RSS is the process's, per op)",
          "| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Peak RSS | Time |",
          "|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|---:|"]
    for r in results:
        peak_cell = f"{r['peak_mb']:,} MB" if r.get("peak_mb") else "n/a"
        L.append(f"| {r['name']} | {_m(r['src'])} | {_m(r['upd'])} | {_m(r['ins'])} | {_m(r['before'])} | "
                 f"{_m(r['after'])} | {_m(r['expected'])} | {'✅' if r['count_ok'] else '❌'} | "
                 f"{'✅' if r['verify_ok'] else '❌'} | {peak_cell} | {r['dt']:.1f}s |")
    L += [""]
    verdict = (f"**Result: ✅ all operations correct.** The chain tail reached **{final_rows:,} rows**, "
               f"peak memory **{peak_s}** — duckrun stayed within the runner's RAM and every "
               f"update/insert landed through the connection API."
               if all_ok else
               "**Result: ❌ one or more operations were wrong** — see the ❌ cells above and the step log.")
    L.append(verdict)
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--sf", type=float, default=10.0, help="target fact table scale factor")
    ap.add_argument("--warehouse", default=None,
                    help="Delta warehouse root. An abfss://… path runs the whole chain against "
                         "OneLake (the small-N integration job); default is the local <dir>/warehouse "
                         "the heavy SF=10 gate uses.")
    ap.set_defaults(func=run)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
