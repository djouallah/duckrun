"""
fuzz_replay: DuckDB's own fuzzer (sqlsmith extension) as the generator, replayed
DIFFERENTIALLY through the engine's conn.sql() vs plain DuckDB as the oracle.

Why replay instead of `CALL sqlsmith()` on the engine's raw connection: the
extension executes generated queries inside the C++ engine directly, which would
bypass the Python .sql() layer entirely — testing DuckDB, not the integration.
Replaying each logged statement through conn.sql() exercises exactly the seam.

Per statement:
  - run on oracle (plain duckdb, threads=1) and on the engine (fresh temp warehouse)
  - both error                    -> agree_err
  - one errors, one succeeds     -> finding: error_mismatch (engine states resync)
  - both succeed, SELECT         -> sorted result-set diff -> finding: result_diff
  - both succeed, DML            -> full table-state diff  -> finding: state_diff
  - every RECONNECT_EVERY DMLs   -> close + reconnect engine, re-diff all tables
                                    -> finding: persistence_diff
Statements with nondeterminism (random/now, TABLESAMPLE, or a LIMIT with no
governing ORDER BY — an ORDER BY inside a window's OVER(...) does not count) are
compared leniently and land in 'suspect' buckets instead of findings.

Usage:
  python fuzz_replay.py --queries 300 --seed 42
  python fuzz_replay.py --queries 2000 --seed $RANDOM --module <engine import name>
"""
import argparse
import importlib
import re
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

SEED_TABLES = [
    # a few shapes: numeric fact, strings/NULLs, dates — enough surface for the fuzzer
    "CREATE TABLE sales AS SELECT range AS id, (range%7)::INT AS cat, "
    "  (range*1.5)::DOUBLE AS amt, (range%3=0) AS flag FROM range(500)",
    "CREATE TABLE customers AS SELECT range AS cid, "
    "  CASE WHEN range%5=0 THEN NULL ELSE 'name_'||range END AS name, "
    "  DATE '2024-01-01' + INTERVAL (range%400) DAY AS joined FROM range(200)",
    "CREATE TABLE tiny AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,NULL)) t(k,v)",
]
TABLES = ["sales", "customers", "tiny"]
# Functions whose value differs between two independent connections (the oracle vs the engine): wall
# clock, transaction/session identity, and randomness. A query using one can't be compared row-for-row,
# so it lands in a 'suspect' bucket, not a finding.
NONDET = re.compile(
    r"\b(random|uuid|gen_random_uuid|now|today|get_current|current_|localtime|localtimestamp|"
    r"transaction_timestamp|statement_timestamp|txid_current|nextval|currval|lastval|"
    r"pg_backend_pid)\w*\s*\(", re.I)
LIMIT_NO_ORDER = re.compile(r"\blimit\b", re.I)
HAS_ORDER = re.compile(r"\border\s+by\b", re.I)
IS_SELECT = re.compile(r"^\s*(select|with|from|values)\b", re.I)
# RETURNING is a deliberately-unsupported delta_rs gap (a Delta write commits via the log and can't
# return affected rows) — filtered out of the corpus so its known fail-loud noise doesn't drown findings.
_RETURNING = re.compile(r"\breturning\b", re.I)
# TABLESAMPLE / bernoulli / system / USING SAMPLE draw a nondeterministic subset — the oracle's native
# tables and the engine's Delta-backed storage sample different rows, so such queries can never be
# compared row-for-row. Bucket them leniently rather than as a data divergence.
SAMPLE = re.compile(r"\btablesample\b|\busing\s+sample\b", re.I)
# An ORDER BY inside a window's OVER(...) governs the frame, NOT the result set, so it must not count
# as the stabilizing order a top-level/subquery LIMIT needs. Strip OVER(...) before the HAS_ORDER test,
# else a query like `… over (order by x) … limit 1 offset 4` is wrongly deemed deterministic and its
# inherent LIMIT/OFFSET nondeterminism surfaces as a false result_diff. (handles one level of nesting)
_OVER_CLAUSE = re.compile(r"\bover\s*\((?:[^()]|\([^()]*\))*\)", re.I)

# Correctness defects — the engine returned different rows, left tables in a different state, or lost
# writes across a reconnect. error_mismatch (engine raised where DuckDB didn't) is the weaker signal.
DATA_KINDS = ("result_diff", "state_diff", "persistence_diff")


def _write_summary_md(path, args, stats, findings, fatal):
    """Render a GitHub-flavored markdown report (for $GITHUB_STEP_SUMMARY) — verdict, the reproduce
    seed, the bucket table, then findings split into DATA divergence (what matters) vs error_mismatch
    (usually fail-loud syntax gaps)."""
    import duckdb
    try:
        from importlib.metadata import version
        dl = version("deltalake")
    except Exception:
        dl = "?"
    data = [f for f in findings if f[0] in DATA_KINDS]
    mism = [f for f in findings if f[0] == "error_mismatch"]
    L = []
    L.append("## fuzz_replay — sqlsmith differential run")
    L.append(f"{'❌ **FAIL**' if fatal else '✅ **PASS**'} · `--fail-on {args.fail_on}` · "
             f"**seed** `{args.seed}` · **queries** {args.queries} · "
             f"duckdb {duckdb.__version__} · deltalake {dl}")
    L.append(f"Reproduce: `python tests/fuzz/fuzz_replay.py "
             f"--queries {args.queries} --seed {args.seed}`")
    L.append("")
    L.append("| bucket | count |")
    L.append("|---|---:|")
    for k, v in sorted(stats.items()):
        L.append(f"| `{k}` | {v} |")
    L.append("")
    L.append(f"### ⚠️ Data divergence — {len(data)} (wrong data / lost write)")
    if data:
        for kind, i, q, a, _ in data:
            L.append(f"- **[{kind}] #{i}** — {a}")
            L.append(f"  ```sql\n  {q.strip()[:400]}\n  ```")
    else:
        L.append("_none — the engine never returned wrong data or lost a write._")
    L.append("")
    L.append(f"### Error mismatch — {len(mism)} "
             "(engine raised where DuckDB succeeded; usually a fail-loud gap on syntax delta_rs "
             "can't route — VALUES(…,DEFAULT) / TABLESAMPLE SYSTEM; RETURNING is filtered out)")
    if mism:
        for kind, i, q, _, b in mism[:60]:
            L.append(f"- **#{i}** {b} — `{' '.join(q.split())[:120]}`")
        if len(mism) > 60:
            L.append(f"- …and {len(mism) - 60} more (see the job log)")
    else:
        L.append("_none_")
    L.append("")
    Path(path).write_text("\n".join(L), encoding="utf-8")


def _sorted_rows(rows):
    # NULL-safe: a column can mix None and values across rows, and None < int
    # raises. Sort None to the front per-position without cross-type compares.
    return sorted((tuple(r) for r in rows),
                  key=lambda row: tuple((v is None, v if v is not None else 0)
                                        for v in row))


def generate_corpus(n, seed):
    import duckdb
    g = duckdb.connect()
    g.sql("INSTALL sqlsmith")
    g.sql("LOAD sqlsmith")
    for s in SEED_TABLES:
        g.sql(s)
    log = tempfile.mktemp(suffix=".sql")
    g.sql(f"CALL sqlsmith(max_queries={n}, seed={seed}, complete_log='{log}')")
    g.close()
    qs = [q.strip() for q in Path(log).read_text().split(";") if q.strip()]
    # sqlsmith qualifies with main. — normalize for both sides
    qs = [re.sub(r"\bmain\.", "", q) for q in qs]
    # Drop RETURNING statements: a Delta write commits through the transaction log and can't hand back
    # the affected rows, so the engine fail-loudly rejects RETURNING by design (session.py _RETURNING_MSG).
    # Replaying it only floods the report with known error_mismatch noise, so filter it at the source.
    kept = [q for q in qs if not _RETURNING.search(q)]
    dropped = len(qs) - len(kept)
    if dropped:
        print(f"filtered out {dropped} RETURNING statement(s) (intentional delta_rs gap)")
    return kept


class Oracle:
    def __init__(self):
        import duckdb
        self.con = duckdb.connect()
        self.con.sql("SET threads=1")
        for s in SEED_TABLES:
            self.con.sql(s)

    def run(self, q):
        try:
            r = self.con.sql(q)
            return ("ok", _sorted_rows(r.fetchall()) if r is not None else None)
        except Exception as e:
            return ("err", type(e).__name__)

    def state(self):
        return {t: _sorted_rows(self.con.sql(f"SELECT * FROM {t}").fetchall())
                for t in TABLES}


class Engine:
    def __init__(self, module):
        self.mod = importlib.import_module(module)
        self.root = tempfile.mkdtemp(prefix="fuzz_replay_")
        # pin the schema so seeds/reads don't ride on the derived default (bare
        # CREATE lands in dbo; reconnect discovers dbo.<t> and resolves bare)
        self.con = self.mod.connect(self.root, schema="dbo", read_only=False)
        for s in SEED_TABLES:
            self.con.sql(s)

    def run(self, q):
        try:
            r = self.con.sql(q)
            return ("ok", _sorted_rows(r.fetchall()) if r is not None else None)
        except Exception as e:
            return ("err", type(e).__name__)

    def state(self):
        return {t: _sorted_rows(self.con.sql(f"SELECT * FROM {t}").fetchall())
                for t in TABLES}

    def reconnect(self):
        self.con.close()
        self.con = self.mod.connect(self.root, schema="dbo", read_only=False)

    def resync_from(self, oracle_state):
        def lit(v):
            import datetime
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, datetime.datetime):
                return f"TIMESTAMP '{v}'"
            if isinstance(v, datetime.date):
                return f"DATE '{v}'"
            if isinstance(v, str):
                return "'" + v.replace("'", "''") + "'"
            return repr(v)
        # after a divergence, rebuild engine tables to match the oracle exactly
        for t in TABLES:
            self.con.sql(f"DROP TABLE IF EXISTS {t}")
        self.reconnect()
        for t, rows in oracle_state.items():
            if rows:
                vals = ",".join(
                    "(" + ",".join(lit(v) for v in r) + ")"
                    for r in rows)
                cols = {"sales": "(id,cat,amt,flag)", "customers": "(cid,name,joined)",
                        "tiny": "(k,v)"}[t]
                ddl = {"sales": "id BIGINT, cat INT, amt DOUBLE, flag BOOLEAN",
                       "customers": "cid BIGINT, name VARCHAR, joined TIMESTAMP",
                       "tiny": "k INT, v VARCHAR"}[t]
                self.con.sql(f"CREATE TABLE {t} ({ddl})")
                self.con.sql(f"INSERT INTO {t} {cols} VALUES {vals}")
            else:
                ddl = {"sales": "id BIGINT, cat INT, amt DOUBLE, flag BOOLEAN",
                       "customers": "cid BIGINT, name VARCHAR, joined TIMESTAMP",
                       "tiny": "k INT, v VARCHAR"}[t]
                self.con.sql(f"CREATE TABLE {t} ({ddl})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--module", default="duckrun",
                    help="import name of the engine under test (exposes connect(path))")
    ap.add_argument("--reconnect-every", type=int, default=10, help="DMLs between persistence checks")
    ap.add_argument("--fail-on", choices=("any", "data"), default="any",
                    help="which findings exit non-zero: 'any' (default — every divergence, incl. "
                         "error_mismatch on exotic syntax delta_rs can't route) or 'data' (only a "
                         "wrong-data / lost-write divergence: result_diff / state_diff / "
                         "persistence_diff). The nightly seed=$RANDOM miner uses 'data' so the known "
                         "fail-loud syntax gaps don't paint it red; every finding is still printed.")
    ap.add_argument("--summary-md", metavar="PATH", default=None,
                    help="also write a markdown report to PATH (point it at $GITHUB_STEP_SUMMARY in CI)")
    ap.add_argument("--corpus-out", metavar="PATH", default=None,
                    help="dump the exact generated statements (numbered, ;-terminated) to PATH BEFORE "
                         "replaying. sqlsmith is NOT seed-stable across duckdb builds, so the seed alone "
                         "can't reproduce a crash — this file is the only faithful record. Upload it as a "
                         "CI artifact so a hard delta_rs panic (which escapes the try/except as a "
                         "BaseException and kills the process) is still reproducible statement-for-statement.")
    args = ap.parse_args()

    print(f"generating {args.queries} statements with sqlsmith(seed={args.seed})…")
    corpus = generate_corpus(args.queries, args.seed)
    mix = Counter(q.split()[0].upper() for q in corpus)
    print(f"corpus: {dict(mix)}\n")

    if args.corpus_out:
        # Written up front, before a single statement runs, so it survives even a process-killing
        # Rust panic mid-replay. Numbered to line up with the '>>> #i' progress markers below.
        Path(args.corpus_out).write_text(
            f"-- sqlsmith corpus · seed={args.seed} · queries={args.queries} · {len(corpus)} statements\n"
            + "".join(f"-- #{i}\n{q};\n" for i, q in enumerate(corpus)),
            encoding="utf-8")
        print(f"wrote corpus ({len(corpus)} statements) to {args.corpus_out}\n")

    oracle, eng = Oracle(), Engine(args.module)
    stats = Counter()
    findings = []
    dml_since_reconnect = 0

    for i, q in enumerate(corpus):
        # Flushed breadcrumb: a hard delta_rs panic kills the process (PanicException subclasses
        # BaseException, so eng.run's `except Exception` can't catch it). This marker is then the LAST
        # log line, naming the exact index to look up in the --corpus-out dump.
        print(f">>> #{i} {q.split()[0].upper()}", flush=True)
        q_no_over = _OVER_CLAUSE.sub("", q)
        nondet = bool(NONDET.search(q)) or bool(SAMPLE.search(q))
        limit_noorder = bool(LIMIT_NO_ORDER.search(q_no_over)) and not HAS_ORDER.search(q_no_over)
        is_sel = bool(IS_SELECT.match(q))

        o = oracle.run(q)
        e = eng.run(q)

        if o[0] == "err" and e[0] == "err":
            stats["agree_err"] += 1
            continue
        if o[0] != e[0]:
            stats["error_mismatch"] += 1
            findings.append(("error_mismatch", i, q, f"oracle={o}", f"engine={e}"))
            eng.resync_from(oracle.state())
            continue

        # both ok
        if is_sel:
            if o[1] == e[1]:
                stats["agree_ok"] += 1
            elif nondet or limit_noorder:
                stats["suspect_nondet"] += 1
            else:
                stats["result_diff"] += 1
                findings.append(("result_diff", i, q,
                                 f"oracle_rows={o[1][:5]}…({len(o[1] or [])})",
                                 f"engine_rows={e[1][:5]}…({len(e[1] or [])})"))
        else:
            os_, es_ = oracle.state(), eng.state()
            if os_ == es_:
                stats["agree_dml"] += 1
            elif nondet or limit_noorder:
                stats["suspect_nondet_dml"] += 1
                eng.resync_from(os_)
            else:
                stats["state_diff"] += 1
                bad = {t: (len(os_[t]), len(es_[t])) for t in TABLES if os_[t] != es_[t]}
                findings.append(("state_diff", i, q, f"tables(oracle_n,engine_n)={bad}", ""))
                eng.resync_from(os_)
            dml_since_reconnect += 1
            if dml_since_reconnect >= args.reconnect_every:
                dml_since_reconnect = 0
                pre = eng.state()
                eng.reconnect()
                post = eng.state()
                if pre != post:
                    stats["persistence_diff"] += 1
                    bad = {t: (len(pre[t]), len(post[t])) for t in TABLES if pre[t] != post[t]}
                    findings.append(("persistence_diff", i, "<reconnect>", f"{bad}", ""))
                    eng.resync_from(oracle.state())

    # final persistence check
    pre = eng.state(); eng.reconnect(); post = eng.state()
    if pre != post:
        findings.append(("persistence_diff", len(corpus), "<final reconnect>", "", ""))

    print("=" * 70)
    print("stats:", dict(stats))
    # Two tiers of divergence. DATA_KINDS (module-level) are correctness defects — the engine returned
    # different rows, left tables in a different state, or lost writes across a reconnect. error_mismatch
    # is weaker: the engine raised where DuckDB succeeded (usually a fail-loud gap on exotic syntax
    # delta_rs can't route). Both are always PRINTED; --fail-on decides which sets the exit code.
    fatal = findings if args.fail_on == "any" else [f for f in findings if f[0] in DATA_KINDS]
    if findings:
        print(f"\n*** {len(findings)} FINDING(S) ***")
        for kind, i, q, a, b in findings:
            print(f"\n[{kind}] stmt #{i}:\n  {q[:300]}\n  {a}\n  {b}")
    if args.summary_md:
        _write_summary_md(args.summary_md, args, stats, findings, fatal)
    if fatal:
        print(f"\nFAIL (--fail-on {args.fail_on}): {len(fatal)} fatal finding(s).")
        sys.exit(1)
    if findings:
        print(f"\nPASS (--fail-on data): no wrong-data / lost-write divergence; "
              f"{len(findings)} error_mismatch(es) reported above for triage.")
        return
    print("\nNO DIVERGENCE: engine matched the DuckDB oracle on every generated statement.")


if __name__ == "__main__":
    main()
