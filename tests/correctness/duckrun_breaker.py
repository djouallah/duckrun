"""
duckrun_breaker: a black-box, multi-process chaos harness for duckrun.

Hunts for:
  V1  lost writes           op reported committed, effect missing (fully or partially)
  V2  phantom writes        op reported refused/errored, effect present (raised-but-applied)
  V3  torn commits          multi-row op partially applied (batch atomicity broken)
  V4  duplicate keys        any key present more than once
  V5  seam disagreement     duckrun read vs deltalake.DeltaTable read different data
  V6  wedged table          fresh connection cannot read or write after chaos
  V7  wedged session        a worker session stops working after an OCC refusal
  V8  crash tear            a SIGKILLed in-flight op is neither fully-in nor fully-out
  V9  fence bypass          a self-ref write (INSERT ... SELECT FROM the same table) commits STALE
                            rows after a foreign commit invades its read window (silent lost update)

Design: N worker processes hammer ONE Delta table through duckrun.connect().sql()
(pure black box, no engine imports). Keys are partitioned per worker, ops are
sequential within a worker, so each worker's expected effect is computed locally
at intent time and written to an fsync'd JSONL ledger BEFORE the op runs; the
outcome is appended AFTER. A killed worker leaves an intent with no outcome ->
checked as all-or-nothing wildcard. The checker replays all ledgers into an
expected key->val map (with wildcard branches) and diffs it against the final
table read through BOTH duckdb and deltalake.

V9 is a SEPARATE, deterministic probe (the chaos harness can't reach it: workers own
disjoint keys, so nobody ever mutates the rows a self-ref reads). It slows a
self-referential INSERT with a UDF, waits for it to reach mid-read, then lands a foreign
commit inside that window and asserts the read-target fence REFUSES the stale write-back
(a fresh post-foreign commit is also fine; only a stale commit is a lost update).

A tamper-evident commit/reveal attestation renders to the CI step summary: --attest-declare seals
the EXPECTED final state (from the ledgers, before the table is read) with a per-run random nonce +
sha256 commitment; a LATER --attest-reveal reads the real table and diffs it against that seal. The
nonce and seed are fresh every run, so the expected page can't be pre-baked, and it is published in
an earlier step than the actual — the prediction visibly lands before the result.

Usage:
  python duckrun_breaker.py --workers 6 --ops 40                # phase A: contention
  python duckrun_breaker.py --workers 6 --ops 200 --kill        # phase B: + SIGKILL chaos
  python duckrun_breaker.py --fence                             # phase C: read-target fence (V9)
  python duckrun_breaker.py --attest-declare --root DIR         # phase D①: seal expected (commit)
  python duckrun_breaker.py --attest-reveal  --root DIR         # phase D②: reveal actual (diff)
  python duckrun_breaker.py ... --keep                          # keep warehouse dir
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

KEYSPACE = 1_000_000  # per-worker key range size

FENCE_TRIGGER_AT = 7    # slow() call count at which the foreign commit fires (mid pass 2 of a 2x5 scan)
FENCE_SLEEP = 1.5       # seconds the UDF stalls per row — wide enough for the foreign commit to land


# --------------------------------------------------------------------------- worker

def worker_main(root: str, wid: int, n_ops: int, seed: int, batch: int) -> None:
    import duckrun  # noqa

    rng = random.Random(seed)
    ledger_path = Path(root) / f"ledger_{wid}.jsonl"
    led = open(ledger_path, "a", buffering=1)

    def log(rec):
        led.write(json.dumps(rec) + "\n")
        led.flush()
        os.fsync(led.fileno())

    con = duckrun.connect(root, read_only=False)

    lo = wid * KEYSPACE
    next_key = lo            # allocator for fresh keys (never reused, even after refusal)
    live = {}                # my believed table state: key -> val (my keys only)
    consecutive_weird = 0

    def alloc(n):
        nonlocal next_key
        ks = list(range(next_key, next_key + n))
        next_key += n
        return ks

    for op_id in range(n_ops):
        kinds = ["ins", "merge", "churn"]
        if live:
            kinds += ["upd", "del", "selfref"]
        kind = rng.choice(kinds)
        oid = f"w{wid}o{op_id}"

        # Build the statement AND the exact expected effect (adds/sets/dels on `live`).
        adds, sets, dels = {}, {}, []
        if kind == "ins":
            ks = alloc(batch)
            val = op_id
            rows = ",".join(f"({k},{val},{wid})" for k in ks)
            stmt = f"INSERT INTO acct VALUES {rows}"
            adds = {k: val for k in ks}
        elif kind == "upd":
            ks = rng.sample(sorted(live), min(batch, len(live)))
            stmt = f"UPDATE acct SET val = {op_id} WHERE k IN ({','.join(map(str, ks))})"
            sets = {k: op_id for k in ks}
        elif kind == "del":
            ks = rng.sample(sorted(live), min(batch, len(live)))
            stmt = f"DELETE FROM acct WHERE k IN ({','.join(map(str, ks))})"
            dels = ks
        elif kind == "selfref":
            # copies MY OWN rows to fresh keys -> reads the target table -> must be fenced.
            src = rng.sample(sorted(live), min(batch, len(live)))
            dst = alloc(len(src))
            sel = " UNION ALL ".join(
                f"SELECT {d} AS k, val, w FROM acct WHERE k = {s}" for s, d in zip(src, dst)
            )
            stmt = f"INSERT INTO acct {sel}"
            adds = {d: live[s] for s, d in zip(src, dst)}  # stale own-read would show here
        elif kind == "merge":
            upd_ks = rng.sample(sorted(live), min(2, len(live))) if live else []
            new_ks = alloc(2)
            rows = [(k, op_id) for k in upd_ks] + [(k, op_id) for k in new_ks]
            vals = ",".join(f"({k},{v},{wid})" for k, v in rows)
            stmt = (
                f"MERGE INTO acct USING (VALUES {vals}) s(k,val,w) ON acct.k = s.k "
                f"WHEN MATCHED THEN UPDATE SET val = s.val "
                f"WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.val, s.w)"
            )
            sets = {k: op_id for k in upd_ks}
            adds = {k: op_id for k in new_ks}
        else:  # churn: CREATE OR REPLACE on a side table, no accounting, must not wedge anything
            stmt = f"CREATE OR REPLACE TABLE churn AS SELECT {op_id} AS op, {wid} AS w"

        log({"oid": oid, "phase": "intent", "kind": kind, "stmt_len": len(stmt),
             "adds": adds, "sets": sets, "dels": dels})
        try:
            con.sql(stmt)
            outcome = "committed"
            live.update(adds); live.update(sets)
            for k in dels:
                live.pop(k, None)
            consecutive_weird = 0
        except Exception as e:  # OCC refusal or anything else — table must show NO trace
            outcome = "refused"
            log({"oid": oid, "phase": "outcome", "outcome": outcome,
                 "err": f"{type(e).__name__}: {str(e)[:200]}"})
            # V7 probe: session must still work after a refusal
            try:
                con.sql("SELECT 1").fetchall()
                consecutive_weird = 0
            except Exception as e2:
                consecutive_weird += 1
                log({"oid": oid, "phase": "wedge", "err": f"{type(e2).__name__}: {str(e2)[:200]}"})
                if consecutive_weird >= 3:
                    log({"oid": oid, "phase": "fatal_wedge"})
                    sys.exit(3)
            continue
        log({"oid": oid, "phase": "outcome", "outcome": outcome})

    con.close()
    log({"oid": "end", "phase": "clean_exit"})


# --------------------------------------------------------------------------- checker

def read_final_state(root):
    import duckrun
    from deltalake import DeltaTable

    path = str(Path(root) / "dbo" / "acct")
    dt = DeltaTable(path)
    rows_rs = {r["k"]: r["val"] for r in dt.to_pyarrow_table().to_pylist()}
    n_rs = len(dt.to_pyarrow_table())

    con = duckrun.connect(root, read_only=True)
    rows_dd_list = con.sql("SELECT k, val FROM acct").fetchall()
    con.close()
    rows_dd = dict(rows_dd_list)
    return rows_rs, n_rs, rows_dd, len(rows_dd_list), dt.version()


def _apply_effect(eff, state):
    """Apply one op's intent (adds/sets/dels) to a key->val state, returning a new dict."""
    s = dict(state)
    s.update({int(k): v for k, v in eff["adds"].items()})
    s.update({int(k): v for k, v in eff["sets"].items()})
    for k in eff["dels"]:
        s.pop(int(k), None)
    return s


def _replay_worker(recs):
    """Replay ONE worker's ledger into its expected key->val map (this worker's keys only) — the
    independent oracle, computed with no table read. Returns (expected, pending, counts): `pending`
    is the last intent with no matching outcome (a killed in-flight op) or None; `counts` carries
    committed / refused / clean_exit / wedge. A `refused` op applies nothing (V2 probe: it must leave
    no trace), so `expected` keeps the pre-op values and burned keys never reappear."""
    expected = {}
    pending = None
    counts = {"committed": 0, "refused": 0, "clean_exit": False, "wedge": []}
    for r in recs:
        if r["phase"] == "intent":
            pending = r
        elif r["phase"] == "outcome":
            eff, pending = pending, None
            if r["outcome"] == "committed":
                counts["committed"] += 1
                expected = _apply_effect(eff, expected)
            else:
                counts["refused"] += 1
        elif r["phase"] == "clean_exit":
            counts["clean_exit"] = True
        elif r["phase"] in ("wedge", "fatal_wedge"):
            counts["wedge"].append(r)
    return expected, pending, counts


def _load_expected(root, n_workers):
    """Replay every ledger into one flattened expected key->val map — the whole run's oracle, built
    with NO table read. (Attestation runs are kill-free, so every op has a definite outcome; any
    leftover in-flight intent is ignored here and would surface as a diff at reveal.)"""
    flat = {}
    for wid in range(n_workers):
        lp = Path(root) / f"ledger_{wid}.jsonl"
        if not lp.exists():
            continue
        recs = [json.loads(l) for l in lp.read_text().splitlines() if l.strip()]
        expected, _pending, _counts = _replay_worker(recs)
        flat.update(expected)
    return flat


def check(root, n_workers):
    violations = []

    rows_rs, n_rs, rows_dd, n_dd, version = read_final_state(root)

    # V4 duplicates (dict collapse hides them -> compare counts)
    if n_rs != len(rows_rs):
        violations.append(f"V4 duplicate keys: deltalake sees {n_rs} rows, {len(rows_rs)} distinct keys")
    if n_dd != len(rows_dd):
        violations.append(f"V4 duplicate keys: duckdb sees {n_dd} rows, {len(rows_dd)} distinct keys")

    # V5 seam disagreement
    if rows_rs != rows_dd:
        only_rs = set(rows_rs) - set(rows_dd)
        only_dd = set(rows_dd) - set(rows_rs)
        diff_val = {k for k in set(rows_rs) & set(rows_dd) if rows_rs[k] != rows_dd[k]}
        violations.append(
            f"V5 seam disagreement: only_deltalake={sorted(only_rs)[:5]} "
            f"only_duckdb={sorted(only_dd)[:5]} val_diff={sorted(diff_val)[:5]}")

    actual = rows_rs
    stats = {"committed": 0, "refused": 0, "inflight": 0, "clean_exits": 0}

    for wid in range(n_workers):
        lp = Path(root) / f"ledger_{wid}.jsonl"
        if not lp.exists():
            continue
        recs = [json.loads(l) for l in lp.read_text().splitlines() if l.strip()]
        expected, pending, counts = _replay_worker(recs)
        stats["committed"] += counts["committed"]
        stats["refused"] += counts["refused"]
        if counts["clean_exit"]:
            stats["clean_exits"] += 1
        for w in counts["wedge"]:
            violations.append(f"V7 wedged session: worker {wid} {w}")

        # this worker's slice of the actual table
        lo, hi = wid * KEYSPACE, (wid + 1) * KEYSPACE
        mine = {k: v for k, v in actual.items() if lo <= k < hi}

        if pending is None:
            if mine != expected:
                _diff(violations, wid, expected, mine, tag="")
        else:
            stats["inflight"] += 1
            branch_out = expected
            branch_in = _apply_effect(pending, expected)
            if mine == branch_out or mine == branch_in:
                pass  # all-or-nothing honored
            else:
                violations.append(
                    f"V8 crash tear: worker {wid} in-flight {pending['oid']} ({pending['kind']}) is "
                    f"neither fully-in nor fully-out")
                _diff(violations, wid, branch_in, mine, tag=" (vs fully-in)")
                _diff(violations, wid, branch_out, mine, tag=" (vs fully-out)")

    # V6: table must accept a fresh write
    try:
        import duckrun
        c = duckrun.connect(root, read_only=False)
        c.sql(f"INSERT INTO acct VALUES ({n_workers * KEYSPACE + 12345}, -1, -1)")
        c.sql("SELECT count(*) FROM acct").fetchall()
        c.close()
    except Exception as e:
        violations.append(f"V6 wedged table: post-chaos write failed: {type(e).__name__}: {str(e)[:200]}")

    return violations, stats, version


def _diff(violations, wid, expected, actual, tag):
    missing = {k: expected[k] for k in set(expected) - set(actual)}
    phantom = {k: actual[k] for k in set(actual) - set(expected)}
    wrongval = {k: (expected[k], actual[k]) for k in set(expected) & set(actual)
                if expected[k] != actual[k]}
    if missing:
        violations.append(f"V1 lost writes{tag}: worker {wid} missing {dict(list(missing.items())[:5])} "
                          f"(+{max(0, len(missing)-5)} more)")
    if phantom:
        violations.append(f"V2 phantom writes{tag}: worker {wid} unexpected {dict(list(phantom.items())[:5])} "
                          f"(+{max(0, len(phantom)-5)} more)")
    if wrongval:
        violations.append(f"V1/V3 wrong values{tag}: worker {wid} {dict(list(wrongval.items())[:5])} "
                          f"(+{max(0, len(wrongval)-5)} more)")


# ----------------------------------------------------------- attestation (commit / reveal)
# A two-phase, tamper-evident report for the CI step summary. Phase ① seals the EXPECTED final state
# — derived only from the workers' fsync'd ledgers, before the table is ever read — together with a
# per-run random nonce and a sha256 commitment binding the two. Phase ② then reads the real table
# (through BOTH duckrun and deltalake), recomputes the commitment to prove the expectation was not
# edited, and diffs. Because the nonce (and the chaos seed) are fresh every run, the "expected" page
# can't be pre-baked, and it is published in an EARLIER step than the actual — so a reader sees the
# prediction land before the result, with no room for after-the-fact fitting.

def _summary_write(md):
    """Append markdown to the GitHub Step Summary (if running under Actions); always echo to stdout."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")
    # stdout may be a non-UTF console (e.g. Windows cp1252) — never let rendering crash the run.
    try:
        print(md)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "ascii"
        print(md.encode(enc, "replace").decode(enc))


def _canonical(expected):
    """Stable, sorted JSON a reader can recompute byte-for-byte to re-verify the commitment."""
    return json.dumps({str(k): expected[k] for k in sorted(expected)}, separators=(",", ":"))


def _commit(nonce, expected):
    import hashlib
    return hashlib.sha256((nonce + "|" + _canonical(expected)).encode()).hexdigest()


def attest_declare(root, n_workers, seed, nonce):
    """Phase ①: from the ledgers ONLY (no table read), publish the expected final state, the per-run
    nonce, and the commitment binding them. Persist to attest.json for the reveal step."""
    expected = _load_expected(root, n_workers)
    commit = _commit(nonce, expected)
    (Path(root) / "attest.json").write_text(json.dumps(
        {"nonce": nonce, "seed": seed, "commit": commit, "n_workers": n_workers,
         "expected": {str(k): v for k, v in expected.items()}}))

    sample = json.dumps(dict(sorted(expected.items())[:8]), indent=2)
    run = os.environ.get("GITHUB_RUN_ID", "local")
    md = f"""## 🔒 duckrun breaker — commit / reveal attestation

### ① EXPECTED — sealed *before* the table is read · run `{run}`

Built straight from the workers' fsync'd ledgers (the independent oracle) — the final Delta table has
**not been read yet** at this point. The nonce is **fresh and random every run**, so this page can't
be pre-baked; the commitment binds that nonce to the exact expected state.

| field | value |
|---|---|
| run nonce (unique per run) | `{nonce}` |
| chaos seed | `{seed}` |
| workers | `{n_workers}` |
| expected distinct keys | **{len(expected)}** |
| commitment = `sha256(nonce \\| canonical_expected)` | `{commit}` |

<details><summary>expected sample — first 8 keys (k → val)</summary>

```json
{sample}
```
</details>

_The reveal below reads the real table and diffs it against this — the nonce and commitment must match._
"""
    _summary_write(md)
    print(f"declared: {len(expected)} expected keys · nonce {nonce} · commit {commit}")


def attest_reveal(root):
    """Phase ②: read the real table through BOTH duckrun and deltalake, recompute the commitment to
    prove the expected map is unchanged, and diff. Exits non-zero on any mismatch."""
    att = json.loads((Path(root) / "attest.json").read_text())
    nonce, commit = att["nonce"], att["commit"]
    expected = {int(k): v for k, v in att["expected"].items()}

    rows_rs, n_rs, rows_dd, n_dd, version = read_final_state(root)

    violations = []
    if n_rs != len(rows_rs):
        violations.append(f"V4 duplicate keys: deltalake {n_rs} rows vs {len(rows_rs)} distinct")
    if n_dd != len(rows_dd):
        violations.append(f"V4 duplicate keys: duckdb {n_dd} rows vs {len(rows_dd)} distinct")
    if rows_rs != rows_dd:
        violations.append("V5 seam disagreement: the duckrun and deltalake reads differ")

    recomputed = _commit(nonce, expected)
    if recomputed != commit:
        violations.append(f"commitment broken: the sealed expected was altered after ① "
                          f"({recomputed} != {commit})")

    actual = rows_rs
    missing = {k: expected[k] for k in set(expected) - set(actual)}
    phantom = {k: actual[k] for k in set(actual) - set(expected)}
    wrong = {k: [expected[k], actual[k]] for k in set(expected) & set(actual) if expected[k] != actual[k]}
    if missing:
        violations.append(f"V1 lost writes: {len(missing)} keys missing, e.g. {dict(list(missing.items())[:5])}")
    if phantom:
        violations.append(f"V2 phantom writes: {len(phantom)} unexpected keys, e.g. {dict(list(phantom.items())[:5])}")
    if wrong:
        violations.append(f"V1/V3 wrong values: {len(wrong)} keys, e.g. {dict(list(wrong.items())[:5])}")

    ok = not violations
    verdict = ("✅ **MATCH** — the actual table equals the expectation sealed in ①, exactly."
               if ok else f"❌ **{len(violations)} VIOLATION(S)**")
    run = os.environ.get("GITHUB_RUN_ID", "local")
    # Only list violations when there are any — on a clean match the verdict line stands alone.
    vlist = ("\n" + "\n".join(f"- {v}" for v in violations)) if violations else ""
    md = f"""### ② ACTUAL — revealed *after* reading the table · run `{run}`

Read back through **both** duckrun and deltalake (two independent readers) and diffed against the
expectation sealed in ①. The nonce and recomputed commitment must equal ① for this to be trustworthy.

| field | value |
|---|---|
| run nonce (echoed from ①) | `{nonce}` |
| commitment recomputed from ①'s expected | `{recomputed}` |
| final Delta version | `{version}` |
| actual distinct keys (duckrun / deltalake) | `{len(rows_dd)}` / `{len(rows_rs)}` |

### {verdict}
{vlist}
"""
    _summary_write(md)
    print(f"revealed: {len(rows_dd)} actual keys · verdict {'MATCH' if ok else 'VIOLATION'}")
    if not ok:
        sys.exit(1)


# --------------------------------------------------------------------------- V9 fence probe

def _fence_victim(root, marker, q):
    # Runs in a spawned child: a self-referential INSERT slowed by a UDF that touches `marker`
    # once per row so the parent can time a foreign commit into the read window. Reports the
    # outcome (committed / refused) back on the queue.
    import os
    import time

    import duckrun
    con = duckrun.connect(root, read_only=False)

    def slow(x):
        with open(marker, "a") as f:
            f.write("x"); f.flush(); os.fsync(f.fileno())
        time.sleep(FENCE_SLEEP)
        return x

    con.con.create_function("slow", slow, [int], int)
    try:
        con.sql("INSERT INTO fence_t SELECT k+1000, slow(val) FROM fence_t")
        q.put("committed")
    except Exception as e:
        q.put(f"refused: {type(e).__name__}: {str(e)[:140]}")
    con.close()


def fence_probe(root):
    """V9 read-target fence. A self-referential INSERT (reads AND writes fence_t) is slowed with a
    UDF; once it is mid-read a foreign writer commits (an insert + an update to a row the victim is
    copying). The victim MUST NOT commit rows derived from its pre-foreign snapshot on top of that
    commit. Refusal (the snapshot-pin CAS tripping) or a commit over the FRESH post-foreign data are
    both correct; a stale commit is a silent lost update. Returns (violations, outcome, struck_at)."""
    import duckrun
    from deltalake import DeltaTable

    ctx = mp.get_context("spawn")   # not set_start_method: don't mutate global mp state
    marker = str(Path(root) / "fence_calls")

    c = duckrun.connect(root, read_only=False)
    c.sql("CREATE TABLE fence_t AS SELECT * FROM (VALUES (0,0),(1,10),(2,20),(3,30),(4,40)) x(k,val)")
    c.close()

    q = ctx.Queue()
    p = ctx.Process(target=_fence_victim, args=(root, marker, q))
    p.start()

    # wait until the victim has scanned into pass 2 (mid write-read), then strike
    while not (os.path.exists(marker) and os.path.getsize(marker) >= FENCE_TRIGGER_AT):
        time.sleep(0.05)
    struck = os.path.getsize(marker)
    fc = duckrun.connect(root, read_only=False)
    fc.sql("INSERT INTO fence_t VALUES (777, 777)")
    fc.sql("UPDATE fence_t SET val = val + 90000 WHERE k = 1")
    fc.close()

    outcome = q.get(timeout=300)
    p.join()

    rows = {r["k"]: r["val"] for r in
            DeltaTable(str(Path(root) / "dbo" / "fence_t")).to_pyarrow_table().to_pylist()}

    violations = []
    if not outcome.startswith("refused"):
        # committed. STALE = the victim copied the pre-foreign snapshot (no row from the foreign
        # insert -> no 1777; the copied k=1 still carries the old 10) yet landed on top of it.
        stale = (1777 not in rows) and rows.get(1001) == 10
        if stale:
            violations.append(
                f"V9 fence bypass: self-ref INSERT committed STALE rows (pre-foreign snapshot) on "
                f"top of the foreign commit — silent lost update. final={sorted(rows.items())}")
        # committed with FRESH rows (saw the foreign commit) is acceptable — no stale write-back.
    return violations, outcome, struck


# --------------------------------------------------------------------------- supervisor

def _run_workers(root, n_workers, ops, batch, seed, rng, kill=False, kill_delay=2.0):
    """Seed the table (single writer), spawn n_workers hammering it in parallel, optionally SIGKILL
    half of them mid-flight, and wait for all to finish. Shared by the chaos run and the attestation."""
    import duckrun
    c = duckrun.connect(root, read_only=False)
    c.sql("CREATE TABLE acct AS SELECT -1::BIGINT AS k, -1::BIGINT AS val, -1::INT AS w")
    c.sql("DELETE FROM acct WHERE k = -1")
    c.close()

    procs = []
    for wid in range(n_workers):
        p = subprocess.Popen(
            [sys.executable, __file__, "--_worker", str(wid), "--root", root,
             "--ops", str(ops), "--seed", str(seed + wid), "--batch", str(batch)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)

    if kill:
        alive = list(range(n_workers))
        rng.shuffle(alive)
        for wid in alive[: max(1, n_workers // 2)]:   # kill half of them
            time.sleep(rng.uniform(0.5, kill_delay))
            if procs[wid].poll() is None:
                os.kill(procs[wid].pid, signal.SIGKILL)
                print(f"  SIGKILL -> worker {wid}")

    for p in procs:
        p.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--ops", type=int, default=40)
    ap.add_argument("--batch", type=int, default=5)
    ap.add_argument("--kill", action="store_true", help="SIGKILL workers at random moments")
    ap.add_argument("--kill-delay", type=float, default=2.0)
    ap.add_argument("--fence", action="store_true",
                    help="run ONLY the deterministic read-target fence probe (V9), no chaos")
    ap.add_argument("--attest-declare", action="store_true",
                    help="run chaos then SEAL the expected state (commit) to the step summary; no table read")
    ap.add_argument("--attest-reveal", action="store_true",
                    help="read the table and REVEAL actual vs the sealed expected (needs --root from declare)")
    ap.add_argument("--nonce", default=None, help="override the per-run nonce (default: run id + random)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--root", default=None)
    args = ap.parse_args()

    # Reveal reads a warehouse that a prior declare step already built — no fresh root, no chaos.
    if args.attest_reveal:
        if not args.root:
            sys.exit("--attest-reveal needs the --root that --attest-declare wrote")
        attest_reveal(args.root)
        return

    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    rng = random.Random(seed)
    root = args.root or tempfile.mkdtemp(prefix="duckrun_breaker_")
    print(f"warehouse: {root}   seed: {seed}   kill={args.kill}")

    # V9 read-target fence: a standalone, deterministic probe (its own table, no workers/ledgers).
    if args.fence:
        violations, outcome, struck = fence_probe(root)
        print(f"fence victim: {outcome}   (foreign struck at slow-call {struck})")
        if violations:
            print(f"\n*** {len(violations)} VIOLATION(S) ***")
            for v in violations:
                print("  " + v)
            sys.exit(1)
        print("\nV9 fence holds — the self-ref write-back did not commit stale rows over the foreign commit.")
        if not args.keep and args.root is None:
            import shutil
            shutil.rmtree(root, ignore_errors=True)
        return

    # Attestation ①: run kill-free chaos (so every op has a definite outcome -> an exact expected map),
    # then seal that expectation. The reveal step runs later against this same --root.
    if args.attest_declare:
        _run_workers(root, args.workers, args.ops, args.batch, seed, rng, kill=False)
        nonce = args.nonce or (f"{os.environ.get('GITHUB_RUN_ID', 'local')}-"
                               f"{os.environ.get('GITHUB_RUN_ATTEMPT', '0')}-{os.urandom(8).hex()}")
        attest_declare(root, args.workers, seed, nonce)
        return

    _run_workers(root, args.workers, args.ops, args.batch, seed, rng, args.kill, args.kill_delay)
    print("workers done; checking invariants…")

    violations, stats, version = check(root, args.workers)
    print(f"\nfinal Delta version: {version}")
    print(f"ops: {stats['committed']} committed, {stats['refused']} refused, "
          f"{stats['inflight']} in-flight (killed), {stats['clean_exits']}/{args.workers} clean exits")
    if violations:
        print(f"\n*** {len(violations)} VIOLATION(S) ***")
        for v in violations:
            print("  " + v)
        sys.exit(1)
    print("\nALL INVARIANTS HOLD — duckrun survived this round.")
    if not args.keep and args.root is None:
        import shutil
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    if "--_worker" in sys.argv:
        i = sys.argv.index("--_worker")
        wid = int(sys.argv[i + 1])
        kv = dict(zip(sys.argv[1::2], sys.argv[2::2]))
        worker_main(kv["--root"], wid, int(kv["--ops"]), int(kv["--seed"]), int(kv["--batch"]))
    else:
        main()
