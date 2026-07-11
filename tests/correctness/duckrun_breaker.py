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

Usage:
  python duckrun_breaker.py --workers 6 --ops 40                # phase A: contention
  python duckrun_breaker.py --workers 6 --ops 200 --kill        # phase B: + SIGKILL chaos
  python duckrun_breaker.py --fence                             # phase C: read-target fence (V9)
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
        expected = {}          # key -> val (this worker's keys, definite branch)
        pending = None         # (oid, effect) intent awaiting outcome

        def apply(eff, state):
            s = dict(state)
            s.update({int(k): v for k, v in eff["adds"].items()})
            s.update({int(k): v for k, v in eff["sets"].items()})
            for k in eff["dels"]:
                s.pop(int(k), None)
            return s

        for r in recs:
            if r["phase"] == "intent":
                pending = (r["oid"], r)
            elif r["phase"] == "outcome":
                oid, eff = pending
                pending = None
                if r["outcome"] == "committed":
                    stats["committed"] += 1
                    expected = apply(eff, expected)
                else:
                    stats["refused"] += 1
                    # V2 probe: a refused op must leave NO trace. Its adds must be absent,
                    # its sets/dels must not be reflected — detected below because `expected`
                    # keeps the pre-op values and burned keys never reappear.
            elif r["phase"] == "clean_exit":
                stats["clean_exits"] += 1
            elif r["phase"] in ("wedge", "fatal_wedge"):
                violations.append(f"V7 wedged session: worker {wid} {r}")

        # this worker's slice of the actual table
        lo, hi = wid * KEYSPACE, (wid + 1) * KEYSPACE
        mine = {k: v for k, v in actual.items() if lo <= k < hi}

        if pending is None:
            if mine != expected:
                _diff(violations, wid, expected, mine, tag="")
        else:
            stats["inflight"] += 1
            oid, eff = pending
            branch_out = expected
            branch_in = apply(eff, expected)
            if mine == branch_out or mine == branch_in:
                pass  # all-or-nothing honored
            else:
                violations.append(
                    f"V8 crash tear: worker {wid} in-flight {oid} ({eff['kind']}) is neither "
                    f"fully-in nor fully-out")
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--ops", type=int, default=40)
    ap.add_argument("--batch", type=int, default=5)
    ap.add_argument("--kill", action="store_true", help="SIGKILL workers at random moments")
    ap.add_argument("--kill-delay", type=float, default=2.0)
    ap.add_argument("--fence", action="store_true",
                    help="run ONLY the deterministic read-target fence probe (V9), no chaos")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--root", default=None)
    args = ap.parse_args()

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

    # seed the table (single writer, no contention)
    import duckrun
    c = duckrun.connect(root, read_only=False)
    c.sql("CREATE TABLE acct AS SELECT -1::BIGINT AS k, -1::BIGINT AS val, -1::INT AS w")
    c.sql("DELETE FROM acct WHERE k = -1")
    c.close()

    procs = []
    for wid in range(args.workers):
        p = subprocess.Popen(
            [sys.executable, __file__, "--_worker", str(wid), "--root", root,
             "--ops", str(args.ops), "--seed", str(seed + wid), "--batch", str(args.batch)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)

    if args.kill:
        alive = list(range(args.workers))
        rng.shuffle(alive)
        for wid in alive[: max(1, args.workers // 2)]:   # kill half of them
            time.sleep(rng.uniform(0.5, args.kill_delay))
            if procs[wid].poll() is None:
                os.kill(procs[wid].pid, signal.SIGKILL)
                print(f"  SIGKILL -> worker {wid}")

    for p in procs:
        p.wait()
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
