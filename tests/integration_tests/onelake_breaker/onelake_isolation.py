"""
onelake_isolation: a BLACK-BOX transactional-isolation checker for duckrun / delta-rs, in pure Python.

This is a from-scratch rebuild of the chaos harness around one idea, borrowed from Elle
(Kingsbury & Alvaro, VLDB 2021) but with NO JVM and NO Clojure — Elle's *implementation* is on the
JVM; its *method* is just graph theory:

  1. Run concurrent transactions and record only what each client OBSERVED (a history) — never peek at
     engine internals. Pure black box.
  2. From that history INFER a dependency graph between transactions: ww (which write came first),
     wr (who read whose write), rw (who failed to see whose write — an anti-dependency).
  3. A CYCLE in that graph is an isolation anomaly, and the SHAPE of the cycle names it:
        ww-only .......... G0   (dirty write / write cycle)
        + wr, no rw ...... G1c  (cyclic information flow)
        exactly one rw ... G-single (read skew — a snapshot-isolation violation)
        two or more rw ... G2   (anti-dependency cycle — a serializability violation)
     Plus non-cycle checks: G1a (read of an aborted/refused write), duplicate writes, garbage reads
     (a value read that nobody ever wrote), and lost updates (an acknowledged append missing at the end).

Why this beats an enumerated V1..V9 oracle: it doesn't need you to think of the failure ahead of
time, it needs the workers to CONTEND. So the datatype and workload are chosen to be maximally
telling under Elle's "list-append" trick:

  • Each key k is a row in table `lists` holding a comma-joined string of unique integer tokens.
  • append(k, tok)  ==  UPDATE lists SET v = v || ',' || tok WHERE k = k   — a self-referential
    read-modify-write (duckrun fences it; delta-rs OCC must serialize concurrent appends to the same
    key or a token is silently lost). This is the exact path a lost update lives on.
  • read(k)  ==  SELECT v FROM lists WHERE k = k   — split on ',' gives the key's version order for
    free (append-only + unique tokens = "recoverability": every observed version maps to one write).

Workers share a SMALL key pool (default 8) so they actually collide. A refused write (OCC conflict)
is a clean :fail — its token must never appear in any read (else G1a). A SIGKILLed in-flight write is
:info (unknown — may or may not have committed).

delta-rs OCC tops out at snapshot isolation, so the interesting verdicts are G-single / G2 / lost
update. Correct behaviour = no cycles = no lost tokens, under heavy same-key contention.

Usage (OneLake — WAREHOUSE_PATH + ONELAKE_TOKEN in env):
  python onelake_isolation.py --workers 6 --ops 30 --keys 8
  python onelake_isolation.py --workers 6 --ops 60 --keys 8 --kill        # + SIGKILL (:info) chaos
Usage (local smoke — no creds):
  python onelake_isolation.py --root /tmp/wh --workers 6 --ops 30
"""
import argparse
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path


# --------------------------------------------------------------------------- OneLake plumbing (shared)

def _storage_options(root):
    if str(root).startswith("abfss://"):
        tok = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
        return {"bearer_token": tok} if tok else None
    return None


def _connect(root, schema, read_only):
    import duckrun
    return duckrun.connect(root, storage_options=_storage_options(root),
                           schema=schema, read_only=read_only)


DEFAULT_SCHEMA = os.environ.get("DBT_SCHEMA") or os.environ.get("DUCKRUN_IT_SCHEMA") or "dbo"


# --------------------------------------------------------------------------- workload (records a history)

def _parse_v(v):
    """A stored comma-string -> ordered list of int tokens (the key's observed version order)."""
    if not v:
        return []
    return [int(x) for x in v.split(",") if x != ""]


def worker_main(root, scratch, schema, wid, n_ops, seed, n_keys, p_read):
    rng = random.Random(seed)
    hist = open(Path(scratch) / f"hist_{wid}.jsonl", "a", buffering=1)

    def emit(rec):
        hist.write(json.dumps(rec) + "\n")
        hist.flush()
        os.fsync(hist.fileno())

    con = _connect(root, schema, read_only=False)

    for op_id in range(n_ops):
        k = rng.randrange(n_keys)
        tid = f"w{wid}o{op_id}"
        if rng.random() < p_read:
            # read(k): observe the whole version order of one key
            t0 = time.time()
            row = con.sql(f"SELECT v FROM lists WHERE k = {k}").fetchone()
            t1 = time.time()
            emit({"id": tid, "process": wid, "type": "ok", "kind": "r", "t0": t0, "t1": t1,
                  "value": [["r", k, _parse_v(row[0] if row else "")]]})
        else:
            # append(k, tok): self-referential read-modify-write; the fenced path where a lost update lives
            tok = wid * 1_000_000 + op_id
            stmt = (f"UPDATE lists SET v = CASE WHEN v = '' THEN '{tok}' "
                    f"ELSE v || ',' || '{tok}' END WHERE k = {k}")
            # write intent BEFORE running: a SIGKILL here leaves a dangling intent -> :info at load time
            emit({"id": tid, "process": wid, "phase": "intent", "kind": "append",
                  "value": [["append", k, tok]]})
            t0 = time.time()
            try:
                con.sql(stmt)
                t1 = time.time()
                emit({"id": tid, "process": wid, "type": "ok", "kind": "append", "t0": t0, "t1": t1,
                      "value": [["append", k, tok]]})
            except Exception as e:
                t1 = time.time()
                emit({"id": tid, "process": wid, "type": "fail", "kind": "append", "t0": t0, "t1": t1,
                      "value": [["append", k, tok]],
                      "err": f"{type(e).__name__}: {str(e)[:160]}"})

    con.close()
    emit({"id": f"w{wid}end", "process": wid, "type": "clean_exit"})


def compactor_main(root, scratch, schema, sentinel, interval):
    """Optional adversary: loop VACUUM (optimize.compact, dataChange=false) while the appends run, so
    layout commits interleave with the read-modify-writes. Lost races are caught, never fatal."""
    con = _connect(root, schema, read_only=False)
    clog = open(Path(scratch) / "compactor.jsonl", "a", buffering=1)
    n_ok = n_race = 0
    while not os.path.exists(sentinel):
        t0 = time.time()
        try:
            con.sql("VACUUM lists")
            n_ok += 1
        except Exception:
            n_race += 1
        clog.write(json.dumps({"dur": time.time() - t0, "ok": n_ok, "race": n_race}) + "\n")
        clog.flush()
        time.sleep(interval)
    con.close()


# --------------------------------------------------------------------------- history loading

def load_history(scratch, n_workers):
    """Fold each worker's JSONL into completed transactions, ordered by completion time. A dangling
    append intent with no matching outcome (a SIGKILLed in-flight write) becomes :info — Elle's "we do
    not know whether this committed", which forbids nothing but permits its token to appear or not."""
    txns = []
    for wid in range(n_workers):
        hp = Path(scratch) / f"hist_{wid}.jsonl"
        if not hp.exists():
            continue
        pending = None
        for line in hp.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("phase") == "intent":
                pending = r
                continue
            if r.get("type") in ("ok", "fail"):
                pending = None
                txns.append(r)
            # clean_exit: ignore
        if pending is not None:                      # killed mid-append -> unknown outcome
            pending.pop("phase", None)
            pending["type"] = "info"
            pending["t1"] = pending.get("t1", 0)
            txns.append(pending)
    txns.sort(key=lambda t: t.get("t1", 0))
    for i, t in enumerate(txns):
        t["seq"] = i                                 # a stable id for witnesses/edges
    return txns


# --------------------------------------------------------------------------- the black-box checker

def _tarjan_sccs(nodes, adj):
    """Iterative Tarjan — strongly-connected components. Any SCC with >1 node (or a self-loop) holds a
    cycle. Iterative so a deep graph over OneLake can't blow the recursion limit."""
    index = {}
    low = {}
    onstack = {}
    stack = []
    out = []
    counter = [0]
    for root in nodes:
        if root in index:
            continue
        work = [(root, iter(adj.get(root, ())))]
        while work:
            v, it = work[-1]
            if v not in index:
                index[v] = low[v] = counter[0]
                counter[0] += 1
                stack.append(v)
                onstack[v] = True
            advanced = False
            for w in it:
                if w not in index:
                    work.append((w, iter(adj.get(w, ()))))
                    advanced = True
                    break
                elif onstack.get(w):
                    low[v] = min(low[v], index[w])
            if advanced:
                continue
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    onstack[w] = False
                    comp.append(w)
                    if w == v:
                        break
                out.append(comp)
            work.pop()
            if work:
                p = work[-1][0]
                low[p] = min(low[p], low[v])
    return out


def check_history(txns, final_state):
    """Infer the dependency graph from observed transactions and classify every cycle. `final_state` is
    the authoritative end-of-run read of the table (key -> ordered token list), used for lost/garbage
    checks. Returns (anomalies, stats)."""
    anomalies = []

    # --- index writes ---------------------------------------------------------------------------
    writer = {}                 # token -> seq of the txn that appended it
    wtype = {}                  # token -> 'ok' | 'fail' | 'info'
    for t in txns:
        for op in t["value"]:
            if op[0] == "append":
                tok = op[2]
                if tok in writer:
                    anomalies.append(("duplicate-write", f"token {tok} appended by two txns "
                                      f"(#{writer[tok]} and #{t['seq']})"))
                writer[tok] = t["seq"]
                wtype[tok] = t["type"]

    # --- per-key version order (list-append recoverability) -------------------------------------
    reads = defaultdict(list)                         # key -> [(seq, [tokens])]
    for t in txns:
        for op in t["value"]:
            if op[0] == "r":
                reads[op[1]].append((t["seq"], op[2]))
    # the authoritative order per key = the final read (superset of any mid-run read).
    version = {k: toks for k, toks in final_state.items()}
    for k, obs in reads.items():
        if k not in version:
            version[k] = max((toks for _, toks in obs), key=len, default=[])

    pos = {k: {tok: i for i, tok in enumerate(order)} for k, order in version.items()}

    # every read must be a PREFIX of the authoritative order (append-only ⇒ nothing else is legal).
    for k, obs in reads.items():
        order = version[k]
        for seq, toks in obs:
            if toks != order[:len(toks)]:
                anomalies.append(("inconsistent-version-order",
                                  f"txn #{seq} read key {k} as {toks} — not a prefix of {order}"))

    # --- G1a / garbage: a read observed a token that was never committed ------------------------
    for t in txns:
        for op in t["value"]:
            if op[0] == "r":
                for tok in op[2]:
                    if tok not in writer:
                        anomalies.append(("garbage-read",
                                          f"txn #{t['seq']} read token {tok} on key {op[1]} that no txn wrote"))
                    elif wtype.get(tok) == "fail":
                        anomalies.append(("G1a-aborted-read",
                                          f"txn #{t['seq']} read token {tok} whose append was REFUSED (#{writer[tok]})"))

    # --- lost updates: an acknowledged (:ok) append missing from the final table -----------------
    final_tokens = {tok for toks in final_state.values() for tok in toks}
    for tok, ty in wtype.items():
        if ty == "ok" and tok not in final_tokens:
            anomalies.append(("lost-update",
                              f"token {tok} was committed (#{writer[tok]}) but is absent from the final table"))

    # --- build the dependency graph: ww, wr, rw --------------------------------------------------
    adj = defaultdict(set)
    etypes = defaultdict(set)                          # (u,v) -> {'ww','wr','rw'}

    def edge(u, v, kind):
        if u is not None and v is not None and u != v:
            adj[u].add(v)
            etypes[(u, v)].add(kind)

    # ww: consecutive tokens in a key's version order -> writer(a) precedes writer(b)
    for k, order in version.items():
        for a, b in zip(order, order[1:]):
            edge(writer.get(a), writer.get(b), "ww")

    # wr / rw: for each read, wr from the writers of tokens it saw; rw to the writer of the next token
    for t in txns:
        for op in t["value"]:
            if op[0] != "r":
                continue
            k, seen = op[1], op[2]
            order = version[k]
            for tok in seen:                           # wr: reader observed this write
                edge(writer.get(tok), t["seq"], "wr")
            p = len(seen)                              # reader saw the prefix [0:p]; it MISSED order[p:]
            if p < len(order):                         # rw: reader precedes the writer of the next token
                edge(t["seq"], writer.get(order[p]), "rw")

    # --- cycles -> anomalies ---------------------------------------------------------------------
    nodes = {t["seq"] for t in txns}
    for comp in _tarjan_sccs(nodes, adj):
        cyc = set(comp)
        has_self = any(v in adj.get(v, ()) for v in cyc)
        if len(comp) < 2 and not has_self:
            continue
        kinds = set()
        for (u, v), ks in etypes.items():
            if u in cyc and v in cyc:
                kinds |= ks
        rw = sum(1 for (u, v), ks in etypes.items() if u in cyc and v in cyc and "rw" in ks)
        if kinds == {"ww"}:
            name = "G0-write-cycle"
        elif "rw" not in kinds:
            name = "G1c-cyclic-info-flow"
        elif rw == 1:
            name = "G-single-read-skew (snapshot-isolation violation)"
        else:
            name = "G2-anti-dependency (serializability violation)"
        anomalies.append((name, f"cycle over txns {sorted(comp)} · edges={sorted(kinds)}"))

    stats = {"txns": len(txns), "appends_ok": sum(1 for v in wtype.values() if v == "ok"),
             "appends_fail": sum(1 for v in wtype.values() if v == "fail"),
             "appends_info": sum(1 for v in wtype.values() if v == "info"),
             "reads": sum(len(o) for o in reads.values()), "keys": len(version)}
    return anomalies, stats


# --------------------------------------------------------------------------- reporting

def _summary_write(md):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")
    try:
        print(md)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "ascii"
        print(md.encode(enc, "replace").decode(enc))


def _qps(txns):
    ap = [t for t in txns if t.get("kind") == "append" and "t0" in t]
    ok = [t for t in ap if t["type"] == "ok"]
    if not ap:
        return 0.0, 0.0, 0
    wall = max(t["t1"] for t in ap) - min(t["t0"] for t in ap)
    return (len(ok) / wall if wall > 0 else 0.0), wall, len(ok)


def report(anomalies, stats, txns, root, schema, version_final, compactor):
    qps, wall, n_ok = _qps(txns)
    lines = ["## onelake_isolation — black-box transactional-isolation check", "",
             f"warehouse `{root}` · schema `{schema}` · final Delta version `{version_final}`", "",
             "| observed | count |", "|---|---|",
             f"| transactions | {stats['txns']} |",
             f"| appends: committed / refused / killed | {stats['appends_ok']} / {stats['appends_fail']} / {stats['appends_info']} |",
             f"| reads | {stats['reads']} |",
             f"| keys under contention | {stats['keys']} |",
             f"| committed-append QPS (wall {wall:.1f}s) | {qps:.2f} |"]
    if compactor is not None:
        lines.append(f"| concurrent compactions ok / lost-race | {compactor['ok']} / {compactor['race']} |")
    lines += ["", "### verdict", ""]
    if not anomalies:
        lines.append("✅ **NO ISOLATION ANOMALIES** — every client observation is consistent with a "
                     "single serial order; no lost/garbage/aborted reads; no lost updates. Snapshot "
                     "isolation held under same-key contention.")
    else:
        by = defaultdict(list)
        for name, detail in anomalies:
            by[name].append(detail)
        lines.append(f"❌ **{len(anomalies)} ANOMALY WITNESS(ES)** across {len(by)} class(es):")
        for name in sorted(by):
            lines.append(f"\n**{name}** ({len(by[name])}):")
            for d in by[name][:8]:
                lines.append(f"- {d}")
            if len(by[name]) > 8:
                lines.append(f"- … +{len(by[name]) - 8} more")
    _summary_write("\n".join(lines))
    return not anomalies


# --------------------------------------------------------------------------- supervisor + CLI

def read_final_state(root, schema):
    """The authoritative end read, through duckrun — key -> ordered token list. Also returns the Delta
    version so the report can show how many commits the run produced."""
    con = _connect(root, schema, read_only=True)
    rows = con.sql("SELECT k, v FROM lists ORDER BY k").fetchall()
    detail = con.sql("DESCRIBE DETAIL lists")
    ver = dict(zip(detail.columns, detail.fetchone())).get("version")
    con.close()
    return {k: _parse_v(v) for k, v in rows}, ver


def run(root, scratch, schema, n_workers, ops, seed, n_keys, p_read, kill, compact_interval):
    c = _connect(root, schema, read_only=False)
    c.sql("DROP TABLE IF EXISTS lists")
    c.sql(f"CREATE TABLE lists AS SELECT range AS k, ''::VARCHAR AS v FROM range({n_keys})")
    c.close()

    common = ["--root", root, "--scratch", scratch, "--schema", schema]
    sentinel = str(Path(scratch) / "done")
    comp = None
    if compact_interval and compact_interval > 0:
        comp = subprocess.Popen([sys.executable, __file__, "--_compactor", "1", *common,
                                 "--sentinel", sentinel, "--interval", str(compact_interval)],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    procs = []
    for wid in range(n_workers):
        procs.append(subprocess.Popen(
            [sys.executable, __file__, "--_worker", str(wid), *common, "--ops", str(ops),
             "--seed", str(seed + wid), "--keys", str(n_keys), "--pread", str(p_read)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

    rng = random.Random(seed)
    if kill:
        order = list(range(n_workers))
        rng.shuffle(order)
        for wid in order[: max(1, n_workers // 2)]:
            time.sleep(rng.uniform(0.5, 2.0))
            if procs[wid].poll() is None:
                os.kill(procs[wid].pid, signal.SIGKILL)
                print(f"  SIGKILL -> worker {wid}")

    for p in procs:
        p.wait()
    Path(sentinel).write_text("done")
    if comp is not None:
        comp.wait()


def _compactor_stats(scratch):
    cp = Path(scratch) / "compactor.jsonl"
    if not cp.exists():
        return None
    last = None
    for line in cp.read_text().splitlines():
        if line.strip():
            last = json.loads(line)
    return last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--ops", type=int, default=30)
    ap.add_argument("--keys", type=int, default=8, help="shared key pool — smaller = more contention")
    ap.add_argument("--pread", type=float, default=0.4, help="fraction of ops that are reads")
    ap.add_argument("--kill", action="store_true", help="SIGKILL half the workers (:info in-flight writes)")
    ap.add_argument("--compact-interval", type=float, default=0.0,
                    help="run a concurrent VACUUM compactor every N seconds (0 = off)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--root", default=None, help="warehouse root (default $WAREHOUSE_PATH); local path for a smoke")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--scratch", default=None)
    args = ap.parse_args()

    root = args.root or os.environ.get("WAREHOUSE_PATH")
    if not root:
        sys.exit("no target: set WAREHOUSE_PATH=abfss://…/Tables or pass --root <local dir>")
    if str(root).startswith("abfss://") and not _storage_options(root):
        sys.exit("OneLake target but no token: set ONELAKE_TOKEN (or AZURE_STORAGE_TOKEN)")
    scratch = args.scratch or tempfile.mkdtemp(prefix="onelake_isolation_")
    os.makedirs(scratch, exist_ok=True)
    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    print(f"warehouse: {root}  schema: {args.schema}  scratch: {scratch}  seed: {seed}  "
          f"keys: {args.keys}  kill: {args.kill}")

    run(root, scratch, args.schema, args.workers, args.ops, seed, args.keys, args.pread,
        args.kill, args.compact_interval)

    txns = load_history(scratch, args.workers)
    final_state, version_final = read_final_state(root, args.schema)
    anomalies, stats = check_history(txns, final_state)
    ok = report(anomalies, stats, txns, root, args.schema, version_final, _compactor_stats(scratch))

    if not args.keep:
        import shutil
        shutil.rmtree(scratch, ignore_errors=True)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    if "--_worker" in sys.argv or "--_compactor" in sys.argv:
        kv = dict(zip(sys.argv[1::2], sys.argv[2::2]))
        if "--_compactor" in kv:
            compactor_main(kv["--root"], kv["--scratch"], kv["--schema"],
                           kv["--sentinel"], float(kv["--interval"]))
        else:
            worker_main(kv["--root"], kv["--scratch"], kv["--schema"], int(kv["--_worker"]),
                        int(kv["--ops"]), int(kv["--seed"]), int(kv["--keys"]), float(kv["--pread"]))
    else:
        main()
