#!/usr/bin/env python3
"""
sqllogictest-style runner for duckrun (or any engine exposing connect(path).sql(q)).

Dialect (close to DuckDB's sqllogictest, one VALUE per line in results):

  # comment                     (only between records)

  statement ok
  <SQL, may span lines, terminated by a blank line>

  statement error
  <SQL>
  ----
  <optional substring that must appear in the error message>

  statement maybe
  <SQL>                         succeed OR raise cleanly -- used for optional
                                features; outcome is reported, never a failure.

  query <types> [rowsort|valuesort|nosort]
  <SQL, terminated by a line containing exactly ---->
  ----
  <expected values, ONE VALUE PER LINE, row-major, terminated by blank line>

  loop <var> <start> <end>      inclusive start, exclusive end; ${var} substituted
  endloop

  reconnect                     close the connection and reopen the same path
                                (durability check across sessions)

Value formatting conventions:
  NULL          -> NULL
  ''            -> (empty)
  bool          -> true / false
  float         -> %.3f  (NaN / Infinity / -Infinity spelled out)
  bytes         -> 0x<hex>
  str           -> \\n \\t \\r and backslash escaped
  everything else -> str(value)   (Decimal keeps scale, lists/structs as Python repr)

Usage:
  python runner.py tests/05_adversarial_parser.slt --db /tmp/db1
  DUCKRUN_MODULE=duckrun python run_all.py tests/
"""

import argparse
import importlib
import math
import os
import sys
import tempfile
import traceback
from decimal import Decimal


class RunnerParseError(Exception):
    pass


# ---------------------------------------------------------------- formatting

def fmt_value(v):
    if v is None:
        return "NULL"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return f"{v:.3f}"
    if isinstance(v, str):
        if v == "":
            return "(empty)"
        return (
            v.replace("\\", "\\\\")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        )
    if isinstance(v, (bytes, bytearray)):
        return "0x" + bytes(v).hex()
    if isinstance(v, Decimal):
        return str(v)
    return str(v)


# ------------------------------------------------------------------- parsing

def parse_records(lines):
    """Parse a list of raw lines into a list of record tuples."""
    recs = []
    i, n = 0, len(lines)
    while i < n:
        stripped = lines[i].strip()
        if stripped == "" or stripped.startswith("#"):
            i += 1
            continue
        tok = stripped.split()
        head = tok[0].lower()

        if head == "loop":
            if len(tok) != 4:
                raise RunnerParseError(f"line {i+1}: loop needs <var> <start> <end>")
            var, lo, hi = tok[1], int(tok[2]), int(tok[3])
            depth, body = 1, []
            i += 1
            while i < n:
                t = lines[i].strip()
                first = t.split()[0].lower() if t.split() else ""
                if first == "loop":
                    depth += 1
                elif first == "endloop":
                    depth -= 1
                    if depth == 0:
                        break
                body.append(lines[i])
                i += 1
            if depth != 0:
                raise RunnerParseError("unterminated loop")
            i += 1  # skip endloop
            recs.append(("loop", var, lo, hi, body))

        elif head == "statement":
            if len(tok) < 2 or tok[1] not in ("ok", "error", "maybe"):
                raise RunnerParseError(f"line {i+1}: statement needs ok|error|maybe")
            mode = tok[1]
            i += 1
            sql = []
            while i < n and lines[i].strip() != "" and lines[i].strip() != "----":
                sql.append(lines[i].rstrip("\n"))
                i += 1
            expected_err = None
            if i < n and lines[i].strip() == "----":
                i += 1
                exp = []
                while i < n and lines[i].strip() != "":
                    exp.append(lines[i].rstrip("\n"))
                    i += 1
                expected_err = "\n".join(exp)
            if not sql:
                raise RunnerParseError(f"line {i+1}: statement with no SQL")
            recs.append(("statement", mode, "\n".join(sql), expected_err))

        elif head == "query":
            sort = "nosort"
            for t in tok[1:]:
                if t in ("rowsort", "valuesort", "nosort"):
                    sort = t
            i += 1
            sql = []
            while i < n and lines[i].strip() != "----":
                if lines[i].strip() == "":
                    raise RunnerParseError(
                        f"line {i+1}: blank line inside query SQL (missing ---- ?)"
                    )
                sql.append(lines[i].rstrip("\n"))
                i += 1
            if i >= n:
                raise RunnerParseError("query without ---- separator")
            i += 1  # skip ----
            exp = []
            while i < n and lines[i].rstrip("\n") != "":
                exp.append(lines[i].rstrip("\n"))
                i += 1
            recs.append(("query", sort, "\n".join(sql), exp))

        elif head == "reconnect":
            recs.append(("reconnect",))
            i += 1

        else:
            raise RunnerParseError(f"line {i+1}: unknown directive {stripped!r}")
    return recs


# ------------------------------------------------------------------- session

class Session:
    def __init__(self, db_path, module_name):
        self.db_path = db_path
        self.module = importlib.import_module(module_name)
        self.con = self.module.connect(db_path)

    def reconnect(self):
        try:
            self.con.close()
        except Exception:
            pass
        self.con = self.module.connect(self.db_path)

    def sql(self, q):
        return self.con.sql(q)


def fetch_rows(res):
    if res is None:
        return []
    fetch = getattr(res, "fetchall", None)
    if callable(fetch):
        return [tuple(r) for r in fetch()]
    if isinstance(res, list):
        return [tuple(r) for r in res]
    try:
        return [tuple(r) for r in res]
    except TypeError:
        raise RunnerParseError(f"cannot extract rows from result type {type(res)!r}")


# ----------------------------------------------------------------- execution

class Stats:
    def __init__(self):
        self.executed = 0
        self.passed = 0
        self.failures = []          # list of (record_desc, message)
        self.maybe = []             # list of (sql_snippet, "supported"/"unsupported: err")

    def fail(self, desc, msg):
        self.failures.append((desc, msg))

    def ok(self):
        self.passed += 1


def snippet(sql, width=90):
    s = " ".join(sql.split())
    return s if len(s) <= width else s[: width - 3] + "..."


def run_records(recs, session, stats, stop_on_fail):
    for rec in recs:
        kind = rec[0]

        if kind == "loop":
            _, var, lo, hi, body = rec
            for val in range(lo, hi):
                sub = [ln.replace("${" + var + "}", str(val)) for ln in body]
                inner = parse_records(sub)
                if not run_records(inner, session, stats, stop_on_fail):
                    return False
            continue

        if kind == "reconnect":
            stats.executed += 1
            try:
                session.reconnect()
                stats.ok()
            except Exception as e:
                stats.fail("reconnect", f"reconnect raised: {e!r}")
                if stop_on_fail:
                    return False
            continue

        if kind == "statement":
            _, mode, sql, expected_err = rec
            stats.executed += 1
            try:
                res = session.sql(sql)
                # force materialization: lazy relations must actually execute
                # so runtime errors surface here, not silently never
                f = getattr(res, "fetchall", None)
                if callable(f):
                    f()
                err = None
            except Exception as e:
                err = e
            if mode == "ok":
                if err is None:
                    stats.ok()
                else:
                    stats.fail(snippet(sql), f"expected success, got: {err!r}")
                    if stop_on_fail:
                        return False
            elif mode == "error":
                if err is None:
                    stats.fail(snippet(sql), "expected an error, statement succeeded")
                    if stop_on_fail:
                        return False
                elif expected_err and expected_err.lower() not in str(err).lower():
                    stats.fail(
                        snippet(sql),
                        f"error did not contain {expected_err!r}: {err!r}",
                    )
                    if stop_on_fail:
                        return False
                else:
                    stats.ok()
            else:  # maybe
                stats.ok()
                stats.maybe.append(
                    (snippet(sql), "supported" if err is None else f"unsupported: {snippet(str(err))}")
                )
            continue

        if kind == "query":
            _, sort, sql, expected = rec
            stats.executed += 1
            try:
                rows = fetch_rows(session.sql(sql))
            except Exception as e:
                stats.fail(snippet(sql), f"query raised: {e!r}")
                if stop_on_fail:
                    return False
                continue
            frows = [[fmt_value(v) for v in row] for row in rows]
            if sort == "rowsort":
                frows.sort()
            flat = [v for row in frows for v in row]
            if sort == "valuesort":
                flat.sort()
            if flat != expected:
                diff = []
                for j in range(max(len(flat), len(expected))):
                    g = flat[j] if j < len(flat) else "<missing>"
                    e = expected[j] if j < len(expected) else "<missing>"
                    mark = "  " if g == e else "->"
                    diff.append(f"  {mark} got {g!r:<40} expected {e!r}")
                stats.fail(snippet(sql), "result mismatch:\n" + "\n".join(diff[:40]))
                if stop_on_fail:
                    return False
            else:
                stats.ok()
            continue

    return True


def run_file(path, db_path, module_name, stop_on_fail=False, verbose=True):
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    recs = parse_records(lines)
    session = Session(db_path, module_name)
    stats = Stats()
    try:
        run_records(recs, session, stats, stop_on_fail)
    finally:
        try:
            session.con.close()
        except Exception:
            pass
    if verbose:
        name = os.path.basename(path)
        status = "PASS" if not stats.failures else "FAIL"
        print(f"[{status}] {name}: {stats.passed}/{stats.executed} records passed")
        for desc, msg in stats.failures:
            print(f"    FAIL: {desc}\n          {msg}")
        for sql, outcome in stats.maybe:
            print(f"    MAYBE [{outcome}] {sql}")
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slt_file")
    ap.add_argument("--db", default=None, help="database path (default: fresh temp dir)")
    ap.add_argument("--module", default=os.environ.get("DUCKRUN_MODULE", "duckrun"))
    ap.add_argument("--stop-on-fail", action="store_true")
    args = ap.parse_args()
    db = args.db or tempfile.mkdtemp(prefix="duckrun_slt_")
    stats = run_file(args.slt_file, db, args.module, args.stop_on_fail)
    sys.exit(1 if stats.failures else 0)


if __name__ == "__main__":
    main()
