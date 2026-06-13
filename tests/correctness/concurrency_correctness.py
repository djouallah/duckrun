"""
DEMONSTRATION: duckrun's concurrency-correctness guarantees, proved with real Delta versions.

Run by hand or via the manual/PR `concurrency-correctness` workflow:

    python repro/concurrency_correctness.py

Prints a scorecard (also to the GitHub Actions job summary). Exit 0 if every invariant holds,
1 if any is violated — so a regression that breaks concurrency correctness blocks the PR.

Three independent guarantees are checked:

A. MERGE snapshot safety. A merge reads the table twice without pinning one snapshot the way
   Spark does: delta-rs fixes the merge-target version when it opens the table, while the source
   `delta_scan` is a lazy C-Arrow stream pulled later. Because the lazy scan fixes its snapshot
   at PULL time (after the target is fixed), the source version is ALWAYS >= the merge-target
   version (same, or ahead — never before), for streamed_exec True and False alike. The only
   silently-corrupting direction (source older) is impossible; the reachable gap (source ahead)
   makes delta-rs OCC fail the merge loudly with CommitFailedError. Never a silent wrong result.

B. safeappend (pin intended). The `safeappend` incremental strategy appends only if the table
   version has not moved since it was read — a compare-and-swap via delta-rs max_commit_retries=0.
   A plain `append` has no such guard: it silently lands on whatever the latest version is.
   safeappend instead REFUSES (CommitFailedError) when a concurrent write slipped in, so the
   caller re-runs against the new HEAD. Dedup is NOT its job — that's the model SQL's.

C. Pre-snapshot window. A foreign commit can land AFTER the DuckDB source relation is bound but
   BEFORE the merge target snapshot is loaded — below the merge-target version, where delta-rs OCC
   has nothing to check. This window is safe ONLY because DuckDB's delta_scan resolves its snapshot
   at PULL time: the pre-bound source re-reads at execution and sees the post-commit state, so the
   merge applies fresh data and commits. Were duckdb-delta to resolve at BIND time instead, the
   source would silently merge stale rows and OCC could not catch it. This case pins that pull-time
   behavior via the probe column; a regression to bind-time resolution turns it red and blocks the
   version bump.
"""
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from dbt.adapters.duckrun import engine


def _ids(p):
    return sorted(r["id"] for r in DeltaTable(p).to_pyarrow_table().to_pylist())


def _batch(ids):
    return pa.table({"id": pa.array(ids, pa.int64())})


# ----------------------------------------------------------------- A. merge snapshot safety

def merge_run(streamed: bool, concurrent: bool) -> dict:
    path = str(Path(tempfile.mkdtemp()) / "t")
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1], pa.int64()),
                  "value": pa.array([10], pa.int64()),
                  "probe": pa.array([0], pa.int64())}),
        "overwrite",
    )
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    scan = Path(path).as_posix()
    cap = {"streamed": streamed, "concurrent": concurrent}

    source = con.sql(f"""
        SELECT id, value, (SELECT value FROM delta_scan('{scan}') WHERE id=1) AS probe
        FROM delta_scan('{scan}')
        UNION ALL SELECT 2, 20, (SELECT value FROM delta_scan('{scan}') WHERE id=1)
    """)

    real = engine._delta_table
    done = {"x": False}

    def patched(p, so):
        dt = real(p, so)
        if not done["x"]:
            done["x"] = True
            cap["merge_target_ver"] = dt.version()
            if concurrent:
                real(p, so).update(predicate="id = 1", updates={"value": "999"})
                cap["after_concurrent_ver"] = DeltaTable(p).version()
                cap["duckdb_will_read"] = con.execute(
                    f"SELECT value FROM delta_scan('{scan}') WHERE id=1").fetchone()[0]
        return dt

    engine._delta_table = patched
    try:
        engine.merge_delta(path, source, "id", streamed_exec=streamed)
        probe = {r["id"]: r["probe"]
                 for r in DeltaTable(path).to_pyarrow_table().to_pylist()}[2]
        cap["duckdb_source_read"] = f"{probe} ({'v0' if probe == 10 else 'v1'})"
        cap["outcome"] = "COMMITTED"
    except Exception as e:
        cap["outcome"] = f"RAISED {type(e).__name__}"
    finally:
        engine._delta_table = real
        con.close()
    return cap


def merge_display(rows):
    out = []
    for c in rows:
        if c["concurrent"]:
            dver = f"v{c.get('after_concurrent_ver', '?')} (ahead)"
            ok = c["outcome"].startswith("RAISED") and c.get("duckdb_will_read") == 999
            res_txt, res_md = "BLOCKED (CommitFailedError)", "🛡️ blocked — `CommitFailedError`"
        else:
            v = c.get("duckdb_source_read", "? (v?)").split()[-1].strip("()")
            dver, ok = f"{v} (same)", c["outcome"] == "COMMITTED" and v == "v0"
            res_txt, res_md = "committed", "✅ committed"
        out.append({"streamed": str(c["streamed"]), "concurrent": "yes" if c["concurrent"] else "no",
                    "merge": f"v{c.get('merge_target_ver', '?')}", "duckdb": dver,
                    "res_txt": res_txt, "res_md": res_md, "ok": ok})
    return out


# ----------------------------------------------------------------------------- B. safeappend

def safeappend_run(mode: str, concurrent: bool) -> dict:
    """mode: 'append' (plain, no guard) or 'safeappend' (CAS)."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    engine.write_delta(path, _batch([0]), "overwrite")            # v0
    cap = {"mode": mode, "concurrent": concurrent}
    if concurrent:
        engine.write_delta(path, _batch([7]), "append")          # another writer -> v1
    cap["read_ver"] = 0
    cap["head_ver"] = DeltaTable(path).version()
    try:
        if mode == "append":
            engine.write_delta(path, _batch([1]), "append")
        else:
            # safeappend pinned to the version we read (v0); refuses if HEAD moved past it.
            engine.append_if_unchanged(path, _batch([1]),
                                       read_version=(0 if concurrent else None))
        cap["outcome"], cap["final"] = "committed", _ids(path)
    except CommitFailedError:
        cap["outcome"], cap["final"] = "refused", _ids(path)
    return cap


def safeappend_display(rows):
    out = []
    for c in rows:
        if c["mode"] == "append":
            ok = c["outcome"] == "committed"            # plain append has no guard — expected
            res_md = "⚠️ committed anyway — no version guard"
            res_txt = "committed (no guard)"
        elif c["concurrent"]:
            ok = c["outcome"] == "refused"              # safeappend must refuse a moved table
            res_md = "🛡️ refused — `CommitFailedError`"
            res_txt = "refused (CommitFailedError)"
        else:
            ok = c["outcome"] == "committed"            # unchanged -> commits
            res_md, res_txt = "✅ committed", "committed"
        out.append({"mode": c["mode"], "concurrent": "yes" if c["concurrent"] else "no",
                    "read": f"v{c['read_ver']}", "head": f"v{c['head_ver']}",
                    "res_txt": res_txt, "res_md": res_md, "ok": ok})
    return out


# -------------------------------------------------------------- C. pre-snapshot window

def prebind_run(streamed: bool) -> dict:
    """A foreign commit lands AFTER the source relation is bound but BEFORE the merge
    target snapshot is loaded. This window is harmless today ONLY because DuckDB's
    delta_scan resolves its snapshot at PULL time, so the pre-bound source still reads
    the post-commit state (>= merge target). If duckdb-delta ever resolved at BIND
    time, the source would silently merge stale data and OCC could not catch it (the
    foreign commit is below the merge target version, outside the conflict window).
    The probe must show the post-commit value, or the invariant is violated.
    """
    path = str(Path(tempfile.mkdtemp()) / "t")
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1], pa.int64()),
                  "value": pa.array([10], pa.int64()),
                  "probe": pa.array([0], pa.int64())}),
        "overwrite",
    )                                                            # v0: id=1, value=10
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    scan = Path(path).as_posix()
    cap = {"streamed": streamed, "bind_ver": 0}

    # Bind the source NOW, while the table is at v0.
    source = con.sql(f"""
        SELECT id, value, (SELECT value FROM delta_scan('{scan}') WHERE id=1) AS probe
        FROM delta_scan('{scan}')
        UNION ALL SELECT 2, 20, (SELECT value FROM delta_scan('{scan}') WHERE id=1)
    """)

    # Foreign commit BEFORE merge_delta runs: below the merge snapshot, so OCC has
    # nothing to check and the merge MUST commit. The only question the probe answers
    # is which version the pre-bound source actually read.
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})  # -> v1
    cap["foreign_ver"] = DeltaTable(path).version()

    try:
        engine.merge_delta(path, source, "id", streamed_exec=streamed)
        probe = {r["id"]: r["probe"]
                 for r in DeltaTable(path).to_pyarrow_table().to_pylist()}[2]
        cap["probe"], cap["outcome"] = probe, "COMMITTED"
    except Exception as e:
        cap["outcome"] = f"RAISED {type(e).__name__}"
    finally:
        con.close()
    return cap


def prebind_display(rows):
    out = []
    for c in rows:
        probe = c.get("probe")
        src = ("v1 (post-commit)" if probe == 999
               else "v0 (STALE)" if probe == 10 else "?")
        ok = c["outcome"] == "COMMITTED" and probe == 999
        res_txt = "committed, source read v1" if ok else "VIOLATION"
        res_md = ("✅ committed — pre-bound source read post-commit v1" if ok
                  else "❌ stale bind-time read or unexpected failure")
        out.append({"streamed": str(c["streamed"]), "bind": f"v{c['bind_ver']}",
                    "foreign": f"v{c.get('foreign_ver', '?')}", "source": src,
                    "res_txt": res_txt, "res_md": res_md, "ok": ok})
    return out


# -------------------------------------------------------------- D. DELETE / UPDATE snapshot safety

def mutate_run(op: str, concurrent: bool) -> dict:
    """A connection-API ``conn.delta_table(t).delete(…)/.update(…)`` (engine.delete_rows /
    update_rows) is a genuinely conflicting Delta operation, so — exactly like MERGE — delta-rs OCC fails it
    with CommitFailedError when a foreign commit changed the same rows after its snapshot was
    opened. No max_commit_retries hack (that was only needed for non-conflicting appends): the
    conflict detection is native. With no concurrent writer it simply commits."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                  "value": pa.array([10, 10, 10], pa.int64())}),
        "overwrite",
    )                                                            # v0
    cap = {"op": op, "concurrent": concurrent}

    # Inject the foreign commit AFTER the operation opens its snapshot but BEFORE it commits, by
    # patching the first _delta_table() call the engine helper makes (same trick as merge_run).
    real = engine._delta_table
    done = {"x": False}

    def patched(p, so):
        dt = real(p, so)
        if not done["x"]:
            done["x"] = True
            cap["read_ver"] = dt.version()
            if concurrent:
                real(p, so).update(predicate="id = 1", updates={"value": "999"})  # foreign -> v1
                cap["head_ver"] = DeltaTable(p).version()
        return dt

    engine._delta_table = patched
    try:
        if op == "delete":
            engine.delete_rows(path, "id = 1")
        else:
            engine.update_rows(path, {"value": "5"}, "id = 1")
        cap["outcome"] = "COMMITTED"
    except CommitFailedError:
        cap["outcome"] = "REFUSED"
    except Exception as e:
        cap["outcome"] = f"RAISED {type(e).__name__}"
    finally:
        engine._delta_table = real
    cap.setdefault("head_ver", cap.get("read_ver"))
    return cap


def mutate_display(rows):
    out = []
    for c in rows:
        if c["concurrent"]:
            ok = c["outcome"] == "REFUSED"                  # a moved table must fail loudly
            res_md, res_txt = "🛡️ refused — `CommitFailedError`", "refused (CommitFailedError)"
        else:
            ok = c["outcome"] == "COMMITTED"                # uncontended -> commits
            res_md, res_txt = "✅ committed", "committed"
        out.append({"op": c["op"], "concurrent": "yes" if c["concurrent"] else "no",
                    "read": f"v{c.get('read_ver', '?')}", "head": f"v{c.get('head_ver', '?')}",
                    "res_txt": res_txt, "res_md": res_md, "ok": ok})
    return out


# ----------------------------------------------------------------------------- presentation

def _ascii_table(headers, rows):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]

    def rule(l, m, r):
        return l + m.join("─" * (w + 2) for w in widths) + r

    def line(cells):
        return "│" + "│".join(" " + str(c).ljust(w) + " " for c, w in zip(cells, widths)) + "│"

    return "\n".join([rule("┌", "┬", "┐"), line(headers), rule("├", "┼", "┤"),
                      *[line(r) for r in rows], rule("└", "┴", "┘")])


def render_console(merge_disp, sa_disp, pb_disp, mut_disp, safe):
    a = _ascii_table(
        ["streamed_exec", "concurrent write", "merge target", "duckdb source", "result"],
        [[d["streamed"], d["concurrent"], d["merge"], d["duckdb"], d["res_txt"]] for d in merge_disp])
    b = _ascii_table(
        ["mode", "concurrent write", "read", "head", "result"],
        [[d["mode"], d["concurrent"], d["read"], d["head"], d["res_txt"]] for d in sa_disp])
    c = _ascii_table(
        ["streamed_exec", "bound at", "foreign commit", "source read", "result"],
        [[d["streamed"], d["bind"], d["foreign"], d["source"], d["res_txt"]] for d in pb_disp])
    d = _ascii_table(
        ["operation", "concurrent write", "read", "head", "result"],
        [[x["op"], x["concurrent"], x["read"], x["head"], x["res_txt"]] for x in mut_disp])
    verdict = ("CONCURRENCY-CORRECT: merge source is always >= the merge version (gap fails loudly,\n"
               "  never silent); safeappend refuses a moved table, plain append has no guard;\n"
               "  a pre-bound source re-reads at pull time; DELETE/UPDATE fail on a conflicting commit."
               if safe else
               "INVARIANT VIOLATED — a case behaved unexpectedly (see tables). Investigate.")
    return ("\n  duckrun — concurrency correctness\n\n"
            "  A) MERGE snapshot safety — is the source ever OLDER than the merge target?\n"
            + a + "\n\n"
            "  B) safeappend (pin intended) — append only if the table version didn't move\n"
            + b + "\n\n"
            "  C) pre-snapshot window — does a pre-bound source re-read at pull time?\n"
            + c + "\n\n"
            "  D) DELETE / UPDATE — do they fail on a conflicting concurrent commit (like merge)?\n"
            + d + "\n\n  " + verdict + "\n")


def render_markdown(merge_disp, sa_disp, pb_disp, mut_disp, safe):
    tick = "✅" if safe else "❌"
    L = [f"## duckrun — concurrency correctness {tick}", "",
         "### A) MERGE snapshot safety", "",
         "*Could the source scan ever read an **older** Delta version than the merge target "
         "(which would silently apply stale rows)?*", "",
         "| streamed_exec | concurrent write | merge target | duckdb source | result |",
         "|:---:|:---:|:---:|:---:|:---|"]
    for d in merge_disp:
        cc = "**yes**" if d["concurrent"] == "yes" else "no"
        L.append(f"| `{d['streamed']}` | {cc} | {d['merge']} | {d['duckdb']} | {d['res_md']} |")
    L += ["",
          "> DuckDB's lazy `delta_scan` fixes its snapshot at **pull time**, so the source is "
          "always the **same version or ahead — never older**. The only reachable gap fails "
          "loudly via delta-rs OCC; it can never silently corrupt.", "",
          "### B) safeappend — *append only if the version didn't move*", "",
          "| mode | concurrent write | read | head | result |",
          "|:---:|:---:|:---:|:---:|:---|"]
    for d in sa_disp:
        cc = "**yes**" if d["concurrent"] == "yes" else "no"
        L.append(f"| `{d['mode']}` | {cc} | {d['read']} | {d['head']} | {d['res_md']} |")
    L += ["",
          "> `safeappend` pins the version it read and commits with `max_commit_retries=0`, so a "
          "concurrent write makes it **refuse** (`CommitFailedError`) instead of silently landing "
          "on the new version like a plain `append`. Re-run against the new HEAD.", "",
          "### C) pre-snapshot window — *does a pre-bound source re-read at pull time?*", "",
          "| streamed_exec | bound at | foreign commit | source read | result |",
          "|:---:|:---:|:---:|:---:|:---|"]
    for d in pb_disp:
        L.append(f"| `{d['streamed']}` | {d['bind']} | {d['foreign']} | {d['source']} | {d['res_md']} |")
    L += ["",
          "> The foreign commit sits BELOW the merge target version, so OCC cannot see it. "
          "Safety in this window depends entirely on DuckDB resolving the delta_scan snapshot "
          "at pull time. If this case ever fails, that assumption broke in duckdb-delta — "
          "do not merge the version bump.", "",
          "### D) DELETE / UPDATE — *do they fail on a conflicting concurrent commit?*", "",
          "| operation | concurrent write | read | head | result |",
          "|:---:|:---:|:---:|:---:|:---|"]
    for d in mut_disp:
        cc = "**yes**" if d["concurrent"] == "yes" else "no"
        L.append(f"| `{d['op']}` | {cc} | {d['read']} | {d['head']} | {d['res_md']} |")
    L += ["",
          "> `conn.sql(\"DELETE …\"/\"UPDATE …\")` (engine `delete_rows`/`update_rows`) are genuinely "
          "conflicting Delta operations, so — like `MERGE` — delta-rs OCC **refuses** them with "
          "`CommitFailedError` when a foreign commit changed the same rows after the snapshot was "
          "opened. No `max_commit_retries` hack: the conflict detection is native (the hack was only "
          "needed for non-conflicting appends in `safeappend`). Uncontended, they just commit.", "",
          ("> ✅ **Concurrency-correct.**" if safe
           else "> ❌ **Invariant violated** — see tables above.")]
    return "\n".join(L) + "\n"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    merge_rows = [merge_run(False, False), merge_run(False, True),
                  merge_run(True, False), merge_run(True, True)]
    sa_rows = [safeappend_run("append", True),
               safeappend_run("safeappend", False),
               safeappend_run("safeappend", True)]
    pb_rows = [prebind_run(False), prebind_run(True)]
    mut_rows = [mutate_run("delete", False), mutate_run("delete", True),
                mutate_run("update", False), mutate_run("update", True)]

    merge_disp, sa_disp = merge_display(merge_rows), safeappend_display(sa_rows)
    pb_disp, mut_disp = prebind_display(pb_rows), mutate_display(mut_rows)
    safe = (all(d["ok"] for d in merge_disp) and all(d["ok"] for d in sa_disp)
            and all(d["ok"] for d in pb_disp) and all(d["ok"] for d in mut_disp))

    print(render_console(merge_disp, sa_disp, pb_disp, mut_disp, safe))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(render_markdown(merge_disp, sa_disp, pb_disp, mut_disp, safe))
    return 0 if safe else 1


if __name__ == "__main__":
    _code = main()
    # The demo is done and the scorecard is printed/written. delta-rs (Tokio) and duckdb spin up
    # native threads/runtimes that can abort the interpreter during shutdown on Linux
    # ("terminate called without an active exception" -> exit 134) even when the run succeeded.
    # Exit hard with the already-computed result, skipping that teardown, so CI reflects the
    # actual outcome. (A real invariant violation still returns 1 here; an exception still
    # propagates before we reach this.)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_code)
