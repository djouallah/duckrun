"""
DEMONSTRATION: duckrun's concurrency-correctness guarantees, proved with real Delta versions.

Run by hand or via the manual/PR `concurrency-correctness` workflow:

    python repro/concurrency_correctness.py

Prints a scorecard (also to the GitHub Actions job summary). Exit 0 if every invariant holds,
1 if any is violated — so a regression that breaks concurrency correctness blocks the PR.

Three guarantees are checked here. (MERGE single-snapshot pinning — read_version →
`load_as_version`, OCC over `(vB, HEAD]` — is proved separately in `test_snapshot_pinning.py`;
merge now ALWAYS pins, there is no unpinned path to demo.)

B. safeappend (pin intended). The `safeappend` incremental strategy appends only if the table
   version has not moved since it was read — a compare-and-swap via delta-rs max_commit_retries=0.
   A plain `append` has no such guard: it silently lands on whatever the latest version is.
   safeappend instead REFUSES (CommitFailedError) when a concurrent write slipped in, so the
   caller re-runs against the new HEAD. Dedup is NOT its job — that's the model SQL's.

D. DELETE / UPDATE snapshot safety. A connection-API delete()/update() is a genuinely conflicting
   Delta op, so delta-rs OCC fails it (CommitFailedError) when a foreign commit changed the same
   rows after its snapshot opened — native conflict detection, no max_commit_retries hack.

E. Connection-API safeappend (self-reference). The same B guarantee, but driven end-to-end through
   the connection API — `conn.sql("select … from t").write.mode("safeappend").saveAsTable("t")` —
   where the source reads the SAME table it writes to. A plain `append` silently lands on the new
   HEAD; `safeappend` REFUSES (CommitFailedError) when a foreign commit moved the table since the
   writer read it.
"""
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

# Run as a plain script (`python tests/correctness/…`): the editable install exposes only the `dbt`
# namespace, not the top-level `duckrun` package, and sys.path[0] is this file's dir — so put the
# repo root on the path before importing the connection API.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import duckrun  # noqa: E402
from dbt.adapters.duckrun import engine  # noqa: E402


def _ids(p):
    return sorted(r["id"] for r in DeltaTable(p).to_pyarrow_table().to_pylist())


def _batch(ids):
    return pa.table({"id": pa.array(ids, pa.int64())})


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
    # patching the first _delta_table() call the engine helper makes.
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


# ------------------------------------------ E. connection-API safeappend (self-reference)

def conn_safeappend_run(mode: str, concurrent: bool) -> dict:
    """Same guarantee as B, but driven end-to-end through the **connection API** — and where the
    source query reads the SAME table it writes to (``select … from t`` → ``saveAsTable('t')``).
    ``mode`` is 'append' (no guard) or 'safeappend' (CAS). The real race: the writer captures the
    version it read (vB), a foreign writer then commits, and safeappend's CAS must refuse the commit
    (HEAD moved past vB) while a plain append silently lands on the new HEAD."""
    root = tempfile.mkdtemp()
    conn = duckrun.connect(root, schema="dbo")
    conn.sql("select 0 as id").write.mode("overwrite").saveAsTable("t")    # v0
    path = conn.table_path("dbo", "t")
    so = conn.storage_options
    cap = {"mode": mode, "concurrent": concurrent}
    read_ver = engine.table_version(path, so)
    cap["read_ver"] = read_ver
    if concurrent:
        engine.write_delta(path, _batch([7]), "append")                  # foreign writer -> v1
    cap["head_ver"] = DeltaTable(path).version()

    # Source reads the SAME table t, then appends back into it (self-reference).
    src = conn.sql("select id + 100 as id from t")

    real_tv = engine.table_version
    if mode == "safeappend" and concurrent:
        # safeappend fences to the version the writer read (vB); a foreign commit since then must
        # make the CAS refuse. Force the captured version to vB to model "read vB, then it moved".
        engine.table_version = lambda *a, **k: read_ver
    try:
        src.write.mode(mode).saveAsTable("t")
        cap["outcome"], cap["final"] = "committed", _ids(path)
    except CommitFailedError:
        cap["outcome"], cap["final"] = "refused", _ids(path)
    finally:
        engine.table_version = real_tv
    return cap


def conn_safeappend_display(rows):
    out = []
    for c in rows:
        if c["mode"] == "append":
            ok = c["outcome"] == "committed"            # plain append has no guard — expected
            res_md, res_txt = "⚠️ committed anyway — no version guard", "committed (no guard)"
        elif c["concurrent"]:
            ok = c["outcome"] == "refused"              # safeappend must refuse a moved table
            res_md, res_txt = "🛡️ refused — `CommitFailedError`", "refused (CommitFailedError)"
        else:
            ok = c["outcome"] == "committed"            # unchanged -> commits
            res_md, res_txt = "✅ committed", "committed"
        out.append({"mode": c["mode"], "concurrent": "yes" if c["concurrent"] else "no",
                    "read": f"v{c['read_ver']}", "head": f"v{c['head_ver']}",
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


def render_console(sa_disp, mut_disp, conn_disp, safe):
    b = _ascii_table(
        ["mode", "concurrent write", "read", "head", "result"],
        [[d["mode"], d["concurrent"], d["read"], d["head"], d["res_txt"]] for d in sa_disp])
    d = _ascii_table(
        ["operation", "concurrent write", "read", "head", "result"],
        [[x["op"], x["concurrent"], x["read"], x["head"], x["res_txt"]] for x in mut_disp])
    e = _ascii_table(
        ["mode", "concurrent write", "read", "head", "result"],
        [[x["mode"], x["concurrent"], x["read"], x["head"], x["res_txt"]] for x in conn_disp])
    verdict = ("CONCURRENCY-CORRECT: safeappend refuses a moved table, plain append has no guard;\n"
               "  DELETE/UPDATE fail on a conflicting commit; conn-API safeappend refuses a moved\n"
               "  table even when reading the same table it writes. (MERGE pinning: test_snapshot_pinning.)"
               if safe else
               "INVARIANT VIOLATED — a case behaved unexpectedly (see tables). Investigate.")
    return ("\n  duckrun — concurrency correctness\n\n"
            "  B) safeappend (pin intended) — append only if the table version didn't move\n"
            + b + "\n\n"
            "  D) DELETE / UPDATE — do they fail on a conflicting concurrent commit (like merge)?\n"
            + d + "\n\n"
            "  E) connection-API safeappend — self-reference (read t, write t) refuses a moved table\n"
            + e + "\n\n  " + verdict + "\n")


def render_markdown(sa_disp, mut_disp, conn_disp, safe):
    tick = "✅" if safe else "❌"
    L = [f"## duckrun — concurrency correctness {tick}", "",
         "> MERGE single-snapshot pinning is proved separately in `test_snapshot_pinning.py` "
         "(read_version → `load_as_version`, OCC over `(vB, HEAD]`).", "",
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
          "### E) connection-API safeappend — *self-reference (read `t`, write `t`)*", "",
          "| mode | concurrent write | read | head | result |",
          "|:---:|:---:|:---:|:---:|:---|"]
    for d in conn_disp:
        cc = "**yes**" if d["concurrent"] == "yes" else "no"
        L.append(f"| `{d['mode']}` | {cc} | {d['read']} | {d['head']} | {d['res_md']} |")
    L += ["",
          "> Driven end-to-end through the connection API — `conn.sql(\"select … from t\")"
          ".write.mode(m).saveAsTable(\"t\")` — where the source reads the **same table it writes**. "
          "`safeappend` still **refuses** (`CommitFailedError`) when a foreign commit moved the table "
          "since the writer read it, while a plain `append` silently lands on the new HEAD.", "",
          ("> ✅ **Concurrency-correct.**" if safe
           else "> ❌ **Invariant violated** — see tables above.")]
    return "\n".join(L) + "\n"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    sa_rows = [safeappend_run("append", True),
               safeappend_run("safeappend", False),
               safeappend_run("safeappend", True)]
    mut_rows = [mutate_run("delete", False), mutate_run("delete", True),
                mutate_run("update", False), mutate_run("update", True)]
    conn_rows = [conn_safeappend_run("append", True),
                 conn_safeappend_run("safeappend", False),
                 conn_safeappend_run("safeappend", True)]

    sa_disp, mut_disp = safeappend_display(sa_rows), mutate_display(mut_rows)
    conn_disp = conn_safeappend_display(conn_rows)
    safe = (all(d["ok"] for d in sa_disp) and all(d["ok"] for d in mut_disp)
            and all(d["ok"] for d in conn_disp))

    print(render_console(sa_disp, mut_disp, conn_disp, safe))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(render_markdown(sa_disp, mut_disp, conn_disp, safe))
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
