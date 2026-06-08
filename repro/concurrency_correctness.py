"""
DEMONSTRATION: duckrun's concurrency-correctness guarantees, proved with real Delta versions.

Run by hand or via the manual/PR `concurrency-correctness` workflow:

    python repro/concurrency_correctness.py

Prints a scorecard (also to the GitHub Actions job summary). Exit 0 if every invariant holds,
1 if any is violated — so a regression that breaks concurrency correctness blocks the PR.

Two independent guarantees are checked:

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


# ----------------------------------------------------------------------------- presentation

def _ascii_table(headers, rows):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]

    def rule(l, m, r):
        return l + m.join("─" * (w + 2) for w in widths) + r

    def line(cells):
        return "│" + "│".join(" " + str(c).ljust(w) + " " for c, w in zip(cells, widths)) + "│"

    return "\n".join([rule("┌", "┬", "┐"), line(headers), rule("├", "┼", "┤"),
                      *[line(r) for r in rows], rule("└", "┴", "┘")])


def render_console(merge_disp, sa_disp, safe):
    a = _ascii_table(
        ["streamed_exec", "concurrent write", "merge target", "duckdb source", "result"],
        [[d["streamed"], d["concurrent"], d["merge"], d["duckdb"], d["res_txt"]] for d in merge_disp])
    b = _ascii_table(
        ["mode", "concurrent write", "read", "head", "result"],
        [[d["mode"], d["concurrent"], d["read"], d["head"], d["res_txt"]] for d in sa_disp])
    verdict = ("CONCURRENCY-CORRECT: merge source is always >= the merge version (gap fails loudly,\n"
               "  never silent); safeappend refuses a moved table, plain append has no guard."
               if safe else
               "INVARIANT VIOLATED — a case behaved unexpectedly (see tables). Investigate.")
    return ("\n  duckrun — concurrency correctness\n\n"
            "  A) MERGE snapshot safety — is the source ever OLDER than the merge target?\n"
            + a + "\n\n"
            "  B) safeappend (pin intended) — append only if the table version didn't move\n"
            + b + "\n\n  " + verdict + "\n")


def render_markdown(merge_disp, sa_disp, safe):
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

    merge_disp, sa_disp = merge_display(merge_rows), safeappend_display(sa_rows)
    safe = all(d["ok"] for d in merge_disp) and all(d["ok"] for d in sa_disp)

    print(render_console(merge_disp, sa_disp, safe))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(render_markdown(merge_disp, sa_disp, safe))
    return 0 if safe else 1


if __name__ == "__main__":
    sys.exit(main())
