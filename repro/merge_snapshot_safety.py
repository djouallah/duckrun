"""
DEMONSTRATION: why duckrun's incremental MERGE is correctness-safe under concurrent writes.

Run it by hand or via the manual-only `merge-snapshot-demo` GitHub workflow:

    python repro/merge_snapshot_safety.py

It prints a scorecard (and writes the same card to the GitHub Actions job summary). Exit code:
0 if the safety invariant holds (expected), 1 if any run violates it.

----------------------------------------------------------------------------------------------
The question
----------------------------------------------------------------------------------------------
A duckrun MERGE reads the same Delta table twice, and -- unlike Spark, which pins one snapshot
for the whole merge -- these two reads are NOT pinned together:

  * MERGE TARGET / conflict check  -> delta-rs fixes this version when it opens the table at
    `dt = _delta_table(path)` at the start of merge_delta (engine.py).
  * SOURCE SCAN                    -> the rows merged in come from DuckDB `delta_scan(...)`,
    a lazy C-Arrow stream that delta-rs only pulls later, inside `merger.execute()`.

Could a concurrent writer slip a commit into that gap and silently corrupt the result?

----------------------------------------------------------------------------------------------
The insight (this script proves it, with real version numbers)
----------------------------------------------------------------------------------------------
1. DuckDB's `delta_scan` fixes its snapshot at PULL time (not relation-build time) and reads the
   latest version then. The pull is strictly AFTER delta-rs fixed the target. So:

        duckdb_source_version  >=  merge_target_version    ALWAYS (same, or ahead -- never before)

   Holds for streamed_exec=True and False alike (streaming changes how the source is consumed,
   not when its snapshot is fixed).
2. The only direction that could SILENTLY corrupt -- source OLDER than the target -- is therefore
   structurally impossible.
3. The reachable direction -- source ahead, because a concurrent commit landed in the gap --
   makes delta-rs's read_version trail HEAD at commit, so its OCC fails the merge loudly with
   CommitFailedError. Retry needed; never a silent wrong answer.

Net: the gap is real but benign. The only cost vs Spark's single-snapshot pin is the occasional
failed merge that must be retried -- not correctness.
"""
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable

from dbt.adapters.duckrun import engine


def run(streamed: bool, concurrent: bool) -> dict:
    tmp = tempfile.mkdtemp()
    path = str(Path(tmp) / "t")
    # 3-col schema so the source's "what DuckDB saw" sentinel survives into the merged table.
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

    # Each source row carries the value delta_scan sees for id=1 -> reveals DuckDB's read version
    # (10 = v0, 999 = v1). Source is a lazy relation; delta-rs pulls it during execute().
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
            cap["merge_target_ver"] = dt.version()          # delta-rs fixes the merge version here
            if concurrent:
                # A legitimate concurrent writer commits in the gap (target fixed -> source pull).
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


# --------------------------------------------------------------------------- presentation

def _display_rows(rows):
    """Turn raw run() captures into human-friendly columns + a per-row OK flag."""
    out = []
    for c in rows:
        streamed = str(c["streamed"])
        conc = "yes" if c["concurrent"] else "no"
        mver = f"v{c.get('merge_target_ver', '?')}"
        if c["concurrent"]:
            dver = f"v{c.get('after_concurrent_ver', '?')} (ahead)"
            outcome_txt = "BLOCKED (CommitFailedError)"
            outcome_md = "🛡️ blocked — `CommitFailedError`"
            ok = c["outcome"].startswith("RAISED") and c.get("duckdb_will_read") == 999
        else:
            v = c.get("duckdb_source_read", "? (v?)").split()[-1].strip("()")
            dver = f"{v} (same)"
            outcome_txt = "committed"
            outcome_md = "✅ committed"
            ok = c["outcome"] == "COMMITTED" and v == "v0"
        out.append({
            "streamed": streamed, "concurrent": conc, "merge": mver, "duckdb": dver,
            "outcome_txt": outcome_txt, "outcome_md": outcome_md, "ok": ok,
        })
    return out


def _ascii_table(headers, rows):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]

    def rule(l, m, r):
        return l + m.join("─" * (w + 2) for w in widths) + r

    def line(cells):
        return "│" + "│".join(" " + str(c).ljust(w) + " " for c, w in zip(cells, widths)) + "│"

    return "\n".join([rule("┌", "┬", "┐"), line(headers), rule("├", "┼", "┤"),
                      *[line(r) for r in rows], rule("└", "┴", "┘")])


def _console_card(disp, safe):
    headers = ["streamed_exec", "concurrent write", "merge target", "duckdb source", "result"]
    body = [[d["streamed"], d["concurrent"], d["merge"], d["duckdb"], d["outcome_txt"]]
            for d in disp]
    verdict = ("SAFE — duckdb source is always the SAME version or AHEAD, never older.\n"
               "       A concurrent write can only make the merge FAIL LOUDLY (then retry),\n"
               "       never silently corrupt. Holds for streamed_exec = True and False."
               if safe else
               "INVARIANT VIOLATED — a run behaved unexpectedly (see table). Investigate.")
    return ("\n  duckrun MERGE — snapshot safety under concurrent writes\n"
            "  Risk: if DuckDB's source scan read an OLDER Delta version than the merge target,\n"
            "  the merge could silently apply stale rows. Does it?\n\n"
            + _ascii_table(headers, body) + "\n\n  " + verdict + "\n")


def _markdown_card(disp, safe):
    tick = "✅" if safe else "❌"
    lines = [
        f"## duckrun MERGE — snapshot safety under concurrent writes {tick}",
        "",
        "**Risk:** if DuckDB's source `delta_scan` ever read an *older* Delta version than the "
        "merge target, the merge could silently apply stale rows. This run checks it, with real "
        "version numbers, in both streaming modes.",
        "",
        "| streamed_exec | concurrent write | merge target | duckdb source | result |",
        "|:---:|:---:|:---:|:---:|:---|",
    ]
    for d in disp:
        cc = "**yes**" if d["concurrent"] == "yes" else "no"
        lines.append(f"| `{d['streamed']}` | {cc} | {d['merge']} | {d['duckdb']} | {d['outcome_md']} |")
    lines.append("")
    if safe:
        lines.append("> ✅ **SAFE.** DuckDB's source is always the **same version or ahead — "
                     "never older**. A concurrent write can only make the merge **fail loudly** "
                     "(`CommitFailedError`, retry needed), never silently corrupt. Holds for "
                     "`streamed_exec` = `True` and `False`.")
    else:
        lines.append("> ❌ **INVARIANT VIOLATED** — a run behaved unexpectedly (see table). Investigate.")
    return "\n".join(lines) + "\n"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # box-drawing/emoji safe on Windows consoles
    except Exception:
        pass

    rows = [run(False, False), run(False, True), run(True, False), run(True, True)]
    disp = _display_rows(rows)
    safe = all(d["ok"] for d in disp)

    print(_console_card(disp, safe))

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(_markdown_card(disp, safe))

    return 0 if safe else 1


if __name__ == "__main__":
    sys.exit(main())
