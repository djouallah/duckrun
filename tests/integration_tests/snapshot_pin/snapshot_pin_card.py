"""Delta version-by-version scorecard: walk the table's history and mark each version right/wrong.

Runs the SAME concurrent-writer race twice against a real incremental dbt model, then prints the
Delta table's VERSION TIMELINE for each — version 0 got this, version 1 got that, version 2 got
this — with a right/wrong verdict on the final state, so it's obvious the pin fixed a real bug.

Scenario (identical for both arms): a `dbt run` seeds the table (v0); a concurrent writer commits
`id=1 = 999` (v1) while the model is running; the model's incremental MERGE then tries to commit.

  • WITHOUT the pin  (old behaviour: the merge reads/commits against HEAD) — it never sees the v1
    write, so it writes a v2 that silently overwrites `id=1 = 999`. WRONG: a lost update, no error.
  • WITH the pin     (duckrun today: read + merge fixed at the version captured at run start, OCC
    over (vB, HEAD]) — the v1 commit is inside the window, so the merge REFUSES: no v2 is written,
    the run fails loudly, and `id=1 = 999` survives. RIGHT.

Both arms drive `dbt run` in-process; the only difference is the `read_version` the merge is pinned
to (HEAD = no effective pin, vs vB = the real pin).

    python tests/integration_tests/snapshot_pin/snapshot_pin_card.py

Prints an ASCII timeline to the console and, in CI, writes the same as Markdown to the GitHub
Actions step summary ($GITHUB_STEP_SUMMARY) and to snapshot_pin_card.md (for injection into
docs/snapshot-pin.md). Exit 0 iff the demonstration holds.
"""
import os
import sys
import tempfile
from pathlib import Path

from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from dbt.cli.main import dbtRunner
from dbt.adapters.duckrun import engine

PROJECT_DIR = str(Path(__file__).parent)
SCHEMA = "main"


def _dbt(warehouse: str):
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke(
        ["run", "--select", "events", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
    )


def _snapshot(path: str, version: int) -> dict:
    """{id: value} as the table looked at a specific Delta version (time travel)."""
    dt = DeltaTable(path)
    dt.load_as_version(version)
    t = dt.to_pyarrow_table()
    return dict(sorted(zip(t.column("id").to_pylist(), t.column("value").to_pylist())))


def run_arm(pinned: bool) -> dict:
    """Seed, then run the incremental merge while a concurrent writer commits mid-merge.

    pinned=True  -> keep the real pin (read_version = vB): the merge REFUSES, no v2 is written.
    pinned=False -> drop the pin (read_version = HEAD): the merge commits a v2 that clobbers v1.
    """
    wh = (Path(tempfile.mkdtemp()) / "wh").as_posix()
    path = f"{wh}/{SCHEMA}/events"

    _dbt(wh)                               # 1st run = seed -> v0 {1:10, 2:20, 3:30}
    vB = engine.table_version(path)        # the version the run captures at start (= v0 here)
    real = engine.merge_delta
    pinned_to = {}                         # the version the merge was actually pinned to

    def wrapped(p, data, key, **kw):
        # A concurrent writer commits to the SAME table now — after the run captured vB, before it
        # commits its own merge (this is the (vB, HEAD] window).
        DeltaTable(p).update(predicate="id = 1", updates={"value": "999"})   # v0 -> v1
        if not pinned:
            kw["read_version"] = engine.table_version(p)   # merge against HEAD == no effective pin
        pinned_to["v"] = kw.get("read_version")
        return real(p, data, key, **kw)

    engine.merge_delta = wrapped
    try:
        cap = {"pinned": pinned, "run_ok": _dbt(wh).success}
    except CommitFailedError:               # (defensive; dbt normally wraps it into success=False)
        cap = {"pinned": pinned, "run_ok": False}
    finally:
        engine.merge_delta = real

    cap["vB"] = vB
    cap["merge_pinned_to"] = pinned_to.get("v")   # vB when pinned; HEAD (v1) when not

    head = engine.table_version(path)
    # Build the version timeline: v0 seed, v1 concurrent writer, v2 the merge (only if it committed).
    ops = {0: "`dbt run` #1 — seed (ids 1..10)", 1: "concurrent writer: `update id=1 -> 999`",
           2: "`dbt run` #2 — incremental MERGE (batch: update id=1 -> 111)"}
    cap["timeline"] = [{"v": v, "rows": _snapshot(path, v), "op": ops.get(v, "?")}
                       for v in range(head + 1)]
    cap["head"] = head
    cap["merge_committed"] = head >= 2          # a v2 means the merge landed
    cap["final"] = _snapshot(path, head)
    cap["writer_preserved"] = cap["final"].get(1) == 999
    return cap


# ----------------------------------------------------------------------------- verdicts

def _arm_ok(cap) -> bool:
    if cap["pinned"]:
        # RIGHT: merge refused (no v2), run failed loudly, the writer's 999 survived.
        return (not cap["merge_committed"]) and (not cap["run_ok"]) and cap["writer_preserved"]
    # The unpinned arm is EXPECTED to be wrong — that's the bug we're demonstrating: it committed a
    # v2 and lost the writer's update. The demo "holds" when it indeed reproduces that loss.
    return cap["merge_committed"] and (not cap["writer_preserved"])


def _final_verdict(cap):
    if cap["pinned"]:
        return ("✅ RIGHT", "merge REFUSED — no v2 written; run failed loudly; `id=1 = 999` preserved")
    return ("❌ WRONG", "merge committed v2 against HEAD; `id=1 = 999` silently LOST (no error raised)")


def _fmt_rows(rows: dict) -> str:
    return ", ".join(f"{k}→{v}" for k, v in rows.items()) if rows else "—"


# ----------------------------------------------------------------------------- presentation

def _ascii(headers, rows):
    w = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    bar = lambda l, m, r: l + m.join("─" * (x + 2) for x in w) + r
    line = lambda c: "│" + "│".join(" " + str(v).ljust(x) + " " for v, x in zip(c, w)) + "│"
    return "\n".join([bar("┌", "┬", "┐"), line(headers), bar("├", "┼", "┤"),
                      *[line(r) for r in rows], bar("└", "┴", "┘")])


def _arm_marker(cap):
    """(version the merge anchored to, label) — vB when pinned, HEAD when not."""
    if cap["pinned"]:
        return cap["vB"], "← merge pinned HERE (vB) — read + commit"
    return cap["merge_pinned_to"], "← merge anchored HERE (HEAD) — NO pin"


def _console_arm(cap):
    vB, mp = cap["vB"], cap["merge_pinned_to"]
    title = (f"WITH pin (duckrun today) — merge pinned to vB=v{vB}" if cap["pinned"]
             else f"WITHOUT pin (merge vs HEAD — old) — merge anchored to v{mp} (HEAD)")
    marker_v, marker_txt = _arm_marker(cap)
    rows = []
    for e in cap["timeline"]:
        op = e["op"]
        if e["v"] == marker_v:
            op += f"   {marker_txt}"
        rows.append([f"v{e['v']}", _fmt_rows(e["rows"]), op])
    if cap["pinned"]:
        rows.append(["v2", "— (never written)",
                     f"MERGE refused — pinned to v{vB}; HEAD is now v{cap['head']}, and the "
                     f"check (v{vB}, v{cap['head']}] catches the concurrent v{cap['head']}"])
    tag, why = _final_verdict(cap)
    return (f"  {title}\n"
            + _ascii(["Delta version", "rows (id→value)", "what produced it"], rows)
            + f"\n  final state: {_fmt_rows(cap['final'])}  →  {tag} — {why}\n")


def _final_compare_rows(no_pin, pin):
    """Per-id final values: (id, WITH-pin value, WITHOUT-pin value, differ?) over the union of ids."""
    without, with_ = no_pin["final"], pin["final"]
    out = []
    for i in sorted(set(without) | set(with_)):
        wv, ov = with_.get(i, "—"), without.get(i, "—")
        out.append((i, wv, ov, wv != ov))
    return out


def render_console(no_pin, pin, all_ok):
    cmp = _final_compare_rows(no_pin, pin)
    n_diff = sum(1 for *_, d in cmp if d)
    final_tbl = _ascii(
        ["id", "WITH pin", "WITHOUT pin", "difference"],
        [[i, wv, ov, "◀ DIFFERENT" if d else "same"] for i, wv, ov, d in cmp])
    verdict = ("THE PIN FIXES IT: the same concurrent write silently loses data without the pin,\n"
               "  but is caught (nothing lost, run fails loudly) with it."
               if all_ok else
               "DEMONSTRATION DID NOT HOLD — an arm behaved unexpectedly. Investigate.")
    return ("\n  duckrun — DuckDB snapshot pin, version by version (via a real dbt run)\n\n"
            "  Scenario: seed (v0, ids 1..10); a concurrent writer commits id=1=999 (v1); the\n"
            "  incremental MERGE then tries to commit (v2).\n\n"
            + _console_arm(no_pin) + "\n" + _console_arm(pin)
            + f"\n  FINAL TABLES — same code, same race, {n_diff} row differs:\n"
            + final_tbl + "\n\n  " + verdict + "\n")


def _md_arm(cap):
    vB, mp = cap["vB"], cap["merge_pinned_to"]
    title = (f"WITH the pin (duckrun today) — merge pinned to `vB = v{vB}`" if cap["pinned"]
             else f"WITHOUT the pin (merge vs HEAD — old behaviour) — merge anchored to `v{mp}` (HEAD)")
    marker_v, marker_txt = _arm_marker(cap)
    L = [f"### {title}", "",
         "| Delta version | rows (`id`→`value`) | what produced it |",
         "|:---:|---|---|"]
    for e in cap["timeline"]:
        op = e["op"]
        if e["v"] == marker_v:
            op += f" &nbsp;**{marker_txt}**"
        L.append(f"| `v{e['v']}` | {_fmt_rows(e['rows'])} | {op} |")
    if cap["pinned"]:
        L.append(f"| `v2` | *— never written* | **MERGE refused** — pinned to `v{vB}`; HEAD is now "
                 f"`v{cap['head']}`, and the check `(v{vB}, v{cap['head']}]` catches the concurrent "
                 f"`v{cap['head']}` |")
    tag, why = _final_verdict(cap)
    L += ["", f"**Final state:** {_fmt_rows(cap['final'])} → {tag} — {why}", ""]
    return L


def render_markdown(no_pin, pin, all_ok):
    tick = "✅" if all_ok else "❌"
    L = [f"## 🔒 DuckDB snapshot pin — version by version {tick}", "",
         "**What this proves (through a real `dbt run`):** the same concurrent-writer race is run "
         "twice against an incremental MERGE model — the only difference is whether the merge is "
         "pinned to `vB` (the version captured at the start of the run) or left to read/commit "
         "against HEAD. The table's Delta versions tell the story: a writer commits `id=1 = 999` "
         "(`v1`) while the model is mid-run.", ""]
    L += _md_arm(no_pin)
    L += _md_arm(pin)

    # The punchline: one final table — id, WITH pin, WITHOUT pin, difference.
    cmp = _final_compare_rows(no_pin, pin)
    n_diff = sum(1 for *_, d in cmp if d)
    L += [f"### 🔴 Final tables — same code, same race, {n_diff} row differs", "",
          "| id | WITH pin | WITHOUT pin | difference |",
          "|:---:|:---:|:---:|:---:|"]
    for i, wv, ov, diff in cmp:
        if diff:
            L.append(f"| `{i}` | **{wv}** | **{ov}** | ⬅️ **DIFFERENT** |")
        else:
            L.append(f"| `{i}` | {wv} | {ov} | same |")
    L += [""]

    L += ["> **Read it like this** (here `vB = v0`, and HEAD = `v1`, the concurrent writer's commit): "
          "without the pin, `v2` overwrote `id=1 = 999` with `111` — the concurrent writer's change "
          "vanished and the run reported success. With the pin, the merge checks the range after its "
          "pinned version up to the current HEAD — `(v0, v1]` — sees the concurrent `v1` there, and "
          "**refused**: no `v2`, the run fails loudly, and `id=1 = 999` is still there. Re-run and it "
          "merges cleanly on top of `v1`.",
          "",
          ("> ✅ **The pin fixes a real correctness bug:** silent data loss becomes a safe, loud failure."
           if all_ok else
           "> ❌ **Demonstration did not hold** — see the timelines above.")]
    return "\n".join(L) + "\n"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    no_pin = run_arm(pinned=False)
    pin = run_arm(pinned=True)
    all_ok = _arm_ok(no_pin) and _arm_ok(pin)

    print(render_console(no_pin, pin, all_ok))
    card = render_markdown(no_pin, pin, all_ok)
    with open("snapshot_pin_card.md", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(card)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(card)
    return 0 if all_ok else 1


if __name__ == "__main__":
    _code = main()
    # delta-rs (Tokio) / duckdb native runtimes can abort the interpreter during shutdown on Linux
    # even on success; exit hard with the already-computed result, like concurrency_correctness.py.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_code)
