"""Why duckrun's snapshot pin makes an incremental `dbt run` fail (or succeed) — proved through dbt.

Read this if you've hit: *"my incremental model used to just run; I upgraded and now a run sometimes
fails with a Delta commit-conflict error. What changed?"* — or the reverse.

WHAT DUCKRUN DOES ON EVERY INCREMENTAL RUN (see _delta_core.sql + engine.merge_delta):

  Before the model executes, duckrun captures the target table's current Delta version — `vB` — and
  anchors BOTH sides of the run to it:

    * READ pin   — it registers the model's self-reference as a frozen snapshot:
                       create or replace view {{ this }} as
                         select * from delta_scan('<location>', version => vB)
                   so the model's SQL sees the table as of vB, not a drifted HEAD. (Needs the
                   duckdb-delta `version => N` param → the duckdb 1.5.4 pin.)
    * WRITE pin  — it opens the merge target at the same vB and lets delta-rs validate the commit
                   over the window (vB, HEAD]. If another writer committed into that window while
                   the model was running, the merge REFUSES to commit (a Delta commit conflict).

  Together: the read and the write agree on ONE version of the table — single-snapshot MERGE
  semantics. (The delta-rs OCC internals are unit-tested in tests/correctness/test_correctness.py;
  this file is the end-to-end "why it matters to a `dbt run`" layer.)

WHY THAT FLIPS A RUN BETWEEN SUCCESS AND FAILURE — three real dbt runs below:

  1. No concurrent writer            → the run succeeds; the batch is merged. (baseline)
  2. A writer commits DURING the run → the pin makes the run FAIL LOUDLY instead of silently
     committing a merge computed against a now-stale snapshot. Without the pin the same run would
     have landed on the new HEAD and clobbered the concurrent writer's change (a lost update). The
     loud failure is the correct outcome: re-run and it picks up the new HEAD cleanly.
  3. A writer commits BEFORE the run → no conflict in (vB, HEAD]; the run succeeds and both changes
     coexist. (The pin only fails the run when the foreign commit lands *inside* the window.)

The harness drives `dbt run` in-process (dbtRunner) and, for case 2, simulates "another writer
committed right now" by landing a foreign Delta commit on the same table at the moment the merge
fires — the only deterministic way to put a commit inside the (vB, HEAD] window from a test.
"""
import json
import os
from pathlib import Path

from deltalake import DeltaTable

from dbt.cli.main import dbtRunner
from dbt.adapters.duckrun import engine

PROJECT_DIR = str(Path(__file__).parent)
SCHEMA = "main"


def _dbt(warehouse: str, model: str = "events") -> object:
    """One in-process `dbt run --select <model>` against a local-fs warehouse. Returns the
    dbtRunnerResult (`.success`, `.result`, `.exception`)."""
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke(
        ["run", "--select", model, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
    )


def _rows(path: str) -> dict:
    """{id: value} for the current HEAD of the Delta table."""
    t = DeltaTable(path).to_pyarrow_table()
    return dict(zip(t.column("id").to_pylist(), t.column("value").to_pylist()))


def _warehouse(tmp_path, model: str = "events") -> tuple:
    wh = (tmp_path / "wh").as_posix()          # forward slashes so delta paths are clean on Windows
    return wh, f"{wh}/{SCHEMA}/{model}"


# ------------------------------------------------------- 1. baseline: no concurrent writer

def _seed_rows():
    return {i: i * 10 for i in range(1, 11)}     # ids 1..10, value = id*10


def _race(tmp_path, model, patch_attrs):
    """Run the standard concurrent-writer race against <model>, returning (dbtRunnerResult, rows).

    Seed the model (-> v0), then wrap the engine write fn(s) the strategy calls (``patch_attrs``) so
    that — exactly once, after the run captured vB but before it commits — a foreign writer lands
    ``update id=1 -> 999`` (v0 -> v1), moving HEAD into the commit's (vB, HEAD] window. Whether the
    run then fails (fenced) or succeeds (unfenced) is the strategy's snapshot-pin behaviour. Patching
    every write fn the strategy *might* use keeps this a real regression guard, not an injection check.
    """
    wh, path = _warehouse(tmp_path, model)
    assert _dbt(wh, model).success                  # seed -> v0 {1:10 .. 10:100}
    reals = {a: getattr(engine, a) for a in patch_attrs}
    fired = []

    def make(real):
        def wrapped(*a, **kw):
            if not fired:                            # land the foreign commit exactly once
                fired.append(True)
                DeltaTable(a[0]).update(predicate="id = 1", updates={"value": "999"})   # v0 -> v1
            return real(*a, **kw)
        return wrapped

    for attr, real in reals.items():
        setattr(engine, attr, make(real))
    try:
        res = _dbt(wh, model)
    finally:
        for attr, real in reals.items():
            setattr(engine, attr, real)
    # The foreign commit MUST have landed, else the race never happened and the assertions below are
    # vacuous (e.g. a full-overwrite rebuild reproduces the seed whether or not 999 was clobbered).
    assert fired, f"no engine write in {patch_attrs} was called for {model} — the race didn't fire"
    return res, _rows(path)


def test_merge_run_succeeds_without_a_concurrent_writer(tmp_path):
    """The normal case: seed (ids 1..10), then an incremental run merges the batch (update id=1 ->
    111). Table stays 10 rows."""
    wh, path = _warehouse(tmp_path)
    assert _dbt(wh).success                     # 1st run = seed -> v0 {1:10 .. 10:100}
    assert _dbt(wh).success                     # 2nd run = incremental merge
    assert _rows(path) == {**_seed_rows(), 1: 111}   # id=1 updated to 111


# ------------------------------------- 2. writer commits DURING the run -> the pin fails it

def test_pin_fails_the_run_when_a_writer_commits_during_the_merge(tmp_path):
    """A concurrent writer lands a commit *inside* the merge's (vB, HEAD] window. Because the run is
    pinned to vB, the merge refuses to commit — the `dbt run` FAILS on purpose, and the concurrent
    writer's change is preserved (no silent lost update)."""
    wh, path = _warehouse(tmp_path)
    assert _dbt(wh).success                     # seed -> v0

    real = engine.merge_delta

    def concurrent_then_merge(p, data, key, **kw):
        # Another writer commits to the SAME table right now — after this run captured vB but before
        # it commits its own merge. This moves HEAD into the merge's (vB, HEAD] validation window.
        DeltaTable(p).update(predicate="id = 1", updates={"value": "999"})   # v0 -> v1
        return real(p, data, key, **kw)         # the pinned merge now hits an OCC conflict and raises

    engine.merge_delta = concurrent_then_merge
    try:
        res = _dbt(wh)
    finally:
        engine.merge_delta = real

    assert not res.success                       # the run FAILED — the pin caught the concurrent write
    rows = _rows(path)
    assert rows[1] == 999                         # the concurrent writer's value stands (not 111) ...
    assert rows == {**_seed_rows(), 1: 999}       # ... and our merge never landed (no lost update)


# --------------------- 2b. SAME race, delete+insert strategy -> its overwrite must be fenced too

def test_delete_insert_is_fenced(tmp_path):
    """delete+insert is a read-modify-write too: duckrun reads the kept rows pinned to vB and writes
    (kept rows UNION the batch) back as a FULL-table overwrite. That overwrite must be fenced to vB
    just like the merge — otherwise a concurrent writer that commits during the run is silently
    clobbered (a lost update, *worse* than classic row-level delete+insert, which preserves untouched
    rows). We patch BOTH the fenced primitive used today (overwrite_if_unchanged) and the unfenced
    pre-fix one (write_delta), so a revert to the bare overwrite would let the run succeed and lose
    999, tripping `not res.success`."""
    res, rows = _race(tmp_path, "events_di", ["overwrite_if_unchanged", "write_delta"])
    assert not res.success                         # the run FAILED — the fenced overwrite caught it
    assert rows == {**_seed_rows(), 1: 999}        # writer's 999 stands; our overwrite never landed


# ------------------------------------- 3. writer commits BEFORE the run -> it just succeeds

def test_run_succeeds_when_the_writer_commits_before_the_run(tmp_path):
    """The same race, but the foreign commit lands BEFORE the incremental run starts. Then vB is
    captured as that new version, nothing sits in (vB, HEAD], and the run succeeds — both changes
    coexist. This is the "vice versa": pinning only fails a run when the commit is concurrent."""
    wh, path = _warehouse(tmp_path)
    assert _dbt(wh).success                       # seed -> v0
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})   # foreign commit -> v1
    assert _dbt(wh).success                        # vB = v1; no conflict; merge commits on top of v1
    assert _rows(path) == {**_seed_rows(), 1: 111}   # our update applied over v1


# ------------------------------------- 4. dbt docs: the catalog carries Delta stats (issue #3)

def _docs_generate(warehouse: str) -> object:
    """In-process `dbt docs generate` against the local-fs warehouse (mirrors `_dbt`)."""
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke(
        ["docs", "generate", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
    )


def test_docs_generate_reports_delta_stats(tmp_path):
    """A Delta-backed (incremental) model must surface row/byte/last-modified stats in the catalog —
    duckrun reads them from the Delta log (issue #3), so `dbt docs generate` is no longer statless."""
    wh, _ = _warehouse(tmp_path)
    assert _dbt(wh).success                          # build the `events` Delta table
    assert _docs_generate(wh).success
    catalog = json.loads((Path(PROJECT_DIR) / "target" / "catalog.json").read_text())
    events = next(n for uid, n in catalog["nodes"].items() if uid.endswith(".events"))
    s = events["stats"]
    assert s.get("has_stats", {}).get("value") is True, "events has no stats"
    for k in ("num_rows", "bytes", "last_modified"):
        assert s.get(k, {}).get("include") is True, f"missing stat {k!r}"
    assert isinstance(s["num_rows"]["value"], int) and s["num_rows"]["value"] > 0
    assert isinstance(s["bytes"]["value"], int)


# ===== 5. the rest of the strategy matrix — every fenced/unfenced strategy through a real dbt run =====
#
# The cases above prove the pin for `merge` (1-3) and `delete+insert` (2b). The snapshot pin is a
# per-strategy property, so the rest of the dbt-reachable strategies are raced here too — each with
# the SAME concurrent `update id=1 -> 999` mid-run — to lock in which are FENCED (a foreign commit
# makes the run fail loud, no lost update) and which are UNFENCED BY DESIGN (the run succeeds):
#
#   merge / insert / delete+insert / safeappend / microbatch  -> fenced   (read-modify-write)
#   append (plain) / table (full overwrite)                   -> unfenced (append only adds;
#                                                                a full rebuild is last-writer-wins)
#
# microbatch is fenced but not separately raced here: it is the ONE fenced strategy that structurally
# cannot have the delete+insert-class bug. Its wiring (_store_microbatch) calls engine.replace_window
# -> replace_where, which *requires* read_version and raises if it's None — so a window replace that
# forgot to pin would fail every run, not silently land unfenced (the way write_delta('overwrite')
# could). Its CAS fence is the same replace_where proven in tests/correctness, and a deterministic
# microbatch race needs time-freezing + bounded batches + a dbt-version gate for little extra signal.


def test_safeappend_is_fenced(tmp_path):
    """safeappend commits compare-and-swap against vB (append_if_unchanged, max_commit_retries=0): a
    writer that lands during the run makes the append REFUSE rather than append onto a drifted HEAD.
    The run fails loud and the batch (id=11) never lands."""
    res, rows = _race(tmp_path, "events_safeappend", ["append_if_unchanged"])
    assert not res.success                          # CAS refused — the run failed loud
    assert rows == {**_seed_rows(), 1: 999}         # writer's 999 stands; id=11 never appended


def test_plain_append_is_unfenced_but_nondestructive(tmp_path):
    """Plain append is UNFENCED by design — it rebases onto HEAD and only ADDS rows, so the run
    succeeds and the concurrent writer's change is preserved alongside the appended row (nothing is
    lost; the two coexist). The deliberate contrast with the fenced strategies."""
    res, rows = _race(tmp_path, "events_append", ["write_delta"])
    assert res.success                              # append rebased onto HEAD — no fence, no failure
    assert rows == {**_seed_rows(), 1: 999, 11: 110}  # writer's 999 kept AND our id=11 landed


def test_full_overwrite_is_unfenced_last_writer_wins(tmp_path):
    """A full-rebuild `table` model is UNFENCED by design: the overwrite replaces the whole table, so
    a concurrent commit mid-run is clobbered (last-writer-wins). This is correct — a full refresh is
    not, and must not be, snapshot-fenced — so the test documents the boundary, it doesn't lament it."""
    res, rows = _race(tmp_path, "events_table", ["write_delta"])
    assert res.success                              # the rebuild always commits — by design
    assert rows == _seed_rows()                     # last writer wins: our rebuild replaced 999
