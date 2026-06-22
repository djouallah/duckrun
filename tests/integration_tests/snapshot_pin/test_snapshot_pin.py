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


def _dbt(warehouse: str) -> object:
    """One in-process `dbt run --select events` against a local-fs warehouse. Returns the
    dbtRunnerResult (`.success`, `.result`, `.exception`)."""
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke(
        ["run", "--select", "events", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR]
    )


def _rows(path: str) -> dict:
    """{id: value} for the current HEAD of the Delta table."""
    t = DeltaTable(path).to_pyarrow_table()
    return dict(zip(t.column("id").to_pylist(), t.column("value").to_pylist()))


def _warehouse(tmp_path) -> tuple:
    wh = (tmp_path / "wh").as_posix()          # forward slashes so delta paths are clean on Windows
    return wh, f"{wh}/{SCHEMA}/events"


# ------------------------------------------------------- 1. baseline: no concurrent writer

def _seed_rows():
    return {i: i * 10 for i in range(1, 11)}     # ids 1..10, value = id*10


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
