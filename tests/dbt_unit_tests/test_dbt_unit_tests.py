"""dbt NATIVE unit tests (the `unit_tests:` given/expect feature) running on the duckrun adapter.

A small, realistic jaffle-style project (project/) — seeds -> staging -> marts, including the
canonical ``dim_customers`` email-validation model — carries the unit tests in
project/models/marts/_unit_tests.yml. Each one supplies explicit input rows for the model's ``ref()``s
and asserts explicit output rows (deterministic by construction).

This harness drives the project in-process with dbtRunner against a fresh local-fs Delta warehouse:

  * ``dbt build``  — seeds load, every model materializes to REAL Delta via duckrun, and ALL data +
    unit tests run. Green proves the native unit-test feature works on duckrun AND the project builds
    end to end.
  * ``dbt test --select test_type:unit`` — runs just the unit tests, in isolation.
  * a read-back of the real ``dim_customers`` Delta table (built from the seeds, not mocked) — ties
    the deterministic rows to an actual duckrun materialization, not only the in-memory unit-test
    temp. The read-back goes through duckrun itself (DuckDB + ``delta_scan``), the same way a user
    reads a duckrun table — no need for the raw delta-rs library here.

The project also covers the ``incremental`` materialization (models/incremental/) across every
duckrun strategy. The unit tests prove the ``unit_tests:`` feature works on an incremental model
(``events_append``: the full-refresh path and the ``is_incremental()`` override + ``this`` input).
Because a unit test sandboxes a model's SELECT and not its write strategy, the strategies are
instead distinguished by a REAL two-run ``dbt run`` here (``test_incremental_strategy_two_runs`` /
``test_incremental_microbatch_two_batches``): an initial load then an incremental load, with the
resulting Delta read back through duckrun and asserted row-by-row so merge (updates), insert
(insert-only), delete+insert (replace), append/safeappend (append-only) and microbatch (per-day
batch) each show their defining behaviour. The exhaustive strategy x key-shape x type x fixture-
format matrix lives in tests/conformance/test_unit_testing_incremental.py; this file is the
realistic-project, row-content counterpart.
"""
import os
from pathlib import Path

import duckrun
import pytest

from dbt.cli.main import dbtRunner

PROJECT_DIR = str(Path(__file__).parent / "project")
SCHEMA = "main"


def _dbt(warehouse: str, *args: str) -> object:
    """One in-process dbt command against a local-fs warehouse (e.g. _dbt(wh, "build"))."""
    os.environ["WAREHOUSE_PATH"] = warehouse
    os.environ["DBT_SCHEMA"] = SCHEMA
    return dbtRunner().invoke([*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR])


def _wh(tmp_path) -> str:
    return (tmp_path / "wh").as_posix()           # forward slashes so delta paths are clean on Windows


def _unit_results(res) -> dict:
    """{unit_test_name: status} from a dbtRunnerResult (unit tests are 'unit_test' nodes)."""
    out = {}
    for r in (res.result or []):
        node = getattr(r, "node", None)
        if node is not None and getattr(node, "resource_type", "") == "unit_test":
            out[node.name] = str(r.status)
    return out


# microbatch is event-time driven: a plain build/run would batch every day from `begin` to today,
# so it is excluded from the bulk commands and exercised on its own with a bounded window below.
_EXCLUDE_MICROBATCH = ("--exclude", "events_microbatch")


def _cold_start(wh):
    """Seed, then run, then build a fresh warehouse. The run-before-build step exists because an
    incremental unit test references ``input: this``: dbt reads the model's columns off the REAL
    relation, so it must already exist before the unit tests resolve — exactly as a real project
    always has a prior build. (Same reason tests/conformance/test_unit_testing_incremental.py runs
    first.) The build then re-runs every model — incremental ones now take their incremental write
    path — plus every data and unit test, so one green sequence proves the project end to end."""
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run", *_EXCLUDE_MICROBATCH).success
    assert _dbt(wh, "build", *_EXCLUDE_MICROBATCH).success


def test_build_passes_including_unit_tests(tmp_path):
    """A clean seed -> run -> build materializes every model to real Delta (duckrun) AND runs every
    data and unit test — one green sequence proves the native unit_tests feature works end to end."""
    _cold_start(_wh(tmp_path))


def test_unit_tests_run_and_pass(tmp_path):
    """Run the unit tests in isolation and confirm all seven executed and passed."""
    wh = _wh(tmp_path)
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run", *_EXCLUDE_MICROBATCH).success
    res = _dbt(wh, "test", "--select", "test_type:unit")
    assert res.success
    statuses = _unit_results(res)
    assert len(statuses) == 7, f"expected 7 unit tests, ran {len(statuses)}: {statuses}"
    assert all(s == "pass" for s in statuses.values()), statuses


def test_dim_customers_real_delta_rows(tmp_path):
    """Beyond the mocked unit tests: dim_customers materialized to REAL Delta from the seeds yields
    the deterministic validation we expect — alice valid, bob's domain unaccepted, carol malformed.
    Read back through duckrun (DuckDB + delta_scan), exactly how a user reads a duckrun table."""
    wh = _wh(tmp_path)
    _cold_start(wh)
    con = duckrun.connect(wh, schema=SCHEMA, read_only=True)
    valid = dict(con.sql("select email, is_valid_email_address from dim_customers").fetchall())
    assert valid == {
        "alice@example.com": True,    # valid format + accepted domain (example.com)
        "bob@unknown.com": False,     # valid format, but unknown.com is not an accepted domain
        "carolgmail.com": False,      # malformed — no @
    }


def test_sort_by_writes_physically_ordered(tmp_path):
    """sort_by (review #6) writes rows physically ordered by the sort key. Read the Delta table's
    parquet back in FILE order (not via an ORDER BY query) and assert it's sorted — proving the
    ordering survives to disk under preserve_insertion_order=false."""
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    _cold_start(wh)
    uri = f"{wh}/{SCHEMA}/sorted_layout"
    keys = DeltaTable(uri).to_pyarrow_table().column("sort_key").to_pylist()
    assert keys == [1, 2, 3, 4, 5], keys  # physically sorted, not the shuffled input order


# Per-strategy two-run expectations: load 1 = events 1-3 @ original amounts, load 2 = events 1-3
# RE-EMITTED @ changed amounts + new 4-6. The final {event_id: amount} below is what each strategy's
# real Delta table must hold after both runs — the row content is what distinguishes the strategies.
_STRATEGY_EXPECTED = {
    "merge":         {1: 111.0, 2: 222.0, 3: 333.0, 4: 400.0, 5: 500.0, 6: 600.0},  # upsert: 1-3 updated
    "delete_insert": {1: 111.0, 2: 222.0, 3: 333.0, 4: 400.0, 5: 500.0, 6: 600.0},  # batch keys replaced
    "insert":        {1: 100.0, 2: 200.0, 3: 300.0, 4: 400.0, 5: 500.0, 6: 600.0},  # insert-only: 1-3 kept
    "append":        {1: 100.0, 2: 200.0, 3: 300.0, 4: 400.0, 5: 500.0, 6: 600.0},  # only new keys appended
    "safeappend":    {1: 100.0, 2: 200.0, 3: 300.0, 4: 400.0, 5: 500.0, 6: 600.0},  # optimistic append
}


@pytest.mark.parametrize("strategy", list(_STRATEGY_EXPECTED))
def test_incremental_strategy_two_runs(tmp_path, strategy):
    """Each incremental strategy through REAL dbt: an initial load (window 1: events 1-3) then an
    incremental load (window 2: 1-3 re-emitted with changed amounts + new 4-6). Read the resulting
    Delta table back through duckrun and assert the strategy's defining row-content behaviour."""
    wh = _wh(tmp_path)
    model = f"events_{strategy}"
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run", "--select", f"+{model}", "--vars", "{load: 1}").success   # first load
    assert _dbt(wh, "run", "--select", f"+{model}", "--vars", "{load: 2}").success   # incremental load
    con = duckrun.connect(wh, schema=SCHEMA, read_only=True)
    rows = dict(con.sql(f"select event_id, amount from {model} order by event_id").fetchall())
    assert rows == _STRATEGY_EXPECTED[strategy], rows


def test_incremental_microbatch_two_batches(tmp_path):
    """The microbatch strategy: dbt runs one delete+insert per daily event_time batch. Process day 1
    (events 1-3) then day 2 (events 4-6) with bounded --event-time windows and assert the real Delta
    table accumulates to all six events — the second batch adds rows without a full refresh."""
    wh = _wh(tmp_path)
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run", "--select", "+events_microbatch",
                "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-02").success
    day1 = sorted(r[0] for r in
                  duckrun.connect(wh, schema=SCHEMA, read_only=True)
                  .sql("select event_id from events_microbatch").fetchall())
    assert day1 == [1, 2, 3], day1
    assert _dbt(wh, "run", "--select", "events_microbatch",
                "--event-time-start", "2024-01-02", "--event-time-end", "2024-01-03").success
    day2 = sorted(r[0] for r in
                  duckrun.connect(wh, schema=SCHEMA, read_only=True)
                  .sql("select event_id from events_microbatch").fetchall())
    assert day2 == [1, 2, 3, 4, 5, 6], day2
