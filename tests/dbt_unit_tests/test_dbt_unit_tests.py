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
(insert-only), delete+insert (replace), append (append-only; auto-fenced when it reads ``{{ this }}``)
and microbatch (per-day batch) each show their defining behaviour. The exhaustive strategy x key-shape x type x fixture-
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


def test_canonical_spellings_sorted_by_partitioned_by(tmp_path):
    """dbt-duckdb 1.11's canonical config spellings (sorted_by/partitioned_by — upstream aliases
    duckrun's sort_by/partition_by both ways) must behave exactly like duckrun's own spellings: a
    verbatim upstream project using them must not be silently unpartitioned/unsorted.
    sorted_layout_canonical uses ONLY the canonical names; assert the partition column reached the
    Delta writer and rows landed physically sorted within each partition (file order, no ORDER BY)."""
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    assert _dbt(wh, "run", "--select", "sorted_layout_canonical").success
    dt = DeltaTable(f"{wh}/{SCHEMA}/sorted_layout_canonical")
    assert dt.metadata().partition_columns == ["bucket"], dt.metadata().partition_columns
    table = dt.to_pyarrow_table()
    by_bucket = {}
    for bucket, key in zip(table.column("bucket").to_pylist(),
                           table.column("sort_key").to_pylist()):
        by_bucket.setdefault(bucket, []).append(key)
    assert sorted(by_bucket) == ["a", "b"], by_bucket
    for bucket, keys in by_bucket.items():
        assert keys == sorted(keys), f"partition {bucket} not physically sorted: {keys}"


def test_alias_precedence_matches_upstream(tmp_path):
    """dbt-duckdb 1.11's precedence, exactly: when both spellings are set the canonical
    partitioned_by wins, and a canonical EMPTY list is upstream's compiler error ("must contain at
    least one column") — never a silent fall-through to the legacy key (which would partition a
    model whose config explicitly said not to) and never a silently unpartitioned table."""
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    assert _dbt(wh, "run", "--select", "alias_precedence_layout", "--vars",
                "{alias_partitioned_by: ['bucket'], alias_partition_by: ['id']}").success
    dt = DeltaTable(f"{wh}/{SCHEMA}/alias_precedence_layout")
    assert dt.metadata().partition_columns == ["bucket"], dt.metadata().partition_columns

    res = _dbt(wh, "run", "--select", "alias_precedence_layout", "--vars",
               "{alias_partitioned_by: [], alias_partition_by: ['id']}")
    assert not res.success, "partitioned_by: [] must be an error, not a fall-through"


def test_geometry_configs_reach_the_writer(tmp_path):
    """max_row_group_size / target_file_size_mb must survive the materialization macro's config
    hand-off. This is the hop that broke in 0.4.43: _delta_core.sql's delta_config carried sort_by
    but neither geometry key, so the plugin saw None and fell back to the adaptive layout — while
    test_geometry_config_validation (parser only) and the parquet_layout CI (pins the engine seam
    directly, no dbt config) both stayed green. 10 rows under a 3-row ceiling = 4 row groups; the
    adaptive default writes 1, so a dropped key fails this loudly."""
    import pyarrow.parquet as pq
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    assert _dbt(wh, "run", "--select", "geometry_layout").success
    files = DeltaTable(f"{wh}/{SCHEMA}/geometry_layout").file_uris()
    assert len(files) == 1, files
    rg = pq.ParquetFile(files[0]).metadata.num_row_groups
    assert rg == 4, f"expected 4 row groups from max_row_group_size=3 over 10 rows, got {rg}"


def test_sort_by_auto_writes_clustered(tmp_path):
    """sort_by='auto' (the dbt spelling of SORTED BY AUTO) profiles the staged model result and
    picks the sort key itself: sorted_layout_auto's hash-scattered 3-value category must land
    physically clustered on disk (read back in FILE order, no ORDER BY), while the unique id is
    grain-stopped out of the key. Also drives the case-insensitive spelling and the rejection of
    'auto' hidden inside a list."""
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    assert _dbt(wh, "run", "--select", "sorted_layout_auto").success
    uri = f"{wh}/{SCHEMA}/sorted_layout_auto"
    cats = DeltaTable(uri).to_pyarrow_table().column("category").to_pylist()
    assert len(cats) == 200, len(cats)
    assert cats == sorted(cats), "category not physically clustered — the auto key was not applied"
    # Case-insensitive: 'AUTO' resolves the same way.
    assert _dbt(wh, "run", "--select", "sorted_layout_auto",
                "--vars", "{auto_sort_by: AUTO}").success
    cats = DeltaTable(uri).to_pyarrow_table().column("category").to_pylist()
    assert cats == sorted(cats), "AUTO spelling did not cluster"
    # 'auto' must be the scalar config value — inside a list it is rejected, the run fails.
    res = _dbt(wh, "run", "--select", "sorted_layout_auto",
               "--vars", "{auto_sort_by: ['auto']}")
    assert not res.success, "sort_by=['auto'] should fail the run"


def test_sort_by_auto_narrows_wide_decimals(tmp_path):
    """The sorted-auto overwrite narrows wide DECIMALs like the connection API's SORTED BY AUTO
    (session._narrow_wide_decimals): DECIMAL(38,2) whose exact max fits lands as DECIMAL(18,2)
    (INT64, dictionary-encodable) while one whose max does not fit stays wide. Through a REAL dbt
    run — the narrowing sits in the plugin's write path, not the engine."""
    from deltalake import DeltaTable

    wh = _wh(tmp_path)
    assert _dbt(wh, "run", "--select", "sorted_auto_narrow").success
    schema = {f.name: str(f.type)
              for f in DeltaTable(f"{wh}/{SCHEMA}/sorted_auto_narrow").schema().to_arrow()}
    assert "Decimal128(18, 2)" in schema["wide_price"], schema
    assert "Decimal128(38, 2)" in schema["wide_keep"], schema


# Per-strategy two-run expectations: load 1 = events 1-3 @ original amounts, load 2 = events 1-3
# RE-EMITTED @ changed amounts + new 4-6. The final {event_id: amount} below is what each strategy's
# real Delta table must hold after both runs — the row content is what distinguishes the strategies.
_STRATEGY_EXPECTED = {
    "merge":         {1: 111.0, 2: 222.0, 3: 333.0, 4: 400.0, 5: 500.0, 6: 600.0},  # upsert: 1-3 updated
    "delete_insert": {1: 111.0, 2: 222.0, 3: 333.0, 4: 400.0, 5: 500.0, 6: 600.0},  # batch keys replaced
    "insert":        {1: 100.0, 2: 200.0, 3: 300.0, 4: 400.0, 5: 500.0, 6: 600.0},  # insert-only: 1-3 kept
    "append":        {1: 100.0, 2: 200.0, 3: 300.0, 4: 400.0, 5: 500.0, 6: 600.0},  # only new keys appended
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


def test_merge_materialize_source_two_runs(tmp_path):
    """merge with merge_materialize_source=true (review #14): the model is staged once into a temp
    table before the guards + delta_rs merge. Result must match the ordinary merge upsert."""
    wh = _wh(tmp_path)
    assert _dbt(wh, "seed").success
    assert _dbt(wh, "run", "--select", "+events_merge_materialized", "--vars", "{load: 1}").success
    assert _dbt(wh, "run", "--select", "+events_merge_materialized", "--vars", "{load: 2}").success
    con = duckrun.connect(wh, schema=SCHEMA, read_only=True)
    rows = dict(con.sql(
        "select event_id, amount from events_merge_materialized order by event_id").fetchall())
    assert rows == {1: 111.0, 2: 222.0, 3: 333.0, 4: 400.0, 5: 500.0, 6: 600.0}, rows


def _fingerprint(wh: str) -> dict:
    """``{table: (row_count, content_hash)}`` for every Delta table in the warehouse.

    The hash is ``sum(hash(<every column>))`` — a whole-row hash, so a value moving between rows is
    caught, summed so the result is independent of row ORDER. Order can't be part of the comparison:
    duckrun runs with ``preserve_insertion_order=false`` and delta-rs is free to lay files out
    differently, so two runs of the same project legitimately differ in physical order.
    """
    con = duckrun.connect(wh, schema=SCHEMA, read_only=True)
    out = {}
    for table in sorted(p.name for p in (Path(wh) / SCHEMA).iterdir() if p.is_dir()):
        cols = [c[0] for c in con.sql(f"describe select * from {table}").fetchall()]
        row_hash = "hash(" + ", ".join('"' + c.replace('"', '""') + '"' for c in cols) + ")"
        rows, digest = con.sql(
            f"select count(*), sum({row_hash}::HUGEINT) from {table}").fetchone()
        out[table] = (rows, str(digest))
    return out


def _build_everything(wh: str, threads: int) -> None:
    """The project's full lifecycle at a given dbt thread count: cold build, then a second
    incremental load of every strategy, then both microbatch day-batches."""
    t = ("--threads", str(threads))
    assert _dbt(wh, "seed", *t).success
    assert _dbt(wh, "run", *t, *_EXCLUDE_MICROBATCH, "--vars", "{load: 1}").success
    assert _dbt(wh, "build", *t, *_EXCLUDE_MICROBATCH, "--vars", "{load: 2}").success
    assert _dbt(wh, "run", *t, "--select", "+events_microbatch",
                "--event-time-start", "2024-01-01", "--event-time-end", "2024-01-02").success
    assert _dbt(wh, "run", *t, "--select", "events_microbatch",
                "--event-time-start", "2024-01-02", "--event-time-end", "2024-01-03").success


def test_threads_produce_identical_tables(tmp_path):
    """The SAME project built serially and in parallel must produce byte-identical table contents.

    duckrun honors `threads`, so models build concurrently — each on its own DuckDB cursor, streaming
    its own relation into delta-rs. Every other suite only asserts that a parallel run doesn't
    ERROR, but the failure mode that matters here is silent: a race in the delta plugin's per-thread
    cursor, in the microbatch first-batch bookkeeping, or in the shared token/secret state produces a
    table with the WRONG ROWS, not a crash. (A concrete example: before the batches were serialized,
    two `--full-refresh` microbatch batches could each believe they were the first and one batch's
    rows would vanish.) So build the whole project twice into separate warehouses and diff.
    """
    from dbt.adapters.duckrun import engine

    serial, parallel = _wh(tmp_path) + "_t1", _wh(tmp_path) + "_t4"
    _build_everything(serial, threads=1)
    _build_everything(parallel, threads=4)

    # Guard against the comparison quietly becoming 1-vs-1: the adapter publishes the run's thread
    # count here, so if `threads` is ever pinned again this test fails loudly instead of passing
    # vacuously. (Set at adapter init, so it reflects the last run above.)
    assert engine.RUN_THREADS == 4, (
        f"the parallel build ran at {engine.RUN_THREADS} thread(s), so this test compared a serial "
        "run against another serial run and proved nothing")

    got, want = _fingerprint(parallel), _fingerprint(serial)
    assert set(got) == set(want), f"different tables built: {set(got) ^ set(want)}"
    differing = {t: (want[t], got[t]) for t in want if want[t] != got[t]}
    assert not differing, f"threads=4 produced different data than threads=1: {differing}"


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
