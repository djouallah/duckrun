"""
Concurrency / optimistic-concurrency-control (OCC) behaviour of the duckrun engine.

Conceptual reference: occ.ipynb (Fabric_Notebooks_Demo) pins a Delta version, lets a
concurrent transaction change the table, then runs a write against the *pinned* version —
which Delta rejects with CommitFailedError (OCC protecting you from clobbering changes you
never saw).

duckrun's engine (engine.merge_delta / engine.write_delta) always loads the *current*
DeltaTable(path); it has no way to pin a version. So a duckrun run built from a stale view of
the table commits successfully even though the table changed underneath it — it does not, and
cannot, detect the conflict. These tests demonstrate exactly that:

  * test_duckrun_merge_succeeds_despite_concurrent_change  -> passes (current reality)
  * test_stale_write_should_be_rejected                    -> xfail  (the gap we can't close)
"""
import pyarrow as pa
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from dbt.adapters.duckrun import engine


def _rows(ids):
    """Build a reusable Arrow table with columns id, value (value = id * 10)."""
    ids = list(ids)
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "value": pa.array([i * 10 for i in ids], pa.int64()),
    })


def _bootstrap(tmp_path):
    """Create the target Delta table with ids 1..5 and return (path, vB, stale_rows)."""
    path = str(tmp_path / "target")
    engine.write_delta(path, _rows(range(1, 6)), "overwrite")

    vB = DeltaTable(path).version()              # the version duckrun "saw"
    stale_rows = _rows(range(1, 6))              # the rows that view contained
    return path, vB, stale_rows


def test_duckrun_merge_succeeds_despite_concurrent_change(tmp_path):
    """duckrun commits a merge even though the table changed since the run was built."""
    path, vB, stale_rows = _bootstrap(tmp_path)

    # A concurrent writer mutates the table behind duckrun's back (merge only, no append):
    # update the values of ids 1..5 and insert 6..8, advancing the table past vB.
    engine.merge_delta(path, _rows(range(1, 9)), "id")
    assert DeltaTable(path).version() > vB

    # duckrun now merges data built from the stale (vB) view. Because the engine reloads the
    # current table and cannot pin vB, this commits successfully — no conflict is detected.
    version_before = DeltaTable(path).version()
    engine.merge_delta(path, stale_rows, "id")
    assert DeltaTable(path).version() > version_before


@pytest.mark.xfail(
    reason="duckrun cannot pin a Delta version, so a write built on a stale view commits "
           "instead of being rejected on the concurrent change",
    strict=False,
)
def test_stale_write_should_be_rejected(tmp_path):
    """What OCC *would* do if duckrun could pin a version — the gap we can't close today."""
    path, vB, stale_rows = _bootstrap(tmp_path)

    # Concurrent change touches the rows the pinned view read (merge only).
    engine.merge_delta(path, _rows(range(1, 9)), "id")

    # duckrun has no pin API; pinning is only possible via deltalake directly. If duckrun
    # could express this, a stale write ought to be rejected:
    with pytest.raises(CommitFailedError):
        DeltaTable(path, version=vB).merge(
            source=stale_rows,
            predicate="t.id = s.id",
            source_alias="s",
            target_alias="t",
        ).when_not_matched_insert_all().execute()
