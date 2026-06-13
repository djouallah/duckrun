"""Issue #1 — snapshot pinning correctness (engine level, runnable without the duckdb version param).

These exercise the delta-rs side of the pin (``read_version`` → ``DeltaTable.load_as_version`` +
``max_commit_retries=0``), which works on the pinned deltalake 1.5.0 floor regardless of the
duckdb-delta build. The duckdb-delta ``delta_scan(version => N)`` staging-read pin is covered by the
connection-API matrix (skipped on older builds).

Covered:
  - merge_delta(read_version=vB): a foreign commit in (vB, HEAD] fails the merge loudly; without a
    concurrent writer it commits. This is the Spark single-snapshot MERGE window.
  - replace_where: a SINGLE atomic Delta commit (replaceWhere) — not a delete-then-append pair —
    and CAS-fenced when pinned.
  - maintenance is NEVER pinned: _maintain takes no version parameter (structural guard against the
    stale-file-list compaction/vacuum that would corrupt the table), and a pinned write leaves a
    clean, readable HEAD.
"""
import inspect
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from dbt.adapters.duckrun import engine


def _seed(path):
    engine.write_delta(
        path,
        pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                  "value": pa.array([10, 10, 10], pa.int64())}),
        "overwrite",
    )  # -> v0


def _tbl(ids_values):
    ids, vals = zip(*ids_values)
    return pa.table({"id": pa.array(ids, pa.int64()), "value": pa.array(vals, pa.int64())})


# --------------------------------------------------------------- merge read_version pinning

def test_pinned_merge_refuses_on_foreign_commit():
    """vB=0; a foreign commit (v1) touches id=1; a merge pinned at v0 that also touches id=1 must
    fail loud — OCC validates (v0, HEAD] and the foreign commit conflicts."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})  # foreign -> v1

    with pytest.raises(CommitFailedError):
        engine.merge_delta(path, _tbl([(1, 77), (4, 77)]), "id", read_version=vB)


def test_pinned_merge_commits_without_conflict():
    """No concurrent writer: a pinned merge commits and applies the upsert."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    engine.merge_delta(path, _tbl([(2, 99), (4, 99)]), "id", read_version=vB)
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 10, 2: 99, 3: 10, 4: 99}


# --------------------------------------------------------------- replaceWhere atomicity

def test_replace_where_is_single_atomic_commit():
    """replaceWhere is ONE commit (v0 -> v1), not a delete-then-append pair, and replaces only the
    matching window."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)  # v0
    engine.replace_where(path, _tbl([(1, 77), (2, 77)]), "id < 3")
    assert DeltaTable(path).version() == 1  # exactly one new version — atomic, not two commits
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 77, 2: 77, 3: 10}


def test_replace_where_cas_refuses_on_foreign_commit():
    """A pinned replaceWhere fails loud if the table moved since vB (compare-and-swap)."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    DeltaTable(path).update(predicate="id = 1", updates={"value": "999"})  # foreign -> v1
    with pytest.raises(CommitFailedError):
        engine.replace_where(path, _tbl([(1, 77)]), "id < 3", read_version=vB)


# --------------------------------------------------------------- maintenance never pinned

def test_maintain_takes_no_version_parameter():
    """Structural guard: _maintain must not accept a version/read_version — it always runs at a
    fresh HEAD. A stale-snapshot compaction/vacuum would delete files live versions reference."""
    params = set(inspect.signature(engine._maintain).parameters)
    assert "version" not in params and "read_version" not in params


def test_pinned_write_leaves_clean_head():
    """After a pinned merge, HEAD is readable and reflects the merge (maintenance reopened fresh,
    did not corrupt by operating on the pinned snapshot)."""
    path = str(Path(tempfile.mkdtemp()) / "t")
    _seed(path)
    vB = engine.table_version(path)
    engine.merge_delta(path, _tbl([(2, 55)]), "id", read_version=vB)
    rows = {r["id"]: r["value"] for r in DeltaTable(path).to_pyarrow_table().to_pylist()}
    assert rows == {1: 10, 2: 55, 3: 10}
