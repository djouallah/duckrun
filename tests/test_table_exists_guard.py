"""WS1 — the ``table_exists`` data-loss window.

Before this fix, ``engine.table_exists`` swallowed *every* exception and returned False. A
transient storage error (ADLS/OneLake 503, expired token) at store time therefore looked like
"no table", sending an incremental write — whose SQL had already filtered to only-new rows —
down the overwrite branch, replacing the whole table with just the increment. Silent loss, green
run.

The fix: ``table_exists`` catches only ``TableNotFoundError`` (genuine absence) and re-raises
everything else, and ``Plugin.store()`` refuses to overwrite when dbt's run-start discovery
believed the table existed but it can't be opened now.
"""
import types

import duckdb
import pyarrow as pa
import pytest

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.delta_plugin import Plugin

try:
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


# ----------------------------------------------------------------- unit: table_exists

def test_table_exists_reraises_generic_error(monkeypatch):
    """A non-TableNotFound error (e.g. a transient OSError) must propagate, not become False."""
    def boom(path, storage_options):
        raise OSError("transient ADLS 503")

    monkeypatch.setattr(engine, "_delta_table", boom)
    with pytest.raises(OSError):
        engine.table_exists("/some/path")


def test_table_exists_false_on_table_not_found(monkeypatch):
    """A genuinely-absent table (TableNotFoundError) is the only case that returns False."""
    def missing(path, storage_options):
        raise TableNotFoundError("no log here")

    monkeypatch.setattr(engine, "_delta_table", missing)
    assert engine.table_exists("/some/path") is False


def test_table_exists_true_for_real_table(tmp_path):
    path = str(tmp_path / "t")
    engine.write_delta(path, pa.table({"id": pa.array([1, 2, 3], pa.int64())}), "overwrite")
    assert engine.table_exists(path) is True


# ----------------------------------------------------------------- unit: delta_version

def test_delta_version_reraises_generic_error(monkeypatch):
    """delta_version feeds safeappend's start-of-build pin; a real error must NOT silently
    become None (which would degrade the pin to HEAD-at-write and reopen the race)."""
    from dbt.adapters.duckrun.impl import DuckrunAdapter

    def boom(location, so):
        raise OSError("transient storage error")

    monkeypatch.setattr(engine, "_delta_table", boom)
    # Build a bare adapter shell: delta_version only touches self.config.credentials.
    adapter = DuckrunAdapter.__new__(DuckrunAdapter)
    adapter.config = types.SimpleNamespace(
        credentials=types.SimpleNamespace(storage_options=None)
    )
    with pytest.raises(OSError):
        adapter.delta_version("/some/path")


# ----------------------------------------------------------------- integration: store() guard

def _fake_target_config(path, relation_name, cfg):
    return types.SimpleNamespace(
        location=types.SimpleNamespace(path=path),
        config=cfg,
        relation=types.SimpleNamespace(render=lambda: relation_name),
    )


def _store_plugin(con):
    p = Plugin.__new__(Plugin)
    p._storage_options = None
    p._compaction_threshold = 100
    p._conn = con
    p._cursor_handle = con
    p._baseline_memory_limit = None
    p._microbatch_seen = set()
    return p


def test_store_refuses_overwrite_when_table_vanishes_midrun(tmp_path, monkeypatch):
    """End-to-end of the loss window: a full table exists on disk; on the 'second run' the open
    fails transiently. store() must fail loudly AND leave the full table (and its version) intact
    — never overwrite it with the filtered increment."""
    path = str(tmp_path / "events")
    full = pa.table({"id": pa.array([1, 2, 3, 4, 5], pa.int64())})
    engine.write_delta(path, full, "overwrite")
    version_before = DeltaTable(path).version()

    con = duckdb.connect()
    con.execute("create view increment as select 99 as id")
    plugin = _store_plugin(con)

    # Simulate the transient storage error the fix stops swallowing.
    monkeypatch.setattr(engine, "table_exists", lambda *a, **k: (_ for _ in ()).throw(OSError("503")))

    tc = _fake_target_config(
        path, "increment",
        {"incremental": True, "full_refresh": False, "dbt_believes_exists": True},
    )
    with pytest.raises(OSError):
        plugin.store(tc)

    # The table on disk is untouched: same version, full contents.
    assert DeltaTable(path).version() == version_before
    ids = sorted(DeltaTable(path).to_pyarrow_table().column("id").to_pylist())
    assert ids == [1, 2, 3, 4, 5]


def test_assert_not_null_raises_with_contract_message():
    """The contract NOT NULL guard fires on a staged null and uses dbt's phrasing."""
    con = duckdb.connect()
    con.execute("create view staged as select * from (values (1,'a'),(null,'b')) t(id, color)")
    with pytest.raises(CompilationError, match="NOT NULL constraint failed"):
        Plugin._assert_not_null(con, "staged", ["id"])


def test_assert_not_null_passes_when_no_nulls():
    con = duckdb.connect()
    con.execute("create view staged as select * from (values (1,'a'),(2,'b')) t(id, color)")
    # No exception for a fully-populated column.
    Plugin._assert_not_null(con, "staged", ["id", "color"])


def test_store_contradiction_guard_when_exists_false(tmp_path, monkeypatch):
    """If table_exists returns False (no table found) but dbt's discovery believed it existed and
    this is an incremental non-full-refresh run, store() refuses rather than overwriting with the
    increment."""
    path = str(tmp_path / "events")
    con = duckdb.connect()
    con.execute("create view increment as select 99 as id")
    plugin = _store_plugin(con)

    monkeypatch.setattr(engine, "table_exists", lambda *a, **k: False)

    tc = _fake_target_config(
        path, "increment",
        {"incremental": True, "full_refresh": False, "dbt_believes_exists": True},
    )
    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        plugin.store(tc)
