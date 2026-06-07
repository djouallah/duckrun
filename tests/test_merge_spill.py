"""merge_delta memory spill: cross-platform RAM detection and the max_spill_size knob.

delta_rs only builds a disk-spilling merge session when ``max_spill_size`` is set; otherwise
the merge runs under an unbounded memory pool and can OOM on large upserts. duckrun defaults
the cap to ~80% of physical RAM (like DuckDB's ``memory_limit``) and lets a model override it
via ``merge_max_spill_size``. These tests pin that wiring down.
"""
import pyarrow as pa

from deltalake import DeltaTable

from dbt.adapters.duckrun import engine


def _table(ids):
    return pa.table({
        "id": pa.array(list(ids), pa.int64()),
        "value": pa.array([i * 10 for i in ids], pa.int64()),
    })


def _rows(path):
    t = DeltaTable(path).to_pyarrow_table().sort_by("id")
    return dict(zip(t.column("id").to_pylist(), t.column("value").to_pylist()))


# --------------------------------------------------------------- RAM detection

def test_total_ram_bytes_is_plausible():
    total = engine._total_ram_bytes()
    # On any real CI/dev box (Windows or Linux) this must resolve to a sane positive value.
    assert total is not None
    assert total > 256 * 1024 * 1024  # > 256 MiB


def test_cgroup_mem_limit_is_none_or_positive():
    # No container limit on a dev box (returns None); inside a memory-capped cgroup it must
    # be a sane positive value. Either way it must never raise.
    lim = engine._cgroup_mem_limit_bytes()
    assert lim is None or lim > 0


def test_effective_limit_is_min_of_physical_and_cgroup():
    eff = engine._effective_mem_limit_bytes()
    phys = engine._total_ram_bytes()
    cg = engine._cgroup_mem_limit_bytes()
    expected = min([v for v in (phys, cg) if v]) if (phys or cg) else None
    assert eff == expected


def test_default_merge_spill_size_is_80_percent_of_effective_limit():
    eff = engine._effective_mem_limit_bytes()
    assert engine._default_merge_spill_size() == int(eff * 0.8)


# ---------------------------------------------------------- kwarg forwarding

class _FakeMerger:
    def when_matched_update_all(self, **k):
        return self

    def when_matched_update(self, **k):
        return self

    def when_not_matched_insert_all(self, **k):
        return self

    def execute(self):
        self.executed = True


class _FakeDeltaTable:
    def __init__(self, captured):
        self._captured = captured

    def merge(self, **kwargs):
        self._captured.clear()
        self._captured.update(kwargs)
        return _FakeMerger()


def _spy(monkeypatch):
    captured = {}
    monkeypatch.setattr(engine, "_delta_table", lambda path, so: _FakeDeltaTable(captured))
    return captured


def test_max_spill_size_defaults_to_80_percent(monkeypatch):
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id")
    assert captured["max_spill_size"] == engine._default_merge_spill_size()


def test_max_spill_size_explicit_is_forwarded(monkeypatch):
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", max_spill_size=123_456)
    assert captured["max_spill_size"] == 123_456


def test_max_spill_size_zero_disables_the_cap(monkeypatch):
    """0 (or any falsy non-None) opts out: the kwarg is omitted so delta_rs runs unbounded."""
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", max_spill_size=0)
    assert "max_spill_size" not in captured


def test_undetectable_ram_omits_the_cap(monkeypatch):
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: None)
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id")
    assert "max_spill_size" not in captured


# ---------------------------------------------------------------- end to end

def test_merge_with_default_spill_upserts_correctly(tmp_path):
    """The real delta_rs merge path still produces a correct upsert with the 80% default on."""
    path = str(tmp_path / "t")
    engine.write_delta(path, _table([1, 2, 3]), "overwrite")
    # update id=3, insert id=4
    engine.merge_delta(path, pa.table({"id": pa.array([3, 4], pa.int64()),
                                       "value": pa.array([999, 40], pa.int64())}), "id")
    assert _rows(path) == {1: 10, 2: 20, 3: 999, 4: 40}


def test_merge_with_explicit_spill_size_upserts_correctly(tmp_path):
    """An explicit (bounded) max_spill_size also produces the correct upsert."""
    path = str(tmp_path / "t")
    engine.write_delta(path, _table([1, 2, 3]), "overwrite")
    engine.merge_delta(
        path,
        pa.table({"id": pa.array([2], pa.int64()), "value": pa.array([222], pa.int64())}),
        "id",
        max_spill_size=256 * 1024 * 1024,
    )
    assert _rows(path) == {1: 10, 2: 222, 3: 30}
