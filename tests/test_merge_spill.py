"""merge_delta memory spill: cross-platform RAM detection and the max_spill_size knob.

delta_rs only builds a disk-spilling merge session when ``max_spill_size`` is set; otherwise
the merge runs under an unbounded memory pool and can OOM on large upserts. duckrun defaults
the cap to a fraction of the *effective* memory limit (min of physical RAM and the cgroup cap)
and lets a model override it via ``merge_max_spill_size``. DuckDB itself — the merge source
producer in the same process — is pinned to its own cgroup-aware share via
``configure_duckdb_memory``. These tests pin that wiring down.
"""
import re

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


def test_default_merge_spill_size_is_fraction_of_effective_limit():
    eff = engine._effective_mem_limit_bytes()
    assert engine._default_merge_spill_size() == int(eff * engine._MERGE_SPILL_FRACTION)
    assert 0 < engine._MERGE_SPILL_FRACTION < 1


def test_duckdb_and_merge_shares_sum_under_one():
    """DuckDB and the merge pool can peak together in one cgroup, so their budgets must leave
    headroom — overcommitting (>= 1.0) would just relocate the OOM."""
    assert 0 < engine._DUCKDB_MEM_FRACTION < 1
    assert engine._DUCKDB_MEM_FRACTION + engine._MERGE_SPILL_FRACTION < 1.0


# ----------------------------------------------------------- byte-size parsing

def test_parse_byte_size_units():
    assert engine._parse_byte_size("25.0 GiB") == int(25.0 * 2 ** 30)
    assert engine._parse_byte_size("512 MiB") == 512 * 2 ** 20
    assert engine._parse_byte_size("10GB") == 10 * 10 ** 9
    assert engine._parse_byte_size("1073741824B") == 1073741824
    assert engine._parse_byte_size("0 bytes") == 0


def test_parse_byte_size_rejects_garbage():
    for bad in (None, "", "garbage", "GiB"):
        assert engine._parse_byte_size(bad) is None


# ------------------------------------------------- DuckDB cgroup-aware memory

class _FakeCon:
    """Minimal DuckDB stand-in: records SETs, answers current_setting()."""

    def __init__(self, settings):
        self._settings = dict(settings)
        self.sets = []

    def execute(self, sql):
        m = re.match(r"\s*SELECT current_setting\('([^']+)'\)", sql)
        if m:
            val = self._settings.get(m.group(1))
            return _FakeResult(val)
        m = re.match(r"\s*SET (\w+)='([^']*)'", sql)
        if m:
            self.sets.append((m.group(1), m.group(2)))
            self._settings[m.group(1)] = m.group(2)
            return _FakeResult(None)
        return _FakeResult(None)


class _FakeResult:
    def __init__(self, val):
        self._val = val

    def fetchone(self):
        return (self._val,)


def test_configure_duckdb_memory_tightens_host_default(monkeypatch):
    """DuckDB's host-RAM default (way above the cgroup cap) gets pulled down to our share."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.configure_duckdb_memory(con)
    target = int(16 * 2 ** 30 * engine._DUCKDB_MEM_FRACTION)
    assert ("memory_limit", f"{target}B") in con.sets


def test_configure_duckdb_memory_preserves_lower_profile_limit(monkeypatch):
    """An explicit, smaller memory_limit from the profile must not be loosened."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "2.0 GiB", "temp_directory": ".tmp"})
    engine.configure_duckdb_memory(con)
    assert not any(k == "memory_limit" for k, _ in con.sets)


def test_configure_duckdb_memory_sets_temp_dir_when_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    monkeypatch.chdir(tmp_path)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ""})
    engine.configure_duckdb_memory(con)
    assert any(k == "temp_directory" for k, _ in con.sets)


def test_configure_duckdb_memory_noop_when_limit_unknown(monkeypatch):
    """No cgroup/physical signal: leave DuckDB's own default alone."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: None)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.configure_duckdb_memory(con)
    assert not any(k == "memory_limit" for k, _ in con.sets)


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


def test_max_spill_size_defaults_to_effective_fraction(monkeypatch):
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
    """The real delta_rs merge path still produces a correct upsert with the spill default on."""
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
