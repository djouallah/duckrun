"""merge_delta memory spill: cross-platform RAM detection and the max_spill_size knob.

delta_rs only builds a disk-spilling merge session when ``max_spill_size`` is set; otherwise
the merge runs under an unbounded memory pool and can OOM on large upserts. duckrun defaults
the cap to a fraction of the *effective* memory limit (min of physical RAM, the cgroup cap, and
currently-available RAM) and lets a model override it via ``merge_max_spill_size``. DuckDB itself
— the merge source producer in the same process — is pinned to its own share via
``set_merge_memory_limit`` on the merge path only (overwrite/append leave it to DuckDB via
``configure_duckdb_session`` + ``restore_memory_limit``). These tests pin that wiring down.
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


def test_available_ram_bytes_is_plausible():
    avail = engine._available_ram_bytes()
    # Some RAM is always free on a live box; and it can't exceed physical RAM.
    assert avail is not None
    assert 0 < avail <= engine._total_ram_bytes()


def test_effective_limit_is_min_of_all_signals(monkeypatch):
    # Pin the leaves so the assertion isn't racing two live samples of fluctuating free RAM.
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: 16 * 2 ** 30)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: None)
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: 9 * 2 ** 30)
    assert engine._effective_mem_limit_bytes() == 9 * 2 ** 30


def test_effective_limit_recomputed_fresh_every_call(monkeypatch):
    """The fix for the anchoring bug: the limit is sampled per call, NOT frozen at startup. As
    free RAM drops between jobs (earlier models / a background DuckDB job took it), the cap must
    drop with it — no stale connection-time snapshot."""
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: 32 * 2 ** 30)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: None)
    seq = iter([20 * 2 ** 30, 8 * 2 ** 30])  # free RAM at job 1, then less at job 2
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: next(seq))
    assert engine._effective_mem_limit_bytes() == 20 * 2 ** 30
    assert engine._effective_mem_limit_bytes() == 8 * 2 ** 30


def test_effective_limit_folds_in_available(monkeypatch):
    """Fabric case: cgroup is the unlimited root, physical RAM is the whole node, but most of it
    is already in use — available RAM must be what bounds the budget."""
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: 16 * 2 ** 30)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: None)
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: 6 * 2 ** 30)
    assert engine._effective_mem_limit_bytes() == 6 * 2 ** 30
    assert engine._effective_mem_limit_source() == "available RAM"


def test_default_merge_spill_size_is_fraction_of_effective_limit(monkeypatch):
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    assert engine._default_merge_spill_size() == int(16 * 2 ** 30 * engine._MERGE_SPILL_FRACTION)
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
        m = re.match(r"\s*SET (\w+)\s*=\s*'?([^']*?)'?\s*$", sql)
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


def test_configure_duckdb_session_disables_preserve_insertion_order():
    """duckrun turns preserve_insertion_order off by default so large writes/merges (which
    stream the whole result into delta_rs) don't make DuckDB buffer everything and OOM."""
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.configure_duckdb_session(con)
    assert ("preserve_insertion_order", "false") in con.sets


def test_configure_duckdb_session_sets_temp_dir_when_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ""})
    engine.configure_duckdb_session(con)
    assert any(k == "temp_directory" for k, _ in con.sets)


def test_configure_duckdb_session_leaves_memory_limit_alone():
    """The session setup must NOT touch memory_limit — overwrite/append let DuckDB manage its
    own memory; only the merge path applies the split."""
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.configure_duckdb_session(con)
    assert not any(k == "memory_limit" for k, _ in con.sets)


def test_set_merge_memory_limit_tightens_host_default(monkeypatch):
    """Before a merge, DuckDB's host-RAM default gets pulled down to its split share."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.set_merge_memory_limit(con)
    target = int(16 * 2 ** 30 * engine._DUCKDB_MEM_FRACTION)
    assert ("memory_limit", f"{target}B") in con.sets


def test_set_merge_memory_limit_preserves_lower_profile_limit(monkeypatch):
    """An explicit, smaller memory_limit from the profile must not be loosened, even for merge."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "2.0 GiB", "temp_directory": ".tmp"})
    engine.set_merge_memory_limit(con)
    assert not any(k == "memory_limit" for k, _ in con.sets)


def test_set_merge_memory_limit_noop_when_limit_unknown(monkeypatch):
    """No cgroup/physical/available signal: leave DuckDB's own default alone."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: None)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.set_merge_memory_limit(con)
    assert not any(k == "memory_limit" for k, _ in con.sets)


def test_restore_memory_limit_sets_baseline():
    """The write path restores DuckDB's baseline limit (undoing any prior merge tighten)."""
    con = _FakeCon({"memory_limit": "1000000000B", "temp_directory": ".tmp"})
    engine.restore_memory_limit(con, "8.0 GiB")
    assert ("memory_limit", "8.0 GiB") in con.sets


def test_restore_memory_limit_noop_when_baseline_unknown():
    con = _FakeCon({"memory_limit": "1000000000B", "temp_directory": ".tmp"})
    engine.restore_memory_limit(con, None)
    assert not any(k == "memory_limit" for k, _ in con.sets)


def test_set_write_memory_limit_clamps_host_default(monkeypatch):
    """The write path bounds DuckDB's host-physical-RAM baseline to the write share, so the
    default 80%-of-node-RAM can't OOM-kill a container (the Fabric bug)."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "100.0 GiB", "temp_directory": ".tmp"})
    engine.set_write_memory_limit(con, "100.0 GiB")
    target = int(16 * 2 ** 30 * engine._WRITE_MEM_FRACTION)
    assert ("memory_limit", f"{target}B") in con.sets


def test_set_write_memory_limit_respects_lower_baseline(monkeypatch):
    """An explicit, smaller profile limit is kept — we clamp DOWN, never loosen above baseline."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    con = _FakeCon({"memory_limit": "2.0 GiB", "temp_directory": ".tmp"})
    engine.set_write_memory_limit(con, "2.0 GiB")
    assert ("memory_limit", f"{2 * 2 ** 30}B") in con.sets


def test_set_write_memory_limit_loosens_from_prior_merge_tighten(monkeypatch):
    """Set absolutely from the baseline (not tighten-only): a prior merge left memory_limit at its
    0.3 share, and the next write must loosen it back up to its larger 0.8 write share."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    merge_share = f"{int(16 * 2 ** 30 * engine._DUCKDB_MEM_FRACTION)}B"
    con = _FakeCon({"memory_limit": merge_share, "temp_directory": ".tmp"})
    engine.set_write_memory_limit(con, "100.0 GiB")
    target = int(16 * 2 ** 30 * engine._WRITE_MEM_FRACTION)
    assert ("memory_limit", f"{target}B") in con.sets
    assert target > int(16 * 2 ** 30 * engine._DUCKDB_MEM_FRACTION)


def test_set_write_memory_limit_keeps_baseline_when_limit_unknown(monkeypatch):
    """No cgroup/physical/available signal: keep the baseline (as its byte equivalent), don't
    invent a floor."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: None)
    con = _FakeCon({"memory_limit": "1000000000B", "temp_directory": ".tmp"})
    engine.set_write_memory_limit(con, "8.0 GiB")
    assert ("memory_limit", f"{8 * 2 ** 30}B") in con.sets


def test_set_write_memory_limit_noop_when_nothing_known(monkeypatch):
    """Effective limit unknown AND baseline unparseable/missing: leave memory_limit untouched."""
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: None)
    con = _FakeCon({"memory_limit": "1000000000B", "temp_directory": ".tmp"})
    engine.set_write_memory_limit(con, None)
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

    def load_as_version(self, version):
        # merge_delta now always pins (read_version is required); the fake just accepts it.
        self._captured["read_version"] = version

    def merge(self, **kwargs):
        self._captured.clear()
        self._captured.update(kwargs)
        return _FakeMerger()

    def file_uris(self):
        # Below the default compaction_threshold, so the post-merge maintenance block is
        # skipped — these tests only care about the kwargs forwarded into .merge().
        return []


def _spy(monkeypatch):
    captured = {}
    monkeypatch.setattr(engine, "_delta_table", lambda path, so: _FakeDeltaTable(captured))
    return captured


def test_max_spill_size_defaults_to_effective_fraction(monkeypatch):
    # Pin the limit so the forwarded cap can be compared exactly (free RAM is now sampled live
    # on every call, so two unpinned reads would differ by a few KB and flake).
    monkeypatch.setattr(engine, "_effective_mem_limit_bytes", lambda: 16 * 2 ** 30)
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", read_version=0)
    assert captured["max_spill_size"] == engine._default_merge_spill_size()


def test_max_spill_size_explicit_is_forwarded(monkeypatch):
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", max_spill_size=123_456, read_version=0)
    assert captured["max_spill_size"] == 123_456


def test_streamed_exec_defaults_to_false(monkeypatch):
    """Default to collecting the source so delta_rs can derive an early prune predicate from its
    stats (streamed_exec=True would stream it and scan the whole target)."""
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", read_version=0)
    assert captured["streamed_exec"] is False


def test_streamed_exec_can_be_enabled(monkeypatch):
    """A huge-source merge can opt back into streaming (no prune) so the source isn't collected."""
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", streamed_exec=True, read_version=0)
    assert captured["streamed_exec"] is True


def test_max_spill_size_zero_disables_the_cap(monkeypatch):
    """0 (or any falsy non-None) opts out: the kwarg is omitted so delta_rs runs unbounded."""
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", max_spill_size=0, read_version=0)
    assert "max_spill_size" not in captured


def test_undetectable_ram_omits_the_cap(monkeypatch):
    # No memory signal at all (physical, cgroup, available all undetectable) -> run unbounded.
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: None)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: None)
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: None)
    captured = _spy(monkeypatch)
    engine.merge_delta("target", _table([1]), "id", read_version=0)
    assert "max_spill_size" not in captured


# ---------------------------------------------------------------- end to end

def test_merge_with_default_spill_upserts_correctly(tmp_path):
    """The real delta_rs merge path still produces a correct upsert with the spill default on."""
    path = str(tmp_path / "t")
    engine.write_delta(path, _table([1, 2, 3]), "overwrite")  # v0
    # update id=3, insert id=4
    engine.merge_delta(path, pa.table({"id": pa.array([3, 4], pa.int64()),
                                       "value": pa.array([999, 40], pa.int64())}), "id", read_version=0)
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
        read_version=0,
    )
    assert _rows(path) == {1: 10, 2: 222, 3: 30}


# ----------------------------------------------- post-merge maintenance

def _commit_ops(path):
    """The operation name of each commit in the table's history, newest first."""
    return [c.get("operation") for c in DeltaTable(path).history()]


def test_merge_compacts_and_vacuums_when_over_threshold(tmp_path):
    """Past compaction_threshold, the merge path must compact + vacuum + cleanup like append —
    so a merged-on-every-run table doesn't grow small files and tombstoned versions forever."""
    path = str(tmp_path / "t")
    engine.write_delta(path, _table([1, 2, 3]), "overwrite")
    # Each merge adds a file; threshold=1 makes maintenance fire on this merge.
    engine.merge_delta(
        path,
        pa.table({"id": pa.array([4], pa.int64()), "value": pa.array([40], pa.int64())}),
        "id",
        compaction_threshold=1,
        read_version=0,
    )
    # Data is still correct, and an OPTIMIZE commit was added (compaction ran).
    assert _rows(path) == {1: 10, 2: 20, 3: 30, 4: 40}
    assert any(op and "OPTIMIZE" in op.upper() for op in _commit_ops(path))


def test_merge_skips_maintenance_under_threshold(tmp_path):
    """A high threshold leaves the merge untouched — maintenance is gated, not unconditional."""
    path = str(tmp_path / "t")
    engine.write_delta(path, _table([1, 2, 3]), "overwrite")
    engine.merge_delta(
        path,
        pa.table({"id": pa.array([4], pa.int64()), "value": pa.array([40], pa.int64())}),
        "id",
        compaction_threshold=1000,
        read_version=0,
    )
    assert _rows(path) == {1: 10, 2: 20, 3: 30, 4: 40}
    assert not any(op and "OPTIMIZE" in op.upper() for op in _commit_ops(path))
