"""Copy-on-write MERGE / UPDATE / DELETE: DuckDB rewrites the touched files and writes the parquet,
delta-rs commits only the add + remove actions. Shapes the commit cannot express stay on delta-rs."""
import deltalake
import pytest

import duckrun
from dbt.adapters.duckrun import engine


@pytest.fixture
def paths(monkeypatch):
    """Every engine log line, to tell which path ran."""
    lines = []
    monkeypatch.setattr(engine.logger, "info", lambda msg, *a, **k: lines.append(str(msg)))
    return lines


def _conn(tmp_path):
    return duckrun.connect(str(tmp_path / "wh"), read_only=False)


def _writers(conn, table):
    files = engine._delta_table(f"{conn.root_path}/dbo/{table}", None).file_uris()
    lit = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    return {r[0] for r in conn.sql(f"select created_by from parquet_file_metadata([{lit}])").fetchall()}


def test_merge_is_copy_on_write(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' || i as v from range(1000) r(i)")
    conn.sql("merge into t using (select 5 as id, 'five' as v union all select 2000, 'new') s "
             "on t.id = s.id when matched then update set v = s.v "
             "when not matched then insert (id, v) values (s.id, s.v)")
    assert any("merge: copy-on-write" in m for m in paths)
    assert conn.sql("select count(*), max(id) from t").fetchall() == [(1001, 2000)]
    assert conn.sql("select v from t where id = 5").fetchall() == [("five",)]
    assert all(w.startswith("DuckDB") for w in _writers(conn, "t"))


def test_update_and_delete_are_copy_on_write(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, i % 3 as g from range(300) r(i)")
    conn.sql("update t set g = -1 where id < 10")
    conn.sql("delete from t where g = 2")
    assert any("update: copy-on-write" in m for m in paths)
    assert any("delete: copy-on-write" in m for m in paths)
    assert conn.sql("select count(*) from t where g = -1").fetchall() == [(10,)]
    assert conn.sql("select count(*) from t where g = 2").fetchall() == [(0,)]


def test_partitioned_table(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t partitioned by (p) as "
             "select i as id, i % 3 as p, 'x' as v from range(90) r(i)")
    conn.sql("merge into t using (select 1 as id, 1 as p, 'y' as v union all select 500, 2, 'z') s "
             "on t.id = s.id when matched then update set v = s.v "
             "when not matched then insert (id, p, v) values (s.id, s.p, s.v)")
    conn.sql("update t set v = 'u' where p = 0")
    conn.sql("delete from t where id = 3")
    assert not any("delta_rs path" in m for m in paths)
    fresh = duckrun.connect(str(tmp_path / "wh"))
    assert fresh.sql("select count(*) from t").fetchall() == [(90,)]
    assert fresh.sql("select v from t where id = 1").fetchall() == [("y",)]
    assert fresh.sql("select p, v from t where id = 500").fetchall() == [(2, "z")]
    assert fresh.sql("select count(*) from t where p = 0 and v <> 'u'").fetchall() == [(0,)]
    dt = deltalake.DeltaTable(f"{conn.root_path}/dbo/t")
    assert dt.metadata().partition_columns == ["p"]
    assert all("p=" in f.replace("\\", "/") for f in dt.file_uris())


def test_change_data_feed_table_stays_on_delta_rs(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(10) r(i)")
    deltalake.DeltaTable(f"{conn.root_path}/dbo/t").alter.set_table_properties(
        {"delta.enableChangeDataFeed": "true"})
    conn.sql("update t set v = 'w' where id = 1")
    assert any("delta_rs path (change data feed)" in m for m in paths)
    assert conn.sql("select v from t where id = 1").fetchall() == [("w",)]


def test_concurrent_commit_fails_loudly(tmp_path, monkeypatch):
    from deltalake.exceptions import CommitFailedError

    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(10) r(i)")
    path = f"{conn.root_path}/dbo/t"
    real = engine.cow.commit

    def race(dt, p, actions, **kw):
        conn2 = duckrun.connect(str(tmp_path / "wh"), read_only=False)
        conn2.sql("insert into t values (99, 'other')")        # lands first
        return real(dt, p, actions, **kw)

    monkeypatch.setattr(engine.cow, "commit", race)
    v0 = engine.table_version(path)
    with pytest.raises(CommitFailedError):
        conn.sql("update t set v = 'mine' where id = 1")
    assert engine.table_version(path) == v0 + 1                # only the other writer's commit


def test_column_mapped_table(tmp_path, paths):
    # A name-mapped table (as Spark/Fabric write them): the rewrite lands under the physical names.
    import os
    conn = _conn(tmp_path)
    path = f"{conn.root_path}/dbo/cm"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn.con.execute("LOAD delta")                  # COPY ... (FORMAT delta) does not autoload it
    conn.con.execute(
        f"""COPY (SELECT i AS id, 'v' AS "Super Name" FROM range(20) r(i)) TO '{path}' """
        "(FORMAT delta, WITH_PROPERTIES {'delta.columnMapping.mode': 'name'})")
    conn.refresh(quiet=True)
    conn.sql("""update cm set "Super Name" = 'w' where id < 5""")
    assert any("update: copy-on-write" in m for m in paths)
    fresh = duckrun.connect(str(tmp_path / "wh"))
    assert fresh.sql("""select count(*) from cm where "Super Name" = 'w'""").fetchall() == [(5,)]
    assert fresh.sql("select count(*) from cm").fetchall() == [(20,)]
    dt = deltalake.DeltaTable(path)
    lit = ", ".join("'" + f.replace("'", "''") + "'" for f in dt.file_uris())
    names = {r[0] for r in conn.sql(f"select name from parquet_schema([{lit}])").fetchall()}
    assert any(n.startswith("col-") for n in names) and "Super Name" not in names


def test_partitioned_column_mapped_table_stays_on_delta_rs(tmp_path, paths):
    import shutil
    from pathlib import Path
    conn = _conn(tmp_path)
    fixture = Path(__file__).parents[1] / "connection_api" / "data" / "table_with_column_mapping"
    shutil.copytree(fixture, Path(conn.root_path) / "dbo" / "cmp")
    conn.refresh(quiet=True)
    conn.sql("""update cmp set "Super Name" = 'x' where "Super Name" is not null""")
    assert any("delta_rs path (column mapping on a partitioned table)" in m for m in paths)


def test_check_constraint_is_enforced(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id from range(10) r(i)")
    path = f"{conn.root_path}/dbo/t"
    deltalake.DeltaTable(path).alter.add_constraint({"non_negative": "id >= 0"})
    v = engine.table_version(path)
    with pytest.raises(ValueError, match="non_negative"):
        conn.sql("update t set id = -1 where id = 3")
    assert engine.table_version(path) == v                    # refused before anything was written
    conn.sql("update t set id = 100 where id = 3")
    assert any("update: copy-on-write" in m for m in paths)
    assert conn.sql("select count(*) from t where id = 100").fetchall() == [(1,)]


def _live(conn, table):
    return set(engine._delta_table(f"{conn.root_path}/dbo/{table}", None).file_uris())


def test_unchanged_rows_touch_no_file(tmp_path, paths):
    # Rows a MERGE / UPDATE matches but leaves identical need no rewrite: an idempotent re-merge
    # or a no-op UPDATE commits nothing, and a partial change rewrites only the file it lands in.
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(100) r(i)")
    conn.sql("insert into t select i as id, 'v' as v from range(100, 200) r(i)")
    path = f"{conn.root_path}/dbo/t"
    before, v = _live(conn, "t"), engine.table_version(path)
    assert len(before) == 2
    merge = ("merge into t using (select i as id, 'v' as v from range(200) r(i)) s on t.id = s.id "
             "when matched then update set v = s.v when not matched then insert (id, v) values (s.id, s.v)")
    conn.sql(merge)
    conn.sql("update t set v = 'v' where id < 150")
    assert engine.table_version(path) == v and _live(conn, "t") == before
    conn.sql("merge into t using (select 5 as id, 'five' as v union all select 150, 'v') s "
             "on t.id = s.id when matched then update set v = s.v")
    after = _live(conn, "t")
    assert engine.table_version(path) == v + 1
    assert len(after & before) == 1                   # the file holding id 150 was not rewritten
    assert conn.sql("select count(*), count(*) filter (v = 'five') from t").fetchall() == [(200, 1)]


def test_unchanged_match_in_untouched_file_is_not_inserted(tmp_path, paths):
    # A source row matching a row in a file that is NOT rewritten must still count as matched.
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(100) r(i)")
    conn.sql("insert into t select i as id, 'v' as v from range(100, 200) r(i)")
    conn.sql("merge into t using (select 5 as id, 'five' as v union all select 150, 'v' "
             "union all select 999, 'new') s on t.id = s.id "
             "when matched then update set v = s.v when not matched then insert (id, v) values (s.id, s.v)")
    assert conn.sql("select count(*), count(distinct id) from t").fetchall() == [(201, 201)]
    conn.sql("merge into t using (select 7 as id, 'x' as v) s on t.id = s.id "
             "when matched then update set v = s.v when not matched by source and t.id >= 100 then delete")
    assert conn.sql("select count(*) from t").fetchall() == [(100,)]
    assert conn.sql("select v from t where id = 7").fetchall() == [("x",)]
