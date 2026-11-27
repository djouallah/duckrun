"""Copy-on-write MERGE / UPDATE / DELETE: DuckDB rewrites the touched files and writes the parquet,
delta-rs commits only the add + remove actions. Shapes the commit cannot express are refused."""
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


def test_change_data_feed_table_refuses_rewrites_and_takes_appends(tmp_path, paths):
    # delta-rs's commit API has no action for change files, so a rewrite of a CDF table is refused
    # before anything is written; an append (adds only) still works.
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(10) r(i)")
    path = f"{conn.root_path}/dbo/t"
    deltalake.DeltaTable(path).alter.set_table_properties({"delta.enableChangeDataFeed": "true"})
    v = engine.table_version(path)
    for stmt in ("update t set v = 'w' where id = 1", "delete from t where id = 1",
                 "merge into t using (select 1 as id, 'm' as v) s on t.id = s.id "
                 "when matched then update set v = s.v"):
        with pytest.raises(ValueError, match="change data feed"):
            conn.sql(stmt)
    assert engine.table_version(path) == v
    conn.sql("insert into t values (100, 'n')")
    assert conn.sql("select count(*) from t").fetchall() == [(11,)]


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


def test_partitioned_column_mapped_table_is_refused(tmp_path, paths):
    import shutil
    from pathlib import Path
    conn = _conn(tmp_path)
    fixture = Path(__file__).parents[1] / "connection_api" / "data" / "table_with_column_mapping"
    shutil.copytree(fixture, Path(conn.root_path) / "dbo" / "cmp")
    conn.refresh(quiet=True)
    with pytest.raises(ValueError, match="column mapping on a partitioned table"):
        conn.sql("""update cmp set "Super Name" = 'x' where "Super Name" is not null""")


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


def test_table_with_a_filename_column(tmp_path, paths):
    # A file-ingest table often has its own `filename` column; the scan's file column must not
    # collide with it (`delta_scan(..., filename = true)` refuses to bind on such a table).
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'f' || i as filename from range(20) r(i)")
    conn.sql("update t set filename = 'x' where id < 5")
    conn.sql("delete from t where id >= 15")
    conn.sql("merge into t using (select 1 as id, 'm' as filename union all select 99, 'n') s "
             "on t.id = s.id when matched then update set filename = s.filename "
             "when not matched then insert (id, filename) values (s.id, s.filename)")
    assert not any("delta_rs path" in m for m in paths)
    assert conn.sql("select count(*), count(*) filter (filename = 'x'), "
                    "max(filename) filter (id = 1), max(filename) filter (id = 99) from t"
                    ).fetchall() == [(16, 4, "m", "n")]


def _descents(conn, table):
    """Adjacent pairs, in each live file's physical row order, where id goes down."""
    files = engine._delta_table(f"{conn.root_path}/dbo/{table}", None).file_uris()
    lit = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    return conn.sql(
        f"select count(*) from (select id, lag(id) over (partition by filename "
        f"order by file_row_number) as prev from read_parquet([{lit}], filename = true)) "
        f"where id < prev").fetchall()[0][0]


def test_rewrite_keeps_the_row_order(tmp_path, paths):
    # The session runs with preserve_insertion_order=false: a rewrite without a sort key keeps each
    # rewritten row's physical position, so a sorted table stays sorted within each old file's run.
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(0, 300000) r(i) order by id")
    for lo in (300000, 600000, 900000):
        conn.sql(f"insert into t select i as id, 'v' as v from range({lo}, {lo + 300000}) r(i) "
                 "order by id")
    before = _descents(conn, "t")
    conn.sql("update t set v = 'w' where id % 1000 = 0")
    conn.sql("delete from t where id % 999 = 0")
    assert any("update: copy-on-write" in m for m in paths)
    assert _descents(conn, "t") <= before + 4       # at most one per old file joined
    assert conn.sql("select count(*) filter (v = 'w') from t").fetchall()[0][0] > 0


def test_delete_ignores_a_rule_duckdb_cannot_bind(tmp_path, paths):
    # A DELETE writes only rows already in the table, so a CHECK in DataFusion-only SQL does not
    # stop it; an UPDATE is refused (its new values must be checked, and DuckDB cannot).
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id from range(10) r(i)")
    path = f"{conn.root_path}/dbo/t"
    deltalake.DeltaTable(path).alter.add_constraint({"df_only": "arrow_typeof(id) = 'Int64'"})
    conn.sql("delete from t where id = 3")
    assert any("delete: copy-on-write" in m for m in paths)
    assert conn.sql("select count(*) from t").fetchall() == [(9,)]
    with pytest.raises(ValueError, match="DuckDB cannot evaluate a table rule"):
        conn.sql("update t set id = 30 where id = 4")


def test_merge_inserts_stream_to_their_own_files(tmp_path, paths):
    # A batch that matches nothing rewrites no file: the existing files stay live and the inserts
    # land in new ones, each row through the first NOT MATCHED clause whose predicate holds.
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v, i as n from range(100) r(i)")
    before = _live(conn, "t")
    conn.sql("merge into t using (select i as id, 'w' as v, i as n from range(1000, 1010) r(i)) s "
             "on t.id = s.id when matched then update set v = s.v "
             "when not matched and s.id < 1003 then insert (id, v) values (s.id, 'low') "
             "when not matched and s.id < 1008 then insert * ")
    assert any("merge: inserts streamed to new files (0 file(s) rewritten)" in m for m in paths)
    assert before <= _live(conn, "t")
    assert conn.sql("select count(*) from t").fetchall() == [(108,)]
    assert conn.sql("select count(*) from t where v = 'low' and n is null").fetchall() == [(3,)]
    assert conn.sql("select count(*) from t where v = 'w' and n = id").fetchall() == [(5,)]


def test_merge_update_and_streamed_insert(tmp_path, paths):
    conn = _conn(tmp_path)
    conn.sql("create table t as select i as id, 'v' as v from range(100) r(i)")
    conn.sql("merge into t using (select i as id, 'w' as v from range(90, 110) r(i)) s "
             "on t.id = s.id when matched then update set v = s.v "
             "when not matched then insert *")
    assert any("merge: inserts streamed to new files (1 file(s) rewritten)" in m for m in paths)
    assert conn.sql("select count(*), count(*) filter (v = 'w') from t").fetchall() == [(110, 20)]
    fresh = duckrun.connect(str(tmp_path / "wh"))
    assert fresh.sql("select count(distinct id) from t").fetchall() == [(110,)]


def test_merge_streamed_insert_keeps_not_null(tmp_path, paths):
    import pyarrow as pa
    conn = _conn(tmp_path)
    path = f"{conn.root_path}/dbo/nn"
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("v", pa.string())])
    deltalake.write_deltalake(path, pa.table({"id": [1, 2], "v": ["a", "b"]}, schema=schema))
    conn.refresh(quiet=True)
    v = engine.table_version(path)
    with pytest.raises(Exception, match="NOT NULL"):
        conn.sql("merge into nn using (select null::bigint as id, 'x' as v) s on nn.id = s.id "
                 "when matched then update set v = s.v when not matched then insert *")
    assert engine.table_version(path) == v
