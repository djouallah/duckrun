"""The Delta verbs conn.sql() runs that DuckDB or delta-rs already support: DROP TABLE deleting the
table, TRUNCATE, time travel in DuckDB's AT (VERSION / TIMESTAMP => …) syntax, table properties
(ALTER TABLE … SET TBLPROPERTIES and CREATE TABLE … WITH (…)), OPTIMIZE [ZORDER BY], VACUUM …
RETAIN n HOURS, CREATE TABLE … PARTITIONED BY with a generated column, and the read-only gate over
every one of them. Each claim is checked against the Delta log or the storage, not only the result."""
import os
import time
from datetime import datetime, timezone

import deltalake
import pyarrow as pa
import pytest

import duckrun
from dbt.adapters.duckrun.delta_dml import TOMBSTONE_COLUMN


def _rw(wh):
    return duckrun.connect(wh, schema="dbo", read_only=False)


@pytest.fixture
def wh(tmp_path):
    root = str(tmp_path / "wh")
    c = _rw(root)
    c.sql("create table t as select i as id, i * 10 as v from range(5) r(i)")
    return root


def _path(conn, table):
    return conn._table_path("dbo", table)


def _last_commit(path):
    return list(deltalake.DeltaTable(path).history(1))[0]


# -- DROP TABLE ----------------------------------------------------------------------------------
def test_drop_table_deletes_the_folder(wh):
    c = _rw(wh)
    path = _path(c, "t")
    c.sql("drop table t")
    assert not os.path.exists(path)
    assert "t" not in c._cat_tables()
    with pytest.raises(Exception):
        c.sql("select * from t").fetchall()
    c.sql("drop table if exists t")                      # already gone: IF EXISTS is a no-op
    with pytest.raises(Exception):
        c.sql("drop table t")                            # plain DROP of a missing table raises
    c.sql("create table t as select 1 as id")            # the name is free again, fresh history
    assert deltalake.DeltaTable(path).version() == 0


def test_a_tombstone_from_an_older_duckrun_stays_hidden(tmp_path):
    root = str(tmp_path / "wh")
    _rw(root).sql("create table keep as select 1 as id")
    old = os.path.join(root, "dbo", "old")
    deltalake.write_deltalake(old, pa.table({TOMBSTONE_COLUMN: [True]}))
    assert "old" not in duckrun.connect(root, schema="dbo")._cat_tables()


# -- TRUNCATE ------------------------------------------------------------------------------------
@pytest.mark.parametrize("stmt", ["truncate t", "truncate table t"])
def test_truncate_removes_every_file_and_writes_none(wh, stmt):
    c = _rw(wh)
    path = _path(c, "t")
    v0 = deltalake.DeltaTable(path).version()
    c.sql(stmt)
    dt = deltalake.DeltaTable(path)
    assert dt.version() == v0 + 1
    assert dt.file_uris() == []
    assert [f.name for f in dt.schema().fields] == ["id", "v"]
    assert c.sql("select count(*) from t").fetchall() == [(0,)]
    assert c.sql("select count(*) from t at (version => 0)").fetchall() == [(5,)]


# -- time travel ---------------------------------------------------------------------------------
def test_time_travel_by_version(wh):
    c = _rw(wh)
    c.sql("insert into t values (100, 1000)")
    assert c.sql("select count(*) from t at (version => 0)").fetchall() == [(5,)]
    assert c.sql("select count(*) from t AT (VERSION => 1 + 0)").fetchall() == [(6,)]
    assert c.sql("select count(*) from dbo.t at (version => 0) x where x.id < 2").fetchall() == [(2,)]
    assert c.sql("select t.id from t at (version => 1) where t.id = 100").fetchall() == [(100,)]
    joined = c.sql("select count(*) from t at (version => 1) a "
                   "join t at (version => 0) b on a.id = b.id").fetchall()
    assert joined == [(5,)]
    c.sql("create table t0 as select * from t at (version => 0)")
    assert c.sql("select count(*) from t0").fetchall() == [(5,)]


def test_time_travel_by_timestamp(wh):
    c = _rw(wh)
    time.sleep(1.1)
    at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    time.sleep(1.1)
    c.sql("insert into t values (100, 1000)")
    found = c.sql(f"select count(*) from t at (timestamp => timestamptz '{at}+00')").fetchall()
    assert found == [(5,)]
    assert c.sql("select count(*) from t").fetchall() == [(6,)]


def test_at_on_a_name_that_is_not_a_duckrun_table_is_left_to_duckdb(wh):
    c = _rw(wh)
    with pytest.raises(Exception):
        c.sql("select * from nope at (version => 0)").fetchall()


# -- table properties ----------------------------------------------------------------------------
def test_set_tblproperties(wh):
    c = _rw(wh)
    c.sql("alter table t set tblproperties ('delta.logRetentionDuration' = 'interval 9 days', "
          "'delta.deletedFileRetentionDuration' = 'interval 3 days')")
    conf = deltalake.DeltaTable(_path(c, "t")).metadata().configuration
    assert conf["delta.logRetentionDuration"] == "interval 9 days"
    assert conf["delta.deletedFileRetentionDuration"] == "interval 3 days"


def test_property_list_parsing():
    from dbt.adapters.duckrun.delta_dml import _parse_properties
    assert _parse_properties("'a' = 'it''s', 'b.c'='2'") == {"a": "it's", "b.c": "2"}
    with pytest.raises(ValueError, match="'key' = 'value'"):
        _parse_properties("a = 1")


def test_create_table_with_properties_partition_and_generated_column(tmp_path):
    c = _rw(str(tmp_path / "wh"))
    c.sql("create table g (id bigint, ts timestamp, d date generated always as (cast(ts as date))) "
          "partitioned by (d) with ('delta.enableChangeDataFeed' = 'true')")
    path = _path(c, "g")
    dt = deltalake.DeltaTable(path)
    assert dt.metadata().partition_columns == ["d"]
    assert dt.metadata().configuration["delta.enableChangeDataFeed"] == "true"
    field = {f.name: f for f in dt.schema().fields}["d"]
    assert "cast(ts as date)" in field.metadata["delta.generationExpression"].lower()
    c.sql("insert into g (id, ts) values (1, timestamp '2026-01-02 03:04:05')")
    assert [e for e in os.listdir(path) if e.startswith("d=")] == ["d=2026-01-02"]
    assert c.sql("select id, cast(d as varchar) from g").fetchall() == [(1, "2026-01-02")]


def test_create_table_tblproperties_spelling(tmp_path):
    c = _rw(str(tmp_path / "wh"))
    c.sql("create table p (id bigint) tblproperties ('delta.appendOnly' = 'true')")
    assert deltalake.DeltaTable(_path(c, "p")).metadata().configuration["delta.appendOnly"] == "true"


# -- OPTIMIZE / VACUUM ---------------------------------------------------------------------------
def test_optimize_compacts(wh):
    c = _rw(wh)
    for i in range(3):
        c.sql(f"insert into t values ({100 + i}, 0)")
    before = len(deltalake.DeltaTable(_path(c, "t")).file_uris())
    c.sql("optimize t")
    assert len(deltalake.DeltaTable(_path(c, "t")).file_uris()) < before
    assert _last_commit(_path(c, "t"))["operation"] == "OPTIMIZE"
    assert c.sql("select count(*) from t").fetchall() == [(8,)]


def test_optimize_zorder(wh):
    c = _rw(wh)
    c.sql("insert into t values (100, 0)")
    c.sql("optimize t zorder by (id, v)")
    commit = _last_commit(_path(c, "t"))
    assert commit["operation"] == "OPTIMIZE"
    assert "id" in str(commit["operationParameters"].get("zOrderBy"))
    with pytest.raises(ValueError, match="unknown column"):
        c.sql("optimize t zorder by (nope)")


def test_vacuum_retain_0_hours_deletes_the_removed_files(wh):
    c = _rw(wh)
    path = _path(c, "t")
    old = deltalake.DeltaTable(path).file_uris()
    c.sql("create or replace table t as select 7 as id, 70 as v")
    assert all(os.path.exists(f.replace("file://", "")) for f in old)
    c.sql("vacuum t retain 0 hours")
    assert not any(os.path.exists(f.replace("file://", "")) for f in old)
    assert c.sql("select * from t").fetchall() == [(7, 70)]


# -- the read-only gate --------------------------------------------------------------------------
@pytest.mark.parametrize("stmt", [
    "drop table t",
    "truncate t",
    "alter table t add column w integer",
    "alter table t set tblproperties ('a' = 'b')",
    "optimize t",
    "vacuum t retain 0 hours",
    "with s as (select 9 as id, 90 as v) insert into t select * from s",
])
def test_read_only_refuses_every_write(wh, stmt):
    ro = duckrun.connect(wh, schema="dbo")
    path = _path(ro, "t")
    v0 = deltalake.DeltaTable(path).version()
    with pytest.raises(PermissionError):
        ro.sql(stmt)
    assert os.path.exists(path)
    assert deltalake.DeltaTable(path).version() == v0


def test_read_only_still_drops_a_temp_table(wh):
    ro = duckrun.connect(wh, schema="dbo")
    ro.sql("create temp table scratch as select 1 as x")
    ro.sql("drop table scratch")
