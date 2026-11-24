"""The DuckDB writer + delta-rs commit path at its edges: the shapes, values and inputs most likely
to break when DuckDB writes the parquet and delta-rs only commits (duckrun 0.6). Every test pins the
exact rows expected, and reads them back through BOTH readers — deltalake (its own Arrow reader) and
DuckDB's delta_scan — so a commit that only one reader understands fails here."""
import json
import os
import sys
from decimal import Decimal

import deltalake
import duckdb
import pyarrow as pa
import pytest
from deltalake.transaction import AddAction, create_table_with_add_actions

import duckrun
from dbt.adapters.duckrun import cow, engine


@pytest.fixture
def c(tmp_path):
    return duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)


def _p(c, t):
    return c._table_path("dbo", t)


def _sorted(rows):
    return sorted(rows, key=repr)


def _duck(c, t, cols="*"):
    return _sorted(duckdb.connect().sql(f"select {cols} from delta_scan('{_p(c, t)}')").fetchall())


def _dl(c, t, cols):
    tbl = deltalake.DeltaTable(_p(c, t)).to_pyarrow_table(columns=cols)
    return _sorted([tuple(r[k] for k in cols) for r in tbl.to_pylist()])


def _both(c, t, cols):
    """The table's rows (``cols``) via DuckDB; asserts deltalake reads exactly the same."""
    duck = _duck(c, t, ", ".join(engine.quote_ident(x) for x in cols))
    assert _dl(c, t, cols) == duck
    return duck


def _version(c, t):
    return deltalake.DeltaTable(_p(c, t)).version()


def _adds(path):
    """The live add actions, replayed from the JSON log (the tables here are never checkpointed
    before they are read this way)."""
    from urllib.parse import unquote
    live = {}
    log = os.path.join(path, "_delta_log")
    for name in sorted(f for f in os.listdir(log) if f.endswith(".json")):
        with open(os.path.join(log, name), encoding="utf-8") as fh:
            for line in fh:
                a = json.loads(line)
                if "add" in a:
                    live[unquote(a["add"]["path"])] = a["add"]
                elif "remove" in a:
                    live.pop(unquote(a["remove"]["path"]), None)
    return list(live.values())


def _live_files(c, t):
    return len(deltalake.DeltaTable(_p(c, t)).file_uris())


# -- partition values -----------------------------------------------------------------------------
def _sql_str(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


ESCAPED = [(1, None), (2, "a b"), (3, "a/b"), (4, "k=v"), (5, "50%"), (6, "été"), (7, "it's"),
           (8, "#hash"), (9, "a?b"), (10, "x:y")]


def test_partition_values_that_need_escaping_survive_every_operation(c):
    # DuckDB escapes these in the folder name; the log stores the path URI-encoded. A remove that
    # re-encodes it matches no file, so the old file stayed live: deleted rows came back, updated
    # rows doubled, a compaction duplicated the partition.
    vals = ", ".join(f"({i}, {_sql_str(p)})" for i, p in ESCAPED)
    c.sql(f"create table pv partitioned by (p) as select * from (values {vals}) t(id, p)")
    assert _both(c, "pv", ["id", "p"]) == _sorted(ESCAPED)
    c.sql("delete from pv where id in (2, 5, 10)")
    c.sql("update pv set id = id + 100 where p = 'a/b' or p is null")
    c.sql("insert into pv values (20, '50%'), (21, NULL), (22, 'a b')")
    c.sql("merge into pv using (select 4 id, 'k=v' p union all select 6, 'été') s "
          "on pv.id = s.id when matched then delete")
    want = _sorted([(101, None), (103, "a/b"), (7, "it's"), (8, "#hash"), (9, "a?b"),
                    (20, "50%"), (21, None), (22, "a b")])
    assert _both(c, "pv", ["id", "p"]) == want
    c.sql("optimize pv")
    assert _both(c, "pv", ["id", "p"]) == want
    assert _live_files(c, "pv") == 7               # one file per partition left after compaction


def test_partition_column_types_delete_and_update(c):
    c.sql("""create table pt partitioned by (d, ts, b, i, dec) as
        select 1 id, DATE '2024-01-02' d, TIMESTAMPTZ '2024-01-02 03:04:05.123456+00' ts,
               true b, 7 i, 12.50::DECIMAL(10,2) dec
        union all select 2, NULL, NULL, NULL, NULL, NULL
        union all select 3, DATE '2024-01-03', TIMESTAMPTZ '2024-01-03 00:00:00+00', false, -1, 0.01""")
    c.sql("delete from pt where id = 1")
    c.sql("update pt set id = 30 where id = 3")
    assert _both(c, "pt", ["id", "i"]) == _sorted([(2, None), (30, -1)])
    pv = [a["partitionValues"] for a in _adds(_p(c, "pt"))]
    assert {"d": "2024-01-03", "ts": "2024-01-03 00:00:00.000000", "b": "false", "i": "-1",
            "dec": "0.01"} in pv


def test_remove_matches_the_whole_path_not_the_file_name(c, tmp_path):
    # Spark writes the same file name into every partition folder (part-00000-<job uuid>.c000…).
    # A DELETE in one partition must remove that partition's file only.
    path = _p(c, "sp")
    d = duckdb.connect()
    adds = []
    for p in ("a", "b"):
        os.makedirs(f"{path}/p={p}", exist_ok=True)
        f = f"{path}/p={p}/part-00000-same.c000.snappy.parquet"
        d.execute(f"copy (select i id from range(3) r(i)) to '{f}' (format parquet)")
        adds.append(AddAction(path=f"p={p}/part-00000-same.c000.snappy.parquet",
                              size=os.path.getsize(f), partition_values={"p": p},
                              modification_time=0, data_change=True,
                              stats='{"numRecords":3}'))
    schema = engine.delta_schema(d.sql("select 1::BIGINT id, 'x' p"))
    create_table_with_add_actions(path, schema, adds, mode="error", partition_by=["p"])
    c.refresh(quiet=True)
    c.sql("delete from sp where p = 'a' and id = 0")
    assert _both(c, "sp", ["id", "p"]) == _sorted([(1, "a"), (2, "a"), (0, "b"), (1, "b"), (2, "b")])
    c.sql("update sp set id = 9 where p = 'b' and id = 2")
    assert _both(c, "sp", ["id", "p"]) == _sorted([(1, "a"), (2, "a"), (0, "b"), (1, "b"), (9, "b")])


def test_update_moves_rows_between_partitions(c):
    c.sql("create table mv partitioned by (p) as select i id, (i % 2)::varchar p from range(10) r(i)")
    c.sql("update mv set p = 'x y' where id < 3")
    got = duckdb.connect().sql(f"select p, count(*) from delta_scan('{_p(c, 'mv')}') "
                               "group by p order by p").fetchall()
    assert got == [("0", 3), ("1", 4), ("x y", 3)]
    assert len(_both(c, "mv", ["id", "p"])) == 10


# -- NOT NULL, column definitions -----------------------------------------------------------------
def test_not_null_column_definition_is_recorded_and_enforced(c):
    c.sql("create table nn (id integer not null, v varchar)")
    dt = deltalake.DeltaTable(_p(c, "nn"))
    assert {f.name: f.nullable for f in dt.schema().fields} == {"id": False, "v": True}
    v = _version(c, "nn")
    for stmt in ("insert into nn values (NULL, 'x')",
                 "insert into nn (v) values ('no id')",
                 "insert into nn select NULL::int id, 'x' v"):
        with pytest.raises(ValueError, match="NOT NULL column 'id'"):
            c.sql(stmt)
    assert _version(c, "nn") == v
    c.sql("insert into nn values (1, 'a')")
    for stmt in ("update nn set id = NULL",
                 "merge into nn using (select NULL::int id, 'b' v) s on nn.id = s.id "
                 "when not matched then insert (id, v) values (s.id, s.v)"):
        with pytest.raises(ValueError, match="NOT NULL"):
            c.sql(stmt)
    assert _both(c, "nn", ["id", "v"]) == [(1, "a")]


def test_column_definitions_need_no_pyarrow(c, monkeypatch):
    # pyarrow is not a runtime dependency; CREATE TABLE (col defs) used to build its schema in Arrow.
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    c.sql("create table np (id bigint not null, tz timestamptz, s struct(a int), "
          "g int generated always as (id * 2), u uuid) partitioned by (u)")
    c.sql("insert into np (id, tz, s, u) values (3, TIMESTAMPTZ '2024-01-01 00:00:00+00', "
          "{'a': 1}, '00000000-0000-0000-0000-000000000001')")
    monkeypatch.delitem(sys.modules, "pyarrow")
    dt = deltalake.DeltaTable(_p(c, "np"))
    types = {f.name: f.type.type if hasattr(f.type, "type") and isinstance(f.type.type, str)
             else str(f.type) for f in dt.schema().fields}
    assert types["tz"] == "timestamp" and types["u"] == "string" and types["id"] == "long"
    assert dt.metadata().partition_columns == ["u"]
    assert c.sql("select id, g from np").fetchall() == [(3, 6)]


# -- types ----------------------------------------------------------------------------------------
def test_nested_types_round_trip_and_rewrite(c):
    c.sql("""create table nt as select 1 id, {'a': 1, 'b': 'x'} s, [1, 2, 3] l, MAP {'k': 1} m,
             [{'q': 1.5}] ls""")
    c.sql("insert into nt select 2, {'a': 2, 'b': NULL}, [], MAP {}, [NULL]")
    c.sql("update nt set l = [9], s = {'a': 9, 'b': 'z'} where id = 1")
    dl = _dl(c, "nt", ["id", "s", "l", "ls"])
    assert dl == _sorted([(1, {"a": 9, "b": "z"}, [9], [{"q": 1.5}]),
                          (2, {"a": 2, "b": None}, [], [None])])
    assert _duck(c, "nt", "id, s, l, m") == _sorted([(1, {"a": 9, "b": "z"}, [9], {"k": 1}),
                                                     (2, {"a": 2, "b": None}, [], {})])


def test_types_delta_lacks_are_cast_the_way_delta_rs_stored_them(c):
    c.sql("""create table ct as select '00000000-0000-0000-0000-000000000001'::UUID u,
             '{"a":1}'::JSON j, 'a'::ENUM('a', 'b') e, 12345678901234567890::HUGEINT h,
             5::UBIGINT ub, 6::UINTEGER ui, 7::USMALLINT us, 8::UTINYINT ut,
             TIMESTAMP_NS '2024-01-01 01:02:03.123456789' tns, TIMESTAMP_MS '2024-01-01 01:02:03' tms""")
    fields = {f.name: f.type for f in deltalake.DeltaTable(_p(c, "ct")).schema().fields}
    prim = {k: getattr(v, "type", str(v)) for k, v in fields.items()}
    assert prim == {"u": "string", "j": "string", "e": "string", "h": "decimal(38,0)",
                    "ub": "decimal(20,0)", "ui": "long", "us": "integer", "ut": "short",
                    "tns": "timestamp", "tms": "timestamp"}
    row = _dl(c, "ct", ["u", "e", "h", "ub", "ui"])
    assert row == [("00000000-0000-0000-0000-000000000001", "a", Decimal(12345678901234567890),
                    Decimal(5), 6)]


@pytest.mark.parametrize("expr", ["INTERVAL 1 DAY", "TIME '01:02:03'", "[INTERVAL 1 DAY]",
                                  "{'a': TIME '01:00'}", "union_value(k := 1)", "'101'::BIT"])
def test_types_delta_has_none_for_are_refused_before_anything_is_written(c, expr):
    with pytest.raises(cow.Unsupported, match="Delta has no type"):
        c.sql(f"create table ut as select 1 id, {expr} x")
    assert not os.path.exists(_p(c, "ut"))


def test_timestamp_ntz_table_carries_its_table_feature(c, tmp_path):
    d = duckdb.connect()
    path = str(tmp_path / "ntz")
    engine.write_delta(path, d.sql("select TIMESTAMP '2024-01-01 01:02:03' ts"), "overwrite",
                       cur=d, timestamp_ntz=True)
    dt = deltalake.DeltaTable(path)
    assert "timestampNtz" in (dt.protocol().writer_features or [])
    assert "timestampNtz" in (dt.protocol().reader_features or [])
    assert dt.schema().fields[0].type.type == "timestamp_ntz"
    assert dt.to_pyarrow_table().column("ts").to_pylist()[0].isoformat() == "2024-01-01T01:02:03"


# -- stats ----------------------------------------------------------------------------------------
def test_long_string_stats_keep_an_upper_bound(c):
    # A string longer than the stats cut keeps a truncated min AND a bumped max; with no max,
    # deltalake's Arrow filter path pruned the file and found nothing.
    c.sql("create table ls as select repeat('é', 100) || i s, i from range(10) r(i)")
    c.sql("insert into ls select repeat('z', 200), 99")
    stats = [json.loads(a["stats"]) for a in _adds(_p(c, "ls"))]
    for st in stats:
        assert len(st["minValues"]["s"]) == cow.STATS_TRUNCATE_LENGTH
        assert st["maxValues"]["s"] > st["minValues"]["s"]
    dt = deltalake.DeltaTable(_p(c, "ls"))
    assert dt.to_pyarrow_table(filters=[("s", "=", "z" * 200)]).num_rows == 1
    assert dt.to_pyarrow_table(filters=[("s", "=", "é" * 100 + "3")]).num_rows == 1
    assert duckdb.connect().sql(f"select i from delta_scan('{_p(c, 'ls')}') "
                                f"where s = repeat('z', 200)").fetchall() == [(99,)]


@pytest.mark.parametrize("s, want", [("abc", "abd"), ("a" * 70, "a" * 63 + "b"),
                                     ("ab\U0010ffff", "ac"), ("\U0010ffff" * 3, None),
                                     ("a퟿", "a")])
def test_truncated_max_is_an_upper_bound(s, want):
    got = cow._truncated_max(s)
    assert got == want
    if got is not None:
        assert got > s[:cow.STATS_TRUNCATE_LENGTH]


def test_filtered_reads_match_full_reads_across_files(c):
    # Wrong file stats prune live rows silently; compare filtered counts with a no-pruning count.
    c.sql("create table ds as select i id, 'name' || lpad(i::varchar, 6, '0') s, i / 3.0 f, "
          "(i / 7)::DECIMAL(18,2) dec, DATE '2024-01-01' + (i % 400)::int d, "
          "TIMESTAMPTZ '2024-01-01 00:00:00+00' + to_seconds(i) tz, "
          "case when i % 7 = 0 then null else i end n from range(5000) r(i)")
    for k in range(4):
        c.sql(f"insert into ds select i + {5000 * (k + 1)}, 'zz' || i, -i, -i, DATE '2020-01-01', "
              f"TIMESTAMPTZ '2020-01-01 00:00:00+00', NULL from range(1000) r(i)")
    d = duckdb.connect()
    full = f"(select * from delta_scan('{_p(c, 'ds')}') limit 1000000000)"
    dt = deltalake.DeltaTable(_p(c, "ds"))
    assert len(dt.file_uris()) == 5
    for pred, flt in [("id = 4321", ("id", "=", 4321)),
                      ("s = 'name004321'", ("s", "=", "name004321")),
                      ("dec = 617.00", ("dec", "=", Decimal("617.00"))),
                      ("d = DATE '2024-02-01'", None), ("n is null", None), ("n = 14", ("n", "=", 14)),
                      ("s >= 'zz5'", ("s", ">=", "zz5")), ("f < -500", ("f", "<", -500.0)),
                      ("tz < TIMESTAMPTZ '2021-01-01 00:00:00+00'", None),
                      ("id between 9990 and 10010", None)]:
        want = d.sql(f"select count(*) from {full} where {pred}").fetchall()[0][0]
        assert d.sql(f"select count(*) from delta_scan('{_p(c, 'ds')}') where {pred}"
                     ).fetchall()[0][0] == want, pred
        if flt is not None:
            assert dt.to_pyarrow_table(filters=[flt]).num_rows == want, pred


def test_many_files_carry_exact_record_counts(c, tmp_path):
    d = duckdb.connect()
    path = str(tmp_path / "mf")
    engine.write_delta(path, d.sql("select i id, md5(i::varchar) h from range(300000) r(i)"),
                       "overwrite", cur=d, target_file_size=1_000_000, row_group_rows=20_480)
    actions = _adds(path)
    assert len(actions) > 3
    assert sum(json.loads(a["stats"])["numRecords"] for a in actions) == 300000
    assert d.sql(f"select count(*), count(distinct id) from delta_scan('{path}')"
                 ).fetchall() == [(300000, 300000)]


# -- empty writes ---------------------------------------------------------------------------------
def test_empty_writes(c):
    c.sql("create table em as select 1 id, 'x' v where false")
    dt = deltalake.DeltaTable(_p(c, "em"))
    assert [f.name for f in dt.schema().fields] == ["id", "v"] and dt.file_uris() == []
    v = _version(c, "em")
    c.sql("insert into em select 1, 'x' where false")
    assert _version(c, "em") == v                   # an empty append commits nothing
    c.sql("insert into em values (1, 'a')")
    c.sql("create or replace table em as select 1 id, 'x' v where false")
    assert c.sql("select count(*) from em").fetchall() == [(0,)]
    assert _live_files(c, "em") == 0
    c.sql("create table emp partitioned by (p) as select 1 id, 'x' p where false")
    assert deltalake.DeltaTable(_p(c, "emp")).metadata().partition_columns == ["p"]
    c.sql("insert into emp values (1, 'a b')")
    assert _both(c, "emp", ["id", "p"]) == [(1, "a b")]


# -- replace / append shape -----------------------------------------------------------------------
def test_create_or_replace_changes_partitioning_and_schema_and_keeps_history(c):
    c.sql("create table rp as select i id, i % 3 g from range(30) r(i)")
    c.sql("create or replace table rp partitioned by (g) as "
          "select i id, (i % 3)::varchar g, 'new' extra from range(9) r(i)")
    dt = deltalake.DeltaTable(_p(c, "rp"))
    assert dt.metadata().partition_columns == ["g"]
    assert [f.name for f in dt.schema().fields] == ["id", "g", "extra"]
    assert len(_both(c, "rp", ["id", "g", "extra"])) == 9
    assert c.sql("select count(*) from rp at (version => 0)").fetchall() == [(30,)]
    c.sql("create or replace table rp as select 1 id")
    dt = deltalake.DeltaTable(_p(c, "rp"))
    assert dt.metadata().partition_columns == [] and dt.version() == 2
    assert _both(c, "rp", ["id"]) == [(1,)]


def test_append_shape_rules(c):
    c.sql("create table ap as select 1::INTEGER id, 'x' v")
    v = _version(c, "ap")
    with pytest.raises(ValueError, match="column"):
        c.sql("insert into ap select 2 id, 'y' v, 3 extra")             # no schema evolution
    with pytest.raises(ValueError, match="narrow"):
        c.sql("insert into ap select 5000000000::BIGINT id, 'z' v")     # would not fit INTEGER
    assert _version(c, "ap") == v
    c.sql("insert into ap (id) values (3)")                             # missing column → NULL
    c.sql("insert into ap select 4::BIGINT id, 'z' v")                  # a value that fits
    c.sql('insert into ap select 6 "ID", \'w\' "V"')                    # names match case-blind
    c.sql("insert with schema evolution into ap select 7 id, 'e' v, 1.5 extra")
    assert _both(c, "ap", ["id", "v", "extra"]) == _sorted(
        [(1, "x", None), (3, None, None), (4, "z", None), (6, "w", None), (7, "e", Decimal("1.5"))])
    fields = {f.name: f.type.type for f in deltalake.DeltaTable(_p(c, "ap")).schema().fields}
    assert fields["id"] == "integer"


def test_replace_where_refuses_a_row_outside_the_predicate(c):
    c.sql("create table rw as select i id, i % 3 g from range(30) r(i)")
    v = _version(c, "rw")
    with pytest.raises(ValueError, match="do not match the predicate"):
        c.sql("insert into rw replace where g = 1 select 100 id, 2 g")
    assert _version(c, "rw") == v
    c.sql("insert into rw replace where g = 1 select 100 id, 1 g")
    assert c.sql("select count(*), sum(id) filter (where g = 1) from rw").fetchall() == [(21, 100)]


# -- tables other writers made --------------------------------------------------------------------
def test_a_table_delta_rs_wrote_takes_every_duckrun_write(c):
    # A table from an older duckrun (delta-rs's writer): its folders, file names and stats differ.
    path = _p(c, "old")
    t = lambda ids, ps: pa.table({"id": pa.array(ids, pa.int64()), "p": ps,  # noqa: E731
                                  "ts": pa.array([None] * len(ids), pa.timestamp("us", "UTC"))})
    deltalake.write_deltalake(path, t(list(range(8)), [f"k{i % 2}" for i in range(8)]),
                              partition_by=["p"])
    deltalake.write_deltalake(path, t([100], ["k 9"]), partition_by=["p"], mode="append")
    c.refresh(quiet=True)
    c.sql("insert into old values (200, 'k0', NULL)")
    c.sql("merge into old using (select 1 id) s on old.id = s.id "
          "when matched then update set ts = TIMESTAMPTZ '2024-01-01 00:00:00+00'")
    c.sql("update old set id = -id where p = 'k 9'")
    c.sql("delete from old where p = 'k1' and id > 1")
    c.sql("optimize old")
    assert _both(c, "old", ["id", "p"]) == _sorted(
        [(0, "k0"), (2, "k0"), (4, "k0"), (6, "k0"), (200, "k0"), (1, "k1"), (-100, "k 9")])
    assert c.sql("select count(*) from old where ts is not null").fetchall() == [(1,)]


def test_column_mapped_table_takes_appends_under_physical_names(c):
    path = _p(c, "cm")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    c.con.execute("LOAD delta")
    c.con.execute(f"""COPY (SELECT 1::BIGINT AS id, 'a' AS "Super Name") TO '{path}' """
                  "(FORMAT delta, WITH_PROPERTIES {'delta.columnMapping.mode': 'name'})")
    c.refresh(quiet=True)
    c.sql("""insert into cm values (2, 'b')""")
    c.sql("""create or replace table cm as select * from cm""")           # same shape: overwrite
    assert c.sql('select id, "Super Name" from cm order by id').fetchall() == [(1, "a"), (2, "b")]
    files = deltalake.DeltaTable(path).file_uris()
    lit = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    names = {r[0] for r in duckdb.connect().sql(f"select name from parquet_schema([{lit}])").fetchall()}
    assert "Super Name" not in names and any(n.startswith("col-") for n in names)


# -- table features -------------------------------------------------------------------------------
def test_change_data_feed_takes_whole_file_writes_and_refuses_rewrites(c):
    # A commit that only adds or removes whole files needs no change files (readers derive the
    # changes from the actions); a rewrite carries rows over and would read as delete + insert.
    c.sql("create table cdf as select 1 id")
    c.sql("alter table cdf set tblproperties ('delta.enableChangeDataFeed' = 'true')")
    c.sql("insert into cdf values (2)")
    c.sql("create or replace table cdf as select 3 id")                 # overwrite, same shape
    c.sql("insert into cdf values (4)")
    v = _version(c, "cdf")
    for stmt in ("delete from cdf where id = 3", "update cdf set id = 5 where id = 3",
                 "merge into cdf using (select 3 id) s on cdf.id = s.id when matched then delete"):
        with pytest.raises(cow.Unsupported, match="change data feed"):
            c.sql(stmt)
    assert _version(c, "cdf") == v
    c.sql("truncate table cdf")
    assert c.sql("select count(*) from cdf").fetchall() == [(0,)]
    assert deltalake.DeltaTable(_p(c, "cdf")).metadata().configuration[
        "delta.enableChangeDataFeed"] == "true"
    changes = deltalake.DeltaTable(_p(c, "cdf")).load_cdf(starting_version=1).read_all()
    got = _sorted(zip(changes.column("id").to_pylist(), changes.column("_change_type").to_pylist()))
    assert got == _sorted([(2, "insert"), (1, "delete"), (2, "delete"), (3, "insert"),
                           (4, "insert"), (3, "delete"), (4, "delete")])


def test_change_data_feed_refusal_names_the_way_out(c):
    c.sql("create table cdf2 as select 1 id")
    c.sql("alter table cdf2 set tblproperties ('delta.enableChangeDataFeed' = 'true')")
    with pytest.raises(cow.Unsupported, match="enableChangeDataFeed' = 'false'"):
        c.sql("delete from cdf2 where id = 1")


def test_deletion_vector_feature_without_vectors_rewrites(c):
    # Databricks turns the feature on by default; only a file that HAS a vector can't be removed.
    c.sql("create table dv (id bigint, v bigint) with ('delta.enableDeletionVectors' = 'true')")
    c.sql("insert into dv select range, range * 10 from range(20)")
    dt = deltalake.DeltaTable(_p(c, "dv"))
    assert "deletionVectors" in (dt.protocol().reader_features or [])
    assert cow.unsupported_reason(dt, "rewrite") is None
    assert cow.unsupported_reason(dt, "compact") is None
    c.sql("delete from dv where id = 1")
    c.sql("update dv set v = -1 where id = 2")
    c.sql("merge into dv using (select 3 id, 33 v union all select 100 id, 1000 v) s "
          "on dv.id = s.id when matched then update set v = s.v when not matched then insert *")
    c.sql("insert into dv replace where id = 4 select 4 id, 44 v")
    c.sql("insert into dv values (5, 50)")
    c.sql("optimize dv")
    want = _sorted([(i, i * 10) for i in range(20) if i not in (1, 2, 3, 4)]
                   + [(2, -1), (3, 33), (4, 44), (100, 1000), (5, 50)])
    # deltalake's own Arrow reader refuses the deletionVectors reader feature: DuckDB twice.
    assert _duck(c, "dv", "id, v") == want
    assert _sorted(c.sql("select id, v from dv").fetchall()) == want


_DV_FIXTURE = os.path.join(os.path.dirname(__file__), "data", "table_with_dv_small")


def test_rewrite_touching_a_file_with_a_deletion_vector_is_refused(c):
    import shutil
    shutil.copytree(_DV_FIXTURE, _p(c, "dvt"))
    c.refresh()
    assert c.sql("select count(*) from dvt").fetchall() == [(8,)]   # 10 rows, 2 deleted by DV
    v = _version(c, "dvt")
    for stmt in ("delete from dvt where value = 5", "update dvt set value = 50 where value = 5",
                 "merge into dvt using (select 5 value) s on dvt.value = s.value "
                 "when matched then delete",
                 "insert into dvt replace where value = 5 select 5 value"):
        with pytest.raises(cow.Unsupported, match="deletion vector"):
            c.sql(stmt)
    assert _version(c, "dvt") == v
    files = set(os.listdir(_p(c, "dvt")))                       # nothing written by the refusals
    assert files == set(os.listdir(_DV_FIXTURE))
    # Appends work; OPTIMIZE compacts them and leaves the file with the vector as it is.
    c.sql("insert into dvt values (100)")
    c.sql("insert into dvt values (101)")
    c.sql("optimize dvt")
    live = {os.path.basename(p) for p in deltalake.DeltaTable(_p(c, "dvt")).file_uris()}
    assert "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet" in live
    assert len(live) == 2
    assert _duck(c, "dvt") == _sorted([(i,) for i in range(1, 9)] + [(100,), (101,)])
    c.sql("truncate table dvt")                                 # an overwrite: delta-rs removes it
    assert c.sql("select count(*) from dvt").fetchall() == [(0,)]


def test_append_only_table_takes_inserts_and_refuses_everything_that_removes(c):
    c.sql("create table ao as select 1 id")
    c.sql("alter table ao set tblproperties ('delta.appendOnly' = 'true')")
    c.sql("insert into ao values (2)")
    v = _version(c, "ao")
    for stmt in ("create or replace table ao as select 3 id",
                 "create or replace table ao as select 3 id, 'x' v",
                 "truncate table ao", "delete from ao where id = 1",
                 "update ao set id = 9", "optimize ao zorder by (id)"):
        with pytest.raises(ValueError):
            c.sql(stmt)
    assert _version(c, "ao") == v
    assert _both(c, "ao", ["id"]) == [(1,), (2,)]


# -- names ----------------------------------------------------------------------------------------
def test_column_names_that_need_quoting(c):
    cols = ["my col", 'it"s', "été", "MixedCase", "select", "a.b"]
    q = [engine.quote_ident(x) for x in cols]
    c.sql("create table cn as select " + ", ".join(f"{i} {n}" for i, n in enumerate(q)))
    c.sql("insert into cn select 10, 11, 12, 13, 14, 15")
    c.sql(f"update cn set {q[0]} = 99 where {q[2]} = 2")
    c.sql(f"delete from cn where {q[5]} = 15")
    c.sql(f"merge into cn using (select 99 {q[0]}, 7 {q[1]}) s on cn.{q[0]} = s.{q[0]} "
          f"when matched then update set {q[1]} = s.{q[1]}")
    c.sql(f"insert into cn replace where {q[1]} = 7 select 1, 7, 2, 3, 4, 5")
    c.sql("optimize cn")
    assert [f.name for f in deltalake.DeltaTable(_p(c, "cn")).schema().fields] == cols
    assert _both(c, "cn", cols) == [(1, 7, 2, 3, 4, 5)]


def test_alter_columns_then_both_readers(c):
    c.sql("create table al as select 1 id, 'x' v, 2 w")
    c.sql("alter table al drop column w")
    c.sql("alter table al rename column v to v2")
    c.sql("alter table al add column z integer")
    c.sql("insert into al values (2, 'y', 3)")
    assert [f.name for f in deltalake.DeltaTable(_p(c, "al")).schema().fields] == ["id", "v2", "z"]
    assert _both(c, "al", ["id", "v2", "z"]) == [(1, "x", None), (2, "y", 3)]


# -- the log --------------------------------------------------------------------------------------
def test_checkpoints_are_written_and_readable(c):
    c.sql("create table cp as select 0 id")
    for i in range(1, 22):
        c.sql(f"insert into cp values ({i})")
    log = os.listdir(os.path.join(_p(c, "cp"), "_delta_log"))
    assert "00000000000000000010.checkpoint.parquet" in log or \
        "00000000000000000009.checkpoint.parquet" in log
    assert "_last_checkpoint" in log
    assert _both(c, "cp", ["id"]) == _sorted([(i,) for i in range(22)])


# -- library inputs -------------------------------------------------------------------------------
def test_write_delta_takes_arrow_tables_streams_and_dataframes_without_a_cursor(tmp_path):
    import pandas as pd
    path = str(tmp_path / "lib")
    t = pa.table({"id": [1, 2], "v": ["a", "b"]})
    engine.write_delta(path, t, "overwrite")
    engine.write_delta(path, pa.RecordBatchReader.from_batches(t.schema, t.to_batches()), "append")
    engine.write_delta(path, pd.DataFrame({"id": [3], "v": ["c"]}), "append")
    rows = deltalake.DeltaTable(path).to_pyarrow_table().to_pylist()
    assert _sorted([(r["id"], r["v"]) for r in rows]) == _sorted(
        [(1, "a"), (2, "b"), (1, "a"), (2, "b"), (3, "c")])
