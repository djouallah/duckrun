"""Raw SQL DML through ``conn.sql()`` — one table of statements and their expected outcome.

Each row is ``(id, statement, outcome, detail)`` run against a fresh connection seeded with two
Delta tables, ``items(id, name)`` = (1,a),(2,b),(3,c) and ``wide(id, name, qty)`` = (1,a,10):

    DELTA  → the statement lands as a real Delta table; ``detail`` = (table, probe_sql, expected)
    NATIVE → passes through to DuckDB (queryable) but is NOT a Delta table; ``detail`` = name
    ERROR  → ``conn.sql()`` raises ``ValueError`` whose message matches ``detail`` (a regex)

This is the supported/unsupported boundary for ``dbt/adapters/duckrun/delta_dml.py`` as executable
documentation: read the table to see exactly what duckrun does with each form.
"""
import deltalake
import pytest

import duckrun

DELTA, NATIVE, ERROR = "delta", "native", "error"

CASES = [
    # id                          statement                                                                   outcome  detail
    ("create_as",                "create table c as select 1 id, 'a' as name",                                DELTA,  ("c", "select count(*) from c", 1)),
    ("create_or_replace",        "create or replace table items as select 9 id, 'z' as name",                 DELTA,  ("items", "select name from items", "z")),
    ("create_as_cte",            "create table c as with s as (select * from (values (1),(2),(3)) t(id)) select id from s", DELTA, ("c", "select count(*) from c", 3)),
    ("create_as_parenthesised",  "create table p as (select 5 id)",                                           DELTA,  ("p", "select id from p", 5)),
    ("create_if_not_exists_noop","create table if not exists items as select 99 id, 'x' as name",             DELTA,  ("items", "select count(*) from items", 3)),
    ("create_coldefs_empty",     "create table e (id integer, name varchar)",                                 DELTA,  ("e", "select count(*) from e", 0)),
    ("create_coldefs_nested",    "create table money (id integer, amount decimal(10, 2))",                    DELTA,  ("money", "select count(*) from money", 0)),
    ("insert_select",            "insert into items select * from (values (4,'d')) t(id, name)",               DELTA,  ("items", "select count(*) from items", 4)),
    ("insert_select_collist",    "insert into items (name, id) select 'd', 4",                                DELTA,  ("items", "select id from items where name = 'd'", 4)),
    ("insert_values",            "insert into items values (5, 'e')",                                         DELTA,  ("items", "select name from items where id = 5", "e")),
    ("insert_values_partial",    "insert into wide (id, name) values (7, 'g')",                               DELTA,  ("wide", "select qty from wide where id = 7", None)),
    ("with_prefixed_insert",     "with s as (select 8 id, 'h' as name) insert into items select * from s",     DELTA,  ("items", "select name from items where id = 8", "h")),
    ("update",                   "update items set name = 'Z' where id = 1",                                  DELTA,  ("items", "select name from items where id = 1", "Z")),
    ("update_quoted_from",       "update items set name = 'from here' where id = 1",                          DELTA,  ("items", "select name from items where id = 1", "from here")),
    ("delete",                   "delete from items where id = 2",                                            DELTA,  ("items", "select count(*) from items", 2)),
    ("alter_add_column",         "alter table items add column qty integer",                                  DELTA,  ("items", "select count(*) from items where qty is null", 3)),
    ("leading_line_comment",     "-- build it\ncreate table cm as select 1 id",                               DELTA,  ("cm", "select id from cm", 1)),
    ("leading_block_comment",    "/* note */ insert into items values (4, 'd')",                              DELTA,  ("items", "select count(*) from items", 4)),

    ("create_temp_is_native",    "create temp table scratch as select 1 x",                                   NATIVE, "scratch"),
    ("create_view_is_native",    "create view v as select 1 x",                                               NATIVE, "v"),

    ("merge",                    "merge into items t using items s on t.id = s.id when matched then update set name = 'x'", ERROR, "MERGE"),
    ("update_from",              "update items set name = o.name from wide o where items.id = o.id",           ERROR,  "UPDATE . FROM"),
    ("delete_using",             "delete from items using wide o where items.id = o.id",                       ERROR,  "DELETE . USING"),
    ("multi_statement",          "insert into items values (4,'d'); insert into items values (5,'e')",         ERROR,  "one statement"),
]


@pytest.fixture
def conn(tmp_path):
    c = duckrun.connect(str(tmp_path / "wh"), schema="dbo")
    c.sql("select * from (values (1,'a'),(2,'b'),(3,'c')) t(id, name)") \
        .write.mode("overwrite").saveAsTable("items")
    c.sql("select * from (values (1,'a',10)) t(id, name, qty)") \
        .write.mode("overwrite").saveAsTable("wide")
    return c


@pytest.mark.parametrize(
    "sql,outcome,detail",
    [(c[1], c[2], c[3]) for c in CASES],
    ids=[c[0] for c in CASES],
)
def test_sql_dml(conn, sql, outcome, detail):
    if outcome == ERROR:
        with pytest.raises(ValueError, match=detail):
            conn.sql(sql)
        return

    conn.sql(sql)

    if outcome == NATIVE:  # DuckDB-local scratch — queryable, but not a Delta table under the wh
        assert conn.sql(f"select count(*) from {detail}").fetchone()[0] is not None
        assert not deltalake.DeltaTable.is_deltatable(conn.table_path("dbo", detail))
        return

    table, probe, expected = detail   # DELTA
    assert deltalake.DeltaTable.is_deltatable(conn.table_path("dbo", table))
    assert conn.sql(probe).fetchone()[0] == expected


@pytest.mark.xfail(strict=True, reason="WITH … UPDATE can't be expressed through a delta_rs predicate")
def test_with_prefixed_update_parked(conn):
    # Leading-CTE UPDATE is parked: the CTE can't be threaded into a delta_rs predicate, so today it
    # falls through and raises rather than applying — pinned here so the gap stays visible.
    conn.sql("with bump as (select 1 id) update items set name = 'Z' where id in (select id from bump)")
    assert conn.sql("select name from items where id = 1").fetchone()[0] == "Z"
