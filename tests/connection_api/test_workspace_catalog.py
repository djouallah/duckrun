"""Workspace catalogs: a whole Fabric workspace as ONE DuckDB catalog, tables addressed as
``ws.lakehouse.schema.table`` through DuckDB's nested schemas (duckdb >= 2.0.1).

The fixture is a local mirror of a workspace — ``<ws>/<name>.Lakehouse/Tables/<schema>/<table>`` —
which ``connect()`` recognises exactly like ``abfss://<ws>@onelake.dfs.fabric.microsoft.com``. Every
write still goes DuckDB → Arrow → delta-rs into the right lakehouse root.
"""
import deltalake
import pytest

import duckrun
import duckrun.session as session_mod
from dbt.adapters.duckrun.delta_dml import dml_target_parts

HOST = "onelake.dfs.fabric.microsoft.com"


def _lakehouse(ws, name, schema="dbo"):
    """A writable session on one lakehouse of the mirror (``schema='Tables'`` = schema-less)."""
    root = ws / f"{name}.Lakehouse"
    if schema == "Tables":
        return duckrun.connect(str(root), schema="Tables", read_only=False)
    return duckrun.connect(str(root / "Tables"), read_only=False)


@pytest.fixture
def ws(tmp_path):
    ws = tmp_path / "ws"
    for lh in ("sales", "hr", "legacy", "empty"):
        (ws / f"{lh}.Lakehouse" / "Tables").mkdir(parents=True)
    (ws / "n.SynapseNotebook").mkdir()                      # not a lakehouse: ignored
    c = _lakehouse(ws, "sales")
    c.sql("create table orders as select 1 as id, 10 as amt")
    c.sql("create table mart.x as select 'm' as v")
    c.close()
    c = _lakehouse(ws, "hr")
    c.sql("create table orders as select 2 as id, 20 as amt")  # same name, other lakehouse
    c.close()
    c = _lakehouse(ws, "legacy", schema="Tables")             # schema-less: Tables/<table>
    c.sql("create table flat as select 1 as k")
    c.close()
    return ws


def _tables_dir(ws, lh, *rest):
    return str(ws.joinpath(f"{lh}.Lakehouse", "Tables", *rest))


def test_workspace_discovery_and_reads(ws, capsys):
    conn = duckrun.connect(str(ws))
    out = capsys.readouterr().out
    assert "skipped lakehouse 'legacy'" in out
    assert conn._current_catalog == "ws"
    assert conn.sql("select amt from ws.sales.dbo.orders").fetchall() == [(10,)]
    assert conn.sql("select amt from ws.hr.dbo.orders").fetchall() == [(20,)]
    assert conn.sql("select v from ws.sales.mart.x").fetchall() == [("m",)]
    lakehouses = {r[0] for r in conn.sql(
        "select schema_name from duckdb_schemas() "
        "where database_name = 'ws' and parent_schema is null").fetchall()}
    assert {"sales", "hr", "empty"} <= lakehouses
    assert not {"legacy", "n"} & lakehouses
    assert set(conn._cat_databases("ws")) == {"hr.dbo", "sales.dbo", "sales.mart"}
    assert conn._cat_tables("sales.dbo", "ws") == ["orders"]


def test_workspace_four_part_writes(ws):
    conn = duckrun.connect(str(ws), read_only=False)
    conn.sql("create table ws.sales.dbo.t as select 1 as id, 'a' as v")
    conn.sql("insert into ws.sales.dbo.t values (2, 'b')")
    conn.sql("update ws.sales.dbo.t set v = 'B' where id = 2")
    conn.sql("delete from ws.sales.dbo.t where id = 1")
    conn.sql("merge into ws.sales.dbo.t as t using (select 3 as id, 'c' as v) as s on t.id = s.id "
             "when matched then update set v = s.v "
             "when not matched then insert (id, v) values (s.id, s.v)")
    want = [(2, "B"), (3, "c")]
    assert conn.sql("select id, v from ws.sales.dbo.t order by id").fetchall() == want
    fresh = duckrun.connect(_tables_dir(ws, "sales"))         # the data landed in sales' own root
    assert fresh.sql("select id, v from dbo.t order by id").fetchall() == want
    assert not deltalake.DeltaTable.is_deltatable(_tables_dir(ws, "hr", "dbo", "t"))

    conn.sql("create table ws.empty.dbo.first as select 1 as k")   # a lakehouse with no tables yet
    assert deltalake.DeltaTable.is_deltatable(_tables_dir(ws, "empty", "dbo", "first"))
    assert conn.sql("select k from ws.empty.dbo.first").fetchall() == [(1,)]

    conn.sql("drop table ws.sales.dbo.t")
    assert "t" not in conn._cat_tables("sales.dbo", "ws")


def test_workspace_use_then_unqualified_writes(ws):
    conn = duckrun.connect(str(ws), read_only=False)
    conn.sql("USE ws.sales.dbo")
    conn.sql("create table u as select 5 as n")
    conn.sql("insert into u values (6)")
    assert conn.sql("select sum(n) from u").fetchall() == [(11,)]
    assert conn.sql("select sum(n) from ws.sales.dbo.u").fetchall() == [(11,)]
    assert deltalake.DeltaTable.is_deltatable(_tables_dir(ws, "sales", "dbo", "u"))
    stray = conn.sql("select count(*) from duckdb_schemas() where database_name = 'ws' "
                     "and parent_schema is null and schema_name = 'dbo'").fetchall()[0][0]
    assert stray == 0                                          # no top-level ws.dbo
    assert conn.root_path.replace("\\", "/").endswith("sales.Lakehouse/Tables")


def test_workspace_three_part_lakehouse_name(ws):
    conn = duckrun.connect(str(ws), read_only=False)          # current catalog: ws
    conn.sql("insert into sales.mart.x values ('n')")
    assert sorted(conn.sql("select v from sales.mart.x").fetchall()) == [("m",), ("n",)]
    assert sorted(conn.sql("select v from ws.sales.mart.x").fetchall()) == [("m",), ("n",)]


def test_workspace_name_errors(ws):
    conn = duckrun.connect(str(ws), read_only=False)
    with pytest.raises(ValueError, match="ambiguous"):
        conn.sql("insert into dbo.orders values (9, 9)")
    with pytest.raises(ValueError, match="is a workspace"):
        conn.sql("insert into orders values (9, 9)")          # USE ws only: no current lakehouse
    with pytest.raises(ValueError, match="no lakehouse 'nope'"):
        conn.sql("create table ws.nope.dbo.t as select 1 as k")
    with pytest.raises(ValueError, match="name the lakehouse"):
        conn.sql("insert into ws.dbo.orders values (9, 9)")
    with pytest.raises(PermissionError):
        duckrun.connect(str(ws)).sql("create table ws.sales.dbo.z as select 1 as k")
    with pytest.raises(ValueError, match="schema= does not apply"):
        duckrun.connect(str(ws), schema="dbo")


def test_lakehouse_catalog_next_to_attached_workspace(ws, tmp_path):
    plain = tmp_path / "plain"
    plain.mkdir()
    conn = duckrun.connect(str(plain), read_only=False)       # catalog 'plain', one lakehouse
    conn.attach(str(ws), name="w")
    conn.sql("create table plain.dbo.p as select 1 as k")     # 3-part on a lakehouse: unchanged
    assert deltalake.DeltaTable.is_deltatable(str(plain / "dbo" / "p"))
    conn.sql("create table w.hr.dbo.h as select 2 as k")
    assert deltalake.DeltaTable.is_deltatable(_tables_dir(ws, "hr", "dbo", "h"))
    assert conn.sql("select k from w.hr.dbo.h").fetchall() == [(2,)]
    assert conn.sql("select k from plain.dbo.p").fetchall() == [(1,)]
    with pytest.raises(ValueError, match="needs a workspace catalog"):
        conn.sql("create table plain.a.dbo.p2 as select 1 as k")


def test_workspace_describe_and_stats(ws):
    conn = duckrun.connect(str(ws))
    loc = conn.sql("describe detail ws.sales.dbo.orders").fetchall()[0][3]
    assert loc.replace("\\", "/").endswith("sales.Lakehouse/Tables/dbo/orders")
    conn.sql("USE ws.hr.dbo")
    assert conn.sql("describe history orders").fetchall()
    rows = {(r[0], r[1], r[2]) for r in conn.get_stats().fetchall()}
    assert {("ws", "sales.dbo", "orders"), ("ws", "hr.dbo", "orders")} <= rows
    picked = {(r[1], r[2]) for r in conn.get_stats("ws.*.dbo.*").fetchall()}
    assert picked == {("sales.dbo", "orders"), ("hr.dbo", "orders")}


def test_is_workspace_root(ws):
    assert session_mod._is_workspace_root(f"abfss://tpch@{HOST}")
    assert session_mod._is_workspace_root(f"abfss://tpch@{HOST}/")
    assert not session_mod._is_workspace_root(f"abfss://tpch@{HOST}/data.Lakehouse/Tables")
    assert session_mod._is_workspace_root(str(ws))
    assert not session_mod._is_workspace_root(_tables_dir(ws, "sales"))


def test_list_workspace_lakehouses_onelake(monkeypatch):
    s = object.__new__(session_mod.DuckSession)
    monkeypatch.setattr(session_mod.remote, "list_delta_tables",
                        lambda root, schema, so: ["data.Lakehouse", "x.SynapseNotebook", "raw.Lakehouse"])
    assert s._list_workspace_lakehouses(f"abfss://tpch@{HOST}", None) == {
        "data": f"abfss://tpch@{HOST}/data.Lakehouse/Tables",
        "raw": f"abfss://tpch@{HOST}/raw.Lakehouse/Tables",
    }


def test_list_workspace_lakehouses_guid(monkeypatch):
    import duckrun.workspace as workspace_mod

    class _Ws:
        def list_lakehouses(self):
            return [{"displayName": "b", "id": "id-b"}, {"displayName": "a", "id": "id-a"}]

    monkeypatch.setattr(workspace_mod, "workspace", lambda ws: _Ws())
    g = "1c52481c-0523-4a5a-bbde-fdc932bd77c2"
    s = object.__new__(session_mod.DuckSession)
    assert s._list_workspace_lakehouses(f"abfss://{g}@{HOST}", None) == {
        "a": f"abfss://{g}@{HOST}/id-a/Tables",
        "b": f"abfss://{g}@{HOST}/id-b/Tables",
    }


@pytest.mark.parametrize("sql, parts", [
    ("insert into t values (1)", ["t"]),
    ("insert into s.t select 1", ["s", "t"]),
    ("delete from c.s.t where id = 1", ["c", "s", "t"]),
    ("merge into ws.lh.dbo.t as x using y on x.id = y.id when matched then delete",
     ["ws", "lh", "dbo", "t"]),
    ("vacuum ws.lh.dbo.t", ["ws", "lh", "dbo", "t"]),
    ("restore table ws.lh.dbo.t to version as of 1", ["ws", "lh", "dbo", "t"]),
    ("insert with schema evolution into ws.lh.dbo.t select 1", ["ws", "lh", "dbo", "t"]),
    ('create or replace table ws.lh."my.s".t as select 1', ["ws", "lh", "my.s", "t"]),
    ("select 1", None),
])
def test_dml_target_parts(sql, parts):
    assert dml_target_parts(sql) == parts
