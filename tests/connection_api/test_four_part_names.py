"""4-part names: a lakehouse attached at ``<ws>/<lh>.Lakehouse/Tables`` also answers to
``ws.lh.schema.table`` (DuckDB >= 2.0.1 nested schemas), next to its usual ``lh.schema.table``.

Nothing else changes: the lakehouse is connected/attached exactly as before, the 4-part name is read
off its path, and every write still goes DuckDB -> Arrow -> delta-rs into that lakehouse's root.
"""
import deltalake
import pytest

import duckrun
import duckrun.session as session_mod

HOST = "onelake.dfs.fabric.microsoft.com"
G1 = "1c52481c-0523-4a5a-bbde-fdc932bd77c2"
G2 = "7cb28f47-65bd-4aac-96c0-f7dd4eeb40ff"


def _tables(tmp_path, lh):
    d = tmp_path / "ws" / f"{lh}.Lakehouse" / "Tables"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def sales(tmp_path):
    root = _tables(tmp_path, "sales")
    c = duckrun.connect(str(root), read_only=False)
    c.sql("create table orders as select 1 as id, 10 as amt")
    c.close()
    return root


def test_path_alias():
    assert session_mod._path_alias(f"abfss://tpch@{HOST}/data.Lakehouse/Tables") == ("tpch", "data")
    assert session_mod._path_alias(f"abfss://tpch@{HOST}/ref.Warehouse/Tables") == ("tpch", "ref")
    assert session_mod._path_alias(f"abfss://{G1}@{HOST}/{G2}/Tables") == (G1, G2)
    assert session_mod._path_alias("/data/ws/sales.Lakehouse/Tables") == ("ws", "sales")
    assert session_mod._path_alias(r"C:\data\ws\sales.Lakehouse\Tables") == ("ws", "sales")
    assert session_mod._path_alias("/data/warehouse") is None
    assert session_mod._path_alias(f"abfss://tpch@{HOST}/data.Lakehouse/Files") is None
    assert session_mod._path_alias("s3://bucket/ws/sales.Lakehouse/Tables") is None


def test_reads_by_both_names(sales):
    conn = duckrun.connect(str(sales))
    assert conn.sql("select amt from ws.sales.dbo.orders").fetchall() == [(10,)]
    assert conn.sql("select amt from sales.dbo.orders").fetchall() == [(10,)]


def test_writes_by_four_part_name(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("create table ws.sales.dbo.t as select 1 as id, 'a' as v")
    conn.sql("insert into ws.sales.dbo.t values (2, 'b')")
    conn.sql("update ws.sales.dbo.t set v = 'B' where id = 2")
    conn.sql("delete from ws.sales.dbo.t where id = 1")
    conn.sql("merge into ws.sales.dbo.t as t using (select 3 as id, 'c' as v) as s on t.id = s.id "
             "when matched then update set v = s.v "
             "when not matched then insert (id, v) values (s.id, s.v)")
    want = [(2, "B"), (3, "c")]
    assert conn.sql("select id, v from ws.sales.dbo.t order by id").fetchall() == want
    assert conn.sql("select id, v from sales.dbo.t order by id").fetchall() == want   # its twin
    assert deltalake.DeltaTable.is_deltatable(str(sales / "dbo" / "t"))
    assert duckrun.connect(str(sales)).sql("select count(*) from dbo.t").fetchall() == [(2,)]
    conn.sql("drop table ws.sales.dbo.t")
    for name in ("ws.sales.dbo.t", "sales.dbo.t"):
        with pytest.raises(duckrun_errors()):
            conn.sql(f"select * from {name}").fetchall()


def duckrun_errors():
    import duckdb
    return duckdb.Error


def test_three_part_write_shows_under_four_parts(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("create table sales.mart.m as select 'x' as v")
    assert conn.sql("select v from ws.sales.mart.m").fetchall() == [("x",)]


def test_use_four_part_then_bare_names(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("USE ws.sales.dbo")
    conn.sql("create table u as select 5 as n")
    conn.sql("insert into u values (6)")
    assert conn.sql("select sum(n) from u").fetchall() == [(11,)]
    assert conn.sql("select sum(n) from sales.dbo.u").fetchall() == [(11,)]
    assert deltalake.DeltaTable.is_deltatable(str(sales / "dbo" / "u"))
    assert conn.root_path.replace("\\", "/").endswith("sales.Lakehouse/Tables")
    with pytest.raises(ValueError, match="ambiguous"):
        conn.sql("insert into dbo.u values (7)")


def test_read_only_gate_through_the_alias(sales):
    conn = duckrun.connect(str(sales))                  # read-only by default
    with pytest.raises(PermissionError):
        conn.sql("create table ws.sales.dbo.z as select 1 as k")


def test_two_lakehouses_of_one_workspace(sales, tmp_path):
    hr = _tables(tmp_path, "hr")
    c = duckrun.connect(str(hr), read_only=False)
    c.sql("create table orders as select 2 as id, 20 as amt")
    c.close()
    conn = duckrun.connect(str(sales), read_only=False)
    conn.attach(str(hr))
    assert conn.sql("select amt from ws.hr.dbo.orders").fetchall() == [(20,)]
    assert conn.sql("select amt from ws.sales.dbo.orders").fetchall() == [(10,)]
    conn.sql("insert into ws.hr.dbo.orders values (3, 30)")
    assert conn.sql("select count(*) from hr.dbo.orders").fetchall() == [(2,)]
    assert conn.sql("select count(*) from ws.sales.dbo.orders").fetchall() == [(1,)]


def test_unknown_four_part_name(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    with pytest.raises(ValueError, match="no attached lakehouse is ws.nope"):
        conn.sql("create table ws.nope.dbo.t as select 1 as k")


def test_plain_local_lakehouse_has_no_four_part_name(tmp_path):
    root = tmp_path / "plain"
    root.mkdir()
    conn = duckrun.connect(str(root), read_only=False)
    conn.sql("create table t as select 1 as k")
    assert conn._aliases == {}
    assert conn.sql("select k from plain.dbo.t").fetchall() == [(1,)]


def test_quoted_four_part_name_with_dashes(tmp_path):
    # A GUID (or any name with `-`) is a quoted identifier; the write router must keep it whole.
    root = tmp_path / "my-ws" / "sales-eu.Lakehouse" / "Tables"
    root.mkdir(parents=True)
    conn = duckrun.connect(str(root), read_only=False)
    conn.sql('create table "my-ws"."sales-eu".dbo.t as select 1 as k')
    conn.sql('insert into "my-ws"."sales-eu".dbo.t values (2)')
    assert conn.sql('select sum(k) from "my-ws"."sales-eu".dbo.t').fetchall() == [(3,)]
    assert deltalake.DeltaTable.is_deltatable(str(root / "dbo" / "t"))


# -- user views are session scratch, not tables (#87) ----------------------------------------------
def _twin(conn, name):
    return conn.sql("select count(*) from duckdb_views() where database_name = 'ws' "
                    f"and view_name = '{name}'").fetchall()[0][0]


def test_user_view_survives_refresh_and_gets_no_twin(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("create view v as select * from orders")
    conn.refresh()
    assert conn.sql("select amt from ws.sales.dbo.orders").fetchall() == [(10,)]
    assert conn.sql("select amt from sales.dbo.orders").fetchall() == [(10,)]
    assert conn.sql("select amt from v").fetchall() == [(10,)]
    assert _twin(conn, "v") == 0
    assert conn._cat_tables("dbo") == ["orders"]


def test_writes_that_refresh_after_a_user_view(sales):
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("create view v as select * from orders")
    conn.sql("insert into orders values (2, 20)")
    conn.sql("create table t2 as select 3 as id")
    assert conn.sql("select sum(amt) from ws.sales.dbo.orders").fetchall() == [(30,)]
    assert conn.sql("select id from ws.sales.dbo.t2").fetchall() == [(3,)]
    assert conn.sql("select sum(amt) from v").fetchall() == [(30,)]
    assert sorted(conn._cat_tables("dbo")) == ["orders", "t2"]


def test_user_view_over_a_delta_scan_elsewhere(sales, tmp_path):
    # The bench shape: a view over another lakehouse's table, by path.
    other = _tables(tmp_path, "ref")
    o = duckrun.connect(str(other), read_only=False)
    o.sql("create table r as select 7 as x")
    o.close()
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql(f"create or replace view fx as select * from "
             f"delta_scan('{(other / 'dbo' / 'r').as_posix()}')")
    conn.refresh()
    conn.sql("insert into orders values (2, 20)")
    assert conn.sql("select x from fx").fetchall() == [(7,)]
    assert _twin(conn, "fx") == 0
    assert [t[2] for t in conn._resolve_stats_targets(None)] == ["orders"]


def test_user_view_on_a_plain_root(tmp_path):
    root = tmp_path / "plain"
    root.mkdir()
    conn = duckrun.connect(str(root), read_only=False)
    conn.sql("create table t as select 1 as k")
    conn.sql("create view v as select k + 1 as k from t")
    conn.refresh()
    conn.sql("insert into t values (2)")
    assert conn.sql("select sum(k) from v").fetchall() == [(5,)]
    assert conn._cat_tables("dbo") == ["t"]


def test_refresh_keeps_four_part_twins_in_step_without_rebuilding_them(sales):
    # The 4-part twins follow the same rule as the 3-part views: a refresh adds the new table's
    # twin and drops a gone one's, and re-creates none of the others.
    import shutil
    conn = duckrun.connect(str(sales), read_only=False)
    conn.sql("create table t2 as select 2 as id")
    made = []
    real = conn.con.execute

    class _Spy:
        def __getattr__(self, name):
            return getattr(real.__self__, name)

        def execute(self, sql, *a, **k):
            if "CREATE OR REPLACE VIEW" in sql:
                made.append(sql.split(" AS ", 1)[0])
            return real(sql, *a, **k)

    conn.con = _Spy()
    conn.refresh(quiet=True)
    assert made == []
    o = duckrun.connect(str(sales), read_only=False)
    o.sql("create table t3 as select 3 as id")
    o.close()
    shutil.rmtree(sales / "dbo" / "t2")
    conn.refresh(quiet=True)
    assert len(made) == 2 and all("t3" in m for m in made)     # 3-part view + its 4-part twin
    assert conn.sql("select id from ws.sales.dbo.t3").fetchall() == [(3,)]
    assert _twin(conn, "t2") == 0
    assert conn.sql("select amt from ws.sales.dbo.orders").fetchall() == [(10,)]


def test_a_table_written_elsewhere_reads_by_four_part_name_without_refresh(sales):
    conn = duckrun.connect(str(sales))
    o = duckrun.connect(str(sales), read_only=False)
    o.sql("create table late as select 5 as x")
    o.close()
    assert conn.sql("select x from ws.sales.dbo.late").fetchall() == [(5,)]
    assert conn.sql("select x from sales.dbo.late").fetchall() == [(5,)]   # its twin came along
