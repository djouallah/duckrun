"""LIVE OneLake: DML on partition values that DuckDB escapes in the folder name (a space, ``%``,
``/``, ``:`` — every timestamp partition). The remove a rewrite commits must name the live file
exactly as the log does; on OneLake the scan reports each file as an ``abfss://`` URL, so this is
the path that matching runs on. Writes go to an isolated schema, dropped at the end. Needs the CI's
OIDC identity (duckrun self-acquires the tokens)."""
import os
import uuid

import pytest

import duckrun

WS_ID, LH_ID = "1c52481c-0523-4a5a-bbde-fdc932bd77c2", "7cb28f47-65bd-4aac-96c0-f7dd4eeb40ff"

pytestmark = pytest.mark.skipif(not os.environ.get("AZURE_CLIENT_ID"),
                                reason="live OneLake: needs the CI's OIDC identity")


def run_partition_paths(conn, schema):
    conn.sql("""create table pv partitioned by (p, ts) as select * from (values
        (1, 'a b', TIMESTAMPTZ '2024-01-01 00:00:00+00'), (2, '50%', TIMESTAMPTZ '2024-01-01 00:00:00+00'),
        (3, 'k=v', TIMESTAMPTZ '2024-01-02 10:30:00+00'), (4, NULL, NULL)) t(id, p, ts)""")
    conn.sql("delete from pv where id = 1")
    conn.sql("update pv set id = 20 where p = '50%'")
    conn.sql("merge into pv using (select 3 id) s on pv.id = s.id when matched then delete")
    conn.sql("insert into pv values (5, 'a b', TIMESTAMPTZ '2024-01-01 00:00:00+00')")
    conn.sql("optimize pv")
    assert sorted(conn.sql("select id, p from pv").fetchall(), key=repr) == sorted(
        [(20, "50%"), (4, None), (5, "a b")], key=repr)
    import deltalake
    dt = deltalake.DeltaTable(conn._table_path(schema, "pv"),
                              storage_options=conn.storage_options)
    assert len(dt.file_uris()) == 3            # every rewritten file really left the table
    conn.sql("drop table pv")


def test_partition_paths_on_onelake():
    schema = f"partition_paths_{uuid.uuid4().hex[:8]}"
    conn = duckrun.connect(f"{WS_ID}/{LH_ID}", schema=schema, read_only=False)
    run_partition_paths(conn, schema)
