"""LIVE OneLake smoke for workspace catalogs: one session binds a whole Fabric workspace and
addresses its tables as ``ws.lakehouse.schema.table``.

Two ways in, both checked: the workspace GUID (lakehouse names come from the Fabric REST API) and
the friendly workspace name (OneLake lists ``<name>.Lakehouse`` folders). Writes go to an isolated
schema of one lakehouse. Needs the CI's OIDC identity (duckrun self-acquires the tokens)."""
import os
import uuid

import pytest

import duckrun

WS_ID = os.environ.get("WORKSPACE_ID", "1c52481c-0523-4a5a-bbde-fdc932bd77c2")
WS_NAME = os.environ.get("WORKSPACE_NAME", "tpch")
LAKEHOUSE = os.environ.get("WORKSPACE_LAKEHOUSE", "duckrun")
HOST = "onelake.dfs.fabric.microsoft.com"

pytestmark = pytest.mark.skipif(not os.environ.get("AZURE_CLIENT_ID"),
                                reason="live OneLake: needs the CI's OIDC identity")


def test_workspace_by_guid_reads_and_writes():
    conn = duckrun.connect(f"abfss://{WS_ID}@{HOST}", name="ws", read_only=False)
    assert LAKEHOUSE in conn._ws_lakehouses["ws"]
    schema = f"ws_catalog_smoke_{uuid.uuid4().hex[:8]}"
    t = f"ws.{LAKEHOUSE}.{schema}.t"
    conn.sql(f"CREATE TABLE {t} AS SELECT 1 AS id, 'a' AS v")
    conn.sql(f"MERGE INTO {t} AS t USING (SELECT 1 AS id, 'b' AS v UNION ALL SELECT 2, 'c') AS s "
             f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
             f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")
    assert conn.sql(f"SELECT id, v FROM {t} ORDER BY id").fetchall() == [(1, "b"), (2, "c")]
    conn.sql(f"USE ws.{LAKEHOUSE}.{schema}")
    assert conn.sql("SELECT count(*) FROM t").fetchall() == [(2,)]
    conn.sql(f"DROP TABLE {t}")


def test_workspace_by_name_reads():
    conn = duckrun.connect(f"abfss://{WS_NAME}@{HOST}")
    assert conn._current_catalog == WS_NAME
    assert LAKEHOUSE in conn._ws_lakehouses[WS_NAME]
    dbs = [d for d in conn._cat_databases(WS_NAME) if d.startswith(f"{LAKEHOUSE}.")]
    assert dbs, f"no schemas discovered in lakehouse {LAKEHOUSE}"
    tables = conn._cat_tables(dbs[0], WS_NAME)
    assert tables
    lh, sch = dbs[0].split(".", 1)
    conn.sql(f'SELECT * FROM "{WS_NAME}"."{lh}"."{sch}"."{tables[0]}" LIMIT 1').fetchall()
