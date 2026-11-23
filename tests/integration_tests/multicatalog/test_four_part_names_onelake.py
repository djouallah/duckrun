"""LIVE OneLake smoke for 4-part names: a lakehouse connected as before also answers to
``ws.lh.schema.table``, by friendly name and by GUIDs. Writes go to an isolated schema.
Needs the CI's OIDC identity (duckrun self-acquires the tokens)."""
import os
import uuid

import pytest

import duckrun

WS_NAME, LH_NAME = "tpch", "duckrun"
WS_ID, LH_ID = "1c52481c-0523-4a5a-bbde-fdc932bd77c2", "7cb28f47-65bd-4aac-96c0-f7dd4eeb40ff"

pytestmark = pytest.mark.skipif(not os.environ.get("AZURE_CLIENT_ID"),
                                reason="live OneLake: needs the CI's OIDC identity")


@pytest.mark.parametrize("path, prefix", [
    (f"{WS_NAME}/{LH_NAME}.Lakehouse", f"{WS_NAME}.{LH_NAME}"),
    (f"{WS_ID}/{LH_ID}", f'"{WS_ID}"."{LH_ID}"'),
])
def test_four_part_read_and_write(path, prefix):
    schema = f"four_part_smoke_{uuid.uuid4().hex[:8]}"
    t = f"{prefix}.{schema}.t"
    conn = duckrun.connect(path, schema=schema, read_only=False)
    conn.sql(f"CREATE TABLE {t} AS SELECT 1 AS id, 'a' AS v")
    conn.sql(f"MERGE INTO {t} AS t USING (SELECT 1 AS id, 'b' AS v UNION ALL SELECT 2, 'c') AS s "
             f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
             f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)")
    assert conn.sql(f"SELECT id, v FROM {t} ORDER BY id").fetchall() == [(1, "b"), (2, "c")]
    conn.sql(f"DROP TABLE {t}")
