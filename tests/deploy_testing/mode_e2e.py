"""End-to-end check of ``deploy(mode=...)`` against real Fabric — the four source x mode combinations.

The mocked suite (``tests/fabric_remote/test_workspace.py``) pins the TMSL duckrun emits; only Fabric
can say whether it ACCEPTS it. So this builds a one-table ``model.bim`` from a live Delta table, deploys
it four ways — Direct Lake and DirectQuery, over a lakehouse and over a warehouse — and runs a DAX query
against each deployed model through the Power BI ``executeQueries`` endpoint. A Direct Lake model here
carries ``directLakeBehavior: directLakeOnly``, so a query it cannot serve from Delta FAILS rather than
falling back to the SQL endpoint: an answer means Direct Lake really served it.

Reads a lakehouse + warehouse that already hold the same table (an aemo-style dbt project builds both);
the only things it writes are the four ``zz_mode_*`` semantic models it owns and overwrites::

    python tests/deploy_testing/mode_e2e.py            # defaults below
    MODE_E2E_WORKSPACE=other python tests/deploy_testing/mode_e2e.py

Manual, like the rest of ``deploy_testing`` — it needs a real workspace, so no CI job runs it.
"""
import json
import os
import sys

import requests

import duckrun
from duckrun.auth import get_powerbi_token

WS = os.environ.get("MODE_E2E_WORKSPACE", "testing")
LAKEHOUSE = os.environ.get("MODE_E2E_LAKEHOUSE", "dbt_delta")
WAREHOUSE = os.environ.get("MODE_E2E_WAREHOUSE", "dbt_dwh")
SCHEMA = os.environ.get("MODE_E2E_SCHEMA", "mart")
TABLE = os.environ.get("MODE_E2E_TABLE", "dim_calendar")

# The AUTHORED model: Direct Lake on OneLake over placeholder GUIDs, as a Desktop export looks. Every
# deploy below rewrites it — the point being that one authored bim ships in either mode.
SRC_WS, SRC_ITEM = "00000000-0000-0000-0000-000000000000", "11111111-1111-1111-1111-111111111111"

TMSL_TYPE = {"VARCHAR": "string", "BIGINT": "int64", "INTEGER": "int64", "SMALLINT": "int64",
             "DOUBLE": "double", "FLOAT": "double", "BOOLEAN": "boolean", "DATE": "dateTime",
             "TIMESTAMP": "dateTime", "TIMESTAMP WITH TIME ZONE": "dateTime"}


def columns():
    """``(name, tmsl_type)`` per column, read off the live Delta schema so the model matches reality."""
    conn = duckrun.connect(f"{WS}/{LAKEHOUSE}.Lakehouse/{SCHEMA}")
    out = []
    for name, dtype, *_ in conn.sql(f"describe select * from {SCHEMA}.{TABLE}").fetchall():
        tmsl = TMSL_TYPE.get(str(dtype).upper())
        if tmsl is None and str(dtype).upper().startswith("DECIMAL"):
            tmsl = "decimal"
        if tmsl is None:
            print(f"  ! skipping column {name} ({dtype}) — no TMSL mapping")
            continue
        out.append((name, tmsl))
    return out


def authored_bim(cols):
    url = f"https://onelake.dfs.fabric.microsoft.com/{SRC_WS}/{SRC_ITEM}"
    return json.dumps({"compatibilityLevel": 1604, "model": {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "expressions": [{"name": "DirectLake", "kind": "m", "expression": [
            "let",
            f'    Source = AzureStorage.DataLake("{url}", [HierarchicalNavigation=true])',
            "in",
            "    Source"]}],
        "tables": [{
            "name": TABLE,
            "columns": [{"name": c, "dataType": t, "sourceColumn": c, "summarizeBy": "none"}
                        for c, t in cols],
            "measures": [{"name": "Rows", "expression": f"COUNTROWS('{TABLE}')"}],
            "partitions": [{"name": TABLE, "mode": "directLake", "source": {
                "type": "entity", "entityName": TABLE, "schemaName": SCHEMA,
                "expressionSource": "DirectLake"}}]}]}}, indent=2)


def dax(ws_id, item_id, query):
    """The rows a deployed model returns for ``query`` — or the error text, which is the failure."""
    resp = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets/{item_id}/executeQueries",
        headers={"Authorization": f"Bearer {get_powerbi_token()}"},
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
        timeout=180)
    if resp.status_code >= 400:
        return f"HTTP {resp.status_code}: {resp.text[:300]}"
    return resp.json()["results"][0]["tables"][0]["rows"]


def main():
    ws = duckrun.workspace(WS)
    cols = columns()
    print(f"source table {SCHEMA}.{TABLE}: {len(cols)} columns")
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zz_mode_model.bim")
    with open(path, "w", encoding="utf-8") as f:
        f.write(authored_bim(cols))

    plan = [("zz_mode_lh_dl", {"lakehouse": LAKEHOUSE, "mode": "direct_lake"}),
            ("zz_mode_lh_dq", {"lakehouse": LAKEHOUSE, "mode": "direct_query"}),
            ("zz_mode_wh_dl", {"warehouse": WAREHOUSE, "mode": "direct_lake"}),
            ("zz_mode_wh_dq", {"warehouse": WAREHOUSE, "mode": "direct_query"})]
    failures = []
    for name, kwargs in plan:
        print(f"\n=== {name}  {kwargs}")
        try:
            item_id = ws.deploy(path, name=name, overwrite=True, **kwargs)
        except Exception as exc:
            print(f"  DEPLOY FAILED: {type(exc).__name__}: {str(exc)[:400]}")
            failures.append(name)
            continue
        rows = dax(ws.id, item_id, f"EVALUATE ROW(\"n\", COUNTROWS('{TABLE}'))")
        print(f"  deployed {item_id} → DAX {rows}")
        if isinstance(rows, str):
            failures.append(name)
    os.remove(path)
    print("\nfailures:", failures or "none")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
