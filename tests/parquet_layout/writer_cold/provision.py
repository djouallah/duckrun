"""Create and destroy the throwaway Fabric items this benchmark measures in.

Every run builds its own lakehouses and semantic models and deletes them at the end. That is not
tidiness — it is the measurement design. A **fresh semantic model is the cold guarantee**: nothing
short of a new dataset reliably empties the resident column store, so "cold" only means anything if
the model did not exist a minute ago. Fresh items also give fresh GUIDs, which is what makes the CU
read unambiguous (the Capacity Metrics app aggregates per item GUID, and deleted items keep their
rows forever, so teardown costs us no measurement).

`create` is idempotent via duckrun's own Workspace.create_lakehouse. `drop` is NOT delegated:
duckrun's `_delete_item` is best-effort by contract (it swallows non-2xx and logs a warning), which
in CI means a leaked lakehouse would pass silently and quietly bill storage forever. So the delete
here goes through `_http_request` and asserts the status itself.

Usage:  provision.py create        (emits LH ids to $GITHUB_ENV)
        provision.py drop          (deletes models first, then lakehouses; loud on failure)

Env: WS_ID (resolve_env), LH_DELTARS / LH_DUCKDB (names, defaulted), MODEL_DELTARS / MODEL_DUCKDB.
"""
import os
import sys

import duckrun
from duckrun import auth
from duckrun.fabric_remote import _FABRIC_API, _http_request

LH = {
    "deltars": os.environ.get("LH_DELTARS") or "wab_deltars",
    "duckdb": os.environ.get("LH_DUCKDB") or "wab_duckdb",
}
MODELS = {
    "deltars": os.environ.get("MODEL_DELTARS") or "writer_cold_deltars",
    "duckdb": os.environ.get("MODEL_DUCKDB") or "writer_cold_duckdb",
}
WS_ID = os.environ["WS_ID"]
# Which writer arms this run measures. delta-rs is a settled baseline (probe_duid 0.9-2.3s across
# nine runs), so re-provisioning and re-measuring it every time is pure cost. Teardown deliberately
# still sweeps BOTH names - a lakehouse leaked by an earlier run bills forever.
ARMS = [a.strip() for a in (os.environ.get("ARMS") or "deltars,duckdb").split(",") if a.strip()]
_unknown = set(ARMS) - set(LH)
if _unknown:
    raise SystemExit(f"provision: unknown arm(s) {sorted(_unknown)}; known: {sorted(LH)}")


def _ws():
    # A Workspace caches its control-plane token for the life of the handle, and this job runs long
    # enough to outlive one. Teardown builds a new handle rather than reusing the create-time token.
    return duckrun.workspace(WS_ID)


def create():
    ws = _ws()
    out = {}
    for key, name in ((k, LH[k]) for k in ARMS):
        lh_id = ws.create_lakehouse(name, schemas=True)
        out[f"LH_ID_{key.upper()}"] = lh_id
        out[f"LH_NAME_{key.upper()}"] = name
        out[f"TABLES_{key.upper()}"] = (
            f"abfss://{ws.id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables")
        print(f"lakehouse {name} -> {lh_id}", flush=True)
    gh = os.environ.get("GITHUB_ENV")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write("".join(f"{k}={v}\n" for k, v in out.items()))
    for k, v in out.items():
        print(f"{k}={v}")


def _delete(ws, token, item_id, label):
    """Delete one item and FAIL LOUDLY. duckrun's own helper deliberately swallows errors."""
    resp = _http_request("DELETE", f"{_FABRIC_API}/workspaces/{ws.id}/items/{item_id}", token=token)
    if resp.status_code not in (200, 202, 204, 404):
        raise SystemExit(f"teardown FAILED to delete {label} ({item_id}): "
                         f"{resp.status_code} {resp.text[:200]}")
    print(f"deleted {label} ({item_id})", flush=True)


def drop():
    ws = _ws()
    token = auth.get_fabric_token()
    # Models before lakehouses: a model whose Direct Lake source vanished is harmless but confusing,
    # and deleting in this order keeps the workspace legible if teardown dies halfway.
    wanted_models = set(MODELS.values())
    for it in ws.list_items("semanticModels"):
        if it.get("displayName") in wanted_models:
            _delete(ws, token, it["id"], f"semanticModel {it['displayName']}")
    wanted_lh = set(LH.values())
    for it in ws.list_items("lakehouses"):
        if it.get("displayName") in wanted_lh:
            _delete(ws, token, it["id"], f"lakehouse {it['displayName']}")

    # Verify rather than trust: re-list and fail if anything we created is still standing.
    left = [it["displayName"] for it in ws.list_items("semanticModels")
            if it.get("displayName") in wanted_models]
    left += [it["displayName"] for it in ws.list_items("lakehouses")
             if it.get("displayName") in wanted_lh]
    if left:
        raise SystemExit(f"teardown INCOMPLETE — still present: {left}")
    print("teardown verified: nothing left behind", flush=True)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "create"
    if cmd == "create":
        create()
    elif cmd == "drop":
        drop()
    else:
        raise SystemExit(f"provision.py: unknown command {cmd!r} (create|drop)")
