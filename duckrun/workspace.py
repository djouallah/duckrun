"""A Fabric workspace handle for control-plane item management.

``connect()`` is storage-neutral (local, ``s3://``, ``gs://``, ``abfss://``) and points *into* an
existing lakehouse's ``Tables/``. Creating a lakehouse is the opposite: Fabric-only, control-plane,
and there is no lakehouse to point at yet. So it lives here, on a separate workspace-scoped handle
rather than on the :class:`~duckrun.session.DuckSession` that ``connect()`` returns::

    import duckrun
    ws = duckrun.workspace("My Workspace")            # name or GUID
    lh_id = ws.create_lakehouse("bronze")             # idempotent; returns the item id
    ws.list_lakehouses()

The Fabric REST plumbing (auth, retry, workspace resolution, LRO polling) is reused as-is from
:mod:`duckrun.fabric_remote`.
"""
from typing import Dict, List, Optional

from .auth import get_fabric_token
from .fabric_remote import (
    _FABRIC_API,
    RemoteRunError,
    _http_request,
    _resolve_workspace_id,
    _await_lro_item_id,
)


class Workspace:
    """A Fabric control-plane handle scoped to one workspace.

    The Fabric token defaults to :func:`duckrun.auth.get_fabric_token`; pass ``token`` to inject one
    (mirrors ``RemoteRunner(fabric_token=...)``, and is the seam tests use).
    """

    def __init__(self, workspace: str, token: Optional[str] = None):
        self.name = workspace
        self._token = token or get_fabric_token()
        self.id = _resolve_workspace_id(self._token, workspace)

    def list_lakehouses(self) -> List[Dict]:
        """Every lakehouse in the workspace as ``[{"displayName": ..., "id": ...}, ...]``."""
        resp = _http_request("GET", f"{_FABRIC_API}/workspaces/{self.id}/lakehouses", token=self._token)
        resp.raise_for_status()
        return resp.json().get("value", [])

    def create_lakehouse(self, name: str, schemas: bool = True) -> str:
        """Ensure a lakehouse named ``name`` exists; return its item id.

        Idempotent: if one already exists it is returned unchanged (no create). ``schemas=True``
        makes it schema-enabled. Raises :class:`RemoteRunError` on a real API failure.
        """
        for lh in self.list_lakehouses():
            if lh.get("displayName") == name:
                return lh["id"]
        body: Dict = {"displayName": name}
        if schemas:
            body["creationPayload"] = {"enableSchemas": True}
        resp = _http_request(
            "POST", f"{_FABRIC_API}/workspaces/{self.id}/lakehouses", token=self._token, json_body=body
        )
        if resp.status_code in (200, 201):
            return resp.json()["id"]
        if resp.status_code == 202:
            return _await_lro_item_id(self._token, resp)
        resp.raise_for_status()
        raise RemoteRunError(f"unexpected status {resp.status_code} creating lakehouse: {resp.text[:200]}")


def workspace(workspace: str, token: Optional[str] = None) -> Workspace:
    """A :class:`Workspace` handle for ``workspace`` (a name or a GUID)."""
    return Workspace(workspace, token=token)
