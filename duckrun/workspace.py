"""A Fabric workspace handle for control-plane item management.

``connect()`` is storage-neutral (local, ``s3://``, ``gs://``, ``abfss://``) and points *into* an
existing lakehouse's ``Tables/``. Workspace-level operations are the opposite: Fabric-only, control-
plane, with no lakehouse to point at yet. So they live here, on a separate workspace-scoped handle
rather than on the :class:`~duckrun.session.DuckSession` that ``connect()`` returns::

    import duckrun
    ws = duckrun.workspace("My Workspace")            # name or GUID
    ws.create_lakehouse("bronze")                     # provision an empty container (by name)
    ws.deploy("etl.ipynb")                            # deploy a file artifact (a notebook)
    ws.deploy("model.bim")                            # deploy + refresh a semantic model

The Fabric REST plumbing (auth, retry, workspace resolution, LRO polling, item create, refresh) is
reused as-is from :mod:`duckrun.fabric_remote`.
"""
import json
import os
import re
from typing import Dict, List, Optional
from urllib.parse import urlsplit

# A Direct-Lake-on-OneLake model.bim references its lakehouse by a OneLake URL carrying the
# workspace + lakehouse GUIDs, e.g. onelake.dfs.fabric.microsoft.com/<ws-guid>/<lakehouse-guid>/...
_ONELAKE_REF = re.compile(
    r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-fA-F-]{36})/([0-9a-fA-F-]{36})")

from .auth import get_fabric_token, get_powerbi_token
from .fabric_remote import (
    _FABRIC_API,
    RemoteRunError,
    _http_request,
    _resolve_workspace_id,
    _await_lro_item_id,
    _create_notebook,
    _create_semantic_model,
    _refresh_semantic_model,
    _create_pipeline,
    _delete_item,
)

# Which Fabric item each source extension deploys to: (item-collection endpoint, human label).
_ARTIFACTS = {
    ".ipynb": ("notebooks", "notebook"),
    ".bim": ("semanticModels", "semantic model"),
    ".json": ("dataPipelines", "data pipeline"),
}


def _is_pipeline_json(obj) -> bool:
    """Whether ``obj`` (parsed JSON) is a Fabric data-pipeline definition: a ``pipeline-content.json``
    is ``{"properties": {"activities": [...]}}``. Guards against deploying a non-pipeline ``.json``
    (a variables/config file, a ``.bim`` exported as ``.json``) as a broken pipeline."""
    return isinstance(obj, dict) and isinstance(obj.get("properties"), dict) \
        and "activities" in obj["properties"]


def _basename(source: str) -> str:
    """The filename of ``source``, whether it's a local path or an ``http(s)`` URL."""
    path = urlsplit(source).path if source.startswith(("http://", "https://")) else source
    return os.path.basename(path)


def _read_source(source: str) -> bytes:
    """The raw bytes of ``source`` — fetched over HTTP for a URL, else read from the local file."""
    if source.startswith(("http://", "https://")):
        import requests
        resp = requests.get(source, timeout=60)
        resp.raise_for_status()
        return resp.content
    with open(source, "rb") as f:
        return f.read()


class Workspace:
    """A Fabric control-plane handle scoped to one workspace.

    The Fabric token defaults to :func:`duckrun.auth.get_fabric_token`; pass ``token`` to inject one
    (mirrors ``RemoteRunner(fabric_token=...)``, and is the seam tests use).
    """

    def __init__(self, workspace: str, token: Optional[str] = None):
        self.name = workspace
        self._token = token or get_fabric_token()
        self.id = _resolve_workspace_id(self._token, workspace)

    def _items(self, kind: str) -> List[Dict]:
        """Every item of ``kind`` (e.g. ``"lakehouses"`` / ``"notebooks"`` / ``"semanticModels"``)."""
        resp = _http_request("GET", f"{_FABRIC_API}/workspaces/{self.id}/{kind}", token=self._token)
        resp.raise_for_status()
        return resp.json().get("value", [])

    def list_lakehouses(self) -> List[Dict]:
        """Every lakehouse in the workspace as ``[{"displayName": ..., "id": ...}, ...]``."""
        return self._items("lakehouses")

    def create_lakehouse(self, name: str, schemas: bool = True) -> str:
        """Ensure a lakehouse named ``name`` exists; return its item id.

        Idempotent: if one already exists it is returned unchanged (no create). ``schemas=True``
        makes it schema-enabled. Raises :class:`RemoteRunError` on a real API failure.
        """
        for lh in self._items("lakehouses"):
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

    def deploy(self, source: str, lakehouse: Optional[str] = None,
               name: Optional[str] = None, overwrite: bool = False) -> str:
        """Deploy a file artifact to the workspace; return its item id.

        ``source`` is a local file path or an ``http(s)`` URL (a raw file URL). The item type is keyed
        off the extension — ``.ipynb`` → notebook, ``.bim`` → semantic model, ``.json`` → data pipeline
        — and the name defaults to the filename stem (overridable via ``name``). A ``.json`` is
        deployed verbatim; a semantic model is also **refreshed** after deploy (a *reframe* onto the
        latest Delta data for Direct Lake), so ``deploy`` returns only once it is live.

        ``lakehouse`` (a lakehouse name in this workspace) points a **Direct Lake** ``.bim`` at that
        lakehouse: duckrun rewrites the OneLake workspace/lakehouse GUIDs baked into the model so it
        connects to the right place — no GUIDs or connection strings to edit by hand. It is inferred
        when the model already targets a lakehouse in this workspace, or the workspace has exactly one;
        with several you must name it. Ignored for ``.ipynb`` / ``.json``.

        Not idempotent: if an item of that name already exists it is replaced only when
        ``overwrite=True``, otherwise this raises.
        """
        stem, ext = os.path.splitext(_basename(source))
        ext = ext.lower()
        if ext not in _ARTIFACTS:
            raise RemoteRunError(
                f"unsupported file type {ext!r}: deploy handles {', '.join(sorted(_ARTIFACTS))}")
        endpoint, label = _ARTIFACTS[ext]
        name = name or stem
        content = _read_source(source)
        if ext == ".json" and not _is_pipeline_json(json.loads(content)):
            raise RemoteRunError(
                f"{source!r} is not a Fabric data-pipeline definition "
                "(expected top-level {'properties': {'activities': ...}})")
        if ext == ".bim":
            content = self._repoint_bim(content, lakehouse, source)   # resolve before any delete

        existing = next((it for it in self._items(endpoint) if it.get("displayName") == name), None)
        if existing:
            if not overwrite:
                raise RemoteRunError(f"{label} {name!r} already exists; pass overwrite=True to replace")
            _delete_item(self._token, self.id, existing["id"])

        if ext == ".ipynb":
            return _create_notebook(self._token, self.id, name, json.loads(content))
        if ext == ".json":
            return _create_pipeline(self._token, self.id, name, content)
        # .bim → create the semantic model, then refresh/reframe it so it is live.
        item_id = _create_semantic_model(self._token, self.id, name, content)
        _refresh_semantic_model(get_powerbi_token(), self.id, item_id)
        return item_id

    def _repoint_bim(self, content: bytes, lakehouse: Optional[str], source: str) -> bytes:
        """Rewrite a Direct-Lake ``model.bim``'s OneLake workspace/lakehouse GUIDs to this workspace
        and the chosen lakehouse. If the model carries no OneLake reference it isn't Direct Lake, so
        it's returned unchanged (and an explicit ``lakehouse=`` then raises — nothing to point)."""
        text = content.decode("utf-8")
        m = _ONELAKE_REF.search(text)
        if not m:
            if lakehouse is not None:
                raise RemoteRunError(
                    f"{source!r} has no OneLake reference to point at lakehouse {lakehouse!r} "
                    "(is it a Direct Lake model?)")
            return content
        source_ws, source_lh = m.group(1), m.group(2)
        target_lh = self._resolve_lakehouse_id(lakehouse, source_lh)
        return text.replace(source_ws, self.id).replace(source_lh, target_lh).encode("utf-8")

    def _resolve_lakehouse_id(self, lakehouse: Optional[str], source_lh: str) -> str:
        """The target lakehouse id for a Direct Lake repoint. Named → looked up (or raise, listing the
        real names). Unnamed → the lakehouse the model already targets if it lives here, else the sole
        lakehouse, else raise asking which one."""
        lakehouses = self._items("lakehouses")
        names = [lh.get("displayName") for lh in lakehouses]
        if lakehouse is not None:
            match = next((lh for lh in lakehouses if lh.get("displayName") == lakehouse), None)
            if not match:
                raise RemoteRunError(f"lakehouse {lakehouse!r} not found in workspace; have: {names}")
            return match["id"]
        ids = {lh["id"] for lh in lakehouses}
        if source_lh in ids:
            return source_lh                    # already targets a lakehouse in this workspace — keep it
        if len(lakehouses) == 1:
            return lakehouses[0]["id"]
        if not lakehouses:
            raise RemoteRunError("no lakehouse in this workspace to point the model at")
        raise RemoteRunError(f"which lakehouse should the model use? pass lakehouse=; have: {names}")


def workspace(workspace: str, token: Optional[str] = None) -> Workspace:
    """A :class:`Workspace` handle for ``workspace`` (a name or a GUID)."""
    return Workspace(workspace, token=token)
