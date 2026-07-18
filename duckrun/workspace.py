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
import datetime
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
    _workspace_display_name,
    _await_lro_item_id,
    _create_notebook,
    _create_semantic_model,
    _refresh_semantic_model,
    _create_pipeline,
    _create_variable_library,
    _run_job_and_wait,
    _schedule_item,
)

_WEEKDAYS = {d[:3].lower(): d for d in
             ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")}
_FAR_FUTURE = "2099-12-31T23:59:00"      # schedule end — effectively "runs forever"


def _interval_minutes(every) -> int:
    """``"30m"`` → 30, ``"2h"`` → 120, ``"1d"`` → 1440, a bare number → minutes."""
    s = str(every).strip().lower()
    unit = {"m": 1, "h": 60, "d": 1440}.get(s[-1:])
    return int(s[:-1]) * unit if unit else int(s)


def _times(v) -> List[str]:
    return [v] if isinstance(v, str) else list(v)


def _schedule_config(every, daily, weekly, at, tz) -> dict:
    """A Fabric schedule ``configuration`` from one cadence knob. Fabric's scheduler is interval /
    daily / weekly (not free-form cron); pass exactly one of ``every`` / ``daily`` / ``weekly`` — none
    defaults to daily at midnight. ``startDateTime`` is now (UTC), so it begins immediately."""
    if sum(x is not None for x in (every, daily, weekly)) > 1:
        raise RemoteRunError("pass only one of every= / daily= / weekly=")
    base = {"startDateTime": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "endDateTime": _FAR_FUTURE, "localTimeZoneId": tz}
    if every is not None:
        return {**base, "type": "Cron", "interval": _interval_minutes(every)}
    if weekly is not None:
        days = [_WEEKDAYS.get(str(d)[:3].lower()) for d in _times(weekly)]
        if None in days:
            raise RemoteRunError(f"invalid weekday in {weekly!r}; use Mon..Sun")
        return {**base, "type": "Weekly", "weekdays": days, "times": _times(at or "00:00")}
    return {**base, "type": "Daily", "times": _times(daily or "00:00")}

# Source extensions deploy handles (a ``.json`` is a pipeline or a variable library, told apart by
# its content — see ``_json_artifact``).
_DEPLOY_EXT = (".ipynb", ".bim", ".json")

# Item collections that ``run`` can execute, and the Fabric job type for each.
_RUNNABLE = {"notebooks": "RunNotebook", "dataPipelines": "Pipeline"}
# Which collections to look in for a given source extension when running by file name.
_RUN_EXT = {".ipynb": ["notebooks"], ".json": ["dataPipelines"]}


def _is_pipeline_json(obj) -> bool:
    """Whether ``obj`` (parsed JSON) is a Fabric data-pipeline definition — a ``pipeline-content.json``
    is ``{"properties": {"activities": [...]}}``."""
    return isinstance(obj, dict) and isinstance(obj.get("properties"), dict) \
        and "activities" in obj["properties"]


def _is_variable_library_json(obj) -> bool:
    """Whether ``obj`` (parsed JSON) is a variable-library ``variables.json`` — ``{"variables": [...]}``."""
    return isinstance(obj, dict) and isinstance(obj.get("variables"), list)


def _apply_variables(obj: dict, overrides: Dict) -> bytes:
    """A ``variables.json`` with each named variable's ``value`` replaced from ``overrides`` — how
    you set environment-specific values (lakehouse name, workspace id, limits) at deploy time. An
    override for a name the library doesn't define raises (catches a typo, rather than silently
    deploying a stale value)."""
    by_name = {v["name"]: v for v in obj.get("variables", [])}
    for key, value in overrides.items():
        if key not in by_name:
            raise RemoteRunError(f"variable {key!r} not in the library; has: {list(by_name)}")
        by_name[key]["value"] = value
    return json.dumps(obj).encode("utf-8")


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
        self._display_name: Optional[str] = None

    @property
    def display_name(self) -> str:
        """This workspace's display name, resolved from its id (the reverse of the name→GUID
        resolution ``workspace()`` does). Connection strings that identify a workspace by name — XMLA
        data sources, ``powerbi://…/myorg/<name>`` — need this rather than the GUID. Cached."""
        if self._display_name is None:
            self._display_name = _workspace_display_name(self._token, self.id)
        return self._display_name

    def lakehouse_id(self, name: str) -> str:
        """The lakehouse id for a lakehouse named ``name`` in this workspace; raises listing the real
        names if there's no match."""
        for lh in self._items("lakehouses"):
            if lh.get("displayName") == name:
                return lh["id"]
        have = ", ".join(sorted(lh.get("displayName", "?") for lh in self._items("lakehouses"))) or "(none)"
        raise RemoteRunError(f"lakehouse {name!r} not found in workspace {self.id}; have: {have}")

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

    def deploy(self, source: str, lakehouse: Optional[str] = None, variables: Optional[Dict] = None,
               name: Optional[str] = None, overwrite: bool = False, notebook: Optional[str] = None) -> str:
        """Deploy a file artifact to the workspace; return its item id.

        ``source`` is a local file path or an ``http(s)`` URL (a raw file URL). The item type is keyed
        off the extension — ``.ipynb`` → notebook, ``.bim`` → semantic model, ``.json`` → a data
        pipeline or a variable library (told apart by content) — and the name defaults to the filename
        stem (overridable via ``name``). A semantic model is also **refreshed** after deploy (a
        *reframe* onto the latest Delta data for Direct Lake), so ``deploy`` returns only once it is live.

        ``lakehouse`` (a lakehouse name in this workspace) points a **Direct Lake** ``.bim`` at that
        lakehouse: duckrun rewrites the OneLake workspace/lakehouse GUIDs baked into the model so it
        connects to the right place — no GUIDs or connection strings to edit by hand. It is inferred
        when the model already targets a lakehouse in this workspace, or the workspace has exactly one;
        with several you must name it.

        ``notebook`` (a notebook name in this workspace) points a **data pipeline**'s notebook
        activities at that notebook: duckrun rewrites the ``notebookId`` / ``workspaceId`` GUIDs baked
        into the pipeline JSON to that notebook's id and this workspace — so a pipeline authored
        elsewhere runs here by NAME, no GUIDs to edit by hand. Deploy the notebook first. Omit it to
        deploy the pipeline JSON verbatim.

        ``variables`` sets values in a **variable library** ``variables.json`` at deploy time
        (``{"lakehouse_name": "bronze", "workspace_id": ws.id, ...}``) — the environment-specific
        injection, without editing the file. An unknown variable name raises.

        ``lakehouse`` / ``notebook`` / ``variables`` are ignored for artifact types they don't apply
        to. Not idempotent: if an item of that name already exists it is replaced only when
        ``overwrite=True``, otherwise this raises.
        """
        stem, ext = os.path.splitext(_basename(source))
        ext = ext.lower()
        if ext not in _DEPLOY_EXT:
            raise RemoteRunError(
                f"unsupported file type {ext!r}: deploy handles {', '.join(_DEPLOY_EXT)}")
        name = name or stem
        content = _read_source(source)

        if ext == ".ipynb":
            endpoint = "notebooks"
        elif ext == ".bim":
            endpoint = "semanticModels"
            content = self._repoint_bim(content, lakehouse, source)   # resolve before any delete
        else:                                                          # .json → pipeline or variable library
            endpoint, content = self._json_artifact(source, content, variables, notebook)

        existing = next((it for it in self._items(endpoint) if it.get("displayName") == name), None)
        if existing and not overwrite:
            raise RemoteRunError(f"an item named {name!r} already exists; pass overwrite=True to replace")
        # Overwrite UPDATES the existing item's definition in place (updateDefinition) rather than
        # delete-then-recreate: the item id and its schedules survive, there is no async-delete name
        # race, and a stuck/undeletable item can't block a redeploy. None → a fresh create.
        item_id = existing["id"] if existing else None

        if endpoint == "notebooks":
            return _create_notebook(self._token, self.id, name, json.loads(content), item_id=item_id)
        if endpoint == "dataPipelines":
            return _create_pipeline(self._token, self.id, name, content, item_id=item_id)
        if endpoint == "variableLibraries":
            return _create_variable_library(self._token, self.id, name, content, item_id=item_id)
        # semanticModels → create/update, then refresh/reframe so it is live.
        item_id = _create_semantic_model(self._token, self.id, name, content, item_id=item_id)
        _refresh_semantic_model(get_powerbi_token(), self.id, item_id)
        return item_id

    def _json_artifact(self, source: str, content: bytes, variables: Optional[Dict],
                       notebook: Optional[str] = None):
        """Resolve a ``.json`` source to ``(endpoint, content)`` by sniffing it — a data pipeline
        (``properties.activities``) or a variable library (``variables``); anything else raises.
        Pipeline notebook activities are repointed at ``notebook`` (by name) and variable-library
        values are applied here."""
        obj = json.loads(content)
        if _is_pipeline_json(obj):
            return "dataPipelines", self._repoint_pipeline(content, notebook)
        if _is_variable_library_json(obj):
            return "variableLibraries", _apply_variables(obj, variables or {})
        raise RemoteRunError(
            f"{source!r} is neither a Fabric data pipeline (properties.activities) nor a "
            "variable library (variables)")

    def run(self, name: str) -> str:
        """Run a deployed notebook or data pipeline on Fabric compute and wait for it; return the
        terminal job status. Raises :class:`RemoteRunError` on a failed run or timeout.

        ``name`` is the item's display name, with or without the source extension —
        ``run("etl.ipynb")`` / ``run("etl")`` run the notebook, ``run("pipeline.json")`` the pipeline.
        A bare name is looked up as a notebook then a pipeline. Parameterizing a run is a pipeline's
        job (a pipeline passes parameters to the notebooks it orchestrates), so ``run`` takes none.
        """
        kind, item_id = self._resolve_runnable(name)
        return _run_job_and_wait(self._token, self.id, item_id, job_type=_RUNNABLE[kind])

    def schedule(self, name: str, every=None, daily=None, weekly=None, at=None, tz: str = "UTC") -> str:
        """Schedule a deployed notebook or pipeline to run on Fabric; return the schedule id.

        Fabric's scheduler is interval / daily / weekly (not free-form cron), so pass one of:
        ``every="30m"`` / ``"2h"`` (interval), ``daily="06:00"`` or ``daily=["06:00","18:00"]``, or
        ``weekly=["Mon","Fri"], at="06:00"``. No cadence defaults to **daily at midnight**. ``tz`` is
        the schedule's time zone (default UTC). Idempotent: re-scheduling the same item updates its
        schedule rather than stacking a duplicate.
        """
        config = _schedule_config(every, daily, weekly, at, tz)   # validate cadence before any lookup
        kind, item_id = self._resolve_runnable(name)
        return _schedule_item(self._token, self.id, item_id, _RUNNABLE[kind], config)

    def _resolve_runnable(self, name: str):
        """``(collection, item_id)`` for a runnable named ``name`` — extension picks the collection
        (``.ipynb`` → notebooks, ``.json`` → pipelines), a bare name searches both. Raises if it
        matches no item or more than one."""
        stem, ext = os.path.splitext(name)
        kinds = _RUN_EXT.get(ext.lower(), list(_RUNNABLE))
        target = stem if ext.lower() in _RUN_EXT else name
        hits = [(kind, it["id"]) for kind in kinds
                for it in self._items(kind) if it.get("displayName") == target]
        if not hits:
            raise RemoteRunError(f"no notebook or pipeline named {target!r} in workspace")
        if len(hits) > 1:
            raise RemoteRunError(f"{target!r} matches multiple items; can't tell which")
        return hits[0]

    def _repoint_pipeline(self, content: bytes, notebook: Optional[str]) -> bytes:
        """Point a data pipeline's notebook activities at ``notebook`` (a notebook name in this
        workspace): rewrite every ``TridentNotebook`` activity's ``notebookId`` to that notebook's id
        and ``workspaceId`` to this workspace. So a pipeline authored elsewhere — its JSON carries the
        source workspace/notebook GUIDs — runs here by NAME, exactly like ``_repoint_bim`` does for a
        Direct Lake model. No-op (deploy verbatim) when ``notebook`` is None."""
        if notebook is None:
            return content
        nb_id = self._notebook_id(notebook)
        obj = json.loads(content)
        activities = (obj.get("properties") or {}).get("activities") or []
        repointed = 0
        for act in activities:
            if act.get("type") == "TridentNotebook":
                tp = act.setdefault("typeProperties", {})
                tp["notebookId"] = nb_id
                tp["workspaceId"] = self.id
                repointed += 1
        if not repointed:
            raise RemoteRunError(
                f"pipeline has no TridentNotebook activity to point at notebook {notebook!r}")
        return json.dumps(obj).encode("utf-8")

    def _notebook_id(self, name: str) -> str:
        """The notebook id for a notebook named ``name`` in this workspace; raises listing the real
        names if there's no match (deploy the notebook before the pipeline that references it)."""
        match = next((it for it in self._items("notebooks") if it.get("displayName") == name), None)
        if match:
            return match["id"]
        have = ", ".join(sorted(it.get("displayName", "?") for it in self._items("notebooks"))) or "(none)"
        raise RemoteRunError(f"notebook {name!r} not found in workspace {self.id}; have: {have}")

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
