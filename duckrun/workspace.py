"""A Fabric workspace handle for control-plane item management.

``connect()`` is storage-neutral (local, ``s3://``, ``gs://``, ``abfss://``) and points *into* an
existing lakehouse's ``Tables/``. Workspace-level operations are the opposite: Fabric-only, control-
plane, with no lakehouse to point at yet. So they live here, on a separate workspace-scoped handle
rather than on the :class:`~duckrun.session.DuckSession` that ``connect()`` returns::

    import duckrun
    ws = duckrun.workspace("My Workspace")            # name or GUID
    ws.create_lakehouse("bronze")                     # provision an empty container (by name)
    ws.create_warehouse("gold_dwh")                   # same, for a warehouse
    ws.deploy("etl.ipynb")                            # deploy a file artifact (a notebook)
    ws.deploy("model.bim")                            # deploy + refresh a semantic model
    ws.deploy("model.bim", mode="direct_lake")        # ...or force its storage mode first
    ws.deploy("fabric_items")                         # deploy a whole folder of items at once
    ws.download("fabric_items")                       # the mirror: export the items to disk

The Fabric REST plumbing (auth, retry, workspace resolution, LRO polling, item create, refresh) is
reused as-is from :mod:`duckrun.fabric_remote`.
"""
import base64
import datetime
import json
import os
import re
from typing import Dict, List, Optional, Union
from urllib.parse import urlsplit

# A Direct-Lake-on-OneLake model.bim references its lakehouse by a OneLake URL carrying the
# workspace + lakehouse GUIDs, e.g. onelake.dfs.fabric.microsoft.com/<ws-guid>/<lakehouse-guid>/...
_ONELAKE_REF = re.compile(
    r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-fA-F-]{36})/([0-9a-fA-F-]{36})")

# A DirectQuery-on-warehouse model.bim references its warehouse through M expressions like
# Sql.Database("<endpoint>.datawarehouse.fabric.microsoft.com", "<warehouse>"). The bim is JSON,
# so in the raw text the quotes appear escaped (\") — the pattern tolerates both forms.
_SQL_DATABASE_REF = re.compile(
    r'Sql\.Database\(\\?"(?P<server>[^"\\]+\.datawarehouse\.fabric\.microsoft\.com)\\?",'
    r'\s*\\?"(?P<db>[^"\\]+)\\?"')

from .auth import get_fabric_token, get_onelake_token, get_powerbi_token
from .fabric_remote import (
    _FABRIC_API,
    RemoteRunError,
    _http_request,
    _paged_values,
    _resolve_workspace_id,
    _workspace_display_name,
    _await_lro_item_id,
    _get_definition,
    _create_notebook,
    _create_semantic_model,
    _refresh_semantic_model,
    _create_pipeline,
    _create_variable_library,
    _run_job_and_wait,
    _schedule_item,
    _ensure_folder,
    _execute_notebook,
    _looks_like_guid,
    _print_remote_log,
    _zip_payload,
    build_script_notebook,
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


_TIME_RE = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")


def _clock_times(v) -> List[str]:
    """Normalize + validate schedule times: each must be a 24h ``HH:MM``. Fabric would otherwise
    reject the schedule with an opaque API error long after the deploy call."""
    out = _times(v)
    for t in out:
        if not _TIME_RE.match(str(t).strip()):
            raise RemoteRunError(f"invalid schedule time {t!r}; use 24h 'HH:MM' (e.g. '06:30')")
    return out


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
        return {**base, "type": "Weekly", "weekdays": days, "times": _clock_times(at or "00:00")}
    return {**base, "type": "Daily", "times": _clock_times(daily or "00:00")}

# Source extensions deploy handles (a ``.json`` is a pipeline or a variable library, told apart by
# its content — see ``_json_artifact``).
_DEPLOY_EXT = (".ipynb", ".bim", ".json")

# Fabric git-integration item types a folder deploy understands, in dependency order: variable
# libraries and notebooks before the pipelines that may reference them, semantic models before
# pipelines too. For each type, the primary definition file inside its item folder — the one the
# single-file deploy path consumes; the other on-disk parts (.platform, definition.pbism,
# settings.json) are regenerated by the create helpers with identical content.
_DEPLOY_ORDER = ("VariableLibrary", "Notebook", "SemanticModel", "DataPipeline")
_ITEM_PRIMARY = {
    "VariableLibrary": "variables.json",
    "Notebook": "notebook-content.ipynb",
    "SemanticModel": "model.bim",
    "DataPipeline": "pipeline-content.json",
}
# The REST collection for each type, and the getDefinition format ``download`` asks for — ipynb /
# TMSL are exactly what the deploy path consumes (notebook-content.ipynb / model.bim), so a
# downloaded folder always round-trips through ``deploy``.
_ITEM_ENDPOINT = {
    "VariableLibrary": "variableLibraries",
    "Notebook": "notebooks",
    "SemanticModel": "semanticModels",
    "DataPipeline": "dataPipelines",
}
_DOWNLOAD_FMT = {"Notebook": "ipynb", "SemanticModel": "TMSL"}

# Item collections that ``run`` can execute, and the Fabric job type for each.
_RUNNABLE = {"notebooks": "RunNotebook", "dataPipelines": "Pipeline"}
# Which collections to look in for a given source extension when running by file name.
_RUN_EXT = {".ipynb": ["notebooks"], ".json": ["dataPipelines"]}


def _is_directlake_bim(content: bytes) -> bool:
    """Whether a ``model.bim`` is Direct Lake — it declares a ``directLake`` partition mode, or
    carries a OneLake table reference. Checked on the ORIGINAL bytes, before any repoint rewrites
    the ids. A Direct Lake model is reframed after deploy; a DirectQuery-only one is not."""
    text = content.decode("utf-8")
    return "directLake" in text or bool(_ONELAKE_REF.search(text))


# The two storage modes ``deploy(mode=...)`` forces a semantic model into. Pure in both directions: a
# Direct Lake model reads Delta straight off OneLake and never mentions a SQL endpoint (and carries
# directLakeOnly, so a query it can't serve fails rather than quietly falling back to DirectQuery); a
# DirectQuery model reads only through the SQL endpoint, with no Direct Lake artifact left behind.
_MODES = {"directlake": "directLake", "directquery": "directQuery"}
_DL_EXPRESSION = "DirectLake"      # name for a synthesized OneLake shared expression
_DL_BEHAVIOR = "directLakeOnly"    # model.directLakeBehavior — no DirectQuery fallback
_MIN_DL_COMPAT = 1604              # directLake partitions / directLakeBehavior need 1604+

# A DirectQuery partition's table read — ``dbo_fact = Source{[Schema="dbo",Item="fact"]}[Data]`` — and
# the ``in <step>`` the query returns. A plain read returns the navigation step itself; anything else
# transforms the table in M, which has no Direct Lake equivalent.
_M_TABLE_READ = re.compile(
    r'(?P<step>[^\s=,]+)\s*=\s*Source\{\[Schema="(?P<schema>[^"]+)",\s*Item="(?P<item>[^"]+)"\]\}'
    r'\[Data\]')
_M_RETURN = re.compile(r'\bin\s+(?P<ret>\S+)\s*$')


def _normalize_mode(mode: str) -> str:
    """``"direct lake"`` / ``"direct_lake"`` / ``"DirectLake"`` → the TMSL spelling ``directLake``."""
    key = re.sub(r"[\s_-]", "", str(mode)).lower()
    if key not in _MODES:
        raise RemoteRunError(f"unknown mode {mode!r}; use 'direct_lake' or 'direct_query'")
    return _MODES[key]


def _m_text(expression) -> str:
    """A TMSL M expression's text — TMSL writes it either as a string or as a list of lines."""
    if isinstance(expression, list):
        return "\n".join(str(line) for line in expression)
    return "" if expression is None else str(expression)


def _onelake_m(ws_id: str, item_id: str) -> List[str]:
    """The Direct-Lake-on-OneLake shared expression: hierarchical navigation over the source item's
    OneLake root, which is what each entity partition resolves its table against."""
    url = f"https://onelake.dfs.fabric.microsoft.com/{ws_id}/{item_id}"
    return ["let",
            f'    Source = AzureStorage.DataLake("{url}", [HierarchicalNavigation=true])',
            "in",
            "    Source"]


def _sql_m(server: str, database: str, schema: str, table: str) -> List[str]:
    """A DirectQuery partition's M: the workspace SQL endpoint, then one schema-qualified table read."""
    step = f"{schema}_{table}"
    return ["let",
            f'    Source = Sql.Database("{server}", "{database}"),',
            f'    {step} = Source{{[Schema="{schema}",Item="{table}"]}}[Data]',
            "in",
            f"    {step}"]


_DATA_SOURCE_TYPES = ("entity", "m", "query")   # partition sources that read a table


def _is_data_partition(part: Dict) -> bool:
    """Whether a partition reads a SOURCE table — so a mode switch applies to it. A calculated table
    or a calculation group is computed in the model itself and is left alone. TMSL may omit the source
    ``type``, in which case an ``entityName`` / ``expression`` says which it is."""
    src = part.get("source") or {}
    kind = src.get("type")
    if kind is None:
        return bool(src.get("entityName") or src.get("expression"))
    return kind in _DATA_SOURCE_TYPES


def _partition_table(part: Dict, table_name: str):
    """The ``(schema, table)`` a data partition reads — off a Direct Lake entity source, or out of a
    DirectQuery partition's M table read. ``None`` when the partition isn't a plain table read (an M
    query that transforms the table, say): there is nothing to restate in the other mode."""
    src = part.get("source") or {}
    if src.get("type") == "entity" or src.get("entityName"):
        return src.get("schemaName") or "dbo", src.get("entityName") or table_name
    text = _m_text(src.get("expression"))
    read, returned = _M_TABLE_READ.search(text), _M_RETURN.search(text.strip())
    if read and returned and returned.group("ret") == read.group("step"):
        return read.group("schema"), read.group("item")
    return None


def _is_pipeline_json(obj) -> bool:
    """Whether ``obj`` (parsed JSON) is a Fabric data-pipeline definition — a ``pipeline-content.json``
    is ``{"properties": {"activities": [...]}}``."""
    return isinstance(obj, dict) and isinstance(obj.get("properties"), dict) \
        and "activities" in obj["properties"]


def _walk_activities(activities):
    """Every activity in ``activities``, recursing into container activities — ForEach/Until carry
    nested ``typeProperties.activities``, If ``ifTrueActivities``/``ifFalseActivities``, Switch
    ``cases[].activities`` + ``defaultActivities`` — so a notebook activity is found wherever it
    sits, not only at the top level."""
    for act in activities or []:
        yield act
        tp = act.get("typeProperties") or {}
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            yield from _walk_activities(tp.get(key))
        for case in tp.get("cases") or []:
            yield from _walk_activities(case.get("activities"))


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


def _scan_loose_files(folder: str) -> List[Dict]:
    """The deployable loose files directly under ``folder`` — a plain directory of ``.ipynb`` /
    ``.bim`` / ``.json`` sources with no git-integration layout (e.g. a repo's ``notebooks/``).
    Each file is typed by its extension (a ``.json`` by sniffing pipeline vs variable library,
    exactly like the single-file deploy) and named by its stem; returned in dependency order.
    Not recursive: only the folder's own files, so a nested project tree isn't swept up by
    accident."""
    items = []
    for fname in sorted(os.listdir(folder)):
        path = os.path.join(folder, fname)
        stem, ext = os.path.splitext(fname)
        ext = ext.lower()
        if not os.path.isfile(path) or ext not in _DEPLOY_EXT:
            continue
        if ext == ".ipynb":
            item_type = "Notebook"
        elif ext == ".bim":
            item_type = "SemanticModel"
        else:
            try:
                with open(path, "rb") as f:
                    obj = json.load(f)
            except ValueError as exc:
                raise RemoteRunError(f"{path!r} is not valid JSON: {exc}")
            if _is_pipeline_json(obj):
                item_type = "DataPipeline"
            elif _is_variable_library_json(obj):
                item_type = "VariableLibrary"
            else:
                raise RemoteRunError(
                    f"{path!r} is neither a Fabric data pipeline (properties.activities) nor a "
                    "variable library (variables)")
        items.append({"type": item_type, "name": stem, "primary": path})
    items.sort(key=lambda it: _DEPLOY_ORDER.index(it["type"]))
    return items


def _scan_item_folders(folder: str) -> List[Dict]:
    """The Fabric items under ``folder`` (git-integration layout: one ``name.ItemType`` subfolder
    per item, each holding a ``.platform`` metadata file). Returns, in dependency order, one
    ``{"type", "name", "primary"}`` per item — ``name`` is the ``.platform`` displayName and
    ``primary`` the definition file the single-file deploy path consumes. Pointing at an item
    folder itself (``.../run.Notebook``) yields just that item. A folder with no ``.platform``
    items falls back to its loose deployable files (``_scan_loose_files``). Unknown types and
    malformed folders raise rather than skip: a partial deploy that looked complete is worse
    than a loud failure."""
    if os.path.isfile(os.path.join(folder, ".platform")):
        dirs = [folder]                     # an item folder itself, not a folder of items
    else:
        dirs = sorted(os.path.join(folder, d) for d in os.listdir(folder)
                      if os.path.isfile(os.path.join(folder, d, ".platform")))
    if not dirs:
        loose = _scan_loose_files(folder)
        if loose:
            return loose
        raise RemoteRunError(
            f"no Fabric items under {folder!r} (no subfolder has a .platform file, and no "
            f"loose {'/'.join(_DEPLOY_EXT)} files)")
    items = []
    for d in dirs:
        platform = os.path.join(d, ".platform")
        try:
            with open(platform, encoding="utf-8") as f:
                meta = json.load(f).get("metadata") or {}
        except ValueError as exc:
            raise RemoteRunError(f"{platform!r} is not valid JSON: {exc}")
        item_type, name = meta.get("type"), meta.get("displayName")
        if not item_type or not name:
            raise RemoteRunError(f"{platform!r} has no metadata.type / metadata.displayName")
        if item_type not in _ITEM_PRIMARY:
            raise RemoteRunError(f"unsupported item type {item_type!r} in {d!r}; "
                                 f"deploy handles {', '.join(_DEPLOY_ORDER)}")
        primary = os.path.join(d, _ITEM_PRIMARY[item_type])
        if not os.path.isfile(primary):
            hint = (" (only ipynb-format notebooks are supported)"
                    if item_type == "Notebook"
                    and os.path.isfile(os.path.join(d, "notebook-content.py")) else "")
            raise RemoteRunError(f"{d!r} has no {_ITEM_PRIMARY[item_type]}{hint}")
        items.append({"type": item_type, "name": name, "primary": primary})
    items.sort(key=lambda it: _DEPLOY_ORDER.index(it["type"]))
    return items


def _has_notebook_activity(primary: str) -> bool:
    """Whether a ``pipeline-content.json`` has a ``TridentNotebook`` activity — however deeply
    nested — i.e. anything a notebook repoint could rewrite (``_repoint_pipeline`` raises on a
    pipeline without one)."""
    with open(primary, "rb") as f:
        obj = json.load(f)
    activities = (obj.get("properties") or {}).get("activities") or []
    return any(act.get("type") == "TridentNotebook" for act in _walk_activities(activities))


class ScriptResult:
    """What :meth:`Workspace.run_python` hands back: ``success`` (returncode == 0),
    ``returncode`` (the remote script's exit status; -1 = it never ran), ``log``
    (the full captured stdout/stderr — already streamed live during the run), and ``item_id``
    (the throwaway notebook the script ran in).

    ``item_id`` is reported whether or not the notebook still exists — Fabric bills the run's
    compute against that item, and once duckrun's teardown has deleted it the id is the only join
    key left to the capacity data."""

    def __init__(self, returncode: int, log: str, item_id: Optional[str] = None):
        self.returncode = int(returncode)
        self.success = self.returncode == 0
        self.log = log
        self.item_id = item_id

    def __repr__(self) -> str:
        return f"ScriptResult(success={self.success}, returncode={self.returncode}, log=<{len(self.log)} chars>)"


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

    def warehouse_id(self, name: str) -> str:
        """The warehouse id for a warehouse named ``name`` in this workspace — the warehouse sibling
        of :meth:`lakehouse_id`; raises listing the real names if there's no match."""
        for wh in self._items("warehouses"):
            if wh.get("displayName") == name:
                return wh["id"]
        have = ", ".join(sorted(wh.get("displayName", "?") for wh in self._items("warehouses"))) or "(none)"
        raise RemoteRunError(f"warehouse {name!r} not found in workspace {self.id}; have: {have}")

    def _items(self, kind: str) -> List[Dict]:
        """Every item of ``kind`` (e.g. ``"lakehouses"`` / ``"notebooks"`` / ``"semanticModels"``),
        across all result pages."""
        return _paged_values(f"{_FABRIC_API}/workspaces/{self.id}/{kind}", token=self._token)

    def list_lakehouses(self) -> List[Dict]:
        """Every lakehouse in the workspace as ``[{"displayName": ..., "id": ...}, ...]``."""
        return self._items("lakehouses")

    def create_lakehouse(self, name: str, schemas: bool = True,
                         folder: Optional[str] = None) -> str:
        """Ensure a lakehouse named ``name`` exists; return its item id.

        Idempotent: if one already exists it is returned unchanged (no create) — and left where
        it already lives, so ``folder`` only places a lakehouse this call CREATES. ``schemas=True``
        makes it schema-enabled. ``folder`` names a workspace folder (created at the root if
        absent), the same placement ``deploy(folder=...)`` does. Raises :class:`RemoteRunError` on
        a real API failure.
        """
        for lh in self._items("lakehouses"):
            if lh.get("displayName") == name:
                return lh["id"]
        body: Dict = {"displayName": name}
        if schemas:
            body["creationPayload"] = {"enableSchemas": True}
        if folder:
            body["folderId"] = _ensure_folder(self._token, self.id, folder, required=True)
        resp = _http_request(
            "POST", f"{_FABRIC_API}/workspaces/{self.id}/lakehouses", token=self._token, json_body=body
        )
        if resp.status_code in (200, 201):
            return resp.json()["id"]
        if resp.status_code == 202:
            return _await_lro_item_id(self._token, resp)
        resp.raise_for_status()
        raise RemoteRunError(f"unexpected status {resp.status_code} creating lakehouse: {resp.text[:200]}")

    def create_warehouse(self, name: str, folder: Optional[str] = None) -> str:
        """Ensure a warehouse named ``name`` exists; return its item id.

        The warehouse sibling of :meth:`create_lakehouse` — idempotent (an existing one is returned
        unchanged, no create, and stays where it lives), 202-aware (a create is a long-running
        operation, polled to completion), and ``folder`` places one it CREATES in that workspace
        folder. Its SQL endpoint is :meth:`sql_endpoint`. Raises :class:`RemoteRunError` on a real
        API failure.
        """
        for wh in self._items("warehouses"):
            if wh.get("displayName") == name:
                return wh["id"]
        body: Dict = {"displayName": name}
        if folder:
            body["folderId"] = _ensure_folder(self._token, self.id, folder, required=True)
        resp = _http_request("POST", f"{_FABRIC_API}/workspaces/{self.id}/warehouses",
                             token=self._token, json_body=body)
        if resp.status_code in (200, 201):
            return resp.json()["id"]
        if resp.status_code == 202:
            return _await_lro_item_id(self._token, resp)
        resp.raise_for_status()
        raise RemoteRunError(f"unexpected status {resp.status_code} creating warehouse: {resp.text[:200]}")

    def deploy(self, source: str, lakehouse: Optional[str] = None, variables: Optional[Dict] = None,
               name: Optional[str] = None, overwrite: bool = False,
               notebook: Optional[str] = None,
               warehouse: Optional[str] = None,
               folder: Optional[str] = None,
               mode: Optional[str] = None) -> Union[str, Dict[str, str]]:
        """Deploy a file artifact — or a whole folder of items — to the workspace.

        A **folder** ``source`` (the Fabric git-integration layout: one ``name.ItemType`` subfolder
        per item, each with a ``.platform`` file) deploys every item in it in dependency order —
        variable libraries, notebooks, semantic models, then pipelines — and returns
        ``{displayName: item id}``. Names come from each item's ``.platform``; a pipeline's
        notebook activities are automatically pointed at the folder's notebook when it has exactly
        one (``notebook=`` picks one otherwise). Each item honours ``lakehouse`` / ``variables`` /
        ``overwrite`` exactly as a single-file deploy of that item would. A **plain folder** with
        no ``.platform`` items works too: its loose ``.ipynb`` / ``.bim`` / ``.json`` files (top
        level only) deploy the same way, each named by its filename stem — so
        ``ws.deploy("notebooks", overwrite=True)`` ships a repo's whole notebook directory.

        A **file** ``source`` is a local path or an ``http(s)`` URL (a raw file URL), and the item
        id is returned. The item type is keyed off the extension — ``.ipynb`` → notebook, ``.bim`` → semantic model, ``.json`` → a data
        pipeline or a variable library (told apart by content) — and the name defaults to the filename
        stem (overridable via ``name``). A semantic model is also **refreshed** after deploy (a
        *reframe* onto the latest Delta data for Direct Lake), so ``deploy`` returns only once it is live.

        ``lakehouse`` (a lakehouse name in this workspace) points a **Direct Lake** ``.bim`` at that
        lakehouse: duckrun rewrites the OneLake workspace/lakehouse GUIDs baked into the model so it
        connects to the right place — no GUIDs or connection strings to edit by hand. It is inferred
        when the model already targets a lakehouse in this workspace, or the workspace has exactly one;
        with several you must name it.

        ``warehouse`` (a warehouse name in this workspace) is the sibling for a **DirectQuery** ``.bim``:
        duckrun rewrites every ``Sql.Database("<endpoint>.datawarehouse.fabric.microsoft.com", "<db>")``
        reference to this workspace's SQL endpoint (:meth:`sql_endpoint`) and that warehouse — so the
        model connects to the right warehouse by NAME, wherever the bim was authored. Inferred when
        the referenced database name matches a warehouse here, or the workspace has exactly one. A
        Direct Lake model is refreshed after deploy; a DirectQuery-only model is NOT (nothing to
        reframe — it queries live).

        ``mode`` (``"direct_lake"`` / ``"direct_query"``) forces the **storage mode** of every data
        table in a ``.bim`` at deploy time, so one authored model ships either way — and on a folder
        deploy, every model in the folder. Omitted (the default), the model deploys exactly as
        authored. Both directions are pure: ``"direct_lake"`` gives each table an entity partition
        over a single ``AzureStorage.DataLake`` expression on the source item's OneLake root and sets
        ``directLakeBehavior`` to ``directLakeOnly`` — no SQL endpoint anywhere, and a query Direct
        Lake can't serve fails instead of silently falling back; ``"direct_query"`` gives each table
        an M partition over ``Sql.Database(<workspace SQL endpoint>, <item>)`` and strips the Direct
        Lake side. The item read is the ``lakehouse`` / ``warehouse`` named here (either kind serves
        either mode — a lakehouse has a SQL analytics endpoint, a warehouse's tables are Delta in
        OneLake), else the one the model already reads if it lives here, else the workspace's only
        candidate. With BOTH named — a mixed folder — each model takes the one matching what it
        already reads: OneLake → ``lakehouse``, ``Sql.Database`` → ``warehouse``. A table that reads
        through a real M query rather than a plain schema/table read raises, naming the table, rather
        than deploying a model with its transformation silently dropped.

        ``notebook`` (a notebook name in this workspace) points a **data pipeline**'s notebook
        activities at that notebook: duckrun rewrites the ``notebookId`` / ``workspaceId`` GUIDs baked
        into the pipeline JSON to that notebook's id and this workspace — so a pipeline authored
        elsewhere runs here by NAME, no GUIDs to edit by hand. Deploy the notebook first. Omit it to
        deploy the pipeline JSON verbatim.

        ``variables`` sets values in a **variable library** ``variables.json`` at deploy time
        (``{"lakehouse_name": "bronze", "workspace_id": ws.id, ...}``) — the environment-specific
        injection, without editing the file. An unknown variable name raises.

        ``folder`` names a WORKSPACE folder to deploy into (created at the root if absent) —
        keeps deployed items grouped instead of landing loose in the workspace. Placement
        applies when the item is CREATED; an ``overwrite`` of an existing item updates its
        definition in place and leaves it wherever it already lives. Unlike the throwaway run
        notebooks' best-effort temp parking, an explicitly requested ``folder`` raises if it
        cannot be resolved.

        ``lakehouse`` / ``warehouse`` / ``notebook`` / ``variables`` are ignored for artifact types
        they don't apply to. Not idempotent: if an item of that name already exists it is replaced
        only when ``overwrite=True``, otherwise this raises.
        """
        if os.path.isdir(source):
            return self._deploy_folder(source, lakehouse, variables, name, overwrite, notebook,
                                       warehouse, folder, mode)
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
            if mode is not None:
                # The conversion resolves the source item itself and writes the final ids / endpoint,
                # so it stands in for both repoints — and it decides the mode, so DL-ness is read off
                # the converted bim rather than the authored one.
                content = self._force_bim_mode(content, _normalize_mode(mode), lakehouse, warehouse,
                                               source)
                directlake = _is_directlake_bim(content)
            else:
                directlake = _is_directlake_bim(content)              # before ids are rewritten
                content = self._repoint_bim(content, lakehouse, source)   # resolve before any delete
                content = self._repoint_dq_bim(content, warehouse, source)
        else:                                                          # .json → pipeline or variable library
            endpoint, content = self._json_artifact(source, content, variables, notebook)

        existing = next((it for it in self._items(endpoint) if it.get("displayName") == name), None)
        if existing and not overwrite:
            raise RemoteRunError(f"an item named {name!r} already exists; pass overwrite=True to replace")
        # Overwrite UPDATES the existing item's definition in place (updateDefinition) rather than
        # delete-then-recreate: the item id and its schedules survive, there is no async-delete name
        # race, and a stuck/undeletable item can't block a redeploy. None → a fresh create.
        item_id = existing["id"] if existing else None
        # Explicitly requested placement is required (raise on failure), applies to fresh creates.
        folder_id = _ensure_folder(self._token, self.id, folder, required=True) if folder else None

        if endpoint == "notebooks":
            return _create_notebook(self._token, self.id, name, json.loads(content),
                                    item_id=item_id, folder_id=folder_id)
        if endpoint == "dataPipelines":
            return _create_pipeline(self._token, self.id, name, content, item_id=item_id,
                                    folder_id=folder_id)
        if endpoint == "variableLibraries":
            return _create_variable_library(self._token, self.id, name, content, item_id=item_id,
                                            folder_id=folder_id)
        # semanticModels → create/update; a Direct Lake model is then refreshed (a reframe onto the
        # latest Delta data) so it is live. A DirectQuery-only model has nothing to reframe — it
        # queries live — so no refresh.
        item_id = _create_semantic_model(self._token, self.id, name, content, item_id=item_id,
                                         folder_id=folder_id)
        if directlake:
            _refresh_semantic_model(get_powerbi_token(), self.id, item_id)
        return item_id

    def _deploy_folder(self, folder: str, lakehouse: Optional[str], variables: Optional[Dict],
                       name: Optional[str], overwrite: bool, notebook: Optional[str],
                       warehouse: Optional[str] = None,
                       ws_folder: Optional[str] = None,
                       mode: Optional[str] = None) -> Dict[str, str]:
        """Deploy every Fabric item under ``folder`` in dependency order; return
        ``{displayName: item id}``. Each item recurses through the single-file ``deploy``, so the
        Direct Lake / warehouse repoints, variable injection, semantic-model refresh and
        ``overwrite`` semantics are all the single-file behavior. A pipeline is pointed at the
        folder's sole notebook when it has a notebook activity and no explicit ``notebook=`` was
        given. ``lakehouse`` / ``warehouse`` are forwarded to each ``.bim`` only when the model
        actually carries that kind of reference — so a mixed folder (Direct Lake + DirectQuery
        models) deploys with both named, each bim getting the rewrite that applies to it. Under
        ``mode`` both go through untouched instead: the conversion repoints whichever it needs, and
        a model is about to STOP carrying the reference it was filtered on."""
        if name is not None:
            raise RemoteRunError("name= applies to a single-file deploy; folder items are named "
                                 "by their .platform displayName")
        items = _scan_item_folders(folder)
        notebooks = [it["name"] for it in items if it["type"] == "Notebook"]
        ids: Dict[str, str] = {}
        for it in items:
            nb = notebook
            lh, wh = lakehouse, warehouse
            if it["type"] == "DataPipeline" and nb is None and len(notebooks) == 1 \
                    and _has_notebook_activity(it["primary"]):
                nb = notebooks[0]
            if it["type"] == "SemanticModel" and mode is None \
                    and (lh is not None or wh is not None):
                with open(it["primary"], encoding="utf-8") as f:
                    text = f.read()
                if lh is not None and not _ONELAKE_REF.search(text):
                    lh = None               # not Direct Lake — lakehouse= doesn't apply to this bim
                if wh is not None and not _SQL_DATABASE_REF.search(text):
                    wh = None               # no warehouse reference — warehouse= doesn't apply
            ids[it["name"]] = self.deploy(it["primary"], lakehouse=lh, variables=variables,
                                          name=it["name"], overwrite=overwrite, notebook=nb,
                                          warehouse=wh, folder=ws_folder, mode=mode)
        return ids

    def download(self, folder: str = ".", name: Optional[str] = None,
                 overwrite: bool = False) -> Dict[str, str]:
        """Download the workspace's items into ``folder`` in the Fabric git-integration layout —
        the mirror of a folder ``deploy``; return ``{displayName: item folder path}``.

        Each item becomes a ``displayName.ItemType/`` subfolder holding its definition parts
        exactly as Fabric returns them (``.platform`` included), so the folder can be committed
        and redeployed anywhere with ``deploy(folder)``. Notebooks are fetched in ipynb format
        and semantic models as TMSL (``model.bim``) — the formats ``deploy`` consumes. The item
        types are the deployable four: variable libraries, notebooks, semantic models, data
        pipelines. ``name=`` downloads just that item; an existing item folder is skipped unless
        ``overwrite=True``.
        """
        wanted = []
        for item_type in _DEPLOY_ORDER:
            for it in self._items(_ITEM_ENDPOINT[item_type]):
                wanted.append((item_type, it["displayName"], it["id"]))
        if name is not None:
            hits = [w for w in wanted if w[1] == name]
            if not hits:
                have = ", ".join(sorted(w[1] for w in wanted)) or "(none)"
                raise RemoteRunError(f"no item named {name!r} in workspace {self.id}; have: {have}")
            if len(hits) > 1:
                raise RemoteRunError(f"{name!r} matches multiple items "
                                     f"({', '.join(w[0] for w in hits)}); can't tell which")
            wanted = hits
        out: Dict[str, str] = {}
        for item_type, display_name, item_id in wanted:
            target = os.path.join(folder, f"{display_name}.{item_type}")
            if os.path.exists(target) and not overwrite:
                print(f"  [skip] exists: {target}")
                out[display_name] = target
                continue
            parts = _get_definition(self._token, self.id, _ITEM_ENDPOINT[item_type], item_id,
                                    fmt=_DOWNLOAD_FMT.get(item_type))
            for part in parts:
                rel = part["path"]
                # Part paths come from the service; never let one escape the item folder.
                if os.path.isabs(rel) or ".." in rel.replace("\\", "/").split("/"):
                    raise RemoteRunError(f"refusing to write definition part {rel!r} "
                                         f"outside {target!r}")
                dest = os.path.join(target, *rel.replace("\\", "/").split("/"))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as f:
                    f.write(base64.b64decode(part["payload"]))
            out[display_name] = target
        return out

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

    def run_python(self, script: str, *, lakehouse: Optional[str] = None,
                   args: Optional[List[str]] = None, env: Optional[Dict[str, str]] = None,
                   cores: Optional[int] = None, pip: Optional[List[str]] = None,
                   setup: Optional[str] = None, entry: Optional[str] = None,
                   name: Optional[str] = None, attempts: int = 3,
                   keep_notebook: bool = False) -> "ScriptResult":
        """Run an arbitrary local Python script on Fabric compute and wait for it.

        ``script`` is a local ``.py`` file, or a folder shipped whole (build/vcs cruft excluded)
        with ``entry='relative/script.py'`` naming what to execute. The script runs as a
        SUBPROCESS of a throwaway Fabric Python notebook — a fresh interpreter on the ~135 GiB
        work disk, data-local to OneLake — with its stdout/stderr streamed back live as
        ``[remote] …`` lines while it runs, exactly like ``RemoteRunner`` streams dbt.

        ``lakehouse`` (name or id; inferred when the workspace has exactly one) hosts the tiny
        result/log round-trip files under ``Files/duckrun_remote/``. ``cores`` sizes the
        notebook (Fabric sizes 4/8/16/32/64; None = workspace default; memory scales with it).
        ``env`` is exported to the script — pass config, never tokens: the remote side
        self-acquires through its Fabric runtime. ``pip`` packages are installed in the
        notebook before the script runs (the harness itself needs nothing). ``setup`` is an
        optional Python snippet exec'd first in the harness namespace (``say``/``tee``/``WORK``
        available) — the hook for odd prerequisites like a portable JDK.

        Session-level failures (the job dies before the script ran — capacity throttling or
        contention) are retried up to ``attempts`` with growing backoff; a script that ran and
        exited non-zero is NOT retried. Returns a :class:`ScriptResult` (``success`` /
        ``returncode`` / ``log`` / ``item_id``); the throwaway notebook is deleted afterwards unless
        ``keep_notebook`` — ``item_id`` names it either way, so the run's Fabric capacity can be
        attributed to the item that was billed for it. When every attempt dies before the script
        ran this raises instead, with the same id on the exception's ``item_id``.
        """
        if cores is not None and cores not in (4, 8, 16, 32, 64):
            raise RemoteRunError(
                f"cores={cores!r} is not a Fabric Python-notebook size; use 4, 8, 16, 32 or 64 "
                "(or omit it for the workspace default).")
        if lakehouse is None:
            lhs = self.list_lakehouses()
            if len(lhs) != 1:
                have = ", ".join(sorted(lh.get("displayName", "?") for lh in lhs)) or "(none)"
                raise RemoteRunError(
                    f"run_python needs lakehouse= when the workspace doesn't have exactly one; "
                    f"have: {have}")
            lh_id = lhs[0]["id"]
        else:
            lh_id = lakehouse if _looks_like_guid(lakehouse) else self.lakehouse_id(lakehouse)

        import uuid as _uuid
        runid = _uuid.uuid4().hex[:12]
        payload, entry_rel = _zip_payload(script, entry=entry)
        result_path = (f"abfss://{self.id}@onelake.dfs.fabric.microsoft.com/{lh_id}"
                       f"/Files/duckrun_remote/{runid}.json")
        notebook = build_script_notebook(runid, base64.b64encode(payload).decode("ascii"),
                                         entry_rel, list(args or []), result_path,
                                         pip, env, cores, setup)
        nb_name = name or f"duckrun-py-{runid}"
        print(f"[duckrun.remote] running {entry_rel} on Fabric as {nb_name} "
              f"(vCores={cores or 'workspace default'})", flush=True)
        result = _execute_notebook(fabric_token=self._token, storage_token=get_onelake_token(),
                                   ws_id=self.id, name=nb_name, notebook=notebook,
                                   result_path=result_path, attempts=attempts,
                                   keep_notebook=keep_notebook)
        streamed = result.pop("_streamed_chars", 0)
        item_id = result.pop("_item_id", None)
        log_text = result.get("log", "")
        if len(log_text) > streamed:
            _print_remote_log(log_text[streamed:])
        return ScriptResult(result.get("returncode", -1), log_text, item_id)

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
        for act in _walk_activities(activities):
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

    def sql_endpoint(self, warehouse: Optional[str] = None) -> str:
        """The workspace's SQL/TDS endpoint hostname — e.g.
        ``xxxx-yyyy.datawarehouse.fabric.microsoft.com`` — what a connection string's ``Server=``
        takes and what a DirectQuery bim's ``Sql.Database()`` references. The hostname is
        workspace-scoped: every warehouse and lakehouse SQL analytics endpoint in the workspace
        shares it. ``warehouse`` names the warehouse to read it from (raises listing the real names
        on a miss); omitted, any warehouse serves, falling back to a lakehouse's SQL analytics
        endpoint when the workspace has no warehouse at all."""
        warehouses = self._items("warehouses")
        if warehouse is not None:
            match = next((w for w in warehouses if w.get("displayName") == warehouse), None)
            if not match:
                have = ", ".join(sorted(w.get("displayName", "?") for w in warehouses)) or "(none)"
                raise RemoteRunError(
                    f"warehouse {warehouse!r} not found in workspace {self.id}; have: {have}")
            warehouses = [match]
        for w in warehouses:
            resp = _http_request("GET", f"{_FABRIC_API}/workspaces/{self.id}/warehouses/{w['id']}",
                                 token=self._token)
            resp.raise_for_status()
            endpoint = (resp.json().get("properties") or {}).get("connectionString")
            if endpoint:
                return endpoint
        for lh in self._items("lakehouses"):
            resp = _http_request("GET", f"{_FABRIC_API}/workspaces/{self.id}/lakehouses/{lh['id']}",
                                 token=self._token)
            resp.raise_for_status()
            endpoint = ((resp.json().get("properties") or {})
                        .get("sqlEndpointProperties") or {}).get("connectionString")
            if endpoint:
                return endpoint
        raise RemoteRunError(f"no SQL endpoint found in workspace {self.id} "
                             "(no warehouse or lakehouse exposes a connectionString)")

    def _force_bim_mode(self, content: bytes, mode: str, lakehouse: Optional[str],
                        warehouse: Optional[str], source: str) -> bytes:
        """Rewrite a ``model.bim`` so every data table reads in storage ``mode`` — the conversion
        behind ``deploy(mode=...)``, and the alternative to the two repoints (it resolves the source
        item and writes the final ids / endpoint itself).

        The rewrite is total, so a converted model keeps no trace of the mode it left: ``directLake``
        gives every table an entity partition over one ``AzureStorage.DataLake`` expression on the
        source item's OneLake root, ``directQuery`` an M partition over
        ``Sql.Database(<workspace SQL endpoint>, <item>)``, and expressions belonging to the other
        mode are dropped once nothing references them. Only plain table reads convert — a partition
        that transforms its table in M raises, naming the table, rather than deploying a model whose
        transformation silently vanished."""
        bim = json.loads(content)
        model = bim.get("model")
        if not isinstance(model, dict):
            raise RemoteRunError(f"{source!r} is not a TMSL model.bim (no 'model' object)")
        tables = model.get("tables") or []
        kind, item_name, item_id = self._mode_source_item(content.decode("utf-8"), lakehouse,
                                                          warehouse, source)
        if mode == "directLake":
            expr_name = self._set_onelake_expression(model, item_id)
        else:
            # The SQL endpoint hostname is workspace-scoped, so a warehouse source reads it from
            # itself and a lakehouse source from whichever item exposes one.
            server = self.sql_endpoint(item_name if kind == "warehouses" else None)

        converted = 0
        for table in tables:
            tname = table.get("name", "?")
            for part in table.get("partitions") or []:
                if not _is_data_partition(part):
                    continue            # a calculated table / calculation group, not a data table
                read = _partition_table(part, tname)
                if read is None:
                    raise RemoteRunError(
                        f"{source!r}: table {tname!r} reads through an M query, not a plain "
                        f"schema/table read, so duckrun can't restate it as {mode} — deploy this "
                        "model without mode=")
                schema, entity = read
                part.setdefault("name", tname)
                if mode == "directLake":
                    part["mode"] = "directLake"
                    part["source"] = {"type": "entity", "entityName": entity, "schemaName": schema,
                                      "expressionSource": expr_name}
                else:
                    part["mode"] = "directQuery"
                    part["source"] = {"type": "m",
                                      "expression": _sql_m(server, item_name, schema, entity)}
                converted += 1
        if not converted:
            raise RemoteRunError(f"{source!r} has no data tables to switch to {mode}")

        # Shared expressions of the mode we left, now unreferenced, would otherwise leave the model
        # naming a source it no longer reads (and a stale OneLake reference reads as Direct Lake).
        referenced = {(p.get("source") or {}).get("expressionSource")
                      for t in tables for p in (t.get("partitions") or [])}
        left_behind = "AzureStorage.DataLake" if mode == "directQuery" else "Sql.Database"
        kept = [e for e in (model.get("expressions") or [])
                if e.get("name") in referenced or left_behind not in _m_text(e.get("expression"))]
        if kept:
            model["expressions"] = kept
        else:
            model.pop("expressions", None)

        if mode == "directLake":
            if int(bim.get("compatibilityLevel") or 0) < _MIN_DL_COMPAT:
                bim["compatibilityLevel"] = _MIN_DL_COMPAT
            model["defaultPowerBIDataSourceVersion"] = "powerBI_V3"
            model["directLakeBehavior"] = _DL_BEHAVIOR
        else:
            model.pop("directLakeBehavior", None)
        return json.dumps(bim, indent=2).encode("utf-8")

    def _set_onelake_expression(self, model: Dict, item_id: str) -> str:
        """Point the model's OneLake shared expression at ``item_id`` in this workspace — adding one
        when the model has none — and return its name for the entity partitions to reference."""
        expressions = model.setdefault("expressions", [])
        expr = next((e for e in expressions
                     if "AzureStorage.DataLake" in _m_text(e.get("expression"))), None)
        if expr is None:
            expr = {"name": _DL_EXPRESSION}
            expressions.append(expr)
        expr.setdefault("name", _DL_EXPRESSION)
        expr.setdefault("kind", "m")
        expr["expression"] = _onelake_m(self.id, item_id)
        return expr["name"]

    def _mode_source_item(self, text: str, lakehouse: Optional[str], warehouse: Optional[str],
                          source: str):
        """The item a ``mode=`` conversion reads from, as ``(kind, name, id)`` with ``kind`` either
        ``"lakehouses"`` or ``"warehouses"`` — either kind serves either mode. Named explicitly wins;
        with BOTH named the model picks (what it reads today decides the kind); unnamed, it's the item
        the model already reads if that lives here, else the workspace's only candidate — raising
        rather than guessing between several."""
        if lakehouse is not None and warehouse is not None:
            kind = self._referenced_kind(text)
            if kind is None:
                raise RemoteRunError(
                    f"{source!r} reads neither OneLake nor a SQL endpoint, and both lakehouse= and "
                    "warehouse= were given — pass just one as the model's source")
            lakehouse, warehouse = (lakehouse, None) if kind == "lakehouses" else (None, warehouse)
        if lakehouse is not None:
            return "lakehouses", lakehouse, self.lakehouse_id(lakehouse)
        if warehouse is not None:
            return "warehouses", warehouse, self.warehouse_id(warehouse)

        already = self._already_read_item(text)
        if already is not None:
            return already
        candidates = [("lakehouses", it) for it in self._items("lakehouses")] \
            + [("warehouses", it) for it in self._items("warehouses")]
        if len(candidates) == 1:
            kind, item = candidates[0]
            return kind, item.get("displayName"), item["id"]
        if not candidates:
            raise RemoteRunError("no lakehouse or warehouse in this workspace for the model to read")
        have = ", ".join(sorted(str(it.get("displayName")) for _, it in candidates))
        raise RemoteRunError("which lakehouse or warehouse should the model read? pass lakehouse= "
                            f"or warehouse=; have: {have}")

    @staticmethod
    def _referenced_kind(text: str) -> Optional[str]:
        """Which kind of source a ``model.bim`` reads TODAY: OneLake → a lakehouse, ``Sql.Database``
        → a warehouse. Only consulted to break a tie when both were named (a mixed folder deploy)."""
        if _ONELAKE_REF.search(text):
            return "lakehouses"
        if _SQL_DATABASE_REF.search(text):
            return "warehouses"
        return None

    def _already_read_item(self, text: str):
        """The item a ``model.bim`` already reads — its OneLake item GUID, or the database its
        ``Sql.Database`` names — as ``(kind, name, id)``, when that item lives in this workspace.
        ``None`` when the model names nothing resolvable here (authored against another workspace)."""
        m = _ONELAKE_REF.search(text)
        if m:
            for kind in ("lakehouses", "warehouses"):
                for it in self._items(kind):
                    if it.get("id") == m.group(2):
                        return kind, it.get("displayName"), it["id"]
        m = _SQL_DATABASE_REF.search(text)
        if m:
            for kind in ("warehouses", "lakehouses"):
                for it in self._items(kind):
                    if it.get("displayName") == m.group("db"):
                        return kind, it.get("displayName"), it["id"]
        return None

    def _repoint_dq_bim(self, content: bytes, warehouse: Optional[str], source: str) -> bytes:
        """Rewrite a DirectQuery ``model.bim``'s ``Sql.Database(server, db)`` references to this
        workspace's SQL endpoint and the chosen warehouse — the warehouse sibling of
        ``_repoint_bim``. If the model carries no warehouse reference it isn't
        DirectQuery-on-warehouse, so it's returned unchanged (and an explicit ``warehouse=`` then
        raises — nothing to point)."""
        text = content.decode("utf-8")
        m = _SQL_DATABASE_REF.search(text)
        if not m:
            if warehouse is not None:
                raise RemoteRunError(
                    f"{source!r} has no Sql.Database(...datawarehouse.fabric.microsoft.com) "
                    f"reference to point at warehouse {warehouse!r} "
                    "(is it a DirectQuery-on-warehouse model?)")
            return content
        target = self._resolve_warehouse(warehouse, m.group("db"))
        server = self.sql_endpoint(target)

        def _swap(match):
            call = match.group(0)
            call = call.replace(match.group("server"), server)
            return call.replace(match.group("db"), target)

        return _SQL_DATABASE_REF.sub(_swap, text).encode("utf-8")

    def _resolve_warehouse(self, warehouse: Optional[str], source_db: str) -> str:
        """The target warehouse NAME for a DirectQuery repoint. Named → verified to exist (raise
        listing the real names). Unnamed → the warehouse the model already references if it lives
        here, else the sole warehouse, else raise asking which one."""
        warehouses = self._items("warehouses")
        names = [w.get("displayName") for w in warehouses]
        if warehouse is not None:
            if warehouse not in names:
                raise RemoteRunError(f"warehouse {warehouse!r} not found in workspace; have: {names}")
            return warehouse
        if source_db in names:
            return source_db                # already references a warehouse here — keep it
        if len(warehouses) == 1:
            return names[0]
        if not warehouses:
            raise RemoteRunError("no warehouse in this workspace to point the model at")
        raise RemoteRunError(f"which warehouse should the model use? pass warehouse=; have: {names}")

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
