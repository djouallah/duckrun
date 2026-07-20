"""Mocked-REST tests for the ``duckrun.workspace(...)`` handle: ``create_lakehouse`` and ``deploy``.

No network: every control-plane call funnels through ``_http_request``, stubbed here. The handle
imports that helper into its own module, and the workspace-resolver / LRO poller call it from
``fabric_remote``'s globals — so both modules are patched to the same fake. These pin: lakehouse
idempotency (existing name → no POST), the create body (``enableSchemas`` on/off), the 201 and
202-LRO paths, a GUID workspace skipping the name lookup, deploy dispatch by extension + name
inference (path and URL sources), the .bim create-then-refresh flow, and overwrite semantics
(exists + ``overwrite=False`` → raise; ``overwrite=True`` → delete-then-create).
"""
import json

import pytest

from importlib import import_module

from duckrun import fabric_remote as fr
from duckrun import workspace          # the factory function (shadows the submodule name)

wsmod = import_module("duckrun.workspace")   # the module object, for monkeypatching its helper


class FakeResp:
    def __init__(self, status=200, body=None, headers=None, text=""):
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeFabric:
    """Scripts the lakehouse REST sequence and records every call (method, url, json_body)."""

    def __init__(self, *, existing=None, create=None):
        self.existing = existing or []          # lakehouses already in the workspace
        self.create = create or FakeResp(201, {"id": "lh-new"})
        self.calls = []                         # (method, url, json_body)

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, json_body))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid"):
            return FakeResp(200, {"id": "ws-guid", "displayName": "Analytics"})
        if method == "GET" and url.endswith("/workspaces/ws-guid/lakehouses"):
            return FakeResp(200, {"value": self.existing})
        if method == "POST" and url.endswith("/workspaces/ws-guid/lakehouses"):
            return self.create
        if method == "GET" and url == "u/lro":                     # LRO poll
            return FakeResp(200, {"status": "Succeeded", "id": "lh-lro"})
        raise AssertionError(f"unexpected call {method} {url}")

    def posts(self):
        return [c for c in self.calls if c[0] == "POST"]


@pytest.fixture(autouse=True)
def _patch(monkeypatch):
    """Route the handle's direct calls, the resolver's, and the LRO poller's to one fake; no sleeps."""
    def install(fake):
        monkeypatch.setattr(wsmod, "_http_request", fake)
        monkeypatch.setattr(fr, "_http_request", fake)
        monkeypatch.setattr(fr, "_sleep", lambda *a, **k: None)
        return fake
    return install


def _ws():
    return workspace("Analytics", token="FAB-TOKEN")


def test_create_new_returns_id_and_enables_schemas(_patch):
    fake = _patch(FakeFabric(existing=[]))
    lh_id = _ws().create_lakehouse("bronze")
    assert lh_id == "lh-new"
    (method, url, body), = fake.posts()
    assert body == {"displayName": "bronze", "creationPayload": {"enableSchemas": True}}


def test_idempotent_existing_name_returns_id_without_post(_patch):
    fake = _patch(FakeFabric(existing=[{"displayName": "bronze", "id": "lh-exist"}]))
    lh_id = _ws().create_lakehouse("bronze")
    assert lh_id == "lh-exist"
    assert fake.posts() == []          # no create issued


def test_schemas_false_omits_creation_payload(_patch):
    fake = _patch(FakeFabric(existing=[]))
    _ws().create_lakehouse("plain", schemas=False)
    (_, _, body), = fake.posts()
    assert body == {"displayName": "plain"}
    assert "creationPayload" not in body


def test_lro_202_resolves_item_id(_patch):
    fake = _patch(FakeFabric(existing=[], create=FakeResp(202, headers={"Location": "u/lro"})))
    assert _ws().create_lakehouse("bronze") == "lh-lro"


def test_list_lakehouses_shape(_patch):
    _patch(FakeFabric(existing=[{"displayName": "a", "id": "1"}, {"displayName": "b", "id": "2"}]))
    got = _ws().list_lakehouses()
    assert got == [{"displayName": "a", "id": "1"}, {"displayName": "b", "id": "2"}]


def test_lakehouse_id_resolves_name(_patch):
    _patch(FakeFabric(existing=[{"displayName": "bronze", "id": "lh-b"}, {"displayName": "silver", "id": "lh-s"}]))
    assert _ws().lakehouse_id("silver") == "lh-s"


def test_lakehouse_id_missing_name_raises_listing_names(_patch):
    _patch(FakeFabric(existing=[{"displayName": "bronze", "id": "lh-b"}]))
    with pytest.raises(Exception, match="gold.*not found.*bronze"):
        _ws().lakehouse_id("gold")


def test_display_name_resolves_from_id(_patch):
    fake = _patch(FakeFabric(existing=[]))
    ws = _ws()
    assert ws.display_name == "Analytics"
    assert ws.display_name == "Analytics"                       # cached
    gets = [u for m, u, _ in fake.calls if m == "GET" and u.endswith("/workspaces/ws-guid")]
    assert len(gets) == 1                                       # only one GET despite two accesses


def test_guid_workspace_skips_name_lookup(_patch):
    guid = "12345678-1234-1234-1234-123456789abc"

    class GuidFabric(FakeFabric):
        def __call__(self, method, url, **kw):
            self.calls.append((method, url, kw.get("json_body")))
            if method == "GET" and url.endswith(f"/workspaces/{guid}/lakehouses"):
                return FakeResp(200, {"value": []})
            if method == "POST" and url.endswith(f"/workspaces/{guid}/lakehouses"):
                return FakeResp(201, {"id": "lh-new"})
            raise AssertionError(f"unexpected {method} {url}")

    fake = _patch(GuidFabric())
    workspace(guid, token="FAB-TOKEN").create_lakehouse("bronze")
    assert not any(u.endswith("/workspaces") for _, u, _ in fake.calls)  # no GET /workspaces


# --- deploy --------------------------------------------------------------------------------------

class DeployFabric:
    """Scripts the deploy sequence (list/create/delete + a Power BI refresh) and records every call."""

    def __init__(self, *, kind="notebooks", existing=None, refresh_status="Completed", lakehouses=None,
                 update_resp=None):
        self.kind = kind                    # "notebooks" | "semanticModels" | "dataPipelines"
        self.existing = existing or []
        self.refresh_status = refresh_status
        self.lakehouses = lakehouses or []  # for the Direct Lake .bim repoint
        self.update_resp = update_resp      # overwrite-path updateDefinition response (default 200 sync)
        self.calls = []                     # (method, url, json_body)

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, json_body))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/lakehouses"):
            return FakeResp(200, {"value": self.lakehouses})
        if method == "GET" and url.endswith(f"/workspaces/ws-guid/{self.kind}"):
            return FakeResp(200, {"value": self.existing})
        if method == "POST" and url.endswith("/updateDefinition"):  # overwrite → update in place
            return self.update_resp if self.update_resp is not None else FakeResp(200)
        if method == "GET" and url == "u/upd-lro":                  # async updateDefinition LRO poll
            return FakeResp(200, {"status": "Succeeded"})
        if method == "POST" and url.endswith(f"/workspaces/ws-guid/{self.kind}"):
            return FakeResp(201, {"id": "item-new"})
        if method == "POST" and url.endswith("/refreshes"):
            return FakeResp(202)
        if method == "GET" and "/refreshes" in url:
            return FakeResp(200, {"value": [{"status": self.refresh_status}]})
        raise AssertionError(f"unexpected call {method} {url}")

    def post_body(self, endpoint):
        return next(c[2] for c in self.calls if c[0] == "POST" and c[1].endswith(endpoint))


def _write(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_deploy_notebook_from_path_infers_name(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="notebooks"))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4, "cells": [], "metadata": {}}))
    assert _ws().deploy(src) == "item-new"
    body = fake.post_body("/workspaces/ws-guid/notebooks")
    assert body["displayName"] == "etl"          # name inferred from the filename stem


def test_deploy_name_override(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="notebooks"))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4, "cells": [], "metadata": {}}))
    _ws().deploy(src, name="etl_prod")
    assert fake.post_body("/workspaces/ws-guid/notebooks")["displayName"] == "etl_prod"


def test_deploy_from_url(_patch, monkeypatch):
    fake = _patch(DeployFabric(kind="notebooks"))
    payload = json.dumps({"nbformat": 4, "cells": [], "metadata": {}}).encode()

    class _Resp:
        content = payload
        def raise_for_status(self): pass

    monkeypatch.setattr("requests.get", lambda url, timeout=60: _Resp())
    url = "https://raw.githubusercontent.com/o/r/main/etl.ipynb"
    assert _ws().deploy(url) == "item-new"
    assert fake.post_body("/workspaces/ws-guid/notebooks")["displayName"] == "etl"


def test_deploy_bim_creates_and_refreshes(_patch, monkeypatch, tmp_path):
    fake = _patch(DeployFabric(kind="semanticModels"))
    monkeypatch.setattr(wsmod, "get_powerbi_token", lambda: "PBI")
    src = _write(tmp_path, "model.bim", json.dumps({"compatibilityLevel": 1702, "model": {}}))
    assert _ws().deploy(src) == "item-new"
    # created the model with the TMSL parts, then fired a refresh and polled it
    parts = {p["path"] for p in fake.post_body("/workspaces/ws-guid/semanticModels")["definition"]["parts"]}
    assert parts == {"model.bim", "definition.pbism"}
    assert any(m == "POST" and u.endswith("/refreshes") for m, u, _ in fake.calls)
    assert any(m == "GET" and "/refreshes" in u for m, u, _ in fake.calls)


def test_deploy_bim_failed_refresh_raises(_patch, monkeypatch, tmp_path):
    _patch(DeployFabric(kind="semanticModels", refresh_status="Failed"))
    monkeypatch.setattr(wsmod, "get_powerbi_token", lambda: "PBI")
    src = _write(tmp_path, "model.bim", "{}")
    with pytest.raises(fr.RemoteRunError, match="refresh"):
        _ws().deploy(src)


# --- Direct Lake .bim repoint (OneLake workspace/lakehouse GUID rewrite) --------------------------

_SRC_WS = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_SRC_LH = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _directlake_bim():
    """A minimal Direct-Lake-on-OneLake model.bim carrying a OneLake ws/lakehouse GUID reference."""
    onelake = f"https://onelake.dfs.fabric.microsoft.com/{_SRC_WS}/{_SRC_LH}/Tables"
    return json.dumps({"model": {"tables": [{"partitions": [
        {"mode": "directLake", "source": {"expression": onelake}}]}]}})


def _deployed_bim(fake):
    import base64
    parts = {p["path"]: p for p in fake.post_body("/workspaces/ws-guid/semanticModels")["definition"]["parts"]}
    return base64.b64decode(parts["model.bim"]["payload"]).decode()


def _bim_fabric(_patch, monkeypatch, lakehouses):
    fake = _patch(DeployFabric(kind="semanticModels", lakehouses=lakehouses))
    monkeypatch.setattr(wsmod, "get_powerbi_token", lambda: "PBI")
    return fake


def test_deploy_bim_repoints_to_named_lakehouse(_patch, monkeypatch, tmp_path):
    fake = _bim_fabric(_patch, monkeypatch, [{"displayName": "silver", "id": "lh-silver"},
                                             {"displayName": "gold", "id": "lh-gold"}])
    src = _write(tmp_path, "model.bim", _directlake_bim())
    _ws().deploy(src, lakehouse="silver")
    out = _deployed_bim(fake)
    assert _SRC_WS not in out and "ws-guid" in out        # workspace GUID repointed to this workspace
    assert _SRC_LH not in out and "lh-silver" in out       # lakehouse GUID repointed to the named one


def test_deploy_bim_infers_single_lakehouse(_patch, monkeypatch, tmp_path):
    fake = _bim_fabric(_patch, monkeypatch, [{"displayName": "only", "id": "lh-only"}])
    src = _write(tmp_path, "model.bim", _directlake_bim())
    _ws().deploy(src)                                       # no lakehouse= → the sole lakehouse
    assert "lh-only" in _deployed_bim(fake)


def test_deploy_bim_ambiguous_lakehouse_raises(_patch, monkeypatch, tmp_path):
    _bim_fabric(_patch, monkeypatch, [{"displayName": "a", "id": "lh-a"}, {"displayName": "b", "id": "lh-b"}])
    src = _write(tmp_path, "model.bim", _directlake_bim())
    with pytest.raises(fr.RemoteRunError, match="which lakehouse"):
        _ws().deploy(src)


def test_deploy_bim_lakehouse_not_found_raises(_patch, monkeypatch, tmp_path):
    _bim_fabric(_patch, monkeypatch, [{"displayName": "silver", "id": "lh-silver"}])
    src = _write(tmp_path, "model.bim", _directlake_bim())
    with pytest.raises(fr.RemoteRunError, match="not found"):
        _ws().deploy(src, lakehouse="bronze")


def test_deploy_lakehouse_arg_ignored_for_notebook(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="notebooks"))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4, "cells": [], "metadata": {}}))
    _ws().deploy(src, lakehouse="whatever")                 # ignored — no lakehouse lookup for a notebook
    assert not any(u.endswith("/lakehouses") for _, u, _ in fake.calls)


# --- run -----------------------------------------------------------------------------------------

class RunFabric:
    """Scripts item lookup + an on-demand job run to a terminal state; records the job POST params."""

    def __init__(self, *, notebooks=None, pipelines=None, status="Completed"):
        self.notebooks = notebooks or []
        self.pipelines = pipelines or []
        self.status = status
        self.calls = []                     # (method, url, params)

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, params))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/notebooks"):
            return FakeResp(200, {"value": self.notebooks})
        if method == "GET" and url.endswith("/workspaces/ws-guid/dataPipelines"):
            return FakeResp(200, {"value": self.pipelines})
        if method == "POST" and "/jobs/instances" in url:
            return FakeResp(202, headers={"Location": "u/inst"})
        if method == "GET" and url == "u/inst":
            return FakeResp(200, {"id": "inst", "status": self.status})
        raise AssertionError(f"unexpected call {method} {url}")

    def job(self):
        return next((u, p) for m, u, p in self.calls if m == "POST" and "/jobs/instances" in u)


def test_run_notebook_by_filename(_patch):
    fake = _patch(RunFabric(notebooks=[{"displayName": "etl", "id": "nb-1"}]))
    assert _ws().run("etl.ipynb") == "Completed"
    url, params = fake.job()
    assert "/items/nb-1/jobs/instances" in url and params == {"jobType": "RunNotebook"}


def test_run_pipeline_by_filename(_patch):
    fake = _patch(RunFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    assert _ws().run("load.json") == "Completed"
    url, params = fake.job()
    assert "/items/pl-1/jobs/instances" in url and params == {"jobType": "Pipeline"}


def test_run_bare_name_searches_both(_patch):
    fake = _patch(RunFabric(pipelines=[{"displayName": "etl", "id": "pl-9"}]))
    assert _ws().run("etl") == "Completed"          # no extension → looks in notebooks then pipelines
    assert fake.job()[1] == {"jobType": "Pipeline"}


def test_run_not_found_raises(_patch):
    _patch(RunFabric())
    with pytest.raises(fr.RemoteRunError, match="no notebook or pipeline"):
        _ws().run("nope")


def test_run_failed_status_raises(_patch):
    _patch(RunFabric(notebooks=[{"displayName": "etl", "id": "nb-1"}], status="Failed"))
    with pytest.raises(fr.RemoteRunError):
        _ws().run("etl.ipynb")


# --- schedule ------------------------------------------------------------------------------------

class ScheduleFabric:
    """Scripts item lookup + the schedule list/create/update calls; records the schedule body."""

    def __init__(self, *, notebooks=None, pipelines=None, existing_schedules=None):
        self.notebooks = notebooks or []
        self.pipelines = pipelines or []
        self.existing = existing_schedules or []
        self.calls = []                     # (method, url, json_body)

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, json_body))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/notebooks"):
            return FakeResp(200, {"value": self.notebooks})
        if method == "GET" and url.endswith("/workspaces/ws-guid/dataPipelines"):
            return FakeResp(200, {"value": self.pipelines})
        if method == "GET" and url.endswith("/schedules"):
            return FakeResp(200, {"value": self.existing})
        if method == "POST" and url.endswith("/schedules"):
            return FakeResp(201, {"id": "sch-new"})
        if method == "PATCH" and "/schedules/" in url:
            return FakeResp(200, {"id": "sch-upd"})
        raise AssertionError(f"unexpected call {method} {url}")

    def body(self):
        return next(b for m, u, b in self.calls if m in ("POST", "PATCH") and "/schedules" in u)


def test_schedule_defaults_to_daily(_patch):
    fake = _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    assert _ws().schedule("load.json") == "sch-new"
    cfg = fake.body()["configuration"]
    assert cfg["type"] == "Daily" and cfg["times"] == ["00:00"]
    assert any("/items/pl-1/jobs/Pipeline/schedules" in u for m, u, _ in fake.calls if m == "POST")


def test_schedule_every_interval(_patch):
    fake = _patch(ScheduleFabric(notebooks=[{"displayName": "etl", "id": "nb-1"}]))
    _ws().schedule("etl.ipynb", every="30m")
    cfg = fake.body()["configuration"]
    assert cfg["type"] == "Cron" and cfg["interval"] == 30


def test_schedule_hours_interval(_patch):
    fake = _patch(ScheduleFabric(notebooks=[{"displayName": "etl", "id": "nb-1"}]))
    _ws().schedule("etl", every="2h")
    assert fake.body()["configuration"]["interval"] == 120


def test_schedule_daily_times(_patch):
    fake = _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    _ws().schedule("load.json", daily=["06:00", "18:00"])
    cfg = fake.body()["configuration"]
    assert cfg["type"] == "Daily" and cfg["times"] == ["06:00", "18:00"]


def test_schedule_weekly(_patch):
    fake = _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    _ws().schedule("load.json", weekly=["Mon", "Fri"], at="06:00")
    cfg = fake.body()["configuration"]
    assert cfg["type"] == "Weekly" and cfg["weekdays"] == ["Monday", "Friday"] and cfg["times"] == ["06:00"]


def test_schedule_updates_existing_not_duplicates(_patch):
    fake = _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}],
                                 existing_schedules=[{"id": "sch-old"}]))
    assert _ws().schedule("load.json") == "sch-upd"
    assert any(m == "PATCH" and u.endswith("/schedules/sch-old") for m, u, _ in fake.calls)
    assert not any(m == "POST" and u.endswith("/schedules") for m, u, _ in fake.calls)


def test_schedule_conflicting_cadence_raises(_patch):
    _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    with pytest.raises(fr.RemoteRunError, match="only one of"):
        _ws().schedule("load.json", every="1h", daily="06:00")


def test_deploy_pipeline_from_path(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="dataPipelines"))
    src = _write(tmp_path, "pipeline.json", json.dumps({"properties": {"activities": []}}))
    assert _ws().deploy(src) == "item-new"
    body = fake.post_body("/workspaces/ws-guid/dataPipelines")
    assert body["displayName"] == "pipeline"
    assert {p["path"] for p in body["definition"]["parts"]} == {"pipeline-content.json", ".platform"}


def test_repoint_pipeline_points_notebook_by_name(_patch):
    # A pipeline authored elsewhere carries the SOURCE workspace/notebook GUIDs. notebook="run" rewrites
    # every TridentNotebook activity to the deployed 'run' notebook's id + this workspace — by name, no
    # GUID surgery. Non-notebook activities are left untouched.
    _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "run", "id": "nb-123"}]))
    ws = _ws()
    pipeline = json.dumps({"properties": {"activities": [
        {"type": "TridentNotebook", "typeProperties": {"notebookId": "SRC-NB", "workspaceId": "SRC-WS"}},
        {"type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}},
    ]}}).encode()
    acts = json.loads(ws._repoint_pipeline(pipeline, "run"))["properties"]["activities"]
    assert acts[0]["typeProperties"] == {"notebookId": "nb-123", "workspaceId": "ws-guid"}
    assert acts[1] == {"type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}   # untouched


def test_repoint_pipeline_reaches_nested_activities(_patch):
    # A notebook activity buried in containers (ForEach sweep, If branch) is repointed too — the
    # walk recurses through typeProperties.activities / ifTrueActivities / ifFalseActivities /
    # Switch cases, not just the top level.
    _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "run", "id": "nb-123"}]))
    ws = _ws()
    pipeline = json.dumps({"properties": {"activities": [
        {"type": "ForEach", "typeProperties": {"isSequential": True, "activities": [
            {"type": "TridentNotebook", "typeProperties": {"notebookId": "SRC-NB", "workspaceId": "SRC-WS"}},
        ]}},
        {"type": "If", "typeProperties": {"ifTrueActivities": [
            {"type": "TridentNotebook", "typeProperties": {"notebookId": "SRC-NB2", "workspaceId": "SRC-WS"}},
        ]}},
    ]}}).encode()
    acts = json.loads(ws._repoint_pipeline(pipeline, "run"))["properties"]["activities"]
    inner = acts[0]["typeProperties"]["activities"][0]["typeProperties"]
    branch = acts[1]["typeProperties"]["ifTrueActivities"][0]["typeProperties"]
    assert inner == {"notebookId": "nb-123", "workspaceId": "ws-guid"}
    assert branch == {"notebookId": "nb-123", "workspaceId": "ws-guid"}


def test_repoint_pipeline_none_is_verbatim(_patch):
    _patch(DeployFabric(kind="notebooks"))
    content = json.dumps({"properties": {"activities": []}}).encode()
    assert _ws()._repoint_pipeline(content, None) is content   # no notebook → deploy the JSON as-is


def test_repoint_pipeline_unknown_notebook_raises(_patch):
    _patch(DeployFabric(kind="notebooks", existing=[]))
    pipeline = json.dumps({"properties": {"activities": [
        {"type": "TridentNotebook", "typeProperties": {}}]}}).encode()
    with pytest.raises(fr.RemoteRunError, match="notebook 'run' not found"):
        _ws()._repoint_pipeline(pipeline, "run")


def test_repoint_pipeline_no_notebook_activity_raises(_patch):
    _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "run", "id": "nb-123"}]))
    pipeline = json.dumps({"properties": {"activities": [{"type": "Wait", "typeProperties": {}}]}}).encode()
    with pytest.raises(fr.RemoteRunError, match="no TridentNotebook activity"):
        _ws()._repoint_pipeline(pipeline, "run")


def test_deploy_unrecognized_json_raises(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="dataPipelines"))
    src = _write(tmp_path, "config.json", json.dumps({"foo": "bar"}))   # neither pipeline nor varlib
    with pytest.raises(fr.RemoteRunError, match="neither a Fabric data pipeline"):
        _ws().deploy(src)
    assert not any(m == "POST" for m, _, _ in fake.calls)   # rejected before any create


def test_deploy_variable_library_sets_values(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="variableLibraries"))
    lib = {"variables": [{"name": "lakehouse_name", "type": "String", "value": "OLD"},
                         {"name": "workspace_id", "type": "String", "value": "OLD"}]}
    src = _write(tmp_path, "variables.json", json.dumps(lib))
    _ws().deploy(src, variables={"lakehouse_name": "bronze", "workspace_id": "ws-guid"})
    parts = {p["path"]: p for p in fake.post_body("/workspaces/ws-guid/variableLibraries")["definition"]["parts"]}
    import base64
    vals = {v["name"]: v["value"]
            for v in json.loads(base64.b64decode(parts["variables.json"]["payload"]))["variables"]}
    assert vals == {"lakehouse_name": "bronze", "workspace_id": "ws-guid"}   # values injected
    assert "settings.json" in parts                                          # supplied internally


def test_deploy_variable_library_unknown_var_raises(_patch, tmp_path):
    _patch(DeployFabric(kind="variableLibraries"))
    src = _write(tmp_path, "variables.json",
                 json.dumps({"variables": [{"name": "a", "type": "String", "value": "x"}]}))
    with pytest.raises(fr.RemoteRunError, match="not in the library"):
        _ws().deploy(src, variables={"typo": "y"})


def test_deploy_exists_without_overwrite_raises(_patch, tmp_path):
    _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "etl", "id": "old"}]))
    src = _write(tmp_path, "etl.ipynb", "{}")
    with pytest.raises(fr.RemoteRunError, match="already exists"):
        _ws().deploy(src)


def test_deploy_overwrite_updates_in_place(_patch, tmp_path):
    # Overwrite UPDATES the existing item's definition in place (updateDefinition) — no delete, no
    # recreate. It returns the EXISTING id (identity + schedules survive) and never POSTs to the
    # collection to create a new item.
    fake = _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "etl", "id": "old"}]))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4}))
    assert _ws().deploy(src, overwrite=True) == "old"          # existing id preserved
    assert not any(m == "DELETE" for m, _, _ in fake.calls)    # no delete-then-recreate
    assert any(m == "POST" and u.endswith("/notebooks/old/updateDefinition") for m, u, _ in fake.calls)
    assert not any(m == "POST" and u.endswith("/workspaces/ws-guid/notebooks") for m, u, _ in fake.calls)


def test_deploy_overwrite_awaits_async_update(_patch, tmp_path):
    # A 202 updateDefinition is a long-running op: deploy polls it to Succeeded before returning.
    fake = _patch(DeployFabric(
        kind="notebooks", existing=[{"displayName": "etl", "id": "old"}],
        update_resp=FakeResp(202, headers={"Location": "u/upd-lro"})))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4}))
    assert _ws().deploy(src, overwrite=True) == "old"
    assert any(m == "GET" and u == "u/upd-lro" for m, u, _ in fake.calls)   # awaited the update LRO


def test_deploy_overwrite_update_failure_raises_loudly(_patch, tmp_path):
    # A real update failure must surface with its body, not be swallowed.
    fake = _patch(DeployFabric(
        kind="notebooks", existing=[{"displayName": "etl", "id": "old"}],
        update_resp=FakeResp(400, text="bad definition")))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4}))
    with pytest.raises(fr.RemoteRunError, match="could not update.*400.*bad definition"):
        _ws().deploy(src, overwrite=True)


def test_deploy_unsupported_extension_raises(_patch, tmp_path):
    _patch(DeployFabric())
    src = _write(tmp_path, "data.csv", "x")
    with pytest.raises(fr.RemoteRunError, match="unsupported"):
        _ws().deploy(src)


# --- folder deploy (Fabric git-integration layout) ------------------------------------------------

class MultiDeployFabric:
    """Scripts a whole-folder deploy: serves every item collection (+ lakehouses and the Power BI
    refresh), tracks creates so later name lookups see earlier items, and records every call."""

    def __init__(self, *, existing=None, lakehouses=None, definitions=None, lro_defs=()):
        self.existing = existing or {}      # collection -> [{"displayName", "id"}]
        self.lakehouses = lakehouses if lakehouses is not None else [{"displayName": "bronze", "id": "lh-1"}]
        self.definitions = definitions or {}  # item id -> [part dicts] served by getDefinition
        self.lro_defs = set(lro_defs)         # item ids whose getDefinition goes the 202 route
        self._pending = None                  # parts awaiting the LRO /result fetch
        self.counter = 0
        self.calls = []                     # (method, url, json_body)
        self.get_defs = []                  # (url, params) of every getDefinition POST

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, json_body))
        if method == "POST" and url.endswith("/getDefinition"):
            self.get_defs.append((url, params))
            item_id = url.rsplit("/", 2)[1]
            if item_id in self.lro_defs:
                self._pending = self.definitions[item_id]
                return FakeResp(202, headers={"Location": "u/def-lro"})
            return FakeResp(200, {"definition": {"parts": self.definitions[item_id]}})
        if method == "GET" and url == "u/def-lro":
            return FakeResp(200, {"status": "Succeeded"})
        if method == "GET" and url == "u/def-lro/result":
            return FakeResp(200, {"definition": {"parts": self._pending}})
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/lakehouses"):
            return FakeResp(200, {"value": self.lakehouses})
        for coll in ("notebooks", "semanticModels", "dataPipelines", "variableLibraries"):
            if url.endswith(f"/workspaces/ws-guid/{coll}"):
                if method == "GET":
                    return FakeResp(200, {"value": self.existing.get(coll, [])})
                self.counter += 1
                item = {"displayName": json_body["displayName"], "id": f"item-{self.counter}"}
                self.existing.setdefault(coll, []).append(item)
                return FakeResp(201, {"id": item["id"]})
        if method == "POST" and url.endswith("/updateDefinition"):
            return FakeResp(200)
        if method == "POST" and url.endswith("/refreshes"):
            return FakeResp(202)
        if method == "GET" and "/refreshes" in url:
            return FakeResp(200, {"value": [{"status": "Completed"}]})
        raise AssertionError(f"unexpected call {method} {url}")

    def created(self):
        """Collections POSTed to (creates only), in call order."""
        return [u.rsplit("/", 1)[1] for m, u, _ in self.calls
                if m == "POST" and "/workspaces/ws-guid/" in u
                and not u.endswith(("/updateDefinition", "/refreshes"))]

    def post_body(self, endpoint):
        return next(c[2] for c in self.calls if c[0] == "POST" and c[1].endswith(endpoint))


def _write_item(root, folder_name, item_type, display_name, files):
    d = root / folder_name
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps(
        {"metadata": {"type": item_type, "displayName": display_name},
         "config": {"version": "2.0", "logicalId": "0000"}}), encoding="utf-8")
    for fname, text in files.items():
        (d / fname).write_text(text, encoding="utf-8")
    return d


_NB_JSON = json.dumps({"nbformat": 4, "cells": [], "metadata": {}})
_PIPE_WITH_NB = json.dumps({"properties": {"activities": [
    {"type": "TridentNotebook", "typeProperties": {"notebookId": "SRC-NB", "workspaceId": "SRC-WS"}}]}})


def _fabric_items(tmp_path):
    """A folder mirroring the reference repo layout: varlib + notebook + Direct Lake model + a
    pipeline whose notebook activity still carries its source-workspace GUIDs."""
    root = tmp_path / "fabric_items"
    root.mkdir()
    _write_item(root, "deploy_config.VariableLibrary", "VariableLibrary", "deploy_config",
                {"variables.json": json.dumps({"variables": [
                    {"name": "lakehouse_name", "type": "String", "value": "OLD"}]}),
                 "settings.json": "{}"})
    _write_item(root, "run.Notebook", "Notebook", "run", {"notebook-content.ipynb": _NB_JSON})
    _write_item(root, "aemo.SemanticModel", "SemanticModel", "aemo_electricity",
                {"model.bim": _directlake_bim(), "definition.pbism": "{}"})
    _write_item(root, "run_pipeline.DataPipeline", "DataPipeline", "run_pipeline",
                {"pipeline-content.json": _PIPE_WITH_NB})
    return str(root)


def _decoded_part(fake, endpoint, path):
    import base64
    parts = {p["path"]: p for p in fake.post_body(endpoint)["definition"]["parts"]}
    return base64.b64decode(parts[path]["payload"]).decode()


def test_deploy_folder_all_items_in_dependency_order(_patch, monkeypatch, tmp_path):
    fake = _patch(MultiDeployFabric())
    monkeypatch.setattr(wsmod, "get_powerbi_token", lambda: "PBI")
    ids = _ws().deploy(_fabric_items(tmp_path))
    # every item deployed, named by its .platform displayName (not the folder stem "aemo")
    assert ids == {"deploy_config": "item-1", "run": "item-2",
                   "aemo_electricity": "item-3", "run_pipeline": "item-4"}
    # varlib and notebook before the model, pipeline last (it references the notebook)
    assert fake.created() == ["variableLibraries", "notebooks", "semanticModels", "dataPipelines"]
    # the model was refreshed, like any single-file .bim deploy
    assert any(m == "POST" and u.endswith("/refreshes") for m, u, _ in fake.calls)


def test_deploy_folder_repoints_pipeline_at_folder_notebook(_patch, monkeypatch, tmp_path):
    # The automagic: with exactly one notebook in the folder, the pipeline's TridentNotebook
    # activity is rewritten to that just-deployed notebook's id + this workspace — no notebook= arg.
    fake = _patch(MultiDeployFabric())
    monkeypatch.setattr(wsmod, "get_powerbi_token", lambda: "PBI")
    _ws().deploy(_fabric_items(tmp_path))
    out = json.loads(_decoded_part(fake, "/workspaces/ws-guid/dataPipelines", "pipeline-content.json"))
    tp = out["properties"]["activities"][0]["typeProperties"]
    nb_id = next(it["id"] for it in fake.existing["notebooks"] if it["displayName"] == "run")
    assert tp == {"notebookId": nb_id, "workspaceId": "ws-guid"}


def test_deploy_folder_pipeline_verbatim_without_notebook(_patch, tmp_path):
    # No notebook in the folder → nothing to point at; the pipeline JSON ships untouched.
    fake = _patch(MultiDeployFabric())
    root = tmp_path / "items"
    _write_item(root, "p.DataPipeline", "DataPipeline", "p", {"pipeline-content.json": _PIPE_WITH_NB})
    _ws().deploy(str(root))
    out = json.loads(_decoded_part(fake, "/workspaces/ws-guid/dataPipelines", "pipeline-content.json"))
    assert out["properties"]["activities"][0]["typeProperties"]["notebookId"] == "SRC-NB"


def test_deploy_folder_variables_injected(_patch, tmp_path):
    fake = _patch(MultiDeployFabric())
    root = tmp_path / "items"
    _write_item(root, "cfg.VariableLibrary", "VariableLibrary", "cfg",
                {"variables.json": json.dumps({"variables": [
                    {"name": "lakehouse_name", "type": "String", "value": "OLD"}]})})
    _ws().deploy(str(root), variables={"lakehouse_name": "bronze"})
    out = json.loads(_decoded_part(fake, "/workspaces/ws-guid/variableLibraries", "variables.json"))
    assert out["variables"][0]["value"] == "bronze"


def test_deploy_item_folder_directly(_patch, tmp_path):
    # Pointing at one name.ItemType folder (rather than its parent) deploys that single item.
    fake = _patch(MultiDeployFabric())
    d = _write_item(tmp_path, "run.Notebook", "Notebook", "run", {"notebook-content.ipynb": _NB_JSON})
    assert _ws().deploy(str(d)) == {"run": "item-1"}
    assert fake.post_body("/workspaces/ws-guid/notebooks")["displayName"] == "run"


def test_deploy_folder_overwrite_forwarded(_patch, tmp_path):
    fake = _patch(MultiDeployFabric(existing={"notebooks": [{"displayName": "run", "id": "old-nb"}]}))
    root = tmp_path / "items"
    _write_item(root, "run.Notebook", "Notebook", "run", {"notebook-content.ipynb": _NB_JSON})
    with pytest.raises(fr.RemoteRunError, match="already exists"):
        _ws().deploy(str(root))
    assert _ws().deploy(str(root), overwrite=True) == {"run": "old-nb"}   # updated in place
    assert any(m == "POST" and u.endswith("/notebooks/old-nb/updateDefinition")
               for m, u, _ in fake.calls)
    assert fake.created() == []                                           # never a fresh create


def test_deploy_loose_folder_by_extension(_patch, monkeypatch, tmp_path):
    # A plain folder — no .platform layout, just files — deploys each loose .ipynb/.bim/.json by
    # extension, named by stem, in dependency order; the pipeline's NESTED notebook activity is
    # automagically pointed at the folder's sole notebook.
    fake = _patch(MultiDeployFabric())
    root = tmp_path / "notebooks"
    root.mkdir()
    (root / "etl.ipynb").write_text(_NB_JSON, encoding="utf-8")
    (root / "sweep.json").write_text(json.dumps({"properties": {"activities": [
        {"type": "ForEach", "typeProperties": {"activities": [
            {"type": "TridentNotebook", "typeProperties": {"notebookId": "SRC-NB", "workspaceId": "SRC-WS"}},
        ]}}]}}), encoding="utf-8")
    ids = _ws().deploy(str(root))
    assert ids == {"etl": "item-1", "sweep": "item-2"}
    assert fake.created() == ["notebooks", "dataPipelines"]      # notebook first, pipeline last
    out = json.loads(_decoded_part(fake, "/workspaces/ws-guid/dataPipelines", "pipeline-content.json"))
    inner = out["properties"]["activities"][0]["typeProperties"]["activities"][0]["typeProperties"]
    assert inner == {"notebookId": "item-1", "workspaceId": "ws-guid"}


def test_deploy_loose_folder_ignores_unsupported_files(_patch, tmp_path):
    # Unsupported loose files are skipped, not errors; a folder with ONLY unsupported files still
    # raises the no-items error.
    fake = _patch(MultiDeployFabric())
    root = tmp_path / "notebooks"
    root.mkdir()
    (root / "etl.ipynb").write_text(_NB_JSON, encoding="utf-8")
    (root / "readme.md").write_text("# notes", encoding="utf-8")
    assert _ws().deploy(str(root)) == {"etl": "item-1"}
    junk = tmp_path / "junk"
    junk.mkdir()
    (junk / "data.csv").write_text("x", encoding="utf-8")
    with pytest.raises(fr.RemoteRunError, match="no Fabric items"):
        _ws().deploy(str(junk))


def test_deploy_folder_unknown_type_raises(_patch, tmp_path):
    fake = _patch(MultiDeployFabric())
    root = tmp_path / "items"
    _write_item(root, "sales.Report", "Report", "sales", {"definition.pbir": "{}"})
    with pytest.raises(fr.RemoteRunError, match="unsupported item type 'Report'"):
        _ws().deploy(str(root))
    assert not any(m == "POST" for m, _, _ in fake.calls)   # rejected before any create


def test_deploy_folder_name_kwarg_raises(_patch, tmp_path):
    _patch(MultiDeployFabric())
    root = tmp_path / "items"
    _write_item(root, "run.Notebook", "Notebook", "run", {"notebook-content.ipynb": _NB_JSON})
    with pytest.raises(fr.RemoteRunError, match="name="):
        _ws().deploy(str(root), name="x")


def test_deploy_empty_folder_raises(_patch, tmp_path):
    _patch(MultiDeployFabric())
    root = tmp_path / "empty"
    root.mkdir()
    with pytest.raises(fr.RemoteRunError, match="no Fabric items"):
        _ws().deploy(str(root))


def test_deploy_folder_py_notebook_raises_with_hint(_patch, tmp_path):
    _patch(MultiDeployFabric())
    root = tmp_path / "items"
    _write_item(root, "run.Notebook", "Notebook", "run", {"notebook-content.py": "# code"})
    with pytest.raises(fr.RemoteRunError, match="ipynb-format"):
        _ws().deploy(str(root))


# --- download (the mirror of folder deploy) -------------------------------------------------------

def _platform_json(item_type, name):
    return json.dumps({"metadata": {"type": item_type, "displayName": name},
                       "config": {"version": "2.0", "logicalId": "0000"}})


def _def_parts(item_type, name):
    """The definition parts Fabric would return for one item of ``item_type`` named ``name``."""
    primary = {"Notebook": ("notebook-content.ipynb", _NB_JSON),
               "SemanticModel": ("model.bim", _directlake_bim()),
               "DataPipeline": ("pipeline-content.json", _PIPE_WITH_NB),
               "VariableLibrary": ("variables.json", json.dumps({"variables": [
                   {"name": "lakehouse_name", "type": "String", "value": "bronze"}]}))}[item_type]
    return [fr._b64_part(primary[0], primary[1]),
            fr._b64_part(".platform", _platform_json(item_type, name))]


def _download_fabric(**kw):
    """A fake workspace holding one item of each supported type, definitions included."""
    existing = {"variableLibraries": [{"displayName": "deploy_config", "id": "vl-1"}],
                "notebooks": [{"displayName": "run", "id": "nb-1"}],
                "semanticModels": [{"displayName": "aemo_electricity", "id": "sm-1"}],
                "dataPipelines": [{"displayName": "run_pipeline", "id": "pl-1"}]}
    definitions = {"vl-1": _def_parts("VariableLibrary", "deploy_config"),
                   "nb-1": _def_parts("Notebook", "run"),
                   "sm-1": _def_parts("SemanticModel", "aemo_electricity"),
                   "pl-1": _def_parts("DataPipeline", "run_pipeline")}
    return MultiDeployFabric(existing=existing, definitions=definitions, **kw)


def test_download_writes_git_layout(_patch, tmp_path):
    _patch(_download_fabric())
    out = _ws().download(str(tmp_path))
    assert set(out) == {"deploy_config", "run", "aemo_electricity", "run_pipeline"}
    nb = tmp_path / "run.Notebook"
    assert (nb / "notebook-content.ipynb").read_text(encoding="utf-8") == _NB_JSON   # decoded
    assert json.loads((nb / ".platform").read_text(encoding="utf-8"))["metadata"]["type"] == "Notebook"
    assert (tmp_path / "aemo_electricity.SemanticModel" / "model.bim").is_file()
    assert (tmp_path / "run_pipeline.DataPipeline" / "pipeline-content.json").is_file()
    assert (tmp_path / "deploy_config.VariableLibrary" / "variables.json").is_file()


def test_download_roundtrips_through_deploy_scan(_patch, tmp_path):
    # What download writes, the folder-deploy scanner must accept unchanged.
    _patch(_download_fabric())
    _ws().download(str(tmp_path))
    items = wsmod._scan_item_folders(str(tmp_path))
    assert [(it["type"], it["name"]) for it in items] == [
        ("VariableLibrary", "deploy_config"), ("Notebook", "run"),
        ("SemanticModel", "aemo_electricity"), ("DataPipeline", "run_pipeline")]


def test_download_asks_roundtrip_formats(_patch, tmp_path):
    # Notebooks come back as ipynb and semantic models as TMSL — the formats deploy consumes.
    fake = _patch(_download_fabric())
    _ws().download(str(tmp_path))
    fmts = {url.rsplit("/", 2)[1]: (params or {}).get("format") for url, params in fake.get_defs}
    assert fmts == {"vl-1": None, "nb-1": "ipynb", "sm-1": "TMSL", "pl-1": None}


def test_download_single_item_by_name(_patch, tmp_path):
    fake = _patch(_download_fabric())
    out = _ws().download(str(tmp_path), name="run")
    assert out == {"run": str(tmp_path / "run.Notebook")}
    assert len(fake.get_defs) == 1                       # only that item's definition fetched
    assert not (tmp_path / "run_pipeline.DataPipeline").exists()


def test_download_unknown_name_raises_listing_items(_patch, tmp_path):
    _patch(_download_fabric())
    with pytest.raises(fr.RemoteRunError, match="no item named 'nope'.*deploy_config"):
        _ws().download(str(tmp_path), name="nope")


def test_download_ambiguous_name_raises(_patch, tmp_path):
    fake = _download_fabric()
    fake.existing["dataPipelines"].append({"displayName": "run", "id": "pl-2"})
    _patch(fake)
    with pytest.raises(fr.RemoteRunError, match="matches multiple items"):
        _ws().download(str(tmp_path), name="run")


def test_download_skips_existing_folder_unless_overwrite(_patch, tmp_path):
    fake = _patch(_download_fabric())
    target = tmp_path / "run.Notebook"
    target.mkdir()
    (target / "notebook-content.ipynb").write_text("LOCAL EDITS", encoding="utf-8")
    _ws().download(str(tmp_path), name="run")
    assert (target / "notebook-content.ipynb").read_text(encoding="utf-8") == "LOCAL EDITS"
    assert fake.get_defs == []                           # skipped before any fetch
    _ws().download(str(tmp_path), name="run", overwrite=True)
    assert (target / "notebook-content.ipynb").read_text(encoding="utf-8") == _NB_JSON


def test_download_lro_definition(_patch, tmp_path):
    # A 202 getDefinition is polled to Succeeded and the parts come from the /result fetch.
    fake = _patch(_download_fabric(lro_defs={"nb-1"}))
    _ws().download(str(tmp_path), name="run")
    assert any(m == "GET" and u == "u/def-lro/result" for m, u, _ in fake.calls)
    assert (tmp_path / "run.Notebook" / "notebook-content.ipynb").read_text(encoding="utf-8") == _NB_JSON


def test_download_rejects_escaping_part_path(_patch, tmp_path):
    fake = _download_fabric()
    fake.definitions["nb-1"].append(fr._b64_part("../evil.txt", "boom"))
    _patch(fake)
    with pytest.raises(fr.RemoteRunError, match="refusing to write"):
        _ws().download(str(tmp_path), name="run")
    assert not (tmp_path / "evil.txt").exists()


def test_schedule_rejects_bad_time(_patch):
    """A malformed HH:MM fails fast with a clear message instead of an opaque Fabric API error."""
    fake = _patch(ScheduleFabric(pipelines=[{"displayName": "load", "id": "pl-1"}]))
    with pytest.raises(fr.RemoteRunError, match="invalid schedule time"):
        _ws().schedule("load.json", daily="6am")
    with pytest.raises(fr.RemoteRunError, match="invalid schedule time"):
        _ws().schedule("load.json", weekly=["Mon"], at="25:00")
    _ws().schedule("load.json", daily="06:30")  # sane time still accepted
