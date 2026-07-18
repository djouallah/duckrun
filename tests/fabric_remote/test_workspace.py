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
