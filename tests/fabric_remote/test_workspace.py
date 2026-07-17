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

    def __init__(self, *, kind="notebooks", existing=None, refresh_status="Completed"):
        self.kind = kind                    # "notebooks" | "semanticModels"
        self.existing = existing or []
        self.refresh_status = refresh_status
        self.calls = []                     # (method, url, json_body)

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, json_body))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith(f"/workspaces/ws-guid/{self.kind}"):
            return FakeResp(200, {"value": self.existing})
        if method == "POST" and url.endswith(f"/workspaces/ws-guid/{self.kind}"):
            return FakeResp(201, {"id": "item-new"})
        if method == "DELETE" and "/items/" in url:
            return FakeResp(200)
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


def test_deploy_exists_without_overwrite_raises(_patch, tmp_path):
    _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "etl", "id": "old"}]))
    src = _write(tmp_path, "etl.ipynb", "{}")
    with pytest.raises(fr.RemoteRunError, match="already exists"):
        _ws().deploy(src)


def test_deploy_overwrite_deletes_then_creates(_patch, tmp_path):
    fake = _patch(DeployFabric(kind="notebooks", existing=[{"displayName": "etl", "id": "old"}]))
    src = _write(tmp_path, "etl.ipynb", json.dumps({"nbformat": 4}))
    assert _ws().deploy(src, overwrite=True) == "item-new"
    assert [m for m, _, _ in fake.calls if m in ("DELETE", "POST")] == ["DELETE", "POST"]


def test_deploy_unsupported_extension_raises(_patch, tmp_path):
    _patch(DeployFabric())
    src = _write(tmp_path, "data.csv", "x")
    with pytest.raises(fr.RemoteRunError, match="unsupported"):
        _ws().deploy(src)
