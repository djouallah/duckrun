"""Mocked-REST tests for the ``duckrun.workspace(...)`` handle and ``create_lakehouse``.

No network: every control-plane call funnels through ``_http_request``, stubbed here. The handle
imports that helper into its own module, and the workspace-resolver / LRO poller call it from
``fabric_remote``'s globals — so both modules are patched to the same fake. These pin: idempotency
(existing name → no POST), the create body (``enableSchemas`` on/off), the 201 and 202-LRO paths,
and that a GUID workspace skips the name lookup.
"""
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
