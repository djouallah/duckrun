"""Mocked-REST tests for the RemoteRunner create -> run -> poll -> read -> delete flow.

No network: ``_http_request`` (every control-plane call), ``_dfs_get`` (the result read), and
``_sleep`` are stubbed. These pin the sequence, the token used, result population, batching, and —
critically — that the temp notebook is deleted even when the job poll raises.
"""
import json

import pytest

from duckrun import fabric_remote as fr
from duckrun import RemoteRunner


ROOT_PATH = "abfss://Analytics@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Tables"


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
    """Scripts the whole control-plane REST sequence and records every call."""

    def __init__(self, *, job_status="Completed"):
        self.job_status = job_status
        self.calls = []          # (method, url, token)
        self.deleted = False

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url, token))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "POST" and url.endswith("/workspaces/ws-guid/notebooks"):
            return FakeResp(201, {"id": "item-guid"})
        if method == "POST" and "/items/item-guid/jobs/instances" in url:
            return FakeResp(202, headers={"Location": fr._FABRIC_API + "/inst/inst-1"})
        if method == "GET" and url.endswith("/inst/inst-1"):
            return FakeResp(200, {"id": "inst-1", "status": self.job_status,
                                  "failureReason": "boom" if self.job_status != "Completed" else None})
        if method == "DELETE" and url.endswith("/items/item-guid"):
            self.deleted = True
            return FakeResp(200)
        raise AssertionError(f"unexpected call {method} {url}")


@pytest.fixture
def project(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: demo\nprofile: demo\nversion: '1.0'\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "demo:\n  target: fabric\n  outputs:\n    fabric:\n"
        f"      type: duckrun\n      root_path: \"{ROOT_PATH}\"\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "m.sql").write_text("select 1\n", encoding="utf-8")
    return tmp_path


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(fr, "_sleep", lambda *a, **k: None)


def _stub_result(monkeypatch, results):
    monkeypatch.setattr(fr, "_dfs_get", lambda url, tok: json.dumps(
        {"runid": "r", "results": results, "log": "10:00 Done. PASS=1"}))


def _runner(project, **kw):
    return RemoteRunner(cores=8, project_dir=str(project),
                        fabric_token="FAB-TOKEN", storage_token="STORE-TOKEN", **kw)


def test_invoke_runs_full_sequence_and_populates_result(project, monkeypatch):
    fake = FakeFabric()
    monkeypatch.setattr(fr, "_http_request", fake)
    _stub_result(monkeypatch, [{"command": ["run"], "success": True,
                                "nodes": [{"node": "m", "status": "success"}]}])

    res = _runner(project).invoke(["run", "--target", "fabric"])

    assert res.success is True
    assert res.result == [{"node": "m", "status": "success"}]

    seq = [m for m, _, _ in fake.calls]
    assert seq == ["GET", "POST", "POST", "GET", "DELETE"]  # workspaces, create, run, poll, delete
    assert fake.deleted is True
    # control-plane calls carry the FABRIC token, never the storage token
    assert all(tok == "FAB-TOKEN" for _, _, tok in fake.calls)


def test_notebook_deleted_even_when_job_fails(project, monkeypatch):
    fake = FakeFabric(job_status="Failed")
    monkeypatch.setattr(fr, "_http_request", fake)
    # result read should never be reached; make it explode if it is
    monkeypatch.setattr(fr, "_dfs_get", lambda *a: (_ for _ in ()).throw(AssertionError("read attempted")))

    with pytest.raises(fr.RemoteRunError):
        _runner(project).invoke(["run", "--target", "fabric"])

    assert fake.deleted is True  # teardown ran despite the failure


def test_batched_with_block_runs_one_notebook_for_all_invokes(project, monkeypatch):
    fake = FakeFabric()
    monkeypatch.setattr(fr, "_http_request", fake)
    _stub_result(monkeypatch, [
        {"command": ["run"], "success": True, "nodes": []},
        {"command": ["test"], "success": False, "nodes": [{"node": "t", "status": "fail"}]},
    ])

    with _runner(project) as dbt:
        run_res = dbt.invoke(["run", "--target", "fabric"])
        test_res = dbt.invoke(["test", "--target", "fabric"])
        # inside the block, proxies are not populated yet
        assert run_res.success is None

    assert run_res.success is True
    assert test_res.success is False
    assert test_res.result == [{"node": "t", "status": "fail"}]
    # exactly ONE notebook created + deleted for both commands
    assert sum(1 for m, u, _ in fake.calls if m == "POST" and "notebooks" in u) == 1
    assert sum(1 for m, u, _ in fake.calls if m == "DELETE") == 1


def test_workspace_guid_skips_resolution(project, monkeypatch):
    guid = "12345678-1234-1234-1234-123456789abc"
    (project / "profiles.yml").write_text(
        "demo:\n  target: fabric\n  outputs:\n    fabric:\n      type: duckrun\n"
        f"      root_path: \"abfss://{guid}@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Tables\"\n",
        encoding="utf-8")
    fake = FakeFabric()
    # notebook create posts to the guid workspace directly (no GET /workspaces first)
    fake_urls = []

    def rec(method, url, *, token, **kw):
        fake_urls.append((method, url))
        if method == "POST" and f"/workspaces/{guid}/notebooks" in url:
            return FakeResp(201, {"id": "item-guid"})
        if method == "POST" and "/jobs/instances" in url:
            return FakeResp(202, headers={"Location": "u/inst"})
        if method == "GET" and url == "u/inst":
            return FakeResp(200, {"status": "Completed"})
        if method == "DELETE":
            return FakeResp(200)
        raise AssertionError(f"unexpected {method} {url}")

    monkeypatch.setattr(fr, "_http_request", rec)
    _stub_result(monkeypatch, [{"command": ["run"], "success": True, "nodes": []}])

    _runner(project).invoke(["run", "--target", "fabric"])
    assert not any(m == "GET" and u.endswith("/workspaces") for m, u in fake_urls)


# ------------------------------------------------------- control-plane pagination + token freshness

def test_paged_values_follows_continuation(monkeypatch):
    """Fabric caps list responses; _paged_values must follow continuationToken/Uri to the end —
    reading only page one made name->id resolution report 'not found' for items that exist."""
    calls = []

    def fake(method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        calls.append((url, dict(params or {})))
        if len(calls) == 1:
            return FakeResp(200, {"value": [{"displayName": "A", "id": "a"}],
                                  "continuationToken": "tok2"})
        if len(calls) == 2:
            assert params.get("continuationToken") == "tok2"
            return FakeResp(200, {"value": [{"displayName": "B", "id": "b"}],
                                  "continuationUri": fr._FABRIC_API + "/workspaces?ct=tok3"})
        assert url.endswith("/workspaces?ct=tok3")
        return FakeResp(200, {"value": [{"displayName": "C", "id": "c"}]})

    monkeypatch.setattr(fr, "_http_request", fake)
    got = fr._paged_values(fr._FABRIC_API + "/workspaces", token="T")
    assert [w["id"] for w in got] == ["a", "b", "c"]


def test_resolve_workspace_finds_item_on_later_page(monkeypatch):
    def fake(method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        if params and params.get("continuationToken") == "next":
            return FakeResp(200, {"value": [{"displayName": "Deep", "id": "deep-guid"}]})
        return FakeResp(200, {"value": [{"displayName": "First", "id": "first-guid"}],
                              "continuationToken": "next"})

    monkeypatch.setattr(fr, "_http_request", fake)
    assert fr._resolve_workspace_id("T", "Deep") == "deep-guid"


def test_fresh_token_churns_only_near_expiry(monkeypatch):
    # A non-JWT (test stub) is never churned.
    assert fr._fresh_token("STORE-TOKEN", lambda: "NEW") == "STORE-TOKEN"
    # Near expiry: re-acquire from the live source.
    monkeypatch.setattr(fr.auth, "token_is_expiring", lambda t: True)
    assert fr._fresh_token("old", lambda: "NEW") == "NEW"
    # A failed re-acquire keeps the old token rather than raising mid-teardown.
    def boom():
        raise RuntimeError("no source")
    assert fr._fresh_token("old", boom) == "old"
    # An empty re-acquire also keeps the old token.
    assert fr._fresh_token("old", lambda: None) == "old"


def test_invalid_cores_rejected_at_construction(project):
    with pytest.raises(fr.RemoteRunError, match="not a Fabric Python-notebook size"):
        RemoteRunner(cores=3, project_dir=str(project), fabric_token="F", storage_token="S")


def test_valid_cores_accepted(project):
    for c in (4, 8, 16, 32, 64, None):
        RemoteRunner(cores=c, project_dir=str(project), fabric_token="F", storage_token="S")
