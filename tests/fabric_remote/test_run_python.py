"""Tests for Workspace.run_python — the arbitrary-script remote runner.

Pure tests pin the payload zipping and the generated notebook (whose config line is EXECUTED,
not merely compiled — json.dumps-style ``false``/``true`` booleans are syntactically valid names
and only explode at runtime). Mocked-REST tests pin the executor flow: session-retry semantics,
result population, teardown, and that a ran-but-failed script is never retried.
"""
import base64
import io
import json
import zipfile

import pytest

from duckrun import fabric_remote as fr
from duckrun.workspace import ScriptResult, Workspace


# --------------------------------------------------------------------------------------------------
# payload zipping
# --------------------------------------------------------------------------------------------------

def test_zip_payload_single_file(tmp_path):
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    data, entry = fr._zip_payload(str(script))
    assert entry == "job.py"
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        assert zf.read("job.py") == script.read_bytes()  # byte-faithful (CRLF on Windows and all)


def test_zip_payload_folder_needs_entry(tmp_path):
    (tmp_path / "job.py").write_text("print(1)\n", encoding="utf-8")
    with pytest.raises(fr.RemoteRunError, match="entry="):
        fr._zip_payload(str(tmp_path))


def test_zip_payload_folder_excludes_cruft_and_validates_entry(tmp_path):
    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / "main.py").write_text("print(1)\n", encoding="utf-8")
    (tmp_path / "helper.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("junk", encoding="utf-8")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "a.pyc").write_text("junk", encoding="utf-8")

    data, entry = fr._zip_payload(str(tmp_path), entry="jobs/main.py")
    assert entry == "jobs/main.py"
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        names = set(zf.namelist())
    assert "jobs/main.py" in names and "helper.py" in names
    assert not any(n.startswith((".git", "__pycache__")) for n in names)

    with pytest.raises(fr.RemoteRunError, match="not found in payload"):
        fr._zip_payload(str(tmp_path), entry="missing.py")


def test_zip_payload_missing_source():
    with pytest.raises(fr.RemoteRunError, match="not found"):
        fr._zip_payload("no/such/thing.py")


# --------------------------------------------------------------------------------------------------
# notebook builder
# --------------------------------------------------------------------------------------------------

RESULT = "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Files/duckrun_remote/r1.json"


def _work_cell(nb):
    return "".join(nb["cells"][-1]["source"])


def test_script_notebook_config_line_executes():
    """The config must be a PYTHON literal: exec the header — a JSON-embedded boolean would
    NameError here while compiling fine."""
    nb = fr.build_script_notebook("r1", "UEsDBA==", "job.py", ["--flag"], RESULT,
                                  pip=["obstore"], env={"K": "v"}, cores=8, setup="say('s')")
    src = _work_cell(nb)
    compile(src, "work", "exec")
    header = src.split("_SCRATCH = ")[0]
    ns: dict = {}
    exec(header, ns)  # noqa: S102 — the whole point of the test
    assert ns["CFG"]["entry"] == "job.py"
    assert ns["CFG"]["pip"] == ["obstore"]
    assert ns["CFG"]["env"] == {"K": "v"}


def test_script_notebook_cells_and_configure():
    nb = fr.build_script_notebook("r1", "UEsDBA==", "job.py", [], RESULT,
                                  pip=None, env=None, cores=16, setup=None)
    first = "".join(nb["cells"][0]["source"])
    assert first.startswith("%%configure") and '"vCores": 16' in first
    assert len(nb["cells"]) == 2
    work = _work_cell(nb)
    assert "LOG_PATH" in work and "_flush_log" in work           # live-log flusher present
    assert "notebookutils.fs.put" in work                        # result write present
    # metadata is the load-bearing pure-python marker
    assert nb["metadata"]["kernel_info"]["name"] == "jupyter"

    nb2 = fr.build_script_notebook("r1", "UEsDBA==", "job.py", [], RESULT,
                                   pip=None, env=None, cores=None, setup=None)
    assert len(nb2["cells"]) == 1                                # no configure cell


# --------------------------------------------------------------------------------------------------
# executor flow (mocked REST)
# --------------------------------------------------------------------------------------------------

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
    """Scripts the control-plane sequence for run_python; ``job_statuses`` is one terminal
    status per job start (so retries can be scripted)."""

    def __init__(self, job_statuses=("Completed",)):
        self.job_statuses = list(job_statuses)
        self.started = 0
        self.deleted = False
        self.calls = []

    def __call__(self, method, url, *, token, params=None, json_body=None, headers=None, timeout=60):
        self.calls.append((method, url))
        if method == "GET" and url.endswith("/workspaces"):
            return FakeResp(200, {"value": [{"displayName": "Analytics", "id": "ws-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/lakehouses"):
            return FakeResp(200, {"value": [{"displayName": "lake", "id": "lh-guid"}]})
        if method == "GET" and url.endswith("/workspaces/ws-guid/folders"):
            return FakeResp(200, {"value": []})
        if method == "POST" and url.endswith("/workspaces/ws-guid/folders"):
            return FakeResp(201, {"id": "folder-guid"})
        if method == "POST" and url.endswith("/workspaces/ws-guid/notebooks"):
            self.notebook_body = json_body
            return FakeResp(201, {"id": "item-guid"})
        if method == "POST" and "/items/item-guid/jobs/instances" in url:
            self.started += 1
            return FakeResp(202, headers={"Location": fr._FABRIC_API + f"/inst/inst-{self.started}"})
        if method == "GET" and "/inst/inst-" in url:
            status = self.job_statuses[min(self.started - 1, len(self.job_statuses) - 1)]
            return FakeResp(200, {"id": f"inst-{self.started}", "status": status,
                                  "failureReason": None if status == "Completed" else "boom"})
        if method == "DELETE" and url.endswith("/items/item-guid"):
            self.deleted = True
            return FakeResp(200)
        raise AssertionError(f"unexpected call {method} {url}")


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(fr, "_sleep", lambda *a, **k: None)
    monkeypatch.setattr(fr, "_dfs_delete", lambda *a, **k: None)


def _ws(fake, monkeypatch):
    import sys
    wsmod = sys.modules["duckrun.workspace"]  # the package attr `duckrun.workspace` is the function
    monkeypatch.setattr(fr, "_http_request", fake)
    monkeypatch.setattr(wsmod, "_http_request", fake)
    monkeypatch.setattr(wsmod, "get_onelake_token", lambda: "STORAGE-TOKEN")
    return Workspace("Analytics", token="FABRIC-TOKEN")


def _stub_result(monkeypatch, payloads):
    """_dfs_get for the RESULT (.json) returns each payload in turn (a dict → the result JSON;
    an exception → raised). Live-tail reads (.log) always raise like an absent file — the tail
    must swallow that silently."""
    seq = list(payloads)

    def fake(url, tok):
        if url.endswith(".log"):
            raise RuntimeError("404: no log yet")
        item = seq.pop(0) if len(seq) > 1 else seq[0]
        if isinstance(item, Exception):
            raise item
        return json.dumps(item)
    monkeypatch.setattr(fr, "_dfs_get", fake)


def test_run_python_success_roundtrip(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    fake = FakeFabric()
    ws = _ws(fake, monkeypatch)
    _stub_result(monkeypatch, [{"runid": "r", "returncode": 0, "success": True, "log": "hi\n"}])

    res = ws.run_python(str(script), lakehouse="lake", cores=4)
    assert isinstance(res, ScriptResult) and res.success and res.returncode == 0
    assert res.log == "hi\n"
    assert fake.deleted, "throwaway notebook must be deleted"
    assert fake.started == 1


def test_run_python_script_failure_is_not_retried(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("raise SystemExit(3)\n", encoding="utf-8")
    fake = FakeFabric(job_statuses=("Completed",))
    ws = _ws(fake, monkeypatch)
    _stub_result(monkeypatch, [{"runid": "r", "returncode": 3, "success": False, "log": "boom"}])

    res = ws.run_python(str(script), lakehouse="lake", attempts=3)
    assert not res.success and res.returncode == 3
    assert fake.started == 1, "a script that RAN and failed must not be retried"
    assert fake.deleted


def test_run_python_session_death_retries_then_succeeds(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    fake = FakeFabric(job_statuses=("Failed", "Completed"))
    ws = _ws(fake, monkeypatch)
    # first read: no result (payload never ran); second: success
    _stub_result(monkeypatch, [RuntimeError("404"),
                               {"runid": "r", "returncode": 0, "success": True, "log": "ok\n"}])

    res = ws.run_python(str(script), lakehouse="lake", attempts=2)
    assert res.success
    assert fake.started == 2, "session-level death must retry"
    assert fake.deleted


def test_run_python_session_death_exhausts_and_raises(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    fake = FakeFabric(job_statuses=("Failed",))
    ws = _ws(fake, monkeypatch)
    _stub_result(monkeypatch, [RuntimeError("404")])

    with pytest.raises(fr.RemoteRunError):
        ws.run_python(str(script), lakehouse="lake", attempts=2)
    assert fake.started == 2
    assert fake.deleted, "notebook deleted even when every attempt died"


def test_run_python_lakehouse_inferred_when_single(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    fake = FakeFabric()
    ws = _ws(fake, monkeypatch)
    _stub_result(monkeypatch, [{"runid": "r", "returncode": 0, "success": True, "log": ""}])

    res = ws.run_python(str(script))          # no lakehouse= — workspace has exactly one
    assert res.success


def test_run_python_parks_notebook_in_temp_folder(tmp_path, monkeypatch):
    """The throwaway notebook is created inside the duckrun_temp workspace folder (created on
    first use). Cosmetic by contract — the root fallback when the folders API errors is covered
    by the RemoteRunner rest-flow tests, whose fakes reject the folders routes."""
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    fake = FakeFabric()
    ws = _ws(fake, monkeypatch)
    _stub_result(monkeypatch, [{"runid": "r", "returncode": 0, "success": True, "log": ""}])

    assert ws.run_python(str(script), lakehouse="lake").success
    assert fake.notebook_body.get("folderId") == "folder-guid"


def test_run_python_rejects_bad_cores(tmp_path, monkeypatch):
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    ws = _ws(FakeFabric(), monkeypatch)
    with pytest.raises(fr.RemoteRunError, match="not a Fabric"):
        ws.run_python(str(script), lakehouse="lake", cores=6)


def test_deploy_honours_workspace_folder(tmp_path, monkeypatch):
    """deploy(folder=...) creates the workspace folder if needed and parks the item in it —
    and, being explicit user intent, it is REQUIRED: a failing folders API raises."""
    import sys
    nb = tmp_path / "etl.ipynb"
    nb.write_text(json.dumps({"nbformat": 4, "nbformat_minor": 5, "cells": [], "metadata": {}}),
                  encoding="utf-8")
    fake = FakeFabric()
    ws = _ws(fake, monkeypatch)
    wsmod = sys.modules["duckrun.workspace"]
    # No pre-existing items, no pre-existing folders — everything lists empty.
    monkeypatch.setattr(fr, "_paged_values", lambda url, token: [])
    monkeypatch.setattr(wsmod, "_paged_values", lambda url, token: [])

    item_id = ws.deploy(str(nb), folder="jobs")
    assert item_id == "item-guid"
    assert fake.notebook_body.get("folderId") == "folder-guid"

    # Explicit folder= is REQUIRED: a dead folders API must raise, not silently fall back.
    monkeypatch.setattr(fr, "_paged_values",
                        lambda url, token: (_ for _ in ()).throw(RuntimeError("no folders API"))
                        if url.endswith("/folders") else [])
    with pytest.raises(fr.RemoteRunError, match="workspace folder"):
        ws.deploy(str(nb), name="etl2", folder="jobs")
