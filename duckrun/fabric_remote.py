"""Run a duckrun dbt project on Fabric compute via a throwaway notebook.

Fabric has no Livy-for-Python endpoint, so the way to run arbitrary Python/dbt on Fabric compute
is: create a temporary notebook through the REST control plane, run it on demand, poll it to
completion, read back a small result file it wrote to OneLake Files, then delete the notebook.

The public surface is :class:`RemoteRunner`, a drop-in for ``dbt.cli.main.dbtRunner``::

    from duckrun import RemoteRunner
    os.chdir(dbt_path)
    dbt = RemoteRunner(cores=8)                       # was: dbt = dbtRunner()
    args = ["--target", "fabric", "--profiles-dir", ".", "--target-path", "/tmp/dbt_target"]
    run_res  = dbt.invoke(["run",  *args])            # one temp notebook: create -> run -> delete
    test_res = dbt.invoke(["test", *args])            # another temp notebook

Local vs remote is chosen purely by which runner you construct — there is no profile flag. The
project comes from the current directory + the ``--project-dir``/``--profiles-dir`` in the args,
exactly like ``dbtRunner``. ``cores`` is the only knob: the vCores of the Fabric Python notebook the
job runs on (memory scales with it); ``None`` takes the workspace default, so ``RemoteRunner()`` is a
true drop-in.

The optional ``with`` form batches every ``.invoke()`` into ONE notebook::

    with RemoteRunner(cores=8) as dbt:
        run_res  = dbt.invoke(["run",  *args])        # queued
        test_res = dbt.invoke(["test", *args])        # queued
    # block exit: one notebook runs [run, test], then is deleted; both results populated.

Nothing here changes how a normal (local) dbt run behaves — it is an alternative launcher.
"""
import base64
import io
import json
import os
import re
import uuid
import zipfile
from typing import Dict, List, Optional

from . import auth

# Fabric REST control plane.
_FABRIC_API = "https://api.fabric.microsoft.com/v1"
# OneLake / ADLS Gen2 data-plane version, for reading back the result file (mirrors remote.py).
_DFS_API_VERSION = "2023-11-03"

# Project dirs that must never travel to the remote notebook: build/output/vcs cruft.
_ZIP_EXCLUDE_DIRS = {"target", "dbt_packages", "logs", ".git", "__pycache__", ".venv", "venv"}
# storage_options keys that carry a laptop bearer token — stripped from the profile we ship, so the
# remote notebook authenticates through its own Fabric runtime token (notebookutils) instead.
_TOKEN_KEYS = ("bearer_token", "token", "access_token")

# Terminal Fabric job-instance states.
_JOB_DONE = {"Completed", "Failed", "Cancelled", "Deduped"}

_RETRY_STATUS = {429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 3
# Overall wall-clock cap on a single remote run, and the poll cadence (seconds).
_POLL_TIMEOUT = 60 * 60
_POLL_INTERVAL = 10


class RemoteRunError(RuntimeError):
    """The remote notebook job failed, timed out, or its result could not be read back."""


class RemoteResult:
    """A lightweight stand-in for dbt's ``dbtRunnerResult``: enough for the common
    ``res.success`` / ``res.result`` check, NOT the full object. ``result`` is a list of
    ``{"node": name, "status": status}`` dicts as reported by the remote run.

    In the batched (``with``) form this is returned empty from ``.invoke()`` and filled in when the
    block exits; in the plain form it is already populated on return."""

    def __init__(self):
        self.success: Optional[bool] = None
        self.result: Optional[List[dict]] = None
        self.exception = None

    def __repr__(self):
        return f"RemoteResult(success={self.success}, nodes={len(self.result or [])})"


# --------------------------------------------------------------------------------------------------
# Profile resolution (minimal dbt profiles.yml read — no dbt internals)
# --------------------------------------------------------------------------------------------------

def _render_env_var(value):
    """Resolve ``{{ env_var('NAME') }}`` / ``env_var("NAME", "default")`` in a scalar profile value.
    A deliberately tiny subset of dbt's Jinja — enough for the common case where root_path or a token
    comes from the environment. Non-strings and plain literals pass through unchanged."""
    if not isinstance(value, str) or "env_var" not in value:
        return value

    def _sub(m):
        name = m.group("name")
        default = m.group("default")
        return os.environ.get(name, default if default is not None else "")

    pattern = re.compile(
        r"(?:\{\{\s*)?"  # optional surrounding Jinja braces
        r"env_var\(\s*['\"](?P<name>[^'\"]+)['\"]\s*(?:,\s*['\"](?P<default>[^'\"]*)['\"]\s*)?\)"
        r"(?:\s*\}\})?"
    )
    return pattern.sub(_sub, value)


def _load_yaml(path: str) -> dict:
    import yaml  # a dbt-core dependency, always present alongside the adapter
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def resolve_target(project_dir: str, profiles_dir: Optional[str], target: Optional[str]) -> dict:
    """The active target's output block from profiles.yml, resolved the way dbt would: profile name
    from ``dbt_project.yml``, target from the ``--target`` (or the profile default). ``env_var`` in
    ``root_path`` is rendered so the OneLake workspace can be derived. Returns the raw output dict."""
    proj = _load_yaml(os.path.join(project_dir, "dbt_project.yml"))
    profile_name = proj.get("profile")
    if not profile_name:
        raise RemoteRunError(f"no `profile:` in {os.path.join(project_dir, 'dbt_project.yml')}")
    pdir = profiles_dir or os.environ.get("DBT_PROFILES_DIR") or project_dir
    profiles = _load_yaml(os.path.join(pdir, "profiles.yml"))
    prof = profiles.get(profile_name)
    if not prof:
        raise RemoteRunError(f"profile {profile_name!r} not found in {pdir}/profiles.yml")
    tgt = target or prof.get("target")
    outputs = prof.get("outputs", {})
    if tgt not in outputs:
        raise RemoteRunError(f"target {tgt!r} not found under profile {profile_name!r}")
    out = dict(outputs[tgt])
    out["root_path"] = _render_env_var(out.get("root_path"))
    return out


def _scrub_profiles_yaml(project_dir: str, profiles_dir: Optional[str]) -> str:
    """The project's profiles.yml as text with every laptop bearer token removed from
    ``storage_options`` — the remote notebook re-acquires a token from its Fabric runtime. Non-token
    storage_options (account names, etc.) are preserved."""
    import yaml
    proj = _load_yaml(os.path.join(project_dir, "dbt_project.yml"))
    pdir = profiles_dir or os.environ.get("DBT_PROFILES_DIR") or project_dir
    profiles = _load_yaml(os.path.join(pdir, "profiles.yml"))
    for prof in profiles.values():
        for out in (prof.get("outputs") or {}).values():
            so = out.get("storage_options")
            if isinstance(so, dict):
                for k in _TOKEN_KEYS:
                    so.pop(k, None)
            for cat in (out.get("catalogs") or {}).values():
                cso = cat.get("storage_options")
                if isinstance(cso, dict):
                    for k in _TOKEN_KEYS:
                        cso.pop(k, None)
    return yaml.safe_dump(profiles, default_flow_style=False, sort_keys=False)


# --------------------------------------------------------------------------------------------------
# OneLake path helpers
# --------------------------------------------------------------------------------------------------

def onelake_parts(root_path: str):
    """``abfss://<workspace>@<host>/<lakehouse>/Tables[/...]`` -> (workspace, lakehouse, host,
    files_base). ``files_base`` is the sibling Files area, ``abfss://<workspace>@<host>/<lakehouse>/Files``.
    workspace/lakehouse may be friendly names or GUIDs."""
    if not root_path or not root_path.startswith("abfss://"):
        raise RemoteRunError(f"RemoteRunner needs an abfss:// (OneLake) root_path; got {root_path!r}")
    rest = root_path[len("abfss://"):]
    fs_host, _, path = rest.partition("/")
    workspace, _, host = fs_host.partition("@")
    lakehouse = path.strip("/").split("/", 1)[0]
    if not (workspace and host and lakehouse):
        raise RemoteRunError(f"could not parse workspace/lakehouse from root_path {root_path!r}")
    files_base = f"abfss://{workspace}@{host}/{lakehouse}/Files"
    return workspace, lakehouse, host, files_base


# --------------------------------------------------------------------------------------------------
# Project packaging + notebook building (pure — no network)
# --------------------------------------------------------------------------------------------------

def zip_project(project_dir: str, profiles_dir: Optional[str] = None) -> bytes:
    """Zip the dbt project (SQL/YAML/seeds/macros/...) minus build/vcs cruft, with a token-scrubbed
    ``profiles.yml`` written at the root so the remote run authenticates through its own Fabric
    token. Returns the raw zip bytes."""
    buf = io.BytesIO()
    project_dir = os.path.abspath(project_dir)
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(project_dir):
            dirs[:] = [d for d in dirs if d not in _ZIP_EXCLUDE_DIRS]
            for name in files:
                full = os.path.join(root, name)
                arc = os.path.relpath(full, project_dir).replace(os.sep, "/")
                if arc == "profiles.yml":
                    continue  # replaced by the scrubbed copy below
                zf.writestr(arc, _read_bytes(full))
        zf.writestr("profiles.yml", _scrub_profiles_yaml(project_dir, profiles_dir))
    return buf.getvalue()


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


# A forwarded env var whose NAME matches this is a secret and is NEVER shipped to the notebook — the
# storage token in particular comes from notebookutils inside Fabric, not from us.
_SECRET_RE = re.compile(r"TOKEN|SECRET|KEY|PASSWORD|PWD|CRED", re.IGNORECASE)
# env_var('NAME') / env_var("NAME", default) as dbt writes it in models/macros/profiles.
_ENV_VAR_RE = re.compile(r"""env_var\(\s*['"]([^'"]+)['"]""")


def _scan_env_var_names(project_dir: str):
    """The set of ``env_var('NAME')`` names referenced anywhere in the project's SQL/YAML/Python —
    i.e. the config the embedded logic will read at run time. Used to forward exactly those (minus
    secrets) into the notebook."""
    names = set()
    for root, dirs, files in os.walk(project_dir):
        dirs[:] = [d for d in dirs if d not in _ZIP_EXCLUDE_DIRS]
        for name in files:
            if not name.endswith((".sql", ".yml", ".yaml", ".py")):
                continue
            try:
                text = _read_bytes(os.path.join(root, name)).decode("utf-8", "ignore")
            except OSError:
                continue
            names.update(_ENV_VAR_RE.findall(text))
    return names


def _normalize_command(args: List[str], proj_var: str) -> List[str]:
    """Drop any user ``--project-dir``/``--profiles-dir`` (they point at laptop paths) so the remote
    cell can force both to the unpacked project dir; keep everything else (verb, --select, --target,
    --target-path, ...) verbatim."""
    out, skip = [], False
    for a in args:
        if skip:
            skip = False
            continue
        if a in ("--project-dir", "--profiles-dir"):
            skip = True
            continue
        out.append(a)
    return out


def build_notebook(runid: str, project_b64: str, commands: List[List[str]],
                   result_path: str, install_target: str, env: Optional[Dict[str, str]],
                   cores: Optional[int]) -> dict:
    """Build the throwaway notebook (nbformat 4) that installs duckrun, unpacks the project, runs
    each dbt command via ``dbtRunner``, writes a small result JSON to OneLake Files, and exits with a
    summary. Pure: no network, no Fabric.

    ``install_target`` is the exact pip requirement (``duckrun==<v>`` or a ``git+…`` spec). ``env`` is
    the non-secret config to export before dbt runs (the project's own ``env_var`` names — never a
    token). ``cores`` is recorded in metadata for the job to pick up."""
    commands = [_normalize_command(c, "PROJ") for c in commands]
    env = env or {}

    # Install into the job kernel. NO restartPython() here: in a RunNotebook job restarting the kernel
    # makes the job exit -9 ("Ipython kernel exits with code -9"). A fresh job kernel hasn't imported
    # duckdb/deltalake yet, so the just-installed wheels load on first import in the work cell below.
    setup = (
        "import subprocess, sys\n"
        f"subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', {install_target!r}], check=False)\n"
    )

    work = (
        "import os, io, json, base64, zipfile, contextlib\n"
        f"for _k, _v in {json.dumps(env)}.items():\n"
        "    os.environ[_k] = _v\n"
        f"RUNID = {runid!r}\n"
        f"RESULT_PATH = {result_path!r}\n"
        f"COMMANDS = {json.dumps(commands)}\n"
        f"PROJECT_B64 = {project_b64!r}\n"
        "PROJ = '/tmp/duckrun_proj/' + RUNID\n"
        "os.makedirs(PROJ, exist_ok=True)\n"
        "with zipfile.ZipFile(io.BytesIO(base64.b64decode(PROJECT_B64))) as _z:\n"
        "    _z.extractall(PROJ)\n"
        "os.chdir(PROJ)\n"
        "from dbt.cli.main import dbtRunner\n"
        "_dbt = dbtRunner()\n"
        "_buf = io.StringIO()\n"
        "results = []\n"
        "for _cmd in COMMANDS:\n"
        "    _full = _cmd + ['--project-dir', PROJ, '--profiles-dir', PROJ]\n"
        "    with contextlib.redirect_stdout(_buf), contextlib.redirect_stderr(_buf):\n"
        "        _res = _dbt.invoke(_full)\n"
        "    _nodes = []\n"
        "    for _r in (getattr(_res, 'result', None) or []):\n"
        "        _node = getattr(getattr(_r, 'node', None), 'name', None)\n"
        "        if _node is not None:\n"
        "            _nodes.append({'node': _node, 'status': str(getattr(_r, 'status', ''))})\n"
        "    results.append({'command': _cmd, 'success': bool(getattr(_res, 'success', False)), 'nodes': _nodes})\n"
        "_payload = json.dumps({'runid': RUNID, 'results': results, 'log': _buf.getvalue()})\n"
        "import notebookutils\n"
        "notebookutils.fs.put(RESULT_PATH, _payload, overwrite=True)\n"
        "notebookutils.notebook.exit(json.dumps({'runid': RUNID, 'results': results}))\n"
    )

    def _cell(src):
        return {"cell_type": "code", "source": src.splitlines(keepends=True),
                "metadata": {}, "execution_count": None, "outputs": []}

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": [_cell(setup), _cell(work)],
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "python", "display_name": "Python"},
            # Compute hint for the Fabric Python notebook. The exact live schema is confirmed at the
            # first real run; recorded here (and echoed into the job's executionData) so cores flows.
            "duckrun": {"cores": cores, "runid": runid},
        },
    }


# --------------------------------------------------------------------------------------------------
# REST plumbing (control plane + a small data-plane read for the result file)
# --------------------------------------------------------------------------------------------------

def _sleep(seconds: float) -> None:
    """Indirection so tests can stub the wait."""
    import time
    time.sleep(seconds)


def _http_request(method: str, url: str, *, token: str, params: Optional[dict] = None,
                  json_body: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 60):
    """A bounded-retry HTTP call (mirrors remote._dfs_request): retries transient 429/5xx honoring
    ``Retry-After``; everything else is returned as-is for the caller's own status handling. All the
    higher-level control-plane calls funnel through here, so tests script the whole REST sequence by
    monkeypatching this one function."""
    import requests
    hdrs = {"Authorization": f"Bearer {token}"}
    if json_body is not None:
        hdrs["Content-Type"] = "application/json"
    if headers:
        hdrs.update(headers)
    resp = None
    for attempt in range(_MAX_ATTEMPTS):
        resp = requests.request(method, url, params=params, json=json_body, headers=hdrs, timeout=timeout)
        if resp.status_code not in _RETRY_STATUS:
            return resp
        if attempt < _MAX_ATTEMPTS - 1:
            ra = resp.headers.get("Retry-After")
            _sleep(float(ra) if ra and str(ra).strip().isdigit() else float(2 ** attempt))
    return resp


def _resolve_workspace_id(token: str, workspace: str) -> str:
    """A workspace GUID for ``workspace`` (a name or an already-GUID). Names are resolved via
    ``GET /workspaces``; a value that already looks like a GUID is returned unchanged."""
    if _looks_like_guid(workspace):
        return workspace
    resp = _http_request("GET", f"{_FABRIC_API}/workspaces", token=token)
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws.get("displayName") == workspace:
            return ws["id"]
    raise RemoteRunError(f"workspace {workspace!r} not found (or token can't see it)")


def _looks_like_guid(value: str) -> bool:
    parts = value.split("-")
    return len(parts) == 5 and all(c in "0123456789abcdefABCDEF" for c in "".join(parts))


def _create_notebook(token: str, ws_id: str, name: str, notebook: dict) -> str:
    """Create the notebook item from an inline base64 definition; return its item id. Handles both
    the synchronous 201 and the long-running-operation 202 (poll the ``Location`` until done)."""
    payload_b64 = base64.b64encode(json.dumps(notebook).encode("utf-8")).decode("ascii")
    body = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {"path": "notebook-content.ipynb", "payload": payload_b64, "payloadType": "InlineBase64"}
            ],
        },
    }
    resp = _http_request("POST", f"{_FABRIC_API}/workspaces/{ws_id}/notebooks", token=token, json_body=body)
    if resp.status_code in (200, 201):
        return resp.json()["id"]
    if resp.status_code == 202:
        return _await_lro_item_id(token, resp)
    resp.raise_for_status()
    raise RemoteRunError(f"unexpected status {resp.status_code} creating notebook: {resp.text[:200]}")


def _await_lro_item_id(token: str, resp) -> str:
    """Poll a create long-running-operation to completion and return the created item id."""
    location = resp.headers.get("Location")
    deadline_polls = _POLL_TIMEOUT // max(_POLL_INTERVAL, 1)
    for _ in range(int(deadline_polls) + 1):
        _sleep(_POLL_INTERVAL)
        r = _http_request("GET", location, token=token)
        r.raise_for_status()
        body = r.json()
        status = body.get("status")
        if status == "Succeeded":
            # The operation result carries the item; some tenants return it from a `/result` sub-url.
            if body.get("id"):
                return body["id"]
            rr = _http_request("GET", location.rstrip("/") + "/result", token=token)
            rr.raise_for_status()
            return rr.json()["id"]
        if status in ("Failed", "Undetermined"):
            raise RemoteRunError(f"notebook create failed: {body}")
    raise RemoteRunError("timed out creating notebook")


def _run_job_and_wait(token: str, ws_id: str, item_id: str, cores: Optional[int]) -> str:
    """Start the on-demand notebook job and poll the instance to a terminal state. Returns the
    terminal status; raises ``RemoteRunError`` on a non-Completed terminal state or timeout."""
    exec_data = {}
    if cores:
        # Compute size for the Python notebook. Exact live field confirmed at the first real run.
        exec_data["configuration"] = {"vCores": cores}
    body = {"executionData": exec_data} if exec_data else None
    resp = _http_request(
        "POST", f"{_FABRIC_API}/workspaces/{ws_id}/items/{item_id}/jobs/instances",
        token=token, params={"jobType": "RunNotebook"}, json_body=body,
    )
    if resp.status_code not in (200, 201, 202):
        resp.raise_for_status()
        raise RemoteRunError(f"unexpected status {resp.status_code} starting job: {resp.text[:200]}")
    instance_url = resp.headers.get("Location")
    if not instance_url:
        raise RemoteRunError("job start returned no instance Location to poll")

    deadline_polls = _POLL_TIMEOUT // max(_POLL_INTERVAL, 1)
    status = "Unknown"
    for _ in range(int(deadline_polls) + 1):
        _sleep(_POLL_INTERVAL)
        r = _http_request("GET", instance_url, token=token)
        r.raise_for_status()
        body = r.json()
        status = body.get("status", "Unknown")
        _log(f"job {body.get('id', '')} status: {status}")
        if status in _JOB_DONE:
            if status != "Completed":
                reason = body.get("failureReason") or body
                raise RemoteRunError(f"remote job {status}: {reason}")
            return status
    raise RemoteRunError(f"remote job did not finish within {_POLL_TIMEOUT}s (last status {status})")


def _delete_item(token: str, ws_id: str, item_id: str) -> None:
    """Best-effort teardown of the temp notebook; warns rather than raising so a delete failure
    never masks the run's own result."""
    try:
        resp = _http_request("DELETE", f"{_FABRIC_API}/workspaces/{ws_id}/items/{item_id}", token=token)
        if resp.status_code not in (200, 202, 204):
            _log(f"warning: could not delete temp notebook {item_id} (HTTP {resp.status_code})")
    except Exception as exc:  # noqa: BLE001 — teardown must not raise
        _log(f"warning: could not delete temp notebook {item_id}: {exc}")


def read_result_json(files_url: str, storage_token: str) -> dict:
    """Read back the small result JSON the notebook wrote to OneLake Files, via a data-plane DFS
    GET (mirrors remote.py). ``files_url`` is the ``abfss://`` path of the result file."""
    return json.loads(_dfs_get(files_url, storage_token))


def _dfs_get(abfss_url: str, storage_token: str) -> str:
    rest = abfss_url[len("abfss://"):]
    fs_host, _, path = rest.partition("/")
    filesystem, _, host = fs_host.partition("@")
    url = f"https://{host}/{filesystem}/{path}"
    headers = {"Authorization": f"Bearer {storage_token}", "x-ms-version": _DFS_API_VERSION}
    import requests
    for attempt in range(_MAX_ATTEMPTS):
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code not in _RETRY_STATUS:
            resp.raise_for_status()
            return resp.text
        if attempt < _MAX_ATTEMPTS - 1:
            ra = resp.headers.get("Retry-After")
            _sleep(float(ra) if ra and str(ra).strip().isdigit() else float(2 ** attempt))
    resp.raise_for_status()
    return resp.text


def _log(message: str) -> None:
    print(f"[duckrun.remote] {message}")


# --------------------------------------------------------------------------------------------------
# The runner
# --------------------------------------------------------------------------------------------------

class RemoteRunner:
    """Drop-in for ``dbtRunner`` that runs each ``.invoke()`` on Fabric compute via a temporary
    notebook. See the module docstring for usage. ``cores`` is the Fabric Python-notebook vCores
    (None = workspace default). ``target``/``profiles_dir``/``project_dir`` default to the values in
    the invoked args / the current directory, matching ``dbtRunner``.

    The ``*_token`` and ``*_fn`` parameters are injection seams for tests; production leaves them
    None and the tokens are acquired via :mod:`duckrun.auth`."""

    def __init__(self, cores: Optional[int] = None, *, target: Optional[str] = None,
                 profiles_dir: Optional[str] = None, project_dir: Optional[str] = None,
                 env: Optional[Dict[str, str]] = None, forward_env: bool = True,
                 pip_spec: Optional[str] = None, duckrun_version: Optional[str] = None,
                 fabric_token: Optional[str] = None, storage_token: Optional[str] = None):
        self.cores = cores
        self.target = target
        self.profiles_dir = profiles_dir
        self.project_dir = project_dir
        self.env = env or {}
        self.forward_env = forward_env
        self.pip_spec = pip_spec
        self._fabric_token = fabric_token
        self._storage_token = storage_token
        if duckrun_version is None:
            from . import __version__
            duckrun_version = None if __version__ in ("0+unknown", "") else __version__
        self.duckrun_version = duckrun_version
        self._queue: Optional[List] = None  # active only inside a `with` block

    def _install_target(self) -> str:
        """The exact pip requirement the notebook installs: an explicit ``pip_spec`` (e.g. a
        ``git+…@sha`` branch install) wins; otherwise the local version as ``duckrun==<v>`` (bare
        ``duckrun`` when the version is unknown)."""
        if self.pip_spec:
            return self.pip_spec
        return f"duckrun=={self.duckrun_version}" if self.duckrun_version else "duckrun"

    def _resolve_env(self, project_dir: str) -> Dict[str, str]:
        """The config env to export in the notebook: the project's own ``env_var`` names pulled from
        the current environment (secrets excluded), overlaid by the explicit ``env=`` dict. Never
        includes a token — inside Fabric the storage token comes from ``notebookutils``."""
        forwarded: Dict[str, str] = {}
        if self.forward_env:
            for name in _scan_env_var_names(project_dir):
                if name in os.environ and not _SECRET_RE.search(name):
                    forwarded[name] = os.environ[name]
        forwarded.update(self.env)
        return forwarded

    def __enter__(self):
        self._queue = []
        return self

    def __exit__(self, exc_type, exc, tb):
        queue, self._queue = self._queue, None
        if exc_type is not None or not queue:
            return False
        commands = [cmd for cmd, _ in queue]
        proxies = [proxy for _, proxy in queue]
        for proxy, res in zip(proxies, self._run(commands)):
            _apply(proxy, res)
        return False

    def invoke(self, args) -> RemoteResult:
        """Run ``args`` (a dbt CLI arg list, e.g. ``["run", "--select", "foo"]``) remotely. In a
        ``with`` block the command is queued and an empty proxy is returned (filled at block exit);
        otherwise it runs immediately on its own notebook and returns a populated result."""
        args = list(args)
        proxy = RemoteResult()
        if self._queue is not None:
            self._queue.append((args, proxy))
            return proxy
        [res] = self._run([args])
        _apply(proxy, res)
        return proxy

    def _run(self, commands: List[List[str]]) -> List[dict]:
        """Package the project, spin up a temp notebook to run ``commands``, tear it down, and return
        the per-command result dicts the notebook reported."""
        first = commands[0] if commands else []
        target = self.target or _flag(first, "--target")
        profiles_dir = self.profiles_dir or _flag(first, "--profiles-dir")
        project_dir = self.project_dir or _flag(first, "--project-dir") or os.getcwd()

        cfg = resolve_target(project_dir, profiles_dir, target)
        workspace, lakehouse, host, files_base = onelake_parts(cfg.get("root_path"))

        fabric_token = self._fabric_token or auth.get_fabric_token()
        storage_token = self._storage_token or auth.get_onelake_token()

        runid = uuid.uuid4().hex[:12]
        result_path = f"{files_base}/duckrun_remote/{runid}.json"
        project_b64 = base64.b64encode(zip_project(project_dir, profiles_dir)).decode("ascii")
        env = self._resolve_env(project_dir)
        if env:
            _log(f"forwarding env: {', '.join(sorted(env))}")
        notebook = build_notebook(runid, project_b64, commands, result_path,
                                  self._install_target(), env, self.cores)

        ws_id = _resolve_workspace_id(fabric_token, workspace)
        _log(f"creating temp notebook duckrun-remote-{runid} in workspace {workspace}")
        item_id = _create_notebook(fabric_token, ws_id, f"duckrun-remote-{runid}", notebook)
        try:
            _run_job_and_wait(fabric_token, ws_id, item_id, self.cores)
            result = read_result_json(result_path, storage_token)
        finally:
            _delete_item(fabric_token, ws_id, item_id)

        log_text = result.get("log", "")
        if log_text:
            _print_log_tail(log_text)
        return result.get("results", [])


def _flag(args: List[str], name: str) -> Optional[str]:
    """The value following ``name`` in ``args``, or None."""
    for i, a in enumerate(args):
        if a == name and i + 1 < len(args):
            return args[i + 1]
    return None


def _apply(proxy: RemoteResult, res: dict) -> None:
    proxy.success = bool(res.get("success"))
    proxy.result = res.get("nodes", [])


def _print_log_tail(log_text: str, lines: int = 40) -> None:
    tail = log_text.splitlines()[-lines:]
    _log("remote dbt log (tail):")
    for line in tail:
        print(line)
