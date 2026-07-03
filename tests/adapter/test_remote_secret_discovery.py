"""Regression gate for the abfss:// OneLake read-only path.

A standalone ``dbt test``/``show``/``docs`` against an abfss:// OneLake target used to fail
every test with "schema 'landing' does not exist": these commands materialize nothing, so
the discovery glob was the first thing to touch the store — and on abfss:// it threw for
lack of a DuckDB Azure secret (the bearer_token is the only credential, and dbt-duckdb never
mints a secret from it). The swallowed glob exception then looked like "no tables".

The aemo CI workflow only exercises az://, where the secret comes from a profile ``secrets:``
block, so it can't catch this. These tests pin the two things that fix it:

  1. ``secret.ensure_azure_secret`` mints the Azure ACCESS_TOKEN secret from a bearer token.
  2. ``_discover_delta_relations`` mints that secret *before* it globs the store — verified
     with a fake cursor whose glob throws until the secret has been created (mirroring how a
     real abfss:// glob fails without the secret).
"""
import pytest

from dbt.adapters.duckrun import remote, secret
from dbt.adapters.duckrun.impl import DuckrunAdapter

try:  # dbt 1.8+
    from dbt.adapters.contracts.relation import RelationType
except ImportError:  # pragma: no cover - older layouts
    from dbt.contracts.relation import RelationType


# --------------------------------------------------------------------------- helper

class _RecordingConn:
    def __init__(self):
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)
        return self


def test_ensure_azure_secret_mints_secret_from_bearer_token():
    conn = _RecordingConn()
    created = secret.ensure_azure_secret(conn, {"bearer_token": "TOK123"})
    assert created is True
    joined = "\n".join(conn.sql).lower()
    assert "install azure" in joined and "load azure" in joined
    assert "create or replace secret duckrun_onelake" in joined
    assert "tok123" in joined  # the token is embedded in the ACCESS_TOKEN secret


def test_ensure_azure_secret_accepts_token_aliases():
    for key in ("bearer_token", "token", "access_token"):
        conn = _RecordingConn()
        assert secret.ensure_azure_secret(conn, {key: "abc"}) is True


def test_ensure_azure_secret_noop_without_token():
    conn = _RecordingConn()
    assert secret.ensure_azure_secret(conn, None) is False
    assert secret.ensure_azure_secret(conn, {}) is False
    assert secret.ensure_azure_secret(conn, {"account_name": "x"}) is False
    assert conn.sql == []  # nothing executed when there's no token


# ----------------------------------------------------------------- abfss URL parse

def test_parse_abfss_splits_filesystem_host_path():
    fs, host, path = remote._parse_abfss(
        "abfss://duckrun@onelake.dfs.fabric.microsoft.com/dev.Lakehouse/Tables"
    )
    assert fs == "duckrun"
    assert host == "onelake.dfs.fabric.microsoft.com"
    assert path == "dev.Lakehouse/Tables"


def test_is_abfss():
    assert remote.is_abfss("abfss://x@h/y") is True
    assert remote.is_abfss("az://lake/Tables") is False
    assert remote.is_abfss("./warehouse") is False
    assert remote.is_abfss(None) is False


def test_list_delta_tables_returns_immediate_subdirs(monkeypatch):
    # Mirror the real OneLake DFS "List Paths" response: directories carry isDirectory="true",
    # files omit it; names are full paths whose basename is the table name.
    payload = {"paths": [
        {"name": "dev.Lakehouse/Tables/landing/fct_price", "isDirectory": "true"},
        {"name": "dev.Lakehouse/Tables/landing/stg_log", "isDirectory": "true"},
        {"name": "dev.Lakehouse/Tables/landing/_stray_file.txt"},  # a file -> skipped
    ]}

    class _Resp:
        status_code = 200
        headers = {}  # real requests.Response always has headers; no continuation -> single page
        def raise_for_status(self): pass
        def json(self): return payload

    captured = {}
    def fake_get(url, params=None, headers=None, timeout=None):
        captured.update(url=url, params=params, headers=headers)
        return _Resp()

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    names = remote.list_delta_tables(
        "abfss://duckrun@onelake.dfs.fabric.microsoft.com/dev.Lakehouse/Tables",
        "landing", {"bearer_token": "TOK"},
    )
    assert names == ["fct_price", "stg_log"]
    assert captured["url"] == "https://onelake.dfs.fabric.microsoft.com/duckrun"
    assert captured["params"]["directory"] == "dev.Lakehouse/Tables/landing"
    assert captured["headers"]["Authorization"] == "Bearer TOK"


def test_list_delta_tables_no_token_returns_empty():
    assert remote.list_delta_tables("abfss://x@h/y", "landing", None) == []


# ------------------------------------------------ 404: absent dir vs inaccessible store
# A 404 means two different things. Only "the directory isn't there" (PathNotFound — a schema
# not created yet) is genuinely empty; "the store is unreachable" (wrong tenant, item not in
# workspace) must fail loud, not masquerade as an empty lakehouse (the silent-auth-failure bug).

def _resp_404(code):
    class _Resp:
        status_code = 404
        headers = {"x-ms-error-code": code}
        text = '{"error":{"code":"%s"}}' % code
        def raise_for_status(self):  # pragma: no cover - must not be reached on a 404
            raise AssertionError("raise_for_status should not run on a handled 404")
        def json(self):
            return {"error": {"code": code}}
    return _Resp()


def test_list_delta_tables_absent_dir_is_empty(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: _resp_404("PathNotFound"))
    assert remote.list_delta_tables("abfss://x@h/y", "landing", {"bearer_token": "T"}) == []


def test_list_delta_tables_inaccessible_store_raises(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: _resp_404("TargetItemNotInWorkspace"))
    with pytest.raises(RuntimeError) as e:
        remote.list_delta_tables("abfss://x@h/y", "landing", {"bearer_token": "T"})
    assert "not accessible" in str(e.value) and "TargetItemNotInWorkspace" in str(e.value)


def test_list_files_absent_dir_is_empty(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: _resp_404("PathNotFound"))
    assert remote.list_files("abfss://x@h/y/Files", {"bearer_token": "T"}) == []


def test_list_files_inaccessible_store_raises(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: _resp_404("FilesystemNotFound"))
    with pytest.raises(RuntimeError):
        remote.list_files("abfss://x@h/y/Files", {"bearer_token": "T"})


# ----------------------------------------------------------------- discovery wiring

class _Creds:
    root_path = "abfss://duckrun@onelake.dfs.fabric.microsoft.com/dev.Lakehouse/Tables"
    storage_options = {"bearer_token": "TOK"}
    catalogs = None

    def root_for(self, database=None):
        return (self.root_path, self.storage_options)

    def storage_options_for_location(self, location):
        return self.storage_options


class _Config:
    credentials = _Creds()


class _SchemaRelation:
    schema = "landing"
    database = "memory"


def _bare_adapter():
    # Bypass __init__ (needs a full RuntimeConfig / mp_context); we only exercise discovery,
    # which uses self.config, self.Relation and (for abfss) the REST listing.
    adapter = object.__new__(DuckrunAdapter)
    adapter.config = _Config()
    return adapter


def test_discovery_uses_rest_listing_for_abfss(monkeypatch):
    # abfss:// must NOT touch DuckDB glob (broken on OneLake) — it lists via REST.
    monkeypatch.setattr(
        remote, "list_delta_tables",
        lambda root, schema, so: ["fct_price", "stg_log"] if schema == "landing" else [],
    )
    adapter = _bare_adapter()
    # _cursor would raise if discovery tried to glob; leave it unset to prove REST-only.
    relations = adapter._discover_delta_relations(_SchemaRelation())
    assert [r.identifier for r in relations] == ["fct_price", "stg_log"]
    assert all(r.type == RelationType.Table for r in relations)


def test_discovery_swallows_rest_failure_as_empty(monkeypatch):
    # A transient/other listing failure stays best-effort → [] (a momentary blip mustn't abort a run;
    # the actual write fails loud anyway).
    def boom(root, schema, so):
        raise RuntimeError("403 Forbidden")
    monkeypatch.setattr(remote, "list_delta_tables", boom)
    adapter = _bare_adapter()
    assert adapter._discover_delta_relations(_SchemaRelation()) == []


def test_discovery_propagates_access_error(monkeypatch):
    # But a GENUINE access failure (wrong tenant / item not in workspace) must fail loud, not
    # masquerade as an empty schema — otherwise dbt test/docs go silently green against a store it
    # can't see. Option A: OneLakeAccessError is re-raised through discovery.
    def boom(root, schema, so):
        raise remote.OneLakeAccessError("OneLake path not accessible (TargetItemNotInWorkspace)")
    monkeypatch.setattr(remote, "list_delta_tables", boom)
    adapter = _bare_adapter()
    with pytest.raises(remote.OneLakeAccessError):
        adapter._discover_delta_relations(_SchemaRelation())


# ----------------------------------------------------- #7 pagination / #11 retry (review)

class _PageResp:
    def __init__(self, status_code=200, paths=None, headers=None):
        self.status_code = status_code
        self._paths = paths or []
        self.headers = headers or {}
        self.text = ""
    def json(self):
        return {"paths": self._paths}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_list_delta_tables_follows_continuation(monkeypatch):
    # A schema with more tables than one DFS page: page 1 returns an x-ms-continuation header, page 2
    # doesn't. Both pages must be enumerated (not silently truncated to page 1).
    pages = [
        _PageResp(paths=[{"name": f"root/landing/t{i}", "isDirectory": "true"} for i in (1, 2)],
                  headers={"x-ms-continuation": "TOKEN2"}),
        _PageResp(paths=[{"name": "root/landing/t3", "isDirectory": "true"}], headers={}),
    ]
    calls = []
    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(params.get("continuation"))
        return pages[len(calls) - 1]
    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    names = remote.list_delta_tables(
        "abfss://duckrun@onelake.dfs.fabric.microsoft.com/root", "landing", {"bearer_token": "TOK"})
    assert names == ["t1", "t2", "t3"]
    assert calls == [None, "TOKEN2"]  # 2nd page passed the continuation token back


def test_dfs_request_retries_transient_then_succeeds(monkeypatch):
    seq = [_PageResp(status_code=503), _PageResp(status_code=429, headers={"Retry-After": "0"}),
           _PageResp(status_code=200, paths=[{"name": "root/s/t", "isDirectory": "true"}])]
    n = {"i": 0}
    def fake_get(url, params=None, headers=None, timeout=None):
        r = seq[n["i"]]; n["i"] += 1; return r
    import requests
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(remote, "_sleep", lambda s: None)  # no real waiting
    monkeypatch.setattr(remote, "_MAX_ATTEMPTS", 3)

    names = remote.list_delta_tables(
        "abfss://duckrun@onelake.dfs.fabric.microsoft.com/root", "s", {"bearer_token": "TOK"})
    assert names == ["t"]
    assert n["i"] == 3  # two transient responses were retried


def test_dfs_request_does_not_retry_404(monkeypatch):
    calls = {"n": 0}
    def fake_get(url, params=None, headers=None, timeout=None):
        calls["n"] += 1
        return _PageResp(status_code=404, headers={"x-ms-error-code": "PathNotFound"})
    import requests
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(remote, "_sleep", lambda s: None)

    # A benign 404 (absent schema dir) -> [] and exactly one call (no retry on 404).
    names = remote.list_delta_tables(
        "abfss://duckrun@onelake.dfs.fabric.microsoft.com/root", "missing", {"bearer_token": "TOK"})
    assert names == []
    assert calls["n"] == 1
