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

    def close(self):  # LocalEnvironment.__del__ calls conn.close(); no-op for the fake
        pass


def test_retry_request_uses_shared_session_when_unpatched(monkeypatch):
    # The DFS retry loop pools connections through ONE module-level Session on the production
    # path (requests.get builds and tears down a Session — a fresh TLS handshake — per call);
    # a monkeypatched requests.get (every other test in this file) still wins over the pool.
    sess = remote._session()
    assert sess is remote._session()  # lazy singleton
    calls = []
    ok = type("_Ok", (), {"status_code": 200})()
    monkeypatch.setattr(sess, "get", lambda url, **kw: calls.append(url) or ok)
    resp = remote.retry_request("get", "https://example.invalid/x", headers={})
    assert resp is ok and calls == ["https://example.invalid/x"]


def test_default_secret_minted_once_per_connection_and_token(monkeypatch):
    # handle() runs per statement and secrets are database-global: an unchanged (connection,
    # token) pair must not re-run INSTALL/LOAD + CREATE SECRET, but a rebuilt connection must.
    from dbt.adapters.duckrun.credentials import DuckrunCredentials
    from dbt.adapters.duckrun.environment import DuckrunEnvironment

    monkeypatch.setattr("duckrun.auth.get_onelake_token", lambda: "OIDCTOK")
    creds = DuckrunCredentials(
        database="db", schema="mart", path=":memory:",
        root_path="abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables",
        storage_options=None)
    env = DuckrunEnvironment(creds)
    env.conn = _RecordingConn()
    env._ensure_default_secret()
    env._ensure_default_secret()  # same connection + token → guarded no-op
    joined = "\n".join(env.conn.sql).lower()
    assert joined.count("create or replace secret duckrun_onelake") == 1
    env.conn = _RecordingConn()   # rebuilt connection → the secret is gone, must re-mint
    env._ensure_default_secret()
    assert "create or replace secret duckrun_onelake" in "\n".join(env.conn.sql).lower()


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


def test_default_read_secret_self_acquires_for_tokenless_abfss(monkeypatch):
    # The OneLake read bug: a pure-OIDC (token-less) abfss profile minted NO DuckDB read secret at
    # connection open, so every in-model delta_scan/read of OneLake 401'd. handle() now mints the
    # default catalog's secret from a SELF-ACQUIRED token (creds.root_for -> with_onelake_token), so
    # reads authenticate exactly like writes. This pins that the connection-open mint self-acquires.
    from dbt.adapters.duckrun.credentials import DuckrunCredentials
    from dbt.adapters.duckrun.environment import DuckrunEnvironment

    monkeypatch.setattr("duckrun.auth.get_onelake_token", lambda: "OIDCTOK")
    creds = DuckrunCredentials(
        database="db", schema="mart", path=":memory:",
        root_path="abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables",
        storage_options=None)  # <-- no bearer_token: the pure-OIDC case
    env = DuckrunEnvironment(creds)
    env.conn = _RecordingConn()
    env._ensure_default_secret()
    joined = "\n".join(env.conn.sql).lower()
    assert "create or replace secret duckrun_onelake" in joined
    assert "oidctok" in joined  # the self-acquired token was minted into the read secret


def test_handle_wires_the_read_secret_mint(monkeypatch):
    # Wiring guard: handle() MUST invoke the read-secret mint. This pins the exact omission that caused
    # the #10 read bug — self-acquire was wired into writes/discovery/connect() but the adapter's
    # connection-open read mint was never called. Removing _ensure_default_secret() from handle() (or
    # never adding it) fails HERE, not silently in production against a token-less OneLake profile.
    from dbt.adapters.duckrun.credentials import DuckrunCredentials
    from dbt.adapters.duckrun.environment import DuckrunEnvironment
    from dbt.adapters.duckdb.environments.local import LocalEnvironment

    monkeypatch.setattr(LocalEnvironment, "handle", lambda self: None)  # no real DuckDB connection
    creds = DuckrunCredentials(
        database="db", schema="mart", path=":memory:",
        root_path="abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables", storage_options=None)
    env = DuckrunEnvironment(creds)
    called = []
    monkeypatch.setattr(env, "_ensure_default_secret", lambda: called.append("mint"))
    monkeypatch.setattr(env, "_attach_catalogs", lambda: None)
    env.handle()
    assert called == ["mint"]  # handle() invoked the connection-open read-secret mint


def test_attach_catalogs_self_acquires_for_tokenless_catalog(monkeypatch):
    # The per-catalog read path: a token-less abfss:// catalog root must self-acquire before minting its
    # scoped secret, so a pure-OIDC MULTI-Lakehouse project can READ each Lakehouse, not just write.
    from dbt.adapters.duckrun.credentials import DuckrunCredentials
    from dbt.adapters.duckrun.environment import DuckrunEnvironment

    monkeypatch.setattr("duckrun.auth.get_onelake_token", lambda: "OIDCTOK")
    creds = DuckrunCredentials(
        database="db", schema="mart", path=":memory:",
        root_path="abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables", storage_options=None,
        catalogs={"bronze": {
            "root_path": "abfss://ws@onelake.dfs.fabric.microsoft.com/bronze/Tables",
            "storage_options": None}})   # <-- token-less catalog
    env = DuckrunEnvironment(creds)
    env.conn = _RecordingConn()
    env._attach_catalogs()
    joined = "\n".join(env.conn.sql).lower()
    assert "oidctok" in joined  # the per-catalog secret self-acquired the OIDC token


# ------------------------------------------------------------- azure HTTP transport

def _transport_set(conn):
    """The value SET for azure_transport_option_type on ``conn``, or None if it wasn't set."""
    for sql in conn.sql:
        low = sql.lower()
        if "set global azure_transport_option_type" in low:
            return sql.split("'")[1]
    return None


def test_transport_defaults_to_curl_outside_fabric(monkeypatch):
    # Off a Fabric notebook and with no override, duckrun picks curl (system CA bundle) so the
    # OneLake TLS handshake works on a laptop / CI runner without any env spoon-feeding.
    monkeypatch.delenv("AZURE_TRANSPORT_OPTION_TYPE", raising=False)
    monkeypatch.setattr(secret, "_in_fabric_notebook", lambda: False)
    conn = _RecordingConn()
    secret.ensure_azure_secret(conn, {"bearer_token": "TOK"})
    assert _transport_set(conn) == "curl"


def test_transport_untouched_in_fabric_notebook(monkeypatch):
    # Inside a Fabric notebook the default transport already works — leave it alone (no regression).
    monkeypatch.delenv("AZURE_TRANSPORT_OPTION_TYPE", raising=False)
    monkeypatch.setattr(secret, "_in_fabric_notebook", lambda: True)
    conn = _RecordingConn()
    secret.ensure_azure_secret(conn, {"bearer_token": "TOK"})
    assert _transport_set(conn) is None            # no SET issued
    assert any("create or replace secret" in s.lower() for s in conn.sql)  # secret still minted


def test_transport_env_override_wins_even_in_fabric(monkeypatch):
    # An explicit env value overrides everything, including the in-notebook "leave it" default.
    monkeypatch.setenv("AZURE_TRANSPORT_OPTION_TYPE", "default")
    monkeypatch.setattr(secret, "_in_fabric_notebook", lambda: True)
    conn = _RecordingConn()
    secret.ensure_azure_secret(conn, {"bearer_token": "TOK"})
    assert _transport_set(conn) == "default"


# ------------------------------------------------- write-path token self-acquire

def test_with_onelake_token_fills_bearer_for_abfss(monkeypatch):
    # The dbt write path calls this for an abfss:// target with no token — it must self-acquire.
    monkeypatch.setattr("duckrun.auth.get_onelake_token", lambda: "SELFACQUIRED")
    out = secret.with_onelake_token(
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo/m", {})
    assert out["bearer_token"] == "SELFACQUIRED"


def test_with_onelake_token_noop_when_token_present(monkeypatch):
    monkeypatch.setattr("duckrun.auth.get_onelake_token",
                        lambda: pytest.fail("must not self-acquire when a token is already set"))
    so = {"bearer_token": "HAVE"}
    assert secret.with_onelake_token("abfss://x@h/y", so) is so


def test_with_onelake_token_noop_for_local_path(monkeypatch):
    monkeypatch.setattr("duckrun.auth.get_onelake_token",
                        lambda: pytest.fail("must not self-acquire for a non-abfss path"))
    assert secret.with_onelake_token("./warehouse", {}) == {}


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


# ------------------------------------ notebookutils token fallback (Fabric, no profile token)
# Inside a Fabric notebook the profile carries NO bearer token — the notebook has its own via
# notebookutils. The adapter must acquire it so read-only discovery (the REST list) and the
# delta_scan views work, exactly like duckrun.connect(). Without this a remote `dbt test` fails
# every test with "schema does not exist".

def test_with_onelake_token_acquires_for_abfss_without_token(monkeypatch):
    import duckrun.auth as auth
    monkeypatch.setattr(auth, "get_onelake_token", lambda: "NBTOK")
    out = secret.with_onelake_token("abfss://x@h/y", {})
    assert out["bearer_token"] == "NBTOK"


def test_with_onelake_token_propagates_acquire_failure_for_abfss(monkeypatch):
    # issue #10: a swallowed token-fetch failure on an abfss root turns into an opaque
    # "Unauthorized" downstream. The RuntimeError must surface as itself, not a silent no-op.
    import duckrun.auth as auth
    monkeypatch.setattr(auth, "get_onelake_token",
                        lambda: (_ for _ in ()).throw(RuntimeError("token fetch timed out")))
    with pytest.raises(RuntimeError, match="token fetch timed out"):
        secret.with_onelake_token("abfss://x@h/y", {})


def test_with_onelake_token_noop_when_token_present(monkeypatch):
    import duckrun.auth as auth
    monkeypatch.setattr(auth, "get_onelake_token",
                        lambda: (_ for _ in ()).throw(AssertionError("must not fetch when present")))
    so = {"bearer_token": "HAVE"}
    assert secret.with_onelake_token("abfss://x@h/y", so) is so  # unchanged, no fetch


def test_with_onelake_token_noop_for_non_onelake(monkeypatch):
    import duckrun.auth as auth
    monkeypatch.setattr(auth, "get_onelake_token",
                        lambda: (_ for _ in ()).throw(AssertionError("must not fetch for non-abfss")))
    assert secret.with_onelake_token("az://lake/Tables", {}) == {}
    assert secret.with_onelake_token("./warehouse", None) is None


class _CredsNoToken:
    """The Fabric-notebook profile: abfss root, no bearer token (it comes from notebookutils)."""
    root_path = "abfss://duckrun@onelake.dfs.fabric.microsoft.com/dev.Lakehouse/Tables"
    storage_options = {}
    catalogs = None

    def root_for(self, database=None):
        return (self.root_path, self.storage_options)


def test_discovery_acquires_notebook_token_when_profile_has_none(monkeypatch):
    import duckrun.auth as auth
    monkeypatch.setattr(auth, "get_onelake_token", lambda: "NBTOK")

    captured = {}
    def fake_list(root, schema, so):
        captured["so"] = so
        return ["fct_price"]
    monkeypatch.setattr(remote, "list_delta_tables", fake_list)

    conn = _RecordingConn()
    adapter = object.__new__(DuckrunAdapter)
    creds = _CredsNoToken()
    adapter.config = type("_Cfg", (), {"credentials": creds})()
    adapter._cursor = lambda: conn  # so the secret-mint has a connection to run on

    rels = adapter._discover_delta_relations(_SchemaRelation())

    assert [r.identifier for r in rels] == ["fct_price"]
    # the notebookutils token reached the REST list, was minted as the DuckDB secret, and persisted
    assert captured["so"]["bearer_token"] == "NBTOK"
    assert "nbtok" in "\n".join(conn.sql).lower()
    assert creds.storage_options.get("bearer_token") == "NBTOK"


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


# ------------------------------------------------- OIDC token-exchange retry (issue #10)

class _FlakyCred:
    """A ClientAssertionCredential stand-in whose get_token fails ``fail_first`` times, then
    returns a token. ``calls`` records how many attempts were made across all instances."""
    calls = {"n": 0}
    fail_first = 0

    def __init__(self, tenant_id, client_id, assertion):
        pass

    def get_token(self, scope):
        _FlakyCred.calls["n"] += 1
        if _FlakyCred.calls["n"] <= _FlakyCred.fail_first:
            raise TimeoutError("The read operation timed out")
        return type("T", (), {"token": "OIDCTOK"})()


def _arm_oidc(monkeypatch, fail_first):
    import duckrun.auth as auth
    import azure.identity
    _FlakyCred.calls = {"n": 0}
    _FlakyCred.fail_first = fail_first
    monkeypatch.setattr(azure.identity, "ClientAssertionCredential", _FlakyCred)
    monkeypatch.setattr(auth, "_sleep", lambda s: None)  # no real waiting
    monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
    monkeypatch.setenv("AZURE_TENANT_ID", "tid")
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://gh/oidc")
    return auth


def test_github_oidc_token_retries_transient_then_succeeds(monkeypatch):
    auth = _arm_oidc(monkeypatch, fail_first=1)  # 1st attempt times out, 2nd succeeds
    assert auth._github_oidc_token() == "OIDCTOK"
    assert _FlakyCred.calls["n"] == 2


def test_github_oidc_token_returns_none_after_all_attempts_fail(monkeypatch):
    auth = _arm_oidc(monkeypatch, fail_first=99)  # every attempt times out
    assert auth._github_oidc_token() is None
    assert _FlakyCred.calls["n"] == auth._OIDC_ATTEMPTS
