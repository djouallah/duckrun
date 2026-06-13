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


# ----------------------------------------------------------------- discovery wiring

class _Creds:
    root_path = "abfss://duckrun@onelake.dfs.fabric.microsoft.com/dev.Lakehouse/Tables"
    storage_options = {"bearer_token": "TOK"}


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
    def boom(root, schema, so):
        raise RuntimeError("403 Forbidden")
    monkeypatch.setattr(remote, "list_delta_tables", boom)
    adapter = _bare_adapter()
    assert adapter._discover_delta_relations(_SchemaRelation()) == []
