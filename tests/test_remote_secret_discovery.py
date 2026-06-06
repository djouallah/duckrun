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
from dbt.adapters.duckrun import secret
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
    # The curl transport must be set too, or discovery globs the OneLake blob endpoint with
    # the default transport (SSL CA failure) and comes back empty — the standalone-test bug.
    assert "azure_transport_option_type='curl'" in joined


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


# ----------------------------------------------------------------- discovery order

class _FakeGlobCursor:
    """Stands in for a DuckDB cursor over abfss://: ``glob`` raises until the Azure secret
    has been created, exactly like an unauthenticated OneLake glob does."""

    def __init__(self, table_dir, rows):
        self._table_dir = table_dir
        self._rows = rows
        self.secret_ready = False
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        low = sql.lower()
        if "create or replace secret" in low:
            self.secret_ready = True
            self._result = []
            return self
        if "glob(" in low:
            if not self.secret_ready:
                raise RuntimeError("IO Error: 401 Unauthorized (no Azure secret)")
            self._result = self._rows
            return self
        self._result = []
        return self

    def fetchall(self):
        return self._result


class _Creds:
    root_path = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables"
    storage_options = {"bearer_token": "TOK"}


class _Config:
    credentials = _Creds()


class _SchemaRelation:
    schema = "landing"
    database = "memory"


def _bare_adapter(cursor):
    # Bypass __init__ (which needs a full RuntimeConfig / mp_context); we only exercise
    # discovery, which uses self.config, self._cursor(), self.Relation and self.create_schema.
    adapter = object.__new__(DuckrunAdapter)
    adapter.config = _Config()
    adapter._cursor = lambda: cursor
    return adapter


def test_discovery_mints_secret_before_globbing_remote_store():
    rows = [
        ("abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/landing/"
         "fct_price/_delta_log/00000000000000000001.json",),
    ]
    cursor = _FakeGlobCursor("fct_price", rows)
    adapter = _bare_adapter(cursor)

    relations = adapter._discover_delta_relations(_SchemaRelation())

    # The glob only succeeded because the secret was minted first; if discovery had globbed
    # before ensuring the secret, the glob would have thrown and relations would be empty.
    assert [r.identifier for r in relations] == ["fct_price"]
    assert all(r.type == RelationType.Table for r in relations)
    # And the ordering is explicit: secret SQL precedes the glob SQL.
    secret_idx = next(i for i, s in enumerate(cursor.executed) if "secret" in s.lower())
    glob_idx = next(i for i, s in enumerate(cursor.executed) if "glob(" in s.lower())
    assert secret_idx < glob_idx
