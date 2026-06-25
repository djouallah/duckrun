"""Shared DuckDB Azure-secret setup for reading remote Delta stores (OneLake/ADLS).

The adapter reads existing Delta tables via DuckDB ``delta_scan``/``glob``. On a remote
object store those calls need a DuckDB secret to authenticate. There are two ways the
secret gets created:

  - az:// Azurite (and any store configured with a profile ``secrets:`` block): dbt-duckdb
    mints the secret itself at connection-open (``creds.secrets_sql()``), before anything
    runs — so discovery just works.
  - abfss:// OneLake: the only credential is a ``bearer_token`` in ``storage_options``;
    dbt-duckdb knows nothing about it, so *we* must mint the matching Azure ACCESS_TOKEN
    secret. Crucially this has to happen before relation discovery globs the store — not
    only when a model writes — or a read-only command (``dbt test``/``show``/``docs``),
    which materializes nothing, globs ``abfss://…`` with no secret, the glob throws, and
    discovery reports zero tables → "schema 'landing' does not exist".

This module is the single place that mints that secret, used by both the connection-setup
path (the plugin's ``configure_connection``) and the discovery path (the adapter), so the
two can't drift.
"""
import os
from typing import Dict, Optional

# Stable name so repeated creation is an idempotent CREATE OR REPLACE, not a pile-up.
SECRET_NAME = "duckrun_onelake"


def bearer_token(storage_options: Optional[Dict[str, str]]) -> Optional[str]:
    """The bearer token from ``storage_options`` under any of the accepted keys, or None."""
    so = storage_options or {}
    return so.get("bearer_token") or so.get("token") or so.get("access_token")


def refreshed(storage_options: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """``storage_options`` with a still-valid OneLake bearer token.

    A run longer than the token's ~1h lifetime would otherwise 401 mid-build, because the token is
    captured once at connection-open. When the current token is a JWT near expiry, re-acquire a fresh
    one from a LIVE source (Fabric notebook / azure-identity — see ``auth.refresh_storage_token``) and
    swap it in. Returns the SAME object when nothing changed (no token, not a JWT, not expiring, or no
    live source available), so callers can identity-check whether a re-mint is needed and short jobs
    pay nothing.
    """
    tok = bearer_token(storage_options)
    if not tok:
        return storage_options
    try:
        from duckrun import auth  # lazy: keep the adapter importable without the connect package
    except Exception:
        return storage_options
    if not auth.token_is_expiring(tok):
        return storage_options
    try:
        fresh = auth.refresh_storage_token()
    except Exception:
        fresh = None
    if not fresh or fresh == tok:
        return storage_options  # best effort — keep the (stale) token; nothing better available
    out = dict(storage_options)
    for k in ("bearer_token", "token", "access_token"):
        if k in out:
            out[k] = fresh
    out.setdefault("bearer_token", fresh)
    return out


def ensure_azure_secret(conn, storage_options: Optional[Dict[str, str]]) -> bool:
    """Mint the DuckDB Azure secret from a bearer token in ``storage_options`` on ``conn``.

    No-op (returns False) when there is no token — the notebook case where the secret is
    already provided, or a local/az:// store that doesn't use one. Otherwise installs the
    azure extension, sets the transport (see below), creates the secret (idempotent
    ``CREATE OR REPLACE``), and returns True.

    Raises on failure so the caller can decide: discovery, which *depends* on this, logs and
    surfaces the empty result instead of letting it masquerade as "no tables".
    """
    token = bearer_token(storage_options)
    if not token:
        return False
    conn.execute("INSTALL azure; LOAD azure;")
    # Set the azure HTTP transport at connection-open (here), NOT via an on-run-start hook:
    # hooks only fire for run/build/seed, so read-only commands that still open the store —
    # dbt test / show / docs generate — would miss it. On some runners the azure extension's
    # default transport fails the OneLake TLS handshake ("Problem with the SSL CA cert"); the
    # curl transport respects the system CA bundle. Driven by AZURE_TRANSPORT_OPTION_TYPE so the
    # value isn't hardcoded; absent → leave DuckDB's default.
    transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if transport:
        transport_sql = transport.replace("'", "''")
        conn.execute(f"SET GLOBAL azure_transport_option_type = '{transport_sql}'")
    # Escape single quotes so a token containing one can't break out of the SQL string literal.
    token_sql = token.replace("'", "''")
    conn.execute(
        f"CREATE OR REPLACE SECRET {SECRET_NAME} "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token_sql}')"
    )
    return True
