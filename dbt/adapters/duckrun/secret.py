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
from typing import Dict, Optional

# Stable name so repeated creation is an idempotent CREATE OR REPLACE, not a pile-up.
SECRET_NAME = "duckrun_onelake"


def bearer_token(storage_options: Optional[Dict[str, str]]) -> Optional[str]:
    """The bearer token from ``storage_options`` under any of the accepted keys, or None."""
    so = storage_options or {}
    return so.get("bearer_token") or so.get("token") or so.get("access_token")


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
    # OneLake/ADLS directory listing (DuckDB glob, used by relation discovery) goes through
    # the blob endpoint, and the azure extension's default transport fails the TLS handshake
    # there on some hosts ("Problem with the SSL CA cert"). The curl transport respects the
    # system CA bundle. This must be set at connection-open: discovery globs the store before
    # any dbt on-run-start `SET` hook runs, so relying on the hook leaves discovery (and thus
    # read-only commands) globbing with the broken transport → empty → "schema does not exist".
    transport = (storage_options or {}).get("azure_transport_option_type", "curl")
    try:
        conn.execute(f"SET GLOBAL azure_transport_option_type='{transport}'")
    except Exception:
        pass  # older azure extensions may not expose this knob; the default may still work
    conn.execute(
        f"CREATE OR REPLACE SECRET {SECRET_NAME} "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')"
    )
    return True
