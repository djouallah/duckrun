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
import re
from typing import Dict, Optional

# Stable name so repeated creation is an idempotent CREATE OR REPLACE, not a pile-up.
SECRET_NAME = "duckrun_onelake"

# The storage_options keys a bearer token may arrive under, most-preferred first. The single source of
# truth for the alias set — bearer_token(), the obstore builder, and fabric_remote all read it, so the
# accepted spellings can't drift between the read, write, and file-transfer paths.
TOKEN_KEYS = ("bearer_token", "token", "access_token")


def scoped_secret_name(catalog_name: str) -> str:
    """A stable DuckDB secret name for an attached catalog's path-scoped Azure token."""
    return "duckrun_cat_" + re.sub(r"[^0-9A-Za-z_]", "_", str(catalog_name))


def bearer_token(storage_options: Optional[Dict[str, str]]) -> Optional[str]:
    """The bearer token from ``storage_options`` under any of the accepted keys, or None."""
    so = storage_options or {}
    for k in TOKEN_KEYS:
        if so.get(k):
            return so[k]
    return None


_IN_FABRIC: Optional[bool] = None


def _in_fabric_notebook() -> bool:
    """True when running inside a Microsoft Fabric notebook (``notebookutils`` importable).
    Cached: a FAILED import is not cached by Python, so without the memo every secret mint
    re-runs the full sys.path scan off-Fabric — and this runs per statement via handle()."""
    global _IN_FABRIC
    if _IN_FABRIC is None:
        try:
            import notebookutils  # type: ignore  # noqa: F401
            _IN_FABRIC = True
        except ImportError:
            _IN_FABRIC = False
    return _IN_FABRIC


_IS_WINDOWS = os.name == "nt"


def _resolve_azure_transport() -> Optional[str]:
    """The DuckDB ``azure_transport_option_type`` to set, or None to leave DuckDB's default.

    An explicit ``AZURE_TRANSPORT_OPTION_TYPE`` env var always wins. Otherwise: on a non-Windows
    machine outside Fabric (a laptop or CI runner) the azure extension's ``default`` transport can
    fail the OneLake TLS handshake ("Problem with the SSL CA cert") — ``curl`` respects the system
    CA bundle, so default to it. On Windows it's the other way around: the extension's libcurl has
    no CA bundle, so ``curl`` fails every handshake with "SSL peer certificate or SSH remote key
    was not OK" (issue #16 regression report), while ``default`` (WinHTTP) trusts the system cert
    store — leave DuckDB's default. Inside a Fabric notebook ``default`` already works too.
    """
    env = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if env:
        return env
    if _IS_WINDOWS or _in_fabric_notebook():
        return None
    return "curl"


def _set_azure_transport(conn) -> None:
    """Set the azure HTTP transport at connection-open (see :func:`_resolve_azure_transport`). Done
    here, NOT via an on-run-start hook: hooks only fire for run/build/seed, so read-only commands that
    still open the store — dbt test / show / docs generate — would miss it. A ``SET`` failure (e.g. an
    unsupported value) must never hard-fail the connection, so it's swallowed."""
    transport = _resolve_azure_transport()
    if not transport:
        return
    transport_sql = transport.replace("'", "''")
    try:
        conn.execute(f"SET GLOBAL azure_transport_option_type = '{transport_sql}'")
    except Exception:
        pass


def with_onelake_token(root_path: Optional[str],
                       storage_options: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """For an ``abfss://`` (OneLake) target whose profile carries **no** bearer token, acquire one
    from the live source — ``notebookutils`` inside a Fabric notebook, else env / azure-identity —
    and return ``storage_options`` with it. This is the same fallback ``duckrun.connect()`` uses, so
    a dbt run inside a Fabric notebook needs no token in ``profiles.yml``: discovery (the OneLake DFS
    REST list) and the DuckDB Azure secret both get a token, and read-only commands (``dbt test`` /
    ``show`` / ``docs``) stop failing with "schema does not exist".

    Returns ``storage_options`` UNCHANGED for a non-OneLake store, when a token is already present,
    or when the ``duckrun`` auth package isn't installed — so local / ``az://`` / s3 / gcs paths are
    untouched.

    For an ``abfss://`` root, a token acquisition *failure* (``RuntimeError``) is deliberately NOT
    swallowed: with no token the subsequent OneLake read falls back to anonymous and dies with an
    opaque ``Unauthorized`` (issue #10). Letting the acquisition error propagate surfaces the real
    cause — a token-fetch timeout — with its actionable guidance instead.
    """
    if not str(root_path or "").startswith("abfss://") or bearer_token(storage_options):
        return storage_options
    try:
        from duckrun import auth  # lazy: keep the adapter importable without the connect package
    except ImportError:
        return storage_options  # connect package absent — nothing to acquire from; leave it untouched
    token = auth.get_onelake_token()  # RuntimeError propagates: an abfss read with no token is fatal
    out = dict(storage_options or {})
    out["bearer_token"] = token
    return out


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
    debug = os.environ.get("DUCKRUN_AUTH_DEBUG")
    if not tok:
        return storage_options
    try:
        from duckrun import auth  # lazy: keep the adapter importable without the connect package
    except Exception:
        return storage_options
    # DUCKRUN_AUTH_FORCE_REFRESH treats the token as always expiring — a CI escape hatch to exercise
    # the full refresh+re-mint path on every model (a 30s build) instead of waiting for real expiry.
    forced = bool(os.environ.get("DUCKRUN_AUTH_FORCE_REFRESH"))
    expiring = forced or auth.token_is_expiring(tok)
    if not expiring:
        return storage_options  # cheap path — runs on every statement, so stays silent here
    if debug:
        exp = auth._token_expiry_epoch(tok)
        import time as _t
        remaining = None if exp is None else round(exp - _t.time())
        print(
            f"[duckrun-auth] refreshed: tok_len={len(tok)} exp={exp} remaining={remaining}s "
            f"expiring={expiring} forced={forced}",
            flush=True,
        )
    try:
        fresh = auth.refresh_storage_token()
    except Exception as e:
        if debug:
            print(f"[duckrun-auth] refresh_storage_token raised: {e!r}", flush=True)
        fresh = None
    if debug:
        print(
            f"[duckrun-auth] refresh result: "
            f"{('OK len=%d' % len(fresh)) if fresh else 'NONE'} changed={bool(fresh and fresh != tok)}",
            flush=True,
        )
    if not fresh or fresh == tok:
        return storage_options  # best effort — keep the (stale) token; nothing better available
    out = dict(storage_options)
    for k in TOKEN_KEYS:
        if k in out:
            out[k] = fresh
    out.setdefault("bearer_token", fresh)
    return out


def _execute_secret_sql(conn, sql: str, token: str) -> None:
    """Run a ``CREATE SECRET`` statement without ever letting the bearer token escape into the
    raised error. DuckDB echoes the offending statement into exception text (``LINE N: <sql>``),
    which would land the token in the caller's error message and logs. Re-raise the same exception
    type with the token redacted, and drop the original (token-bearing) exception from the chain."""
    redacted = None
    try:
        conn.execute(sql)
    except Exception as exc:
        # Redact both the escaped form (as embedded in the SQL) and the raw token.
        msg = str(exc).replace(token.replace("'", "''"), "<redacted>").replace(token, "<redacted>")
        try:
            redacted = type(exc)(msg)
        except Exception:
            redacted = RuntimeError(msg)
    # Raised OUTSIDE the handler: `raise … from None` inside it would only suppress the display of
    # the original (token-bearing) exception, which stays reachable via __context__ for any log
    # framework that walks the chain. Out here no context is attached at all.
    if redacted is not None:
        raise redacted


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
    _set_azure_transport(conn)
    # Escape single quotes so a token containing one can't break out of the SQL string literal.
    token_sql = token.replace("'", "''")
    _execute_secret_sql(
        conn,
        f"CREATE OR REPLACE SECRET {SECRET_NAME} "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token_sql}')",
        token,
    )
    return True


def mint_scoped_secret(conn, secret_name: str, root: str,
                       storage_options: Optional[Dict[str, str]]) -> bool:
    """Mint a path-**scoped** DuckDB Azure secret for an attached (non-default) catalog.

    Two different OneLake bearer tokens (one per Lakehouse/workspace) can then coexist in one
    connection: DuckDB picks the longest-matching ``SCOPE`` per ``delta_scan``, so each catalog's
    reads use its own token. The default catalog keeps the unscoped ``ensure_azure_secret``. No-op
    (returns False) when there is no token. Lifted from ``duckrun.session._mint_scoped_secret`` so
    the dbt adapter and the notebook session share one implementation."""
    token = bearer_token(storage_options)
    if not token:
        return False
    conn.execute("INSTALL azure; LOAD azure;")
    _set_azure_transport(conn)
    token_sql = token.replace("'", "''")
    scope_sql = str(root).replace("'", "''")
    _execute_secret_sql(
        conn,
        f"CREATE OR REPLACE SECRET {secret_name} "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token_sql}', SCOPE '{scope_sql}')",
        token,
    )
    return True


def refresh_catalog_secret(cursor, catalog_name: str,
                           storage_options: Optional[Dict[str, str]], *,
                           is_default: bool, root: Optional[str]) -> Optional[Dict[str, str]]:
    """Refresh a near-expiry OneLake token in ``storage_options`` and re-mint its DuckDB secret on
    ``cursor``, so a session/build outliving the token's ~1h life doesn't 401 mid-run. The default
    catalog re-mints the unscoped :func:`ensure_azure_secret`; an attached catalog re-mints its
    path-scoped :func:`mint_scoped_secret` (needs ``root`` for the SCOPE).

    Returns the (possibly new) ``storage_options`` — the SAME object when nothing changed (no token,
    not a JWT, not expiring, or no live source), so callers can identity-check whether to persist it.
    Best-effort: a re-mint failure keeps the old secret but still returns the refreshed options. Used
    by both the dbt cursor guard and the connect() session so the two can't drift."""
    if not bearer_token(storage_options):
        return storage_options
    fresh = refreshed(storage_options)
    if fresh is storage_options:
        return storage_options  # token still valid (the common path) — nothing to do
    try:
        if is_default:
            ensure_azure_secret(cursor, fresh)
        else:
            mint_scoped_secret(cursor, scoped_secret_name(catalog_name), root, fresh)
        if os.environ.get("DUCKRUN_AUTH_DEBUG"):
            print(f"[duckrun-auth] re-minted DuckDB secret for catalog {catalog_name!r}", flush=True)
    except Exception as e:  # best-effort: a transient refresh failure keeps the old secret
        if os.environ.get("DUCKRUN_AUTH_DEBUG"):
            print(f"[duckrun-auth] re-mint failed for {catalog_name!r}: {e!r}", flush=True)
    return fresh
