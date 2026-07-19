"""OneLake bearer-token acquisition for the duckrun connection API.

Only used for ``abfss://`` (OneLake) stores when the caller didn't already supply a token in
``storage_options``. Every other backend (local / s3 / gcs / az://) authenticates through
``storage_options`` / DuckDB secrets / the environment, and never calls in here.

Acquisition order, cheapest first:
  1. inside a Microsoft Fabric notebook → ``notebookutils.credentials.getToken`` (no extra deps);
  2. an existing ``AZURE_STORAGE_TOKEN`` in the environment;
  3. ``azure-identity`` (Azure CLI, then interactive browser) — a core dependency, for use on a
     laptop.

This is a hard-trimmed descendant of the legacy duckrun ``auth.py``: no device-code / Colab /
Fabric-API-token branches — just what a storage read/write needs.
"""
import base64
import json
import os
import sys
import time
from typing import Optional

# OneLake storage scope; the resource notebookutils issues a "pbi"/"storage" token for.
_STORAGE_SCOPE = "https://storage.azure.com/.default"

# Fabric CONTROL-plane scope (api.fabric.microsoft.com) — a *different* audience than storage.
# Needed only by RemoteRunner (fabric_remote.py) to create/run/delete a temp notebook; a storage
# token 401s here. Inside a Fabric notebook the "pbi" audience already covers this API.
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Power BI REST scope (api.powerbi.com) — a *third* audience, needed only to refresh a semantic model
# after deploying it (workspace.deploy of a .bim). Inside a Fabric notebook the same "pbi" audience
# token covers it, so only the local azure-identity path needs this distinct scope.
_POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# GitHub-OIDC token exchange retries: the assertion fetch + AAD exchange is a short network hop that
# intermittently times out inside a busy dbt process (see issue #10). A single 15s timeout must not
# lose the whole token, so retry a couple of times with exponential backoff before giving up.
_OIDC_ATTEMPTS = 3


def _sleep(seconds: float) -> None:
    """Indirection so tests can stub out the retry wait."""
    time.sleep(seconds)

# Per-scope token cache. get_*_token() mints a token once and reuses it across the process instead of
# re-acquiring on every call — a dbt run over N catalogs mints a scoped secret + discovers + writes per
# catalog, so without this the OIDC/token endpoint is hammered (and rate-limits, then a later call fails
# and delta-rs falls back to Azure IMDS). Refreshed automatically when the cached token nears expiry.
_TOKEN_CACHE: dict = {}


def _cache_key(scope: str):
    """Cache key = (tenant, scope). The scope string is identical across Azure tenants, so a
    scope-only key would hand tenant A's cached token to a request meant for tenant B when the
    process switches AZURE_TENANT_ID (notebooks, tests). Single-tenant behavior is unchanged."""
    return (os.environ.get("AZURE_TENANT_ID") or "", scope)


def _cached_token(scope: str, acquire):
    """Return a cached token for ``scope`` if it's still comfortably valid, else acquire + cache one.
    ``acquire`` is a zero-arg callable returning a token or None. A non-JWT token (can't read expiry)
    is cached for the process — the same lifetime the caller would otherwise re-fetch it at."""
    key = _cache_key(scope)
    cached = _TOKEN_CACHE.get(key)
    if cached and not token_is_expiring(cached):
        return cached
    token = acquire()
    if token:
        _TOKEN_CACHE[key] = token
    return token


def _fabric_token() -> Optional[str]:
    """A storage token from the Fabric notebook runtime, or None when not in one."""
    try:
        import notebookutils  # type: ignore
    except ImportError:
        return None
    # Fabric exposes the storage token under the "storage" audience; older runtimes used "pbi".
    for audience in ("storage", "pbi"):
        try:
            token = notebookutils.credentials.getToken(audience)
            if token:
                return token
        except Exception:
            continue
    return None


def _azure_identity_token(scope: str = _STORAGE_SCOPE, interactive: bool = True) -> Optional[str]:
    """An Azure AD token for ``scope`` via azure-identity; None if the optional dependency is missing
    or every credential fails. ``scope`` selects the audience — storage (default), the Fabric control
    plane, or Power BI — so one credential chain serves all three.

    Non-interactive sources (``AzureCliCredential``) are always tried. ``InteractiveBrowserCredential``
    is only appended when ``interactive`` is True AND we're attached to a TTY — never on a headless
    runner, where it would try to open a browser and block on a local redirect listener (a long hang
    mid-build). ``refresh_storage_token`` passes ``interactive=False`` so a best-effort token refresh
    can never launch a browser."""
    try:
        from azure.identity import AzureCliCredential
    except ImportError:
        return None
    credentials = [AzureCliCredential]
    if interactive and sys.stdin.isatty():
        try:
            from azure.identity import InteractiveBrowserCredential
            credentials.append(InteractiveBrowserCredential)
        except ImportError:
            pass
    for credential in credentials:
        try:
            return credential().get_token(scope).token
        except Exception:
            continue
    return None


def _github_oidc_assertion() -> Optional[str]:
    """A FRESH GitHub Actions OIDC JWT to present as an Azure AD client assertion, or None when not
    running under GitHub Actions with ``id-token: write``. The request endpoint
    (``ACTIONS_ID_TOKEN_REQUEST_URL``) stays live for the whole job and mints a new JWT on every call,
    so it can re-authenticate long after the initial login token has expired — which is exactly what a
    multi-hour OneLake build needs."""
    import urllib.request

    req_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    req_tok = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not (req_url and req_tok):
        return None
    # api://AzureADTokenExchange is the audience azure/login configures the federated credential for.
    sep = "&" if "?" in req_url else "?"
    request = urllib.request.Request(
        f"{req_url}{sep}audience=api://AzureADTokenExchange",
        headers={"Authorization": f"Bearer {req_tok}"},
    )
    with urllib.request.urlopen(request, timeout=15) as resp:  # nosec - GitHub-internal endpoint
        return json.loads(resp.read().decode()).get("value")


def _github_oidc_token(scope: str = _STORAGE_SCOPE) -> Optional[str]:
    """An Azure AD token for ``scope`` via GitHub Actions **workload-identity federation**: exchange a
    fresh GitHub OIDC JWT for the token (client-assertion flow — no client secret, no refresh token).
    This keeps working after an ``az``-CLI session token has expired, which the CLI cannot renew under
    OIDC (the federated assertion it logged in with is long gone), so it is the one source that
    survives a build longer than the token's ~1h life. The same assertion authenticates the service
    principal for any scope it is authorized for — storage, the Fabric control plane, Power BI. None
    unless running under GitHub Actions with ``AZURE_CLIENT_ID`` / ``AZURE_TENANT_ID`` set and
    azure-identity present."""
    client_id = os.environ.get("AZURE_CLIENT_ID")
    tenant_id = os.environ.get("AZURE_TENANT_ID")
    if not (client_id and tenant_id and os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")):
        return None
    try:
        from azure.identity import ClientAssertionCredential
    except ImportError:
        return None
    cred = ClientAssertionCredential(tenant_id, client_id, _github_oidc_assertion)
    for attempt in range(_OIDC_ATTEMPTS):
        try:
            return cred.get_token(scope).token
        except Exception:
            if attempt < _OIDC_ATTEMPTS - 1:
                _sleep(float(2 ** attempt))  # 1s, 2s — ride out a transient endpoint timeout
    return None


def get_onelake_token() -> str:
    """Return a OneLake storage bearer token, trying Fabric → env → azure-identity in turn.

    Raises ``RuntimeError`` with actionable guidance if none is available, rather than handing
    back an empty/placeholder token that would fail later as an opaque 403.
    """
    token = _cached_token(_STORAGE_SCOPE, lambda: (
        _fabric_token()
        or _github_oidc_token()
        or os.environ.get("AZURE_STORAGE_TOKEN")
        or _azure_identity_token()
    ))
    if token:
        return token
    raise RuntimeError(
        "Could not acquire a OneLake token. Inside a Fabric notebook this is automatic; "
        "elsewhere set AZURE_STORAGE_TOKEN, or install the optional dependency "
        "(`pip install duckrun[local]`) and run `az login --scope "
        "https://storage.azure.com/.default`, or pass storage_options={'bearer_token': '...'} "
        "to connect()."
    )


def get_fabric_token() -> str:
    """Return a Fabric CONTROL-plane bearer token (``api.fabric.microsoft.com`` scope), trying the
    Fabric notebook runtime → ``FABRIC_TOKEN`` env → azure-identity (Azure CLI) in turn.

    This is the sibling of :func:`get_onelake_token` for the *control* plane: creating, running and
    deleting the temporary notebook that ``RemoteRunner`` uses. A storage token cannot call this API,
    so it is acquired separately. Raises ``RuntimeError`` with actionable guidance if none is
    available rather than returning a token that would 401 later.
    """
    token = _cached_token(_FABRIC_SCOPE, lambda: (
        _notebook_fabric_api_token()
        or _github_oidc_token(_FABRIC_SCOPE)
        or os.environ.get("FABRIC_TOKEN")
        or _azure_identity_token(_FABRIC_SCOPE)
    ))
    if token:
        return token
    raise RuntimeError(
        "Could not acquire a Fabric API token. Inside a Fabric notebook this is automatic; "
        "elsewhere set FABRIC_TOKEN, or install the optional dependency "
        "(`pip install duckrun[local]`) and run "
        "`az login --scope https://api.fabric.microsoft.com/.default`."
    )


def _notebook_fabric_api_token() -> Optional[str]:
    """A Fabric control-plane token from the Fabric notebook runtime, or None when not in one.
    Unlike :func:`_fabric_token` (which wants the *storage* audience), the control plane is served by
    the "pbi" audience token."""
    try:
        import notebookutils  # type: ignore
    except ImportError:
        return None
    try:
        return notebookutils.credentials.getToken("pbi") or None
    except Exception:
        return None


def get_powerbi_token() -> str:
    """Return a Power BI REST token (``api.powerbi.com`` scope), trying the Fabric notebook runtime →
    ``POWERBI_TOKEN`` env → azure-identity (Azure CLI) in turn.

    Needed only to refresh a semantic model after ``workspace.deploy`` of a ``.bim``. Inside a Fabric
    notebook the ``"pbi"`` audience token already covers the Power BI API, so the same runtime source
    as the Fabric control-plane token is reused. Raises ``RuntimeError`` with actionable guidance if
    none is available rather than returning a token that would 401 later.
    """
    token = _cached_token(_POWERBI_SCOPE, lambda: (
        _notebook_fabric_api_token()
        or _github_oidc_token(_POWERBI_SCOPE)
        or os.environ.get("POWERBI_TOKEN")
        or _azure_identity_token(_POWERBI_SCOPE)
    ))
    if token:
        return token
    raise RuntimeError(
        "Could not acquire a Power BI token (needed to refresh the deployed semantic model). "
        "Inside a Fabric notebook this is automatic; elsewhere set POWERBI_TOKEN, or install the "
        "optional dependency (`pip install duckrun[local]`) and run "
        "`az login --scope https://analysis.windows.net/powerbi/api/.default`."
    )


def _token_expiry_epoch(token: str) -> Optional[float]:
    """The ``exp`` (epoch seconds) of a JWT bearer token, or None if it isn't a decodable JWT.
    No signature check — we only read the expiry to know when to refresh."""
    try:
        seg = token.split(".")[1]
        seg += "=" * (-len(seg) % 4)  # restore base64url padding
        return float(json.loads(base64.urlsafe_b64decode(seg.encode())).get("exp"))
    except Exception:
        return None


def token_is_expiring(token: Optional[str], margin_seconds: int = 600) -> bool:
    """True if ``token`` is a JWT within ``margin_seconds`` of expiry (or already expired). False for
    a non-JWT / unparseable token (we can't tell, so don't churn) and for an empty token."""
    if not token:
        return False
    exp = _token_expiry_epoch(token)
    return exp is not None and time.time() >= exp - margin_seconds


def refresh_storage_token() -> Optional[str]:
    """A FRESH OneLake storage token from a *live, self-refreshing* source — the Fabric notebook
    runtime, then GitHub Actions workload-identity federation, then ``azure-identity`` (Azure CLI /
    managed identity). Deliberately SKIPS the static ``AZURE_STORAGE_TOKEN`` env var, which is exactly
    what may have gone stale on a long run. The GitHub-OIDC source is ordered before plain
    azure-identity because, under OIDC CI, ``AzureCliCredential`` cannot renew an expired token (no
    refresh token) whereas re-exchanging a fresh OIDC assertion always can. Returns None when no live
    source is available (then the caller keeps the token it has).

    Non-interactive only: ``_azure_identity_token(interactive=False)`` so a mid-run refresh (called
    from the per-statement cursor guard) can never pop a browser and hang a headless build."""
    token = _fabric_token() or _github_oidc_token() or _azure_identity_token(interactive=False)
    if token:
        # Keep the per-scope cache in sync: the next get_onelake_token() then reuses this fresh
        # token instead of finding the near-expiry one and re-acquiring on its own — two callers,
        # one notion of "current token".
        _TOKEN_CACHE[_cache_key(_STORAGE_SCOPE)] = token
    return token
