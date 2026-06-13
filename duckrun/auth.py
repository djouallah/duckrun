"""OneLake bearer-token acquisition for the duckrun connection API.

Only used for ``abfss://`` (OneLake) stores when the caller didn't already supply a token in
``storage_options``. Every other backend (local / s3 / gcs / az://) authenticates through
``storage_options`` / DuckDB secrets / the environment, and never calls in here.

Acquisition order, cheapest first:
  1. inside a Microsoft Fabric notebook → ``notebookutils.credentials.getToken`` (no extra deps);
  2. an existing ``AZURE_STORAGE_TOKEN`` in the environment;
  3. ``azure-identity`` (Azure CLI, then interactive browser) — the optional ``duckrun[local]``
     extra, for use on a laptop.

This is a hard-trimmed descendant of the legacy duckrun ``auth.py``: no device-code / Colab /
Fabric-API-token branches — just what a storage read/write needs.
"""
import os
from typing import Optional

# OneLake storage scope; the resource notebookutils issues a "pbi"/"storage" token for.
_STORAGE_SCOPE = "https://storage.azure.com/.default"

# Azure CLI's well-known public client id, used for the interactive/CLI fallbacks.
_AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"


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


def _azure_identity_token() -> Optional[str]:
    """A storage token via azure-identity (CLI, then interactive browser); None if the optional
    dependency is missing or every credential fails."""
    try:
        from azure.identity import AzureCliCredential, InteractiveBrowserCredential
    except ImportError:
        return None
    for credential in (AzureCliCredential, InteractiveBrowserCredential):
        try:
            return credential().get_token(_STORAGE_SCOPE).token
        except Exception:
            continue
    return None


def get_onelake_token() -> str:
    """Return a OneLake storage bearer token, trying Fabric → env → azure-identity in turn.

    Raises ``RuntimeError`` with actionable guidance if none is available, rather than handing
    back an empty/placeholder token that would fail later as an opaque 403.
    """
    token = _fabric_token() or os.environ.get("AZURE_STORAGE_TOKEN") or _azure_identity_token()
    if token:
        return token
    raise RuntimeError(
        "Could not acquire a OneLake token. Inside a Fabric notebook this is automatic; "
        "elsewhere set AZURE_STORAGE_TOKEN, or install the optional dependency "
        "(`pip install duckrun[local]`) and run `az login --scope "
        "https://storage.azure.com/.default`, or pass storage_options={'bearer_token': '...'} "
        "to connect()."
    )
