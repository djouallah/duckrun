"""Does duckrun mint its own tokens on THIS laptop? No mocks, no env vars, no `az` required.

    pytest tests/adapter/test_local_auth.py -v -s

Every other auth test in this folder is mocked — they pin the *wiring* (which credential is tried,
in what order, what gets cached). None of them prove a real token actually comes back on a laptop,
and ``tests/integration_tests/coffee`` doesn't either: it reads ONELAKE_TOKEN/AZURE_STORAGE_TOKEN
from the environment, which tests the user's shell, not duckrun.

So this file deliberately does the opposite of the rest of the folder: it *unsets* every
pre-supplied token, clears the token cache, and calls the real acquisition path. Green means
duckrun signed in by itself — via an existing Azure CLI session if there is one, otherwise by
opening a browser.

Skipped on CI: there is no browser there, and the CI path (GitHub OIDC workload identity) is a
different source entirely, already exercised for real by aemo.yml and integration_tests_onelake.yml.
"""
import base64
import json
import os
import time

import pytest

from duckrun import auth

pytestmark = pytest.mark.skipif(
    bool(os.environ.get("CI")),
    reason="laptop-only: CI has no browser, and its OIDC path is covered by the OneLake workflows",
)


def _claims(token):
    """The JWT payload as a dict. Unverified — we only read `aud`/`exp` to prove what we got."""
    seg = token.split(".")[1]
    seg += "=" * (-len(seg) % 4)  # restore base64url padding
    return json.loads(base64.urlsafe_b64decode(seg.encode()))


@pytest.fixture
def unassisted(monkeypatch):
    """Strip every way a token could be handed to duckrun, so acquisition is the only path left.

    Without this the test is a tautology: a stale AZURE_STORAGE_TOKEN in the shell (or a token
    cached earlier in the same pytest process) would satisfy get_*_token without duckrun signing in
    at all. Also forces `isatty` True — pytest replaces stdin, so `_azure_identity_token` would
    otherwise skip InteractiveBrowserCredential and quietly test only the `az`-CLI branch, which is
    the branch we're trying not to depend on.
    """
    for var in ("AZURE_STORAGE_TOKEN", "FABRIC_TOKEN", "POWERBI_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(auth.sys.stdin, "isatty", lambda: True)
    auth._TOKEN_CACHE.clear()
    yield
    auth._TOKEN_CACHE.clear()


def _assert_live_token(token, audience):
    """A real, unexpired bearer token for `audience` — not a placeholder, not something stale."""
    assert token, "no token returned"
    claims = _claims(token)  # raises if it isn't a decodable JWT
    aud = claims.get("aud", "")
    assert audience in aud, f"token audience is {aud!r}, expected {audience}"
    exp = claims.get("exp")
    assert exp and exp > time.time(), "token is already expired"
    print(f"\n  {audience}: valid for {(exp - time.time()) / 60:.0f} more minutes")


@pytest.fixture
def which_credential(monkeypatch):
    """Records which azure-identity credential actually produced the token, so a passing run says
    *how* it signed in instead of just going green. Observational only — nothing asserts on it,
    because either source (an existing `az` session, or the browser) is a legitimate pass."""
    import azure.identity

    used = []
    for name in ("AzureCliCredential", "InteractiveBrowserCredential"):
        cls = getattr(azure.identity, name)
        original = cls.get_token

        def get_token(self, *args, _name=name, _original=original, **kwargs):
            result = _original(self, *args, **kwargs)
            used.append(_name)
            return result

        monkeypatch.setattr(cls, "get_token", get_token)
    return used


def test_laptop_mints_onelake_token(unassisted, which_credential):
    """The storage token every read/write of an abfss:// table needs."""
    _assert_live_token(auth.get_onelake_token(), "https://storage.azure.com")
    print(f"  signed in via {which_credential[-1] if which_credential else 'a non-laptop source'}")


def test_laptop_mints_fabric_token(unassisted):
    """The control-plane token — a different audience; workspace.py and RemoteRunner need it to
    resolve locally too, and a storage token 401s against that API."""
    _assert_live_token(auth.get_fabric_token(), "https://api.fabric.microsoft.com")
