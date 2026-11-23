"""The runtime version guardrail (``duckrun._runtime.check_runtime_versions``).

duckrun requires ``deltalake>=1.6.4`` — the first release whose commit API takes remove actions as
well as adds — and ``duckdb>=2.0.1`` (nested schemas). The guard
exists for the Fabric "pip-installed but forgot ``restartPython()``" case, where the *loaded* module
version differs from the on-disk requirement.
"""
import duckdb
import deltalake
import pytest

from duckrun import _runtime


def test_floor_deltalake_accepted(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.6.4")
    monkeypatch.setattr(duckdb, "__version__", "2.0.1")
    _runtime.check_runtime_versions()  # no raise


@pytest.mark.parametrize("newer", ["1.6.6", "1.7.0", "1.8.0"])
def test_newer_deltalake_accepted(monkeypatch, newer):
    monkeypatch.setattr(deltalake, "__version__", newer)
    monkeypatch.setattr(duckdb, "__version__", "2.0.1")
    _runtime.check_runtime_versions()  # no raise


@pytest.mark.parametrize("old", ["1.6.3", "1.5.0"])
def test_too_old_deltalake_rejected(monkeypatch, old):
    monkeypatch.setattr(deltalake, "__version__", old)
    monkeypatch.setattr(duckdb, "__version__", "2.0.1")
    with pytest.raises(RuntimeError, match="deltalake"):
        _runtime.check_runtime_versions()


def test_too_old_duckdb_rejected(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.6.4")
    monkeypatch.setattr(duckdb, "__version__", "2.0.0")
    with pytest.raises(RuntimeError, match="duckdb"):
        _runtime.check_runtime_versions()
