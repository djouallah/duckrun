"""The runtime version guardrail (``duckrun._runtime.check_runtime_versions``).

duckrun requires ``deltalake>=1.6.2,<1.6.3`` (#84: 1.6.3+ writes the dictionary page last in
``encoding_stats``) and ``duckdb>=1.5.4``. The guard exists for the Fabric "pip-installed but forgot
``restartPython()``" case, where the *loaded* module version is not the installed one.
"""
import duckdb
import deltalake
import pytest

from duckrun import _runtime


@pytest.mark.parametrize("ok", ["1.6.2", "1.6.2.post1"])
def test_pinned_deltalake_accepted(monkeypatch, ok):
    monkeypatch.setattr(deltalake, "__version__", ok)
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    _runtime.check_runtime_versions()  # no raise


def test_too_old_deltalake_rejected(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.6.1")
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    with pytest.raises(RuntimeError, match="deltalake"):
        _runtime.check_runtime_versions()


@pytest.mark.parametrize("bad", ["1.6.3", "1.6.6", "1.7.0"])
def test_deltalake_with_dictionary_last_footer_rejected(monkeypatch, bad):
    monkeypatch.setattr(deltalake, "__version__", bad)
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    with pytest.raises(RuntimeError, match="deltalake"):
        _runtime.check_runtime_versions()


def test_too_old_duckdb_rejected(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.6.2")
    monkeypatch.setattr(duckdb, "__version__", "1.5.3")
    with pytest.raises(RuntimeError, match="duckdb"):
        _runtime.check_runtime_versions()
