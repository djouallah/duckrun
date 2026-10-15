"""The runtime version guardrail (``duckrun._runtime.check_runtime_versions``).

duckrun requires ``deltalake>=1.5.0`` and ``duckdb>=1.5.4`` — floors, not pins (0.5.1 lifted the
exact ``deltalake==1.5.0`` pin). The guard exists for the Fabric "pip-installed but forgot
``restartPython()``" case, where the *loaded* module version trails the installed one.
"""
import duckdb
import deltalake
import pytest

from duckrun import _runtime


@pytest.mark.parametrize("ok", ["1.5.0", "1.5.1", "1.6.0", "2.0.0"])
def test_deltalake_at_or_above_the_floor_accepted(monkeypatch, ok):
    monkeypatch.setattr(deltalake, "__version__", ok)
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    _runtime.check_runtime_versions()  # no raise


def test_too_old_deltalake_rejected(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.4.0")
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    with pytest.raises(RuntimeError, match="deltalake"):
        _runtime.check_runtime_versions()


def test_too_old_duckdb_rejected(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.5.0")
    monkeypatch.setattr(duckdb, "__version__", "1.5.3")
    with pytest.raises(RuntimeError, match="duckdb"):
        _runtime.check_runtime_versions()
