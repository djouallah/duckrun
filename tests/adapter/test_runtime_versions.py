"""The runtime version guardrail (``duckrun._runtime.check_runtime_versions``).

duckrun requires *exactly* ``deltalake==1.5.0`` — every newer release breaks MERGE at scale and
batch DELETE — and ``duckdb>=1.5.4``. The guard exists for the Fabric "pip-installed but forgot
``restartPython()``" case, where the *loaded* module version differs from the on-disk pin. It must
fire on a too-new deltalake, not just a too-old one (the original bug: a floor-only check let a
loaded 1.6.x sail through and silently run broken merges).
"""
import duckdb
import deltalake
import pytest

from duckrun import _runtime


def test_exact_deltalake_accepted(monkeypatch):
    monkeypatch.setattr(deltalake, "__version__", "1.5.0")
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    _runtime.check_runtime_versions()  # no raise


@pytest.mark.parametrize("bad", ["1.6.0", "1.5.1", "2.0.0"])
def test_too_new_deltalake_rejected(monkeypatch, bad):
    monkeypatch.setattr(deltalake, "__version__", bad)
    monkeypatch.setattr(duckdb, "__version__", "1.5.4")
    with pytest.raises(RuntimeError, match="deltalake"):
        _runtime.check_runtime_versions()


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
