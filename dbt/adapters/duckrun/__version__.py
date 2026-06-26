"""Adapter version — single-sourced from the installed package metadata (pyproject.toml's
``version``) so it can never drift from a hand-edited string. dbt reads this for the
"Registered adapter: duckrun=<version>" line; bump only ``pyproject.toml``."""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    version = _pkg_version("duckrun")
except PackageNotFoundError:  # running from a source tree with no install — cosmetic only, never fatal
    version = "0.0.0+local"
