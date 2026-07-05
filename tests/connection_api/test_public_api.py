"""The public-API removal gate.

Asserts the live, introspected public surface of ``duckrun.connect()`` still matches the committed
baseline (``public_api_baseline.txt``). A method silently vanishing — deleted along with its test, so
nothing else fails — is caught HERE: the surface no longer matches the baseline. Removing (or adding)
a public API is therefore a deliberate act — you regenerate the baseline on purpose:

    python tests/tools/public_api.py --write

Runs in the `adapter` job of cores.yml, which gates the PyPI publish.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir, "tools"))
import public_api  # noqa: E402


_HINT = "\n\nIf intentional, regenerate the baseline:\n    python tests/tools/public_api.py --write"


def test_public_api_matches_baseline():
    removed, added, changed = public_api.diff()
    assert not removed, "BREAKING — public API removed since the baseline:\n  " + "\n  ".join(removed) + _HINT
    assert not changed, "BREAKING — parameter signature changed since the baseline:\n  " + "\n  ".join(changed) + _HINT
    assert not added, "public API added since the baseline:\n  " + "\n  ".join(added) + _HINT
