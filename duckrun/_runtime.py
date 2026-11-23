"""Runtime version guardrail.

duckrun needs ``duckdb`` >= 2.0.1 — the first release with working nested schemas (workspace
catalogs), on top of 2.0's read-side upgrade and ``delta_scan``'s ``version => N`` parameter (used
for snapshot-pinned reads) — and ``deltalake`` >= 1.7.0: the first release past 1.5.0 whose parquet
writer lists the dictionary page first in ``encoding_stats`` (#84) and whose DELETE/UPDATE work for
duckrun. A Microsoft Fabric Python notebook ships a *stable* set of these packages, which may trail
the versions duckrun needs. ``pip install duckrun --upgrade`` writes the new wheels to disk, but the
already-loaded modules stay bound until the kernel restarts — so a user who skips the restart keeps
running on the old modules, quietly losing snapshot-pinned reads / nested schemas (too-old duckdb)
or writing files Direct Lake reads slowly (too-old deltalake).

This check turns that into a loud, actionable error. It inspects the *loaded* versions (not the
pin), so it fires exactly on the forgot-to-restart case.
"""
from packaging.version import Version

# Versions duckrun needs at *runtime* — keep in sync with the pins in pyproject.toml:
#   duckdb >= 2.0.1    -> nested schemas (workspace catalogs); delta_scan('...', version => N)
#   deltalake >= 1.7.0 -> dictionary page first in encoding_stats (#84); working DELETE/UPDATE
_MIN_DUCKDB = "2.0.1"
_MIN_DELTALAKE = "1.7.0"

_REMEDY = (
    "In a Fabric Python notebook, upgrade then restart the kernel so the right versions load:\n"
    "    !pip install duckrun --upgrade\n"
    "    notebookutils.session.restartPython()\n"
    "then re-run. (Elsewhere: pip install -U 'duckdb>={duckdb}' 'deltalake>={deltalake}' and "
    "restart the interpreter.)"
).format(duckdb=_MIN_DUCKDB, deltalake=_MIN_DELTALAKE)


def check_runtime_versions():
    """Raise ``RuntimeError`` if the *loaded* duckdb/deltalake are not what duckrun requires.

    duckdb must be >= 2.0.1; deltalake must be >= 1.7.0. Catches the notebook "installed but forgot ``restartPython()``" case: the kernel keeps
    the previously-loaded duckdb/deltalake bound until restart. Idempotent and cheap; called at each
    entry point (``duckrun.connect()`` and the dbt connection open).
    """
    import duckdb
    import deltalake

    wrong = []
    if Version(duckdb.__version__) < Version(_MIN_DUCKDB):
        wrong.append(f"duckdb {duckdb.__version__} (need >= {_MIN_DUCKDB})")
    if Version(deltalake.__version__) < Version(_MIN_DELTALAKE):
        wrong.append(f"deltalake {deltalake.__version__} (need >= {_MIN_DELTALAKE})")

    if wrong:
        raise RuntimeError(
            "duckrun has the wrong " + " and ".join(wrong) + " loaded in the kernel.\n"
            + _REMEDY
        )
