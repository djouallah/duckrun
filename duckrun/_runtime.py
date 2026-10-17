"""Runtime version guardrail.

duckrun needs ``duckdb`` >= 1.5.4 — the release where ``delta_scan`` gained its ``version => N``
parameter (used for snapshot-pinned reads) — and ``deltalake`` >= 1.6.4, the first release that
can roll files on row-group boundaries (no truncated tail row groups). A Microsoft Fabric Python notebook ships a *stable* set of these
packages, which may trail the versions duckrun needs. ``pip install duckrun --upgrade`` writes the
new wheels to disk, but the already-loaded modules stay bound until the kernel restarts — so a user
who skips the restart keeps running on the old modules, quietly losing snapshot-pinned reads or the
spill cap.

This check turns that into a loud, actionable error. It inspects the *loaded* versions (not the
requirement), so it fires exactly on the forgot-to-restart case.
"""
from packaging.version import Version

# Versions duckrun needs at *runtime* — keep in sync with the floors in pyproject.toml:
#   duckdb >= 1.5.4     -> delta_scan('...', version => N) for snapshot-pinned incremental reads
#   deltalake >= 1.6.4  -> files roll on row-group boundaries (delta-rs#4677)
_MIN_DUCKDB = "1.5.4"
_MIN_DELTALAKE = "1.6.4"

_REMEDY = (
    "In a Fabric Python notebook, upgrade then restart the kernel so the right versions load:\n"
    "    !pip install duckrun --upgrade\n"
    "    notebookutils.session.restartPython()\n"
    "then re-run. (Elsewhere: pip install -U 'duckdb>={duckdb}' 'deltalake>={deltalake}' and "
    "restart the interpreter.)"
).format(duckdb=_MIN_DUCKDB, deltalake=_MIN_DELTALAKE)


def check_runtime_versions():
    """Raise ``RuntimeError`` if the *loaded* duckdb/deltalake are older than duckrun requires.

    duckdb must be >= 1.5.4 and deltalake >= 1.6.4. Catches the notebook "installed but forgot
    ``restartPython()``" case: the kernel keeps the previously-loaded duckdb/deltalake bound until
    restart. Idempotent and cheap; called at each entry point (``duckrun.connect()`` and the dbt
    connection open).
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
