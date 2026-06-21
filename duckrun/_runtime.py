"""Runtime version guardrail.

duckrun needs a recent ``duckdb`` (>= 1.5.4, where ``delta_scan`` gained the ``version => N``
parameter used for snapshot-pinned reads) and ``deltalake`` (>= 1.5.0, for the merge ``max_spill_size``
cap). In a notebook these can already be imported at an earlier version when ``duckrun`` is first
used: ``pip install duckrun --upgrade`` writes the new wheels to disk, but the already-loaded modules
stay bound until the kernel restarts. A user who skips the restart would keep running on the older
modules, quietly losing snapshot-pinned reads and the spill cap.

This check turns that into a loud, actionable error. It inspects the *loaded* versions (not the
pin), so it fires exactly on the forgot-to-restart case.
"""
from packaging.version import Version

# Floors duckrun needs at *runtime* — keep in sync with the pins in pyproject.toml:
#   duckdb 1.5.4    -> delta_scan('...', version => N) for snapshot-pinned incremental reads
#   deltalake 1.5.0 -> max_spill_size on MERGE to cap merge RAM and avoid OOM on large upserts
_MIN_DUCKDB = "1.5.4"
_MIN_DELTALAKE = "1.5.0"

_REMEDY = (
    "In a Fabric Python notebook, upgrade then restart the kernel so the new versions load:\n"
    "    !pip install duckrun --upgrade\n"
    "    notebookutils.session.restartPython()\n"
    "then re-run. (Elsewhere: pip install -U 'duckdb>={duckdb}' 'deltalake>={deltalake}' and "
    "restart the interpreter.)"
).format(duckdb=_MIN_DUCKDB, deltalake=_MIN_DELTALAKE)


def check_runtime_versions():
    """Raise ``RuntimeError`` if the *loaded* duckdb/deltalake are older than duckrun requires.

    Catches the Fabric "installed but forgot ``restartPython()``" footgun: the kernel keeps the
    stale preinstalled versions bound until restart. Idempotent and cheap; called at each entry
    point (``duckrun.connect()`` and the dbt connection open).
    """
    import duckdb
    import deltalake

    too_old = []
    if Version(duckdb.__version__) < Version(_MIN_DUCKDB):
        too_old.append(f"duckdb {duckdb.__version__} (need >= {_MIN_DUCKDB})")
    if Version(deltalake.__version__) < Version(_MIN_DELTALAKE):
        too_old.append(f"deltalake {deltalake.__version__} (need >= {_MIN_DELTALAKE})")

    if too_old:
        raise RuntimeError(
            "duckrun needs a newer " + " and ".join(too_old) + " than the kernel has loaded.\n"
            + _REMEDY
        )
