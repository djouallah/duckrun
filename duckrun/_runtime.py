"""Runtime version guardrail.

duckrun pins exact ``duckdb`` / ``deltalake`` versions (see ``pyproject.toml``), but in a
Microsoft Fabric Python notebook those packages come **preinstalled** at older versions and are
already imported into the kernel. ``pip install duckrun --upgrade`` writes the new wheels to disk,
but the already-loaded modules stay bound until the kernel restarts — so a user who skips the
restart silently keeps running on stale ``duckdb`` / ``deltalake``, losing snapshot-pinned reads
(``delta_scan(..., version => N)``) and the merge spill cap (``max_spill_size``).

This check turns that silent degradation into a loud, actionable error. It inspects the *loaded*
versions (not the pin), so it fires exactly on the forgot-to-restart case.
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

    stale = []
    if Version(duckdb.__version__) < Version(_MIN_DUCKDB):
        stale.append(f"duckdb {duckdb.__version__} (need >= {_MIN_DUCKDB})")
    if Version(deltalake.__version__) < Version(_MIN_DELTALAKE):
        stale.append(f"deltalake {deltalake.__version__} (need >= {_MIN_DELTALAKE})")

    if stale:
        raise RuntimeError(
            "duckrun: this kernel has stale " + " and ".join(stale) + " loaded.\n" + _REMEDY
        )
