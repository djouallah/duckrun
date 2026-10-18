"""Runtime version guardrail.

duckrun needs ``duckdb`` >= 1.5.4 — the release where ``delta_scan`` gained its ``version => N``
parameter (used for snapshot-pinned reads) — and ``deltalake`` **exactly 1.5.0**: it is the only
release with the merge ``max_spill_size`` cap that *also* has a working MERGE-at-scale and batch
DELETE. Every deltalake above 1.5.0 is broken for duckrun (MERGE breaks at scale, batch DELETE is
unsupported), which is why pyproject pins it exactly. A Microsoft Fabric Python notebook ships a
*stable* set of these packages, which may trail (an older ``duckdb``) or lead (a newer
``deltalake``) the versions duckrun needs. ``pip install duckrun --upgrade`` writes the new wheels
to disk, but the already-loaded modules stay bound until the kernel restarts — so a user who skips
the restart keeps running on the wrong modules, quietly losing snapshot-pinned reads / the spill
cap (too-old duckdb) or hitting broken merges/deletes (too-new deltalake).

This check turns that into a loud, actionable error. It inspects the *loaded* versions (not the
pin), so it fires exactly on the forgot-to-restart case.
"""
from packaging.version import Version

# Versions duckrun needs at *runtime* — keep in sync with the pins in pyproject.toml:
#   duckdb >= 1.5.4  -> delta_scan('...', version => N) for snapshot-pinned incremental reads
#   deltalake == 1.5.0 -> max_spill_size on MERGE; the ONLY release where MERGE-at-scale and batch
#                         DELETE also work (every newer deltalake is broken for duckrun).
_MIN_DUCKDB = "1.5.4"
_REQUIRED_DELTALAKE = "1.5.0"

_REMEDY = (
    "In a Fabric Python notebook, upgrade then restart the kernel so the right versions load:\n"
    "    !pip install duckrun --upgrade\n"
    "    notebookutils.session.restartPython()\n"
    "then re-run. (Elsewhere: pip install -U 'duckdb>={duckdb}' 'deltalake=={deltalake}' and "
    "restart the interpreter.)"
).format(duckdb=_MIN_DUCKDB, deltalake=_REQUIRED_DELTALAKE)


def check_runtime_versions():
    """Raise ``RuntimeError`` if the *loaded* duckdb/deltalake are not what duckrun requires.

    duckdb must be >= 1.5.4; deltalake must be *exactly* 1.5.0 (newer releases break MERGE and batch
    DELETE). Catches the notebook "installed but forgot ``restartPython()``" case: the kernel keeps
    the previously-loaded duckdb/deltalake bound until restart. Idempotent and cheap; called at each
    entry point (``duckrun.connect()`` and the dbt connection open).
    """
    import duckdb
    import deltalake

    wrong = []
    if Version(duckdb.__version__) < Version(_MIN_DUCKDB):
        wrong.append(f"duckdb {duckdb.__version__} (need >= {_MIN_DUCKDB})")
    if Version(deltalake.__version__) != Version(_REQUIRED_DELTALAKE):
        wrong.append(
            f"deltalake {deltalake.__version__} (need exactly {_REQUIRED_DELTALAKE}; newer "
            "releases break MERGE and batch DELETE)"
        )

    if wrong:
        raise RuntimeError(
            "duckrun has the wrong " + " and ".join(wrong) + " loaded in the kernel.\n"
            + _REMEDY
        )
