"""Gate the duckrun-slt conformance suite (a black-box, DuckDB-oracle-validated sqllogictest
suite vendored under this directory) WITHOUT editing the referee.

Two invariants:

* ``test_oracle_all_green`` — every expected result must still hold against plain DuckDB
  (the semantics oracle in ``duckrun_shim.py``). If this ever goes red the *expectations*
  drifted from DuckDB, not duckrun — a bug in the suite, and the whole gate is meaningless.

* ``test_no_new_failures`` — running the suite against real duckrun (writes enabled) may fail
  ONLY the records in ``EXPECTED_DEVIATIONS`` below. Those are duckrun's deliberate deviations
  from DuckDB semantics (a documented invariant) or upstream delta-rs engine limits — each stays
  red on purpose, exactly like the parked blind-append / case-collision conformance items. A
  failure NOT on the list is a real regression and fails the build. Fixing a deviation (so it
  drops off the list) never fails the build — it just means the allowlist can be trimmed.

The ``.slt`` files and ``runner.py`` are the referee and are NEVER edited to make duckrun pass —
a deviation is recorded HERE, outside the suite, so the black box stays honest.
"""
import os
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:                        # so runner.importlib can find duckrun_rw / duckrun_shim
    sys.path.insert(0, HERE)

from runner import run_file  # noqa: E402  (needs the sys.path insert above)

TESTS_DIR = os.path.join(HERE, "tests")
SLT_FILES = sorted(f for f in os.listdir(TESTS_DIR) if f.endswith(".slt"))

# Records that stay red on purpose, keyed by .slt filename → the runner's failure snippet (the
# frozen .slt text makes each snippet stable). Grouped by root cause. See tests/conformance_slt/
# README.md and the CHANGELOG for the rationale behind each group.
EXPECTED_DEVIATIONS = {
    "01_ddl.slt": {
        # #9 whitespace-in-identifier — a duckrun INVARIANT (delta_dml._create_target_error): a space
        # becomes %20 in the Delta path and trips abfss globbing, and matches no OneLake table name.
        'CREATE TABLE "Order Details" ("Unit Price" DECIMAL(10,2), "from" VARCHAR, "select" INTE...',
        'INSERT INTO "Order Details" VALUES (9.99, \'x\', 1), (0.01, \'y\', 2)',
        'SELECT "from", "select", "Unit Price"::VARCHAR FROM "Order Details" ORDER BY "select"',
        # #6 non-ASCII identifiers — the bare-identifier scanner is ASCII-only, and a quoted Unicode
        # name round-trips wrong on the write/scan path. Parked (naming model); emoji names are already
        # `statement maybe` in the suite.
        'SELECT id FROM "販売データ"',
        'CREATE TABLE métriques ("温度" DOUBLE)',
        "INSERT INTO métriques VALUES (36.6)",
        'SELECT "温度" FROM métriques',
    },
    "05_adversarial_parser.slt": {
        # #9 whitespace-in-identifier (as above) + its downstream reads.
        'CREATE TABLE "select from where" (v INTEGER)',
        'INSERT INTO "select from where" VALUES (1)',
        'INSERT INTO "select from where" SELECT v + 1 FROM "select from where"',
        'SELECT v FROM "select from where"',
        # #7 delta-rs engine limits: no RETURNING, no column DEFAULT — the write can't be expressed,
        # and the following count/read cascades from the refused write.
        "INSERT INTO colorder VALUES (200, 0, 0) RETURNING a",
        "SELECT count(*) FROM colorder",
        "INSERT INTO defaults_t VALUES (1, DEFAULT)",
        "SELECT created FROM defaults_t",
        # case-collision gap: duckrun maps a table name to a DIRECTORY, so a case-variant WRITE
        # (`INSERT INTO DEFAULTS_T` vs the `defaults_t` dir) resolves only on a case-INSENSITIVE
        # filesystem — it fails on Linux/OneLake (the CI oracle) though it "passes" on Windows. The
        # case-variant READ (`FROM Defaults_T`) resolves via DuckDB's case-insensitive view lookup but
        # then returns a mismatched count, so it's red on every platform. See case-collision-table-names-gap.
        "INSERT INTO DEFAULTS_T VALUES (4, 'upper ref')",
        "SELECT count(*) FROM Defaults_T",
        "FROM defaults_t SELECT count(*)",
        "FROM defaults_t",
        # (CTE-prefixed DELETE/UPDATE is now supported via the DuckDB overwrite fallback with the CTE
        # re-attached — no longer a deviation, dropped from this allowlist.)
    },
}


def _run(module):
    """Run every .slt against ``module``; return {filename: [failure snippets]}."""
    out = {}
    for name in SLT_FILES:
        stats = run_file(os.path.join(TESTS_DIR, name), tempfile.mkdtemp(prefix="slt_"),
                         module, verbose=False)
        out[name] = [desc for desc, _msg in stats.failures]
    return out


def test_oracle_all_green():
    """The expectations must hold against the DuckDB oracle — else the suite itself is wrong."""
    failures = _run("duckrun_shim")
    assert all(not v for v in failures.values()), \
        "oracle (plain DuckDB) is not all-green — a suite expectation drifted from DuckDB semantics: " \
        + repr({k: v for k, v in failures.items() if v})


def test_no_new_failures():
    """Against real duckrun, only the pinned deliberate-deviation records may fail."""
    results = _run("duckrun_rw")
    unexpected = {
        name: sorted(set(fails) - EXPECTED_DEVIATIONS.get(name, set()))
        for name, fails in results.items()
    }
    unexpected = {k: v for k, v in unexpected.items() if v}
    assert not unexpected, (
        "NEW conformance-SLT failures (a real regression — not on the deliberate-deviation "
        "allowlist in EXPECTED_DEVIATIONS): " + repr(unexpected)
    )
