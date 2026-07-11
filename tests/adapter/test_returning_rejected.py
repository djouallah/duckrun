"""RETURNING is rejected up front on every DML form — the four-variant probe.

A delta_rs write commits through the transaction log and does NOT hand back the affected rows, so
``conn.sql()`` can't honor a RETURNING clause. This is caught in ``session._unsupported_dml`` BEFORE
routing, next to the existing ``UPDATE … FROM`` / ``DELETE … USING`` rejects.

The bug this pins: before the guard, an ``UPDATE … RETURNING`` fell through to the SET-clause parser,
which comma-split the RETURNING projection into bogus assignments and raised a misleading
``sets unknown column(s) ['customers.cid as c1']`` — an error about the wrong thing entirely. The
guard turns that into an honest, form-specific rejection.

Surfaced by the sqlsmith differential fuzzer (tests/fuzz/fuzz_replay.py); minimized to a permanent
unit test here so the knowledge survives a corpus regeneration.
"""
import tempfile

import pytest

import duckrun


@pytest.fixture
def w(tmp_path):
    """A writable local-fs session (schema ``dbo``) seeded with one two-row table."""
    conn = duckrun.connect(str(tmp_path / "wh"), schema="dbo", read_only=False)
    conn.sql("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(k, val)")
    return conn


# The four DML forms that accept a RETURNING clause in DuckDB — each must be REJECTED by duckrun.
_RETURNING = {
    "insert": "INSERT INTO t VALUES (3, 'c') RETURNING k",
    "update": "UPDATE t SET val = val RETURNING k, val",
    "delete": "DELETE FROM t WHERE k = 1 RETURNING k",
    "merge": ("MERGE INTO t USING (SELECT 1 AS k, 'z' AS val) s ON t.k = s.k "
              "WHEN MATCHED THEN UPDATE SET val = s.val RETURNING k"),
}
# The same four without RETURNING — these must still route and succeed (the guard is about the
# clause, not the verb).
_PLAIN = {
    "insert": "INSERT INTO t VALUES (3, 'c')",
    "update": "UPDATE t SET val = val",
    "delete": "DELETE FROM t WHERE k = 1",
    "merge": ("MERGE INTO t USING (SELECT 1 AS k, 'z' AS val) s ON t.k = s.k "
              "WHEN MATCHED THEN UPDATE SET val = s.val"),
}


@pytest.mark.parametrize("form", list(_RETURNING))
def test_returning_rejected_with_clear_message(w, form):
    """Every RETURNING DML form is rejected up front with the RETURNING-specific message — never the
    old misleading 'sets unknown column(s)' from the SET-clause parser swallowing the projection."""
    with pytest.raises(ValueError, match="RETURNING") as exc:
        w.sql(_RETURNING[form])
    assert "unknown column" not in str(exc.value)


@pytest.mark.parametrize("form", list(_PLAIN))
def test_same_form_without_returning_still_works(w, form):
    """Dropping RETURNING makes the identical statement route to delta_rs and commit — proof the
    guard keys on the clause, not on the verb."""
    w.sql(_PLAIN[form])  # no exception


def test_returning_inside_a_string_literal_is_not_tripped(w):
    """The guard scans only the top level (string literals blanked), so 'returning' as DATA must NOT
    be rejected — a false positive here would break ordinary inserts."""
    w.sql("INSERT INTO t VALUES (9, 'returning home')")
    assert w.sql("SELECT count(*) FROM t WHERE val = 'returning home'").fetchall() == [(1,)]
