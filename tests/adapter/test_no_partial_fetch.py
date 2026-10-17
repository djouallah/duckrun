"""No duckrun code — package, tests or scripts — reads a DuckDB result with fetchone/fetchmany.

DuckDB 2.0 (duckdb#26747) keeps an ``execute()`` result open until a fetch returns no rows, and
while it is open every relation / Arrow-stream / ``register`` call on that connection raises
ResourceInUseException. ``fetchone()`` / ``fetchmany()`` stop partway, so the next write died —
``pip install --pre duckrun`` (which resolves duckdb 2.0 dev builds) broke on the first
``CREATE TABLE``. And 2.0 relations (what ``conn.sql()`` returns) have no ``fetchone()`` at all.
Read results with ``fetchall()`` — it works on 1.x and 2.0 alike — in the package AND the tests /
scripts, so the suites stay green when 2.0 goes stable. The ``pre-install`` cores job runs 2.0 for real.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [ROOT / "dbt" / "adapters" / "duckrun", ROOT / "duckrun", ROOT / "tests", ROOT / "plugins"]
PARTIAL_FETCH = re.compile(r"\.fetch(one|many)\(")


def test_no_partial_fetch_anywhere():
    hits = [f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
            for src in SOURCES for path in sorted(src.rglob("*.py"))
            for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
            if PARTIAL_FETCH.search(line)]
    assert not hits, "use fetchall() — a partial fetch blocks the connection on duckdb 2.0:\n" + "\n".join(hits)
