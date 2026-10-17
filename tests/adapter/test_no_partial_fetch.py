"""duckrun's own code never stops partway through a DuckDB result.

DuckDB 2.0 (duckdb#26747) keeps an ``execute()`` result open until a fetch returns no rows, and
while it is open every relation / Arrow-stream / ``register`` call on that connection raises
ResourceInUseException. ``fetchone()`` / ``fetchmany()`` stop partway, so the next write died —
``pip install --pre duckrun`` (which resolves duckdb 2.0 dev builds) broke on the first
``CREATE TABLE``. Read results with ``fetchall()``; the ``pre-install`` cores job runs the real thing.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = [ROOT / "dbt" / "adapters" / "duckrun", ROOT / "duckrun"]
PARTIAL_FETCH = re.compile(r"\.fetch(one|many)\(")


def test_no_partial_fetch_in_the_package():
    hits = [f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
            for pkg in PACKAGES for path in sorted(pkg.rglob("*.py"))
            for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
            if PARTIAL_FETCH.search(line)]
    assert not hits, "use fetchall() — a partial fetch blocks the connection on duckdb 2.0:\n" + "\n".join(hits)
