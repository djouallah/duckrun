"""``_parse_parquet_identifier`` — normalize a delta-spark ``parquet.`<path>`` identifier.

The DataFrame / DeltaTable veneer that used to live here was removed in the SQL-only refactor; the
connection API is ``conn.sql(...)`` (see :mod:`duckrun.session`). Only this small parser survives,
shared by ``conn.convert_to_delta``.
"""
import re

# delta-spark addresses the source parquet as ``"parquet.`<path>`"``; we accept that and a bare path.
_PARQUET_IDENT = re.compile(r"^\s*parquet\s*\.\s*`(?P<path>.+)`\s*$", re.IGNORECASE | re.DOTALL)


def _parse_parquet_identifier(identifier: str) -> str:
    """Pull the directory out of a delta-spark ``"parquet.`<path>`"`` identifier; a bare path
    (no ``parquet.`…``` wrapper) is returned as-is."""
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValueError("convert_to_delta identifier must be a non-empty path string.")
    m = _PARQUET_IDENT.match(identifier)
    return m.group("path").strip() if m else identifier.strip()
