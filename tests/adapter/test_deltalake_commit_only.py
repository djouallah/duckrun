"""deltalake is a commit library here, never a writer.

DuckDB writes every parquet file duckrun produces (one COPY in the fixed read layout, stats from
RETURN_STATS) and delta-rs only commits the file actions. This scans the package source and fails on
any call to delta-rs's own data writers, so one cannot creep back in.

Allowed: opening a DeltaTable and reading its log, ``create_write_transaction``,
``create_table_with_add_actions``, ``DeltaTable.create``, ``alter.add_columns`` /
``set_table_properties``, ``restore``, ``vacuum``, ``cleanup_metadata``, ``convert_to_deltalake``."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = [ROOT / "dbt" / "adapters" / "duckrun", ROOT / "duckrun"]

# A delta-rs call that writes data files (or plans a DataFusion rewrite of them).
FORBIDDEN = {
    "write_deltalake": re.compile(r"\bwrite_deltalake\s*\("),
    "DeltaTable.merge": re.compile(r"\b(?:dt|table|target|_dt|existing_dt)\.merge\s*\("),
    "DeltaTable.update": re.compile(r"\b(?:dt|table|target|_dt|existing_dt)\.update\s*\("),
    "DeltaTable.delete": re.compile(r"\b(?:dt|table|target|_dt|existing_dt)\.delete\s*\("),
    "optimize.compact / z_order": re.compile(r"\.optimize\.(?:compact|z_order)\s*\("),
    "WriterProperties": re.compile(r"\bWriterProperties\b"),
}


def _code(text: str) -> str:
    """The source without comments and docstrings, so a mention in prose is not a call."""
    text = re.sub(r'"""[\s\S]*?"""', "", text)
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def test_no_delta_rs_data_writer_is_called():
    hits = []
    for pkg in PACKAGES:
        for f in sorted(pkg.rglob("*.py")):
            code = _code(f.read_text(encoding="utf-8"))
            for what, pattern in FORBIDDEN.items():
                for m in pattern.finditer(code):
                    line = code.count("\n", 0, m.start()) + 1
                    hits.append(f"{f.relative_to(ROOT)}:{line}: {what}")
    assert not hits, "delta-rs writes data here (DuckDB must, delta-rs only commits):\n" + "\n".join(hits)
