"""
Render the duckrun.connect() method matrix (pytest --junitxml from
tests/connection_api/test_method_matrix.py) as a Markdown scorecard for the GitHub job summary
and the README: a totals box plus, per API surface, every method with a ✅/❌/⏭️.

    python tests/tools/connection_summary.py connection.xml            # print the card
    python tests/tools/connection_summary.py connection.xml --check    # exit 1 if anything failed

The connection-API analogue of tests/tools/conformance_summary.py.
"""
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# API surface order + friendly labels, keyed by the matrix's test class names.
GROUPS = [
    ("TestSession", "DuckSession — connect & query"),
    ("TestCatalog", "Catalog (Spark catalog)"),
    ("TestDataFrame", "DataFrame"),
    ("TestDataFrameReader", "DataFrameReader (read)"),
    ("TestDataFrameWriter", "DataFrameWriter (write)"),
    ("TestDeltaTable", "DeltaTable (merge / upsert)"),
]
_EMOJI = {"passed": "✅", "failed": "❌", "error": "💥", "skipped": "⏭️"}

# Short surface names for the cards.
SHORT = {
    "TestSession": "DuckSession", "TestCatalog": "Catalog", "TestDataFrame": "DataFrame",
    "TestDataFrameReader": "DataFrameReader", "TestDataFrameWriter": "DataFrameWriter",
    "TestDeltaTable": "DeltaTable",
}

# Which established API each method mirrors, so a reader can tell what's a real Spark/Delta method
# vs a duckrun-specific helper. Default per group; per-method overrides for the exceptions.
_GROUP_API = {
    "TestSession": "duckrun",          # the session object is duckrun's, but several entry points are Spark
    "TestCatalog": "Spark",            # pyspark.sql.Catalog
    "TestDataFrame": "Spark",          # pyspark.sql.DataFrame
    "TestDataFrameReader": "Spark",    # pyspark.sql.DataFrameReader
    "TestDataFrameWriter": "Spark",    # pyspark.sql.DataFrameWriter
    "TestDeltaTable": "Spark",         # delta.tables.DeltaTable — the Delta-on-Spark API (≈ Spark)
}
_METHOD_API = {
    ("TestSession", "sql"): "Spark", ("TestSession", "table"): "Spark",
    ("TestSession", "read_property"): "Spark", ("TestSession", "catalog_property"): "Spark",
    ("TestSession", "show_tables"): "Spark",
    ("TestDataFrame", "relation_passthrough"): "duckrun",   # DuckDB relation escape hatch
    ("TestDataFrameReader", "delta"): "duckrun",            # convenience shortcut; Spark uses format().load()
}


def _api(group: str, method: str) -> str:
    return _METHOD_API.get((group, method), _GROUP_API.get(group, "duckrun"))


def _outcome(case):
    if case.find("error") is not None:
        return "error"
    if case.find("failure") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def _group(classname: str) -> str:
    return classname.split(".")[-1]  # "...test_method_matrix.TestSession" -> "TestSession"


def _method(name: str) -> str:
    return name[len("test_"):] if name.startswith("test_") else name


def main(path: str, check: bool = False) -> int:
    root = ET.parse(path).getroot()
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]

    total = Counter()
    by_group = defaultdict(list)  # group -> [(method, outcome)]
    for suite in suites:
        for case in suite.findall("testcase"):
            outcome = _outcome(case)
            total[outcome] += 1
            by_group[_group(case.get("classname", ""))].append((_method(case.get("name", "")), outcome))

    n = sum(total.values())
    passed, failed, error, skipped = total["passed"], total["failed"], total["error"], total["skipped"]

    if check:
        bad = failed + error
        print(f"connection API: {passed}/{n} methods passing, {bad} failing, {skipped} skipped")
        return 1 if bad else 0

    pct = round(100 * passed / n) if n else 0
    out = ["## duckrun connection API — method scorecard", ""]
    headline = f"✅ {passed} passed   ❌ {failed} failed   💥 {error} errors"
    if skipped:
        headline += f"   ⏭️ {skipped} skipped"
    sub = f"{n} methods · {pct}% passing"
    width = max(len(headline), len(sub)) + 2
    out += ["```", "┌" + "─" * width + "┐", "│ " + headline.ljust(width - 1) + "│",
            "│ " + sub.ljust(width - 1) + "│", "└" + "─" * width + "┘", "```", ""]
    # Split every method by its API tag into two cards: Spark/Delta-on-Spark vs duckrun-specific.
    labels = dict(GROUPS)
    ordered = [g for g, _ in GROUPS if g in by_group] + [g for g in by_group if g not in labels]

    spark = []   # (surface, [(method, outcome)])
    duck = []    # (method, surface, outcome)
    for g in ordered:
        surface = SHORT.get(g, g)
        sp = []
        for method, outcome in by_group[g]:
            if _api(g, method) == "duckrun":
                duck.append((method, surface, outcome))
            else:
                sp.append((method, outcome))
        if sp:
            spark.append((surface, sp))

    def _cell(rows):
        p = sum(1 for _, o in rows if o == "passed")
        t = len(rows)
        fails = [m for m, o in rows if o != "passed"]
        return f"{p}/{t} ✅" if not fails else f"{p}/{t} ❌ ({', '.join(fails)})"

    sp_pass = sum(1 for _, rows in spark for _, o in rows if o == "passed")
    sp_tot = sum(len(rows) for _, rows in spark)
    out.append(f"### Spark / Delta-on-Spark API — {sp_pass}/{sp_tot} ✅")
    out.append("")
    out.append("> Methods that mirror PySpark (and Delta Lake's `DeltaTable` on Spark) 1:1.")
    out.append("")
    out.append("| Surface | Methods | Pass |")
    out.append("| --- | --- | :-: |")
    for surface, rows in spark:
        names = ", ".join(f"`{m}`" for m, _ in rows)
        out.append(f"| `{surface}` | {names} | {_cell(rows)} |")
    out.append("")

    d_pass = sum(1 for _, _, o in duck if o == "passed")
    out.append(f"### duckrun-specific helpers — {d_pass}/{len(duck)} ✅")
    out.append("")
    out.append("> Conveniences with no Spark equivalent (session plumbing + two shortcuts).")
    out.append("")
    out.append("| Method | Surface | Pass |")
    out.append("| --- | --- | :-: |")
    for method, surface, outcome in duck:
        out.append(f"| `{method}` | `{surface}` | {_EMOJI.get(outcome, outcome)} |")
    out.append("")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    path = args[0] if args else "connection.xml"
    sys.exit(main(path, check="--check" in sys.argv))
