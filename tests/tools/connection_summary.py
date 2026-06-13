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

    # Known groups first (in declared order), then any stragglers.
    labels = dict(GROUPS)
    ordered = [g for g, _ in GROUPS if g in by_group] + [g for g in by_group if g not in labels]
    for g in ordered:
        rows = by_group[g]
        ok = sum(1 for _, o in rows if o == "passed")
        out.append(f"### {labels.get(g, g)} — {ok}/{len(rows)}")
        out.append("")
        out.append("| Method | Result |")
        out.append("| --- | :-: |")
        for method, outcome in rows:
            out.append(f"| `{method}` | {_EMOJI.get(outcome, outcome)} |")
        out.append("")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    path = args[0] if args else "connection.xml"
    sys.exit(main(path, check="--check" in sys.argv))
