"""
Render the duckrun.connect() method matrix (pytest --junitxml from the Test* classes of
tests/connection_api/test_connection_api.py) as a Markdown scorecard for the GitHub job summary
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
    ("TestCatalog", "Catalog"),
    ("TestDataFrame", "DataFrame"),
    ("TestDataFrameReader", "DataFrameReader (read)"),
    ("TestDataFrameWriter", "DataFrameWriter (write)"),
    ("TestDeltaTable", "DeltaTable (merge / delete / update / version)"),
    ("TestSqlDml", "sql() — reads + Delta DML"),
]
_EMOJI = {"passed": "✅", "failed": "❌", "error": "💥", "skipped": "⏭️"}

# Short surface names for the cards.
SHORT = {
    "TestSession": "DuckSession", "TestCatalog": "Catalog", "TestDataFrame": "DataFrame",
    "TestDataFrameReader": "DataFrameReader", "TestDataFrameWriter": "DataFrameWriter",
    "TestDeltaTable": "DeltaTable", "TestSqlDml": "sql()",
}

# Which established API each method mirrors, so a reader can tell what's a real DataFrame/Delta method
# vs a duckrun-specific helper. Default per group; per-method overrides for the exceptions.
_GROUP_API = {
    "TestSession": "duckrun",            # the session object is duckrun's, but several entry points mirror the DataFrame API
    "TestCatalog": "DataFrame",          # Catalog
    "TestDataFrame": "DataFrame",        # DataFrame
    "TestDataFrameReader": "DataFrame",  # DataFrameReader
    "TestDataFrameWriter": "DataFrame",  # DataFrameWriter
    "TestDeltaTable": "DataFrame",       # delta.tables.DeltaTable — the Delta DataFrame API
    "TestSqlDml": "duckrun",             # conn.sql reads + delta_rs DML routing are duckrun behaviors
}
_METHOD_API = {
    ("TestSession", "sql"): "DataFrame", ("TestSession", "table"): "DataFrame",
    ("TestSession", "read_property"): "DataFrame", ("TestSession", "catalog_property"): "DataFrame",
    ("TestSession", "show_tables"): "DataFrame",
    ("TestDataFrame", "relation_passthrough"): "duckrun",   # DuckDB relation escape hatch
    ("TestDataFrameReader", "delta"): "duckrun",            # convenience shortcut; the DataFrame API uses format().load()
}


def _api(group: str, method: str) -> str:
    return _METHOD_API.get((group, method), _GROUP_API.get(group, "duckrun"))


def _label(test: str) -> str:
    """Map a test name to the actual API method it exercises, so the card lists *methods*, not
    test cases. e.g. test_mode_overwrite/append/ignore/error all exercise the one `mode()` method."""
    test = test.split("[", 1)[0]  # drop a pytest parametrize suffix, e.g. foo[CASE] -> foo
    if test.startswith("mode_"):
        return "mode"
    if test.startswith("option_"):
        return "option"
    if test.startswith("merge"):  # merge_upsert / _update_columns / _insert_only / _by_source_delete / _read_version…
        return "merge"
    if test in ("delete", "update"):
        return test
    if test == "select_passthrough":
        return "SELECT (passthrough)"
    if test == "version_pinned_read":
        return "version-pinned read"
    if test.startswith("sql_insert_values"):  # _insert_values / _insert_values_named_subset → one method
        return "insert…values"
    if test.startswith("sql_"):
        return {
            "sql_create_table_as": "create table as",
            "sql_insert_select": "insert…select",
            "sql_update": "update",
            "sql_delete": "delete",
            "sql_alter_add_column": "alter add column",
            "sql_drop_tombstone": "drop (tombstone)",
            "sql_merge_rejected": "merge guard (→ builder)",
        }.get(test, test)
    if test == "update_only_rejected":
        return "merge"
    return {
        "format_load_delta": "format/load",
        "read_property": "read",
        "catalog_property": "catalog",
        "show_tables": "sql",
        "relation_passthrough": "__getattr__",
    }.get(test, test)


def _outcome(case):
    if case.find("error") is not None:
        return "error"
    if case.find("failure") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def _group(classname: str) -> str:
    return classname.split(".")[-1]  # "...test_connection_api.TestSession" -> "TestSession"


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

    # Roll tests up into the actual API methods (the four mode_* tests are ONE `mode` method, etc.),
    # preserving order within each surface; a method passes iff all its tests passed. Then split by
    # API tag into two cards: DataFrame API vs duckrun-specific.
    ordered = [g for g, _ in GROUPS if g in by_group] + [g for g in by_group if g not in dict(GROUPS)]
    dataframe = []   # (surface, [(method, ok)])
    duck = []    # (method, surface, ok)
    m_pass = m_tot = 0
    for g in ordered:
        surface = SHORT.get(g, g)
        order, agg = [], {}   # label -> {"outs": [...], "api": str}
        for test, outcome in by_group[g]:
            lab = _label(test)
            if lab not in agg:
                order.append(lab)
                agg[lab] = {"outs": [], "api": _api(g, test)}
            agg[lab]["outs"].append(outcome)
        sp = []
        for lab in order:
            ok = all(o == "passed" for o in agg[lab]["outs"])
            m_tot += 1
            m_pass += 1 if ok else 0
            (duck.append((lab, surface, ok)) if agg[lab]["api"] == "duckrun" else sp.append((lab, ok)))
        if sp:
            dataframe.append((surface, sp))

    m_fail = m_tot - m_pass
    pct = round(100 * m_pass / m_tot) if m_tot else 0
    out = ["## duckrun connection API — method scorecard", ""]
    headline = f"✅ {m_pass} passed   ❌ {m_fail} failed"
    sub = f"{m_tot} methods · {pct}% passing"
    width = max(len(headline), len(sub)) + 2
    out += ["```", "┌" + "─" * width + "┐", "│ " + headline.ljust(width - 1) + "│",
            "│ " + sub.ljust(width - 1) + "│", "└" + "─" * width + "┘", "```", ""]

    def _cell(rows):  # rows: [(method, ok)]
        p = sum(1 for _, ok in rows if ok)
        fails = [m for m, ok in rows if not ok]
        return f"{p}/{len(rows)} ✅" if not fails else f"{p}/{len(rows)} ❌ ({', '.join(fails)})"

    sp_pass = sum(1 for _, rows in dataframe for _, ok in rows if ok)
    sp_tot = sum(len(rows) for _, rows in dataframe)
    out.append(f"### DataFrame API — {sp_pass}/{sp_tot} ✅")
    out.append("")
    out.append("> Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.")
    out.append("")
    out.append("| Surface | Methods | Pass |")
    out.append("| --- | --- | :-: |")
    for surface, rows in dataframe:
        names = ", ".join(f"`{m}`" for m, _ in rows)
        out.append(f"| `{surface}` | {names} | {_cell(rows)} |")
    out.append("")

    d_pass = sum(1 for _, _, ok in duck if ok)
    out.append(f"### duckrun-specific helpers — {d_pass}/{len(duck)} ✅")
    out.append("")
    out.append("> Conveniences with no DataFrame-API equivalent (session plumbing + two shortcuts).")
    out.append("")
    out.append("| Method | Surface | Pass |")
    out.append("| --- | --- | :-: |")
    for method, surface, ok in duck:
        out.append(f"| `{method}` | `{surface}` | {'✅' if ok else '❌'} |")
    out.append("")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    path = args[0] if args else "connection.xml"
    sys.exit(main(path, check="--check" in sys.argv))
