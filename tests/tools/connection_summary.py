"""
Render the duckrun.connect() **supported-method contract** as a Markdown card for the GitHub job
summary and the docs (docs/api-reference.md).

    python tests/tools/connection_summary.py connection.xml            # print the card
    python tests/tools/connection_summary.py connection.xml --check    # exit 1 if any test failed

The method list is **introspected from the shipped classes** (the SQL-only surface — DuckSession) —
NOT from pytest test names — so it lists the exact public API and can't drift or invent a "method"
out of a test-case name. The
pytest JUnit XML from tests/connection_api/test_connection_api.py contributes only the pass/fail
badge (and the `--check` CI gate): the card is honest that it's a contract, and the green suite
vouches the contract works.

The connection-API analogue of tests/tools/conformance_summary.py.
"""
import inspect
import os
import sys
import xml.etree.ElementTree as ET
from collections import Counter

# The API surface (which classes, which excluded members) and the signature/entry formatting are
# defined once in public_api.py — the same definitions the removal gate checks — so the card and the
# gate can never disagree about what's public or what a method's parameters are.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckrun   # noqa: E402
from public_api import SURFACES, EXCLUDE, member_entry, signature   # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

def _members(cls, extra):
    """Public methods + properties of a class (names without a leading underscore), plus the named
    instance-attribute accessors introspection can't see. Deterministic order: sorted."""
    names = {n for n, _ in inspect.getmembers(cls) if not n.startswith("_")}
    names.update(extra)
    return sorted(names)


def _outcome(case):
    if case.find("error") is not None:
        return "error"
    if case.find("failure") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def _test_totals(path):
    root = ET.parse(path).getroot()
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]
    total = Counter()
    fails = []
    for suite in suites:
        for case in suite.findall("testcase"):
            o = _outcome(case)
            total[o] += 1
            if o in ("failed", "error"):
                fails.append(case.get("name", "?"))
    return total, fails


_MARKERS = {"(property)": "property", "(accessor)": "accessor", "(attr)": "attribute"}


def _rows():
    """The code-derived contract as flat (surface, method, params) rows — one per public member, with
    `params` taken from the real signature. `nmethods` is the row count."""
    rows = [("duckrun", "connect", signature(duckrun.connect)[1:-1])]   # module entry point
    for surface, cls, extra in SURFACES:
        for m in _members(cls, extra):
            if (surface, m) in EXCLUDE:
                continue
            entry = member_entry(cls, m)                      # "sql(query)" | "read (property)"
            params = entry[len(m) + 1:-1] if entry.startswith(m + "(") else entry[len(m):].strip()
            rows.append((surface, m, params))
    return rows


def _render(path):
    total, fails = _test_totals(path)
    passed, failed, error, skipped = (total["passed"], total["failed"], total["error"], total["skipped"])
    ntests = sum(total.values())
    green = not (failed or error)
    rows = _rows()

    out = ["## duckrun connection API — supported methods", ""]
    methods = f"✅ **{len(rows)} public methods**" if green else f"⚠️ **{len(rows)} methods · {failed + error} tests failing**"
    suite = f"{passed}/{ntests} tests passing" + (f" · {skipped} skipped" if skipped else "")
    out += [f"{methods} · {suite}", ""]
    out += ["> Introspected from the shipped classes — the exact public surface of `duckrun.connect()`, "
            "signatures and all, not a hand-maintained list. The green suite "
            f"([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works. "
            "`conn.sql()` also routes raw Delta DML — see the DML matrix on the "
            "[Connection API](connection-api.md) page.", ""]

    if not green:
        out += [f"**{failed + error} failing:** " + ", ".join(f"`{f}`" for f in fails), ""]

    out += ["| Surface | Method | Parameters |", "| --- | --- | --- |"]
    for surface, method, params in rows:
        if params in _MARKERS:
            cell = f"*{_MARKERS[params]}*"
        elif params == "":
            cell = "*(none)*"
        else:
            cell = f"`{params}`"
        out.append(f"| `{surface}` | `{method}` | {cell} |")
    out.append("")
    return "\n".join(out), (failed + error)


def main(path: str, check: bool = False) -> int:
    if check:
        total, fails = _test_totals(path)
        bad = total["failed"] + total["error"]
        print(f"connection API: {total['passed']}/{sum(total.values())} tests passing, "
              f"{bad} failing, {total['skipped']} skipped")
        return 1 if bad else 0
    card, _ = _render(path)
    print(card)
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    path = args[0] if args else "connection.xml"
    sys.exit(main(path, check="--check" in sys.argv))
