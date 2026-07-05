"""
Render the duckrun.connect() **supported-method contract** as a Markdown card for the GitHub job
summary and the docs (docs/api-reference.md).

    python tests/tools/connection_summary.py connection.xml            # print the card
    python tests/tools/connection_summary.py connection.xml --check    # exit 1 if any test failed

The method list is **introspected from the shipped classes** (DuckSession, Catalog, DataFrame,
DataFrameReader, DataFrameWriter, DeltaTable, DeltaMergeBuilder) — NOT from pytest test names — so
it lists the exact public API and can't drift or invent a "method" out of a test-case name. The
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

# The API surface (which classes, which excluded members) is defined once in public_api.py — the same
# definition the removal gate checks — so the card and the gate can never disagree about what's public.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from public_api import SURFACES, EXCLUDE   # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

MODULE_FUNCS = [("connect", "DuckSession")]   # duckrun.connect(...) -> a DuckSession

# Which established API each method mirrors, so a reader can tell a real DataFrame/Delta method from a
# duckrun-specific convenience. Default per surface; per-method overrides for the handful of exceptions.
_SURFACE_API = {
    "DuckSession": "duckrun",       # the session is duckrun's; a few entry points mirror the DataFrame API
    "Catalog": "spark",
    "DataFrame": "spark",
    "DataFrameReader": "spark",
    "DataFrameWriter": "spark",
    "DeltaTable": "spark",          # delta.tables.DeltaTable
    "DeltaMergeBuilder": "spark",   # delta.tables.DeltaMergeBuilder
}
_METHOD_API = {
    ("DuckSession", "sql"): "spark", ("DuckSession", "table"): "spark",
    ("DuckSession", "read"): "spark", ("DuckSession", "catalog"): "spark",
    ("DataFrame", "optimize"): "duckrun",   # duckrun's profiling maintenance ladder, not a Spark method
}


def _api(surface: str, method: str) -> str:
    return _METHOD_API.get((surface, method), _SURFACE_API.get(surface, "duckrun"))


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


def _contract():
    """The code-derived method contract: two lists of (surface, [methods]) — Spark/Delta-mirroring
    vs duckrun-specific — plus the total method count."""
    spark, duck, n = [], [], 0
    # duckrun.connect() is a module function; show it as a DuckSession entry point.
    duck_extra = {"DuckSession": [m for m, _ in MODULE_FUNCS]}
    for surface, cls, extra in SURFACES:
        sp, dk = [], list(duck_extra.get(surface, []))
        for m in _members(cls, extra):
            if (surface, m) in EXCLUDE:
                continue
            (sp if _api(surface, m) == "spark" else dk).append(m)
        n += len(sp) + len(dk)
        if sp:
            spark.append((surface, sp))
        if dk:
            duck.append((surface, sorted(dk)))
    return spark, duck, n


def _render(path):
    total, fails = _test_totals(path)
    passed, failed, error, skipped = (total["passed"], total["failed"], total["error"], total["skipped"])
    ntests = sum(total.values())
    green = not (failed or error)
    spark, duck, nmethods = _contract()

    out = ["## duckrun connection API — supported methods", ""]
    badge = f"✅ {nmethods} public methods" if green else f"⚠️  {nmethods} methods · {failed + error} TESTS FAILING"
    sub = f"suite: {passed}/{ntests} tests passing" + (f" · {skipped} skipped" if skipped else "")
    width = max(len(badge), len(sub)) + 2
    out += ["```", "┌" + "─" * width + "┐", "│ " + badge.ljust(width - 1) + "│",
            "│ " + sub.ljust(width - 1) + "│", "└" + "─" * width + "┘", "```", ""]
    out += ["> Introspected from the shipped classes — this is the exact public surface of "
            "`duckrun.connect()`, not a hand-maintained list. The green suite "
            f"([`test_connection_api.py`](../tests/connection_api/test_connection_api.py)) vouches it works.", ""]

    if not green:
        out += ["", f"**{failed + error} failing:** " + ", ".join(f"`{f}`" for f in fails), ""]

    def _table(title, note, groups):
        rows = ["### " + title, "", "> " + note, "", "| Surface | Methods | # |", "| --- | --- | :-: |"]
        for surface, methods in groups:
            names = ", ".join(f"`{m}`" for m in methods)
            rows.append(f"| `{surface}` | {names} | {len(methods)} |")
        rows.append("")
        return rows

    sp_n = sum(len(m) for _, m in spark)
    dk_n = sum(len(m) for _, m in duck)
    out += _table(f"DataFrame / Delta API — {sp_n} methods",
                  "Methods that mirror the established DataFrame / Delta `DeltaTable` API 1:1.", spark)
    out += _table(f"duckrun-specific helpers — {dk_n} methods",
                  "Conveniences with no DataFrame-API equivalent (session plumbing + shortcuts). "
                  "`conn.sql()` also routes raw Delta DML — see the DML matrix on the "
                  "[Connection API](connection-api.md) page.", duck)
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
