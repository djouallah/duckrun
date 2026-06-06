"""
Render a JUnit XML report (from `pytest --junitxml`) as a Markdown "card" for the GitHub job
summary: an at-a-glance totals box, a per-suite pass-rate table, and a collapsible list of
everything that did not pass.

Used by .github/workflows/conformance.yml, and handy locally:
    python tools/conformance_summary.py tests/conformance/_report.xml
"""
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

# The summary contains emoji / box-drawing; force UTF-8 so it prints on a Windows cp1252
# console too (GitHub runners are already UTF-8). Guarded for older Pythons.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _outcome(case):
    if case.find("error") is not None:
        return "error"
    if case.find("failure") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def _suite_name(classname: str) -> str:
    # "tests.conformance.test_basic.TestX" -> "basic"
    parts = classname.split(".")
    for p in parts:
        if p.startswith("test_"):
            return p[len("test_"):]
    return classname or "?"


def _bar(passed: int, total: int, width: int = 10) -> str:
    filled = round((passed / total) * width) if total else 0
    return "█" * filled + "░" * (width - filled)


def main(path: str) -> int:
    root = ET.parse(path).getroot()
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]

    total = Counter()
    by_suite = defaultdict(Counter)
    not_passing = []  # (outcome, classname, name, message)

    for suite in suites:
        for case in suite.findall("testcase"):
            outcome = _outcome(case)
            total[outcome] += 1
            by_suite[_suite_name(case.get("classname", ""))][outcome] += 1
            if outcome in ("failed", "error"):
                node = case.find("failure") if outcome == "failed" else case.find("error")
                msg = (node.get("message") or "").strip() if node is not None else ""
                not_passing.append((outcome, case.get("classname", ""), case.get("name", ""), msg))

    n = sum(total.values())
    passed, failed, error, skipped = (total["passed"], total["failed"], total["error"], total["skipped"])
    pct = round(100 * passed / n) if n else 0

    out = []
    out.append("## dbt adapter conformance — duckrun")
    out.append("")
    # Totals card (fenced so the box-drawing renders monospaced).
    headline = f"✅ {passed} passed   ❌ {failed} failed   💥 {error} errors"
    if skipped:
        headline += f"   ⏭️ {skipped} skipped"
    sub = f"{n} total · {pct}% passing"
    width = max(len(headline), len(sub)) + 2
    out.append("```")
    out.append("┌" + "─" * width + "┐")
    out.append("│ " + headline.ljust(width - 1) + "│")
    out.append("│ " + sub.ljust(width - 1) + "│")
    out.append("└" + "─" * width + "┘")
    out.append("```")
    out.append("")

    # Per-suite table, best pass-rate first.
    out.append("### By suite")
    out.append("")
    out.append("| Suite | Pass rate | ✅ | ❌ | 💥 | ⏭️ | Total |")
    out.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |")

    def rate(c):
        t = sum(c.values())
        return (c["passed"] / t) if t else 0

    for suite in sorted(by_suite, key=lambda s: (-rate(by_suite[s]), s)):
        c = by_suite[suite]
        t = sum(c.values())
        out.append(
            f"| `{suite}` | `{_bar(c['passed'], t)}` {round(100*rate(c))}% "
            f"| {c['passed']} | {c['failed']} | {c['error']} | {c['skipped']} | {t} |"
        )
    out.append(
        f"| **Total** | `{_bar(passed, n)}` **{pct}%** "
        f"| **{passed}** | **{failed}** | **{error}** | **{skipped}** | **{n}** |"
    )
    out.append("")

    # Per-suite breakdown of what didn't pass — one collapsible section per non-100% suite,
    # worst pass-rate first, so e.g. `incremental` lists exactly which tests fail and why.
    if not_passing:
        np_by_suite = defaultdict(list)
        for outcome, classname, name, message in not_passing:
            np_by_suite[_suite_name(classname)].append((outcome, classname, name, message))

        out.append("### Not passing — details by suite")
        out.append("")
        for suite in sorted(np_by_suite, key=lambda s: (rate(by_suite[s]), s)):
            rows = np_by_suite[suite]
            c = by_suite[suite]
            out.append(
                f"<details><summary><b>{suite}</b> — {len(rows)} not passing "
                f"({c['passed']}/{sum(c.values())} pass)</summary>"
            )
            out.append("")
            out.append("| Outcome | Test | Message |")
            out.append("| --- | --- | --- |")
            for outcome, classname, name, message in rows:
                emoji = "💥" if outcome == "error" else "❌"
                short = " ".join(message.split())[:160].replace("|", "\\|")
                cls = classname.split(".")[-1]
                out.append(f"| {emoji} | `{cls}::{name}` | {short} |")
            out.append("")
            out.append("</details>")
        out.append("")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "tests/conformance/_report.xml"))
