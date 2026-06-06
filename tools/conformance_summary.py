"""
Render a JUnit XML report (from `pytest --junitxml`) as a Markdown pass/fail summary.

Used by the conformance workflow to write the result to the GitHub step summary, and handy
locally:  python tools/conformance_summary.py tests/conformance/_report.xml
"""
import sys
import xml.etree.ElementTree as ET
from collections import Counter


def main(path: str) -> int:
    tree = ET.parse(path)
    root = tree.getroot()
    # --junitxml emits either a <testsuites> wrapper or a single <testsuite>.
    suites = root.findall("testsuite") if root.tag == "testsuites" else [root]

    counts = Counter()
    failures = []  # (outcome, classname, name, message)
    for suite in suites:
        for case in suite.findall("testcase"):
            name = case.get("name", "")
            classname = case.get("classname", "")
            failure = case.find("failure")
            error = case.find("error")
            skipped = case.find("skipped")
            if error is not None:
                counts["error"] += 1
                failures.append(("error", classname, name, (error.get("message") or "").strip()))
            elif failure is not None:
                counts["failed"] += 1
                failures.append(("failed", classname, name, (failure.get("message") or "").strip()))
            elif skipped is not None:
                counts["skipped"] += 1
            else:
                counts["passed"] += 1

    total = sum(counts.values())
    lines = []
    lines.append("## dbt adapter conformance — duckrun")
    lines.append("")
    lines.append("| Result | Count |")
    lines.append("| --- | ---: |")
    lines.append(f"| ✅ passed | {counts['passed']} |")
    lines.append(f"| ❌ failed | {counts['failed']} |")
    lines.append(f"| 💥 error | {counts['error']} |")
    lines.append(f"| ⏭️ skipped | {counts['skipped']} |")
    lines.append(f"| **total** | **{total}** |")
    lines.append("")

    if failures:
        lines.append(f"<details><summary>{len(failures)} not passing</summary>")
        lines.append("")
        lines.append("| Outcome | Test | Message |")
        lines.append("| --- | --- | --- |")
        for outcome, classname, name, message in failures:
            short = " ".join(message.split())[:140].replace("|", "\\|")
            lines.append(f"| {outcome} | `{classname}::{name}` | {short} |")
        lines.append("")
        lines.append("</details>")

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "tests/conformance/_report.xml"))
