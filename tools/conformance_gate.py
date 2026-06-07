#!/usr/bin/env python
"""Baseline regression gate for the dbt adapter conformance suite.

The conformance suite is intentionally not 100% green: duckrun is Delta-only, so a number of
standard dbt tests can't pass. We don't chase full green — we guard against *regression*. A
test that passes today must not start failing tomorrow.

This compares the set of tests that passed in a JUnit XML report against a committed baseline
(``tests/conformance/baseline_passing.txt``) and exits non-zero if any baseline test is no
longer passing (now failing, errored, skipped, or no longer collected). Failures in tests
that were never in the baseline don't gate; newly-passing tests are reported as candidates to
add to the baseline so coverage can ratchet up over time.

Usage:
  conformance_gate.py REPORT.xml BASELINE.txt          # check (exit 1 on regression)
  conformance_gate.py --write REPORT.xml BASELINE.txt  # (re)generate the baseline
"""
import sys
import xml.etree.ElementTree as ET


def scan(report_path):
    """Return (passed_keys, all_keys) from a JUnit XML report.

    A testcase 'passed' iff it has no failure/error/skipped child element. The key
    ``classname::name`` is stable across runs and unique per (parametrized) test.
    """
    tree = ET.parse(report_path)
    passed, all_keys = set(), set()
    for tc in tree.iter("testcase"):
        key = f"{tc.get('classname')}::{tc.get('name')}"
        all_keys.add(key)
        if not any(child.tag in ("failure", "error", "skipped") for child in tc):
            passed.add(key)
    return passed, all_keys


def load_baseline(path):
    try:
        with open(path, encoding="utf-8") as f:
            return {ln.strip() for ln in f if ln.strip() and not ln.startswith("#")}
    except FileNotFoundError:
        return set()


def write_baseline(path, passed):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("# Conformance baseline: the tests that pass today. CI fails if any of these\n")
        f.write("# regress (start failing / erroring / skipping / disappearing). This is a\n")
        f.write("# no-regression gate, not a 100%-green target — duckrun is Delta-only.\n")
        f.write("# Regenerate after an intentional coverage change with:\n")
        f.write("#   python tools/conformance_gate.py --write conformance.xml \\\n")
        f.write("#     tests/conformance/baseline_passing.txt\n")
        for key in sorted(passed):
            f.write(key + "\n")


def main(argv):
    args = list(argv)
    write = bool(args) and args[0] == "--write"
    if write:
        args = args[1:]
    if len(args) != 2:
        print(__doc__)
        return 2
    report_path, baseline_path = args
    passed, all_keys = scan(report_path)

    if write:
        write_baseline(baseline_path, passed)
        print(f"Wrote {len(passed)} passing tests to {baseline_path}")
        return 0

    baseline = load_baseline(baseline_path)
    if not baseline:
        print("No baseline found; gate is a no-op (run with --write to create one).")
        return 0

    regressed = sorted(baseline - passed)        # in baseline, not passing now (incl. absent)
    new_passes = sorted(passed - baseline)       # passing now, not yet in baseline

    print(f"conformance gate: baseline={len(baseline)} passed_now={len(passed)} "
          f"regressed={len(regressed)} new_passes={len(new_passes)}")
    if new_passes:
        print("\nNewly passing (consider adding to the baseline):")
        for key in new_passes:
            print("  +", key)
    if regressed:
        print("\nREGRESSED — these passed in the baseline but no longer pass:")
        for key in regressed:
            print("  -", key)
        print(f"\n{len(regressed)} conformance regression(s); failing the gate.")
        return 1
    print("\nNo conformance regressions. ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
