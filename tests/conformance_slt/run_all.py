#!/usr/bin/env python3
"""
Run every .slt file in a directory, each against a fresh database path.

  python run_all.py tests/                       # runs against `duckrun`
  DUCKRUN_MODULE=duckrun_shim python run_all.py tests/   # any connect(path).sql() module
"""
import os
import sys
import tempfile

from runner import run_file


def main():
    test_dir = sys.argv[1] if len(sys.argv) > 1 else "tests"
    module = os.environ.get("DUCKRUN_MODULE", "duckrun")
    files = sorted(
        os.path.join(test_dir, f) for f in os.listdir(test_dir) if f.endswith(".slt")
    )
    if not files:
        print(f"no .slt files found in {test_dir}")
        sys.exit(2)

    total_exec = total_pass = total_fail = 0
    failed_files = []
    for path in files:
        db = tempfile.mkdtemp(prefix="duckrun_slt_")
        stats = run_file(path, db, module)
        total_exec += stats.executed
        total_pass += stats.passed
        total_fail += len(stats.failures)
        if stats.failures:
            failed_files.append(os.path.basename(path))

    print()
    print(f"TOTAL: {total_pass}/{total_exec} records passed, {total_fail} failures")
    if failed_files:
        print("failing files: " + ", ".join(failed_files))
        sys.exit(1)
    print("ALL GREEN")


if __name__ == "__main__":
    main()
