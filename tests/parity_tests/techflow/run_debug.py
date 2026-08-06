"""Debug-session check on techflow — the `is_incremental()` half, on a real incremental model.

Run:  python tests/parity_tests/techflow/run_debug.py
Exit: 0 = every promise held, 1 = one or more broken.

Standalone and READ-ONLY: clones the project source, installs its packages (dbt cannot parse an
unresolved packages.yml), and compiles. It builds nothing and writes nothing.

jaffle_shop's `run_debug.py` covers the relation / CTE / read-only surface. What techflow adds is
scale and an incremental model: ~30 models, 137 data tests (so "does a plain model name resolve" is
a real question here) and `fct_mrr_daily`, which branches on `is_incremental()`. The branch it
reports has a correct answer to be checked against as long as the warehouse already holds that
table — which is why WAREHOUSE_PATH must point at a warehouse techflow has been built into.

Compile-only, deliberately: techflow's marts read from intermediate models whose materialization is
the project's business, not ours.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]

import debug_session                                                        # noqa: E402
from run_parity import DUCKRUN_DIR, DUCKRUN_SCHEMA, DUCKRUN_WH, REPO_URL    # noqa: E402


def main():
    debug_session.ensure_project(DUCKRUN_DIR, REPO_URL)
    ok = debug_session.check(
        DUCKRUN_DIR, HERE, DUCKRUN_WH, DUCKRUN_SCHEMA,
        incremental_model="fct_mrr_daily",
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
