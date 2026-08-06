"""Debug-session check on techflow — the `is_incremental()` half, on a real incremental model.

Run:  python tests/parity_tests/techflow/run_debug.py   (AFTER run_parity.py has built it)
Exit: 0 = every promise held, 1 = one or more broken.

Reads only; the clone is never modified. jaffle_shop covers the relation/CTE/read-only surface —
what techflow adds is scale and an incremental model: ~30 models, 137 data tests (so "does a plain
model name resolve" is a real question here) and `fct_mrr_daily`, which branches on
`is_incremental()`. The parity build materializes it, so the branch the session reports has a
correct answer to be checked against.

The checks here are compile-only, which is deliberate: techflow's marts read from intermediate
models whose materialization is the project's business, not ours.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]

import debug_session                                                        # noqa: E402
from run_parity import DUCKRUN_DIR, DUCKRUN_SCHEMA, DUCKRUN_WH              # noqa: E402


def main():
    if not (DUCKRUN_DIR / "dbt_project.yml").exists():
        sys.exit(f"no project at {DUCKRUN_DIR} — run run_parity.py first")
    ok = debug_session.check(
        DUCKRUN_DIR, HERE, DUCKRUN_WH, DUCKRUN_SCHEMA,
        incremental_model="fct_mrr_daily",
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
