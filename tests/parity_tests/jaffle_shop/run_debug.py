"""Debug-session check on jaffle_shop — `duckrun.dbt_project()` against a real project.

Run:  python tests/parity_tests/jaffle_shop/run_debug.py   (AFTER run_parity.py has built it)
Exit: 0 = every promise held, 1 = one or more broken.

Reads only. The clone is never modified, nothing is written to the warehouse, and the models it
reads are the ones the parity build just materialized. Constants come from run_parity so the two
always look at the same clone, warehouse and schema.

jaffle_shop is the right first project for this: its staging models are CTE-shaped and read straight
from seeds (real Delta tables, so a cold session can execute them), its marts read from `view`
models (which duckrun does not persist — the limitation this pins), and every model carries generic
tests, which is what made a plain model name look ambiguous on the first real project this feature
met.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]

import debug_session                                                        # noqa: E402
from run_parity import CLONE_DIR, DUCKRUN_SCHEMA, DUCKRUN_WH                # noqa: E402


def main():
    if not (CLONE_DIR / "dbt_project.yml").exists():
        sys.exit(f"no project at {CLONE_DIR} — run run_parity.py first")
    ok = debug_session.check(
        CLONE_DIR, HERE, DUCKRUN_WH, DUCKRUN_SCHEMA,
        # Staging models read from seeds, which duckrun persists as Delta — so a cold session can
        # actually run them. They are also CTE-shaped (`source` -> `renamed`).
        seed_backed_model="stg_customers",
        cte_model="stg_orders",
        seed_ref="raw_customers",
        # `customers` refs the staging VIEWS. Compiles fine, cannot be read cold.
        view_backed_model="customers",
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
