"""Debug-session check on jaffle_shop — `duckrun.dbt_project()` against a real project.

Run:  python tests/parity_tests/jaffle_shop/run_debug.py
Exit: 0 = every promise held, 1 = one or more broken.

Standalone and READ-ONLY. It clones the project source (dbt needs the .sql files to compile) and
reads the tables jaffle_shop already has in the warehouse — it builds nothing, writes nothing, and
does not depend on `run_parity.py` having run in the same job. If the warehouse has no jaffle_shop
tables, that is what it reports; point WAREHOUSE_PATH at one that does (the parity lakehouse, or a
local warehouse a previous `run_parity.py` produced).

jaffle_shop is the right project for this: its staging models are CTE-shaped and read straight from
seeds (real Delta tables, so a cold session can execute them), its marts read from `view` models
(which duckrun does not persist — the limitation this pins), and every model carries generic tests,
which is what made a plain model name look ambiguous on the first real project this feature met.
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]

import debug_session                                                        # noqa: E402
from run_parity import CLONE_DIR, DUCKRUN_SCHEMA, DUCKRUN_WH, REPO_URL      # noqa: E402


def main():
    debug_session.ensure_project(CLONE_DIR, REPO_URL)
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
