"""Single existence check for the benchmark's derived tables, hoisted out of the individual build
steps so the whole build phase (incl. the Livy Spark session spin-up) is skipped when everything
is already present. Writes build_needed=true/false to GITHUB_OUTPUT; rebuild is handled in the
workflow, orthogonally. One duckrun connection, no writes.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, GITHUB_OUTPUT.
"""
import os

import duckrun

# All tables the two build steps produce (each reads mart.fct_summary directly). Missing any ⇒ the
# build phase must run (each build script still skips the tables that already exist, so a partial
# rebuild only rebuilds the gap).
TABLES = ["tests.fct_summary_optimized",
          "tests.fct_summary_vorder_base_sorted"]

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"],
                      storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})

missing = []
for t in TABLES:
    try:
        con.sql(f"select 1 from {t} limit 1").fetchone()
        print(f"  exists : {t}", flush=True)
    except Exception:
        missing.append(t)
        print(f"  MISSING: {t}", flush=True)

need = "true" if missing else "false"
print(f"build_needed={need} (missing: {', '.join(missing) or 'none'})", flush=True)

out = os.environ.get("GITHUB_OUTPUT")
if out:
    with open(out, "a", encoding="utf-8") as f:
        f.write(f"build_needed={need}\n")
