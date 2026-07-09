"""Existence gate for the Contoso benchmark's derived layout tables, hoisted out of the build steps
so the whole build phase (incl. the Livy Spark session spin-up) is skipped when everything is
already present. A missing Contoso base (contoso.sales) also forces the build. Writes
build_needed=true/false to GITHUB_OUTPUT; rebuild is handled in the workflow, orthogonally.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, GITHUB_OUTPUT.
"""
import os

import duckrun

# The base (contoso.sales) plus the two layout copies the build steps produce. Missing any ⇒ the
# build phase must run (each build script still skips what already exists, so a partial rebuild
# only fills the gap).
TABLES = ["contoso.sales",
          "tests.sales_auto_sort",
          "tests.sales_vorder"]

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
