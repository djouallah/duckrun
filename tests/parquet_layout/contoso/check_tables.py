"""Existence gate for the Contoso benchmark's derived layout tables, hoisted out of the build steps
so the whole build phase (incl. the Livy Spark session spin-up) is skipped when everything is
already present. A missing base (the raw sales.parquet in Files) also forces the build. Writes
build_needed=true/false to GITHUB_OUTPUT; rebuild is handled in the workflow, orthogonally.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, GITHUB_OUTPUT.
"""
import os
import sys

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_base  # noqa: E402  — sales_files_urls / _files_exists: base lives in Files, not a table

# The two layout copies the build steps produce. Missing any (or the raw sales.parquet base in Files)
# ⇒ the build phase must run (each build script still skips what already exists, so a partial rebuild
# only fills the gap).
TABLES = ["tests.sales_auto_sort",
          "tests.sales_vorder"]

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"])

missing = []
_abfss_url, _store_root = build_base.sales_files_urls()
if build_base._files_exists(_store_root, os.environ.get("ONELAKE_TOKEN")):
    print(f"  exists : {build_base.SALES_FILES_REL} (raw base)", flush=True)
else:
    missing.append(build_base.SALES_FILES_REL)
    print(f"  MISSING: {build_base.SALES_FILES_REL} (raw base)", flush=True)

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
