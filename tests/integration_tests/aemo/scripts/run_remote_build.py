"""Run the WHOLE aemo dbt lifecycle on Fabric compute via throwaway notebooks (RemoteRunner).

The real end-to-end exercise of duckrun's remote executor: instead of running dbt on the runner,
ship ONLY the aemo dbt logic (models + macros + scrubbed profiles.yml) into a temporary Fabric
notebook at cores=8, run dbt there, delete the notebook. External assets (dim_calendar, the AEMO
CSVs) are NOT embedded — they live in OneLake and are read via source(); the calendar is generated
by scripts/generate_calendar.py in a separate step first.

Mirrors the local aemo CI's dbt sequence, but each command runs in its OWN notebook — i.e. a fresh
process, which is exactly what the standalone `dbt test` needs (it's the abfss:// read-path gate:
it materializes nothing, so it only passes if the adapter re-registers every Delta table as a
delta_scan view at run start):

    dbt build --exclude tag:heavy     # models + inline tests + snapshots
    dbt test  --exclude tag:heavy     # fresh-process read-path gate
    dbt docs generate --static        # real catalog over the abfss:// Delta tables

RemoteRunner auto-forwards the project's non-secret env_var config (WAREHOUSE_PATH, FILES_PATH,
DBT_SCHEMA, download_limit, process_limit) into the notebook and deliberately drops ONELAKE_TOKEN —
inside Fabric the storage token comes from notebookutils.

Run from the aemo project dir, after `az login`, with FABRIC_TOKEN + ONELAKE_TOKEN set (see
.github/workflows/aemo_remote.yml). Exits non-zero if any remote step fails.
"""
import os
import sys

from duckrun import RemoteRunner

# The full aemo dbt lifecycle. Each entry runs in its own temp notebook (fresh process).
STEPS = [
    ["build", "--target", "dev", "--exclude", "tag:heavy"],
    ["test", "--target", "dev", "--exclude", "tag:heavy"],
    ["docs", "generate", "--static", "--target", "dev"],
]


def main() -> int:
    runner = RemoteRunner(
        cores=8,
        target="dev",
        # The notebook runs the aemo project with the normal released duckrun from PyPI — the thing
        # under test is RemoteRunner's orchestration (create/run/delete), which runs here on the runner.
        pip_spec="duckrun",
        # Control plane vs storage: distinct scopes, minted separately in CI.
        fabric_token=os.environ["FABRIC_TOKEN"],
        storage_token=os.environ["ONELAKE_TOKEN"],
    )

    ok = True
    for cmd in STEPS:
        res = runner.invoke(cmd)
        print(f"\n=== remote `dbt {' '.join(cmd)}` -> success={res.success} ===")
        for node in res.result or []:
            print(f"  {node.get('node')}: {node.get('status')}")
        ok = ok and bool(res.success)

    print(f"\nAll remote steps {'PASSED' if ok else 'FAILED'}.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
