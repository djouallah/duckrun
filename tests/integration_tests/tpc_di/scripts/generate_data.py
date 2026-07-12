"""Generate TPC-DI source data by driving PDGF directly.

The TPC data generator is a standalone Java tool. It is TPC-licensed and not
redistributable, so we do NOT vendor it — instead we shallow-clone the public
databricks-tpc-di repo, which carries the whole datagen toolkit (DIGen.jar + the
PDGF engine it drives), and run it at build time. Override the source repo with
DBX_TPCDI_REPO / DBX_TPCDI_REF if the default becomes unavailable.

Why we drive `pdgf/pdgf.jar` and NOT `DIGen.jar`
------------------------------------------------
DIGen generates nothing itself; it shells out to `pdgf.jar -closeWhenDone -start`.
That flag combo makes PDGF parse the schema during command-line processing and exit
immediately — it NEVER enters the interactive shell, and the shell is the only place
PDGF scans its `plugins/` folder. So the TPC-DI plugin (`plugins/tpc-di.jar`, which
registers custom timeframe modes DailyMarketTimeFrameMode / TradeTimeFrameMode and
generators like HRJobIdGenerator) is never registered, and the schema parse dies on
`gen_ReferenceGenerator from="DailyMarket-FinWireSecMapping"` ("value unsupported").
Putting the plugin classes on the classpath can't help — registration is an active
step PDGF only performs on a plugin scan, not a classloader lookup.

So we launch `pdgf.jar` ourselves (cwd = pdgf/, so config/ and plugins/ resolve) and
drive its interactive shell: `reloadPlugins` (scan plugins/ → register the custom
modes/generators) BEFORE `load config/tpc-di-generation.xml` and `start`. That's the
one order in which the schema parse succeeds headlessly. `-sf` is the TPC-DI scale
factor ×1000, the same scaling DIGen applies. The TPC-DI plugin's own output writers
(TradeSourceOutput / CustomerMgmtScheduler / AuditPersistenceStore) produce
Batch1/2/3 during `start`, so we get the same files DIGen would — only DIGen's
`digen_report.txt` is skipped (the test doesn't need it).

Produces <out>/Batch1, Batch2, Batch3. The CustomerMgmt.xml files are read
directly by dbt via the `webbed` DuckDB extension (see models/staging), so there
is no XML pre-stage here. Requires a JDK (`java` on PATH); does not install Java.

Usage:
    python generate_data.py --sf 3 --out ./staging
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import threading

DEFAULT_REPO = os.environ.get(
    "DBX_TPCDI_REPO", "https://github.com/shannon-barrow/databricks-tpc-di.git")
DEFAULT_REF = os.environ.get("DBX_TPCDI_REF", "main")


def _require_java():
    if shutil.which("java") is None:
        sys.exit(
            "ERROR: `java` not found on PATH. DIGen needs a JDK (8+). "
            "Install one (CI uses actions/setup-java) — this script will not."
        )


def _fetch_datagen(work: str) -> str:
    """Shallow-clone the dbx repo into <work> and return its datagen dir."""
    repo_dir = os.path.join(work, "databricks-tpc-di")
    datagen = os.path.join(repo_dir, "src", "tools", "datagen")
    if os.path.isfile(os.path.join(datagen, "DIGen.jar")):
        print(f"  datagen already present at {datagen}", flush=True)
        return datagen
    os.makedirs(work, exist_ok=True)
    print(f"  cloning {DEFAULT_REPO}@{DEFAULT_REF} ...", flush=True)
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", DEFAULT_REF,
         DEFAULT_REPO, repo_dir],
        check=True,
    )
    if not os.path.isfile(os.path.join(datagen, "DIGen.jar")):
        sys.exit(f"ERROR: DIGen.jar not found under {datagen} after clone")
    return datagen


def _run_digen(datagen: str, sf: int, out: str):
    """Generate the data by driving PDGF's interactive shell directly.

    We launch `pdgf/pdgf.jar` (cwd = pdgf/, so config/ and plugins/ resolve) with
    the TPC-DI scale (`-sf N*1000`, matching DIGen) and the output dir, but WITHOUT
    `-start`/`-closeWhenDone` on the command line — those would parse the schema
    before any plugin scan. Instead we feed the shell, in order:

        <ENTER> / YES   accept the BANKMARK license
        reloadPlugins   scan plugins/ → register the TPC-DI modes + generators
        closeWhenDone   exit once generation finishes
        load <config>   load the generation project (references the schema)
        start           generate
        exit            safety net

    reloadPlugins BEFORE load is the whole point: it registers the custom timeframe
    modes/generators so the schema parse resolves `gen_ReferenceGenerator`.
    """
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    pdgf_dir = os.path.join(datagen, "pdgf")
    if not os.path.isfile(os.path.join(pdgf_dir, "pdgf.jar")):
        sys.exit(f"ERROR: pdgf.jar not found under {pdgf_dir}")

    # -sf is the TPC-DI scale factor (floored at 3) ×1000 — the PDGF-native scale
    # DIGen applies internally. Config path is relative to cwd (pdgf/).
    scale = max(sf, 3)
    config = "config/tpc-di-generation.xml"
    cmd = ["java", "-Xmx2g", "-jar", "pdgf.jar", "-sf", str(scale * 1000), "-o", out]
    print(f"  running PDGF: {' '.join(cmd)}  (cwd={pdgf_dir}, tpcdi_sf={scale})", flush=True)
    p = subprocess.Popen(
        cmd, cwd=pdgf_dir, text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    # License prompt (ENTER reveals it, YES accepts), then drive the shell. Sending
    # the whole script up front is fine — PDGF reads commands line by line.
    p.stdin.write(
        "\nYES\n"
        "reloadPlugins\n"
        "closeWhenDone\n"
        f"load {config}\n"
        "start\n"
        "exit\n"
    )
    p.stdin.flush()

    # Watchdog: if generation hangs, kill it so the job fails fast with full
    # output instead of running to the 6h runner limit.
    timeout = int(os.environ.get("TPCDI_GEN_TIMEOUT", "1200"))
    timed_out = {"hit": False}

    def _kill():
        timed_out["hit"] = True
        print(f"  ERROR: PDGF exceeded {timeout}s — killing.", flush=True)
        p.kill()

    watchdog = threading.Timer(timeout, _kill)
    watchdog.start()
    try:
        # Drain with readline() so PDGF never blocks on a full stdout pipe.
        while True:
            line = p.stdout.readline()
            if not line and p.poll() is not None:
                break
            if line:
                print(line.rstrip("\n"), flush=True)
        rc = p.wait()
    finally:
        watchdog.cancel()
    try:
        p.stdin.close()
    except OSError:
        pass
    if timed_out["hit"]:
        sys.exit("ERROR: PDGF timed out")
    if rc != 0:
        sys.exit(f"ERROR: PDGF exited {rc}")
    if not os.path.isdir(os.path.join(out, "Batch1")):
        sys.exit(f"ERROR: no Batch1/ produced under {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "3")),
                    help="scale factor (min 3)")
    ap.add_argument("--out", default=os.environ.get("TPCDI_DIR", "./staging"))
    ap.add_argument("--work", default=os.environ.get("TPCDI_WORK", "./_tpcdi_work"))
    ap.add_argument("--force", action="store_true", help="regenerate even if present")
    args = ap.parse_args()

    if args.sf < 3:
        sys.exit("ERROR: TPC-DI minimum scale factor is 3")

    if os.path.isdir(os.path.join(args.out, "Batch1")) and not args.force:
        print(f"  {args.out}/Batch1 exists; skipping generation (use --force).", flush=True)
        return
    _require_java()
    datagen = _fetch_datagen(args.work)
    _run_digen(datagen, args.sf, args.out)
    _summarize(args.out)
    print("  done.", flush=True)


def _summarize(out: str):
    """Print concrete proof of what was generated: every Batch*/  file with its
    size and row count. Fails loudly if a batch produced nothing."""
    total_bytes = 0
    total_rows = 0
    for batch in ("Batch1", "Batch2", "Batch3"):
        bdir = os.path.join(out, batch)
        if not os.path.isdir(bdir):
            print(f"  [{batch}] MISSING", flush=True)
            continue
        files = sorted(os.listdir(bdir))
        print(f"  [{batch}] {len(files)} files:", flush=True)
        for f in files:
            fp = os.path.join(bdir, f)
            if not os.path.isfile(fp):
                continue
            size = os.path.getsize(fp)
            total_bytes += size
            # Row count for text sources (skip huge/binary — these are all text).
            try:
                with open(fp, "rb") as fh:
                    rows = sum(1 for _ in fh)
            except OSError:
                rows = -1
            total_rows += max(rows, 0)
            print(f"       {f:<24} {size:>12,} bytes  {rows:>10,} rows", flush=True)
    print(f"  TOTAL generated: {total_bytes:,} bytes, {total_rows:,} rows across batches", flush=True)
    if total_rows == 0:
        sys.exit("ERROR: generation produced no rows")


if __name__ == "__main__":
    main()
