"""Generate TPC-DI source data by driving PDGF directly.

The TPC data generator is a standalone Java tool. It is TPC-licensed and not
redistributable, so we do NOT vendor it — instead we shallow-clone the public
databricks-tpc-di repo, which carries the whole datagen toolkit (DIGen.jar + the
PDGF engine it drives), and run it at build time. Override the source repo with
DBX_TPCDI_REPO / DBX_TPCDI_REF if the default becomes unavailable.

Why we drive `pdgf.Controller` via `-cp` and NOT `DIGen.jar`
------------------------------------------------------------
DIGen generates nothing itself; it shells out to `java -jar pdgf.jar …`. The TPC-DI
generators and custom timeframe modes (tpc.di.generators.HRJobIdGenerator,
tpc/di/DailyMarketTimeFrameMode, …) live in `plugins/tpc-di.jar`, but this datagen's
`pdgf.jar` manifest Class-Path does NOT list that plugin — and PDGF discovers its
generator/mode classes by scanning `java.class.path`, which under `java -jar pdgf.jar`
contains only pdgf.jar. So the plugin is never seen and the schema parse dies on
`tpc.di.generators.HRJobIdGenerator was not found` (and then on the custom
`gen_ReferenceGenerator from="DailyMarket-…"` mode). Manifest/classloader tricks don't
help: the scan reads `java.class.path` specifically.

So we launch PDGF ourselves as `java -cp pdgf.jar:plugins/tpc-di.jar:extlib/*
pdgf.Controller -sf N000 -start -closeWhenDone` (cwd = pdgf/). Putting the plugin on
`-cp` is what carries it onto `java.class.path` so the scan registers every custom
class. Two headless quirks we handle:
  • No `-o`. PDGF splices `-o` into a javassist-compiled fileTemplate *un-quoted*, so
    any real path fails to compile ("no such field" / "; is missing"). We let PDGF
    write to its default `<pdgf>/output/Batch{1,2,3}` and move those under <out>.
  • Keep stdin OPEN. `-start` runs alongside an interactive shell thread; if stdin
    hits EOF that thread loops on null commands into PDGF's flood-prevention kill,
    aborting generation mid-run. We send ENTER+YES (license) then leave stdin open so
    the thread blocks. `-sf` is the TPC-DI scale ×1000, the scaling DIGen applies.

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
    """Generate the data by running pdgf.Controller with the TPC-DI plugin on the classpath.

    See the module docstring for why: the plugin must be on `java.class.path` (hence `-cp`,
    not `-jar`), we must NOT pass `-o` (it breaks a javassist-compiled fileTemplate), and we
    keep stdin open so PDGF's shell thread doesn't flood-kill the run. PDGF writes to
    `<pdgf>/output/Batch{1,2,3}`, which we then move under <out>.
    """
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    pdgf_dir = os.path.join(datagen, "pdgf")
    if not os.path.isfile(os.path.join(pdgf_dir, "pdgf.jar")):
        sys.exit(f"ERROR: pdgf.jar not found under {pdgf_dir}")
    if not os.path.isfile(os.path.join(pdgf_dir, "plugins", "tpc-di.jar")):
        sys.exit(f"ERROR: plugins/tpc-di.jar not found under {pdgf_dir}")

    pdgf_out = os.path.join(pdgf_dir, "output")
    shutil.rmtree(pdgf_out, ignore_errors=True)  # start clean so we only move this run's files

    # -sf is the TPC-DI scale factor (floored at 3) ×1000 — the PDGF-native scale DIGen applies.
    # `extlib/*` is glob-expanded by the JVM; os.pathsep keeps the classpath portable.
    scale = max(sf, 3)
    classpath = os.pathsep.join(["pdgf.jar", "plugins/tpc-di.jar", "extlib/*"])
    cmd = ["java", "-Xmx2g", "-cp", classpath, "pdgf.Controller",
           "-sf", str(scale * 1000), "-start", "-closeWhenDone"]
    print(f"  running PDGF: {' '.join(cmd)}  (cwd={pdgf_dir}, tpcdi_sf={scale})", flush=True)
    p = subprocess.Popen(
        cmd, cwd=pdgf_dir, text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    # ENTER+YES accepts the BANKMARK license; then leave stdin OPEN (see docstring) so the
    # interactive shell thread blocks on read instead of EOF-looping into flood-prevention.
    p.stdin.write("\nYES\n")
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

    # PDGF wrote to <pdgf>/output/Batch{1,2,3}; move that tree under <out>.
    if not os.path.isdir(pdgf_out):
        sys.exit(f"ERROR: PDGF produced no output/ under {pdgf_dir}")
    for name in sorted(os.listdir(pdgf_out)):
        dst = os.path.join(out, name)
        if os.path.isdir(dst):
            shutil.rmtree(dst)
        elif os.path.exists(dst):
            os.remove(dst)
        shutil.move(os.path.join(pdgf_out, name), dst)
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
