"""Generate TPC-DI source data with DIGen.

DIGen (the TPC data generator) is a standalone Java tool. It is TPC-licensed and
not redistributable, so we do NOT vendor it — instead we shallow-clone the public
databricks-tpc-di repo, which carries the whole datagen toolkit (DIGen.jar + the
PDGF engine it drives), and run it at build time. Override the source repo with
DBX_TPCDI_REPO / DBX_TPCDI_REF if the default becomes unavailable.

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


def _stage_plugin_classes(datagen: str):
    """Make PDGF's TPC-DI plugin classes resolvable from JVM startup.

    DIGen launches PDGF as `java -jar pdgf.jar -closeWhenDone -start ...` (cwd =
    pdgf/) and passes NO -libjars. PDGF only scans plugins/*.jar when it enters its
    interactive shell, but `-start` on the command line makes it parse the schema
    during cmdline processing — *before* that scan — so the custom generator
    classes (tpc.di.generators.*) aren't registered yet and the parse dies with
    "Class 'tpc.di.generators.HRJobIdGenerator' was not found". (Interactive use
    works because the shell scan runs first; that's why this only bites headless.)

    Fix without fighting the load order: pdgf.jar's manifest Class-Path begins with
    "." (its working dir, pdgf/), so we extract plugins/*.jar into pdgf/ itself.
    The classes then live at pdgf/tpc/di/... and resolve on the classpath from the
    very first schema parse — no shell, no libjars, no ordering dependency.
    """
    pdgf_dir = os.path.join(datagen, "pdgf")
    plugins = os.path.join(pdgf_dir, "plugins")
    marker = os.path.join(pdgf_dir, "tpc", "di")
    if os.path.isdir(marker):
        print("  plugin classes already staged into pdgf/", flush=True)
        return
    if not os.path.isdir(plugins):
        sys.exit(f"ERROR: {plugins} not found — cannot stage TPC-DI plugin classes")
    import zipfile
    for jar in sorted(os.listdir(plugins)):
        if not jar.endswith(".jar") or jar.endswith("_src.jar"):
            continue  # skip the source jar; we only need compiled classes
        jar_path = os.path.join(plugins, jar)
        with zipfile.ZipFile(jar_path) as z:
            members = [m for m in z.namelist() if not m.startswith("META-INF/")]
            z.extractall(pdgf_dir, members)
        print(f"  staged {len(members)} entries from plugins/{jar} into pdgf/", flush=True)
    if not os.path.isdir(marker):
        sys.exit(f"ERROR: staged plugins but {marker} still missing")


def _run_digen(datagen: str, sf: int, out: str):
    """Generate the data by running DIGen.jar directly.

    DIGen.jar launches the bundled PDGF engine (`java -jar pdgf.jar -closeWhenDone
    -start -sf N*1000 -o OUT`, cwd = pdgf/) and only needs two stdin lines — ENTER
    then YES — to accept the BANKMARK license. We first stage the TPC-DI plugin
    classes into pdgf/ (see _stage_plugin_classes) so PDGF's headless `-start`
    schema parse can resolve tpc.di.generators.* on the classpath.
    """
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    if not os.path.isfile(os.path.join(datagen, "DIGen.jar")):
        sys.exit(f"ERROR: DIGen.jar not found under {datagen}")
    _stage_plugin_classes(datagen)

    # DIGen's -sf is the TPC-DI scale factor (floored at 3); DIGen scales PDGF
    # internally (x1000). -o is the output dir; DIGen writes Batch1/2/3 beneath it.
    scale = max(sf, 3)
    cmd = ["java", "-Xmx2g", "-jar", "DIGen.jar", "-sf", str(scale), "-o", out]
    print(f"  running DIGen: {' '.join(cmd)}  (cwd={datagen}, sf={scale})", flush=True)
    p = subprocess.Popen(
        cmd, cwd=datagen, text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    # DIGen prompts for license acceptance: ENTER to reveal it, YES to accept.
    p.stdin.write("\nYES\n")
    p.stdin.flush()

    # Watchdog: if generation hangs, kill it so the job fails fast with full
    # output instead of running to the 6h runner limit.
    timeout = int(os.environ.get("TPCDI_GEN_TIMEOUT", "1200"))
    timed_out = {"hit": False}

    def _kill():
        timed_out["hit"] = True
        print(f"  ERROR: DIGen exceeded {timeout}s — killing.", flush=True)
        p.kill()

    watchdog = threading.Timer(timeout, _kill)
    watchdog.start()
    try:
        # Drain with readline() so DIGen never blocks on a full stdout pipe.
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
        sys.exit("ERROR: DIGen timed out")
    if rc != 0:
        sys.exit(f"ERROR: DIGen exited {rc}")
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
