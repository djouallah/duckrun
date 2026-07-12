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


def _run_digen(datagen: str, sf: int, out: str):
    """Generate the data by driving PDGF directly (we bypass DIGen.jar).

    DIGen.jar just launches PDGF with a hardcoded `-closeWhenDone -start` and NO
    `-load`/`-libjars`, so PDGF tries to `start` before its config (and TPC-DI
    plugin classes) are loaded, fails, and drops to its interactive shell. Rather
    than fight that through DIGen's stdin relay, we run `pdgf.jar` ourselves from
    the pdgf/ dir with only `-o`/`-sf` (no `-start`, so no premature error) and
    then feed its shell the correct, ordered commands:

      ""       ENTER — reveal the BANKMARK license
      YES      accept the license
      libjars  load plugins/tpc-di.jar — the schema references custom generator
               classes (tpc.di.generators.*, tpc.di.output.*) that live there and
               are NOT on pdgf.jar's Class-Path; without this the schema fails to
               parse ("Class 'tpc.di.generators.HRJobIdGenerator' was not found")
      load     load the schema, then the generation config
      closeWhenDone  exit once generation finishes
      start    begin generation
    """
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    pdgf_dir = os.path.join(datagen, "pdgf")
    if not os.path.isfile(os.path.join(pdgf_dir, "pdgf.jar")):
        sys.exit(f"ERROR: pdgf.jar not found under {pdgf_dir}")

    # DIGen floors -sf at 3 and multiplies by 1000 (sf=3 -> PDGF scale 3000).
    # Running PDGF directly we set the scale ourselves; TPCDI_PDGF_SCALE lets CI use
    # a small scale for fast iteration.
    scale = os.environ.get("TPCDI_PDGF_SCALE", "").strip() or str(sf * 1000)
    # PDGF's -o is a path *expression*, so a literal path must be tick-quoted (this
    # is exactly what DIGen does). Trailing slash = the output directory.
    out_arg = "'" + out.replace(os.sep, "/") + "/'"
    cmd = ["java", "-Xmx1g", "-jar", "pdgf.jar", "-o", out_arg, "-sf", scale]
    print(f"  running PDGF: {' '.join(cmd)}  (cwd={pdgf_dir}, scale={scale})", flush=True)
    p = subprocess.Popen(
        cmd, cwd=pdgf_dir, text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    cmds = [
        "",
        "YES",
        "libjars plugins/tpc-di.jar",
        "load config/tpc-di-schema.xml",
        "load config/tpc-di-generation.xml",
        "closeWhenDone",
        "start",
    ]
    p.stdin.write("\n".join(cmds) + "\n")
    p.stdin.flush()

    # Watchdog: if generation hangs (PDGF waiting at its shell), kill it so the job
    # fails fast with full output instead of running to the 6h runner limit.
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
    print("  done.", flush=True)


if __name__ == "__main__":
    main()
