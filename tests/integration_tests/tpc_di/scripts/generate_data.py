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
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    # DIGen must run with cwd=datagen so it finds the sibling pdgf/ engine.
    cmd = ["java", "-jar", "DIGen.jar", "-sf", str(sf), "-o", out]
    print(f"  running: {' '.join(cmd)}  (cwd={datagen})", flush=True)
    p = subprocess.Popen(
        cmd, cwd=datagen, text=True,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    # DIGen has no prompt of its own — it spawns PDGF (cwd=pdgf/), passes
    # `-closeWhenDone -start` but deliberately NOT `-load` ("-load not recognized
    # by PDGF"), and relays its stdin straight to PDGF. So PDGF shows the BANKMARK
    # license, then — because nothing was loaded — its cmdline `-start` fails and it
    # drops into its interactive shell asking us to "load <Filename>".
    #
    # We drive that shell explicitly through the relay:
    #   ""     -> ENTER, reveal the license terms
    #   YES    -> accept the license
    #   load … -> load the schema then the generation config (paths relative to
    #             PDGF's cwd = pdgf/; the generation file's XInclude hrefs resolve
    #             from there too)
    #   start  -> begin generation; -closeWhenDone makes PDGF exit when finished
    #
    # Keep stdin OPEN afterwards: DIGen's relay loops on readLine(), and EOF (a
    # closed pipe) becomes an endless stream of "null" commands that trips PDGF's
    # flooding prevention. Leaving it open just parks the relay harmlessly until
    # PDGF exits and DIGen calls System.exit.
    p.stdin.write(
        "\n"
        "YES\n"
        "load config/tpc-di-schema.xml\n"
        "load config/tpc-di-generation.xml\n"
        "start\n"
    )
    p.stdin.flush()
    # Drain with readline() (not `for line in p.stdout`, whose read-ahead buffering
    # delays lines) so DIGen/PDGF never block on a full stdout pipe.
    while True:
        line = p.stdout.readline()
        if not line and p.poll() is not None:
            break
        if line:
            print(line.rstrip("\n"), flush=True)
    rc = p.wait()
    try:
        p.stdin.close()
    except OSError:
        pass
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
    print("  done.", flush=True)


if __name__ == "__main__":
    main()
