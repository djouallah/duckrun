"""Generate the Contoso base with SQLBI's Contoso Data Generator V2, then land it to Delta.

This is the Contoso analog of AEMO's pre-built ``mart.fct_summary`` — except the base doesn't
pre-exist, we *make* it by running SQLBI's tool (https://github.com/sql-bi/Contoso-Data-Generator-V2,
MIT). We download the self-contained ``DatabaseGenerator`` binary for this platform, run it with the
vendored ``config.json`` (``SalesOrders: BOTH`` → the full schema, ``OutputFormat: PARQUET``), and
write all 8 tables to the ``contoso`` schema of the target lakehouse via duckrun. This pristine base
is never mutated afterward — both layout copies (auto_sort / vorder) derive from ``contoso.sales``.

The generator's own semantic model (``pbit-Import``) is the source of the ported Direct Lake models
under ``contoso_*.SemanticModel/`` — see README.md for full credit + source links.

Env in:
  ONELAKE_TABLES_PATH  target Tables root — abfss://… (OneLake) OR a local path (offline dry-run)
  ONELAKE_TOKEN        storage bearer token (omit for a local path)
  CONTOSO_ORDERS       OrdersCount override (default: config.json's 1,000,000) — the scale knob
  FORCE_REBUILD        "true" → rebuild even if contoso.sales already exists
  CONTOSO_WORK         scratch dir for the generator download + OUT/CACHE (default: ./_contoso_work)
"""
import os
import platform
import stat
import subprocess
import sys
import time
import urllib.request
import zipfile

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
GEN_VERSION = "2.0.1"
GEN_BASE = (f"https://github.com/sql-bi/Contoso-Data-Generator-V2/releases/download/{GEN_VERSION}")

# parquet file (generator output, lowercase) -> Delta table name in the `contoso` schema.
TABLES = [
    ("sales", "sales"), ("orders", "orders"), ("orderrows", "orderrows"),
    ("customer", "customer"), ("product", "product"), ("store", "store"),
    ("date", "date"), ("currencyexchange", "currency_exchange"),
]


def _asset():
    """Release asset + in-zip exe name for this platform (self-contained builds, no .NET SDK)."""
    sysname, machine = platform.system(), platform.machine().lower()
    if sysname == "Windows":
        return "DatabaseGenerator.winx64.zip", "DatabaseGenerator.exe"
    if sysname == "Linux":
        return "DatabaseGenerator.linuxx64.zip", "DatabaseGenerator"
    if sysname == "Darwin":
        arm = machine in ("arm64", "aarch64")
        return (f"DatabaseGenerator.osx-{'arm64' if arm else 'x64'}.zip", "DatabaseGenerator")
    raise SystemExit(f"unsupported platform for the Contoso generator: {sysname}/{machine}")


def _fetch_generator(work):
    """Download + unzip the generator once; return the path to its executable. The zip also bundles
    a default config.json and data.xlsx — we use the bundled data.xlsx and our own config.json."""
    asset, exe_name = _asset()
    gen_dir = os.path.join(work, "gen")
    exe = os.path.join(gen_dir, exe_name)
    if os.path.exists(exe):
        return exe
    os.makedirs(gen_dir, exist_ok=True)
    zpath = os.path.join(work, asset)
    url = f"{GEN_BASE}/{asset}"
    print(f"downloading {url} ...", flush=True)
    urllib.request.urlretrieve(url, zpath)
    with zipfile.ZipFile(zpath) as z:
        z.extractall(gen_dir)
    if platform.system() != "Windows":
        os.chmod(exe, os.stat(exe).st_mode | stat.S_IEXEC | stat.S_IRWXU)
    print(f"generator ready: {exe}", flush=True)
    return exe


def _connect():
    path = os.environ["ONELAKE_TABLES_PATH"]
    token = os.environ.get("ONELAKE_TOKEN")
    so = {"bearer_token": token} if token else None
    return duckrun.connect(path, storage_options=so, read_only=False)


def _exists(con, tbl):
    try:
        con.sql(f'select 1 from contoso."{tbl}" limit 1').fetchone()
        return True
    except Exception:
        return False


def main():
    force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
    con = _connect()
    try:
        con.sql("create schema if not exists contoso")
    except Exception:
        con.con.execute("create schema if not exists contoso")

    # Fast path: the whole base already present → skip generation entirely (unless rebuild).
    if not force and all(_exists(con, tbl) for _, tbl in TABLES):
        rows = con.sql('select count(*) from contoso."sales"').fetchone()[0]
        print(f"contoso base already present (sales={rows:,} rows) — skipping "
              "(rebuild=true to regenerate)", flush=True)
        report.merge({"tables": {"contoso_base": {"build": {
            "engine": "sqlbi_generator", "status": "skipped"}}}})
        return

    work = os.path.abspath(os.environ.get("CONTOSO_WORK", os.path.join(HERE, "_contoso_work")))
    out = os.path.join(work, "OUT")
    cache = os.path.join(work, "CACHE")
    os.makedirs(out, exist_ok=True)
    os.makedirs(cache, exist_ok=True)

    exe = _fetch_generator(work)
    data_xlsx = os.path.join(os.path.dirname(exe), "data.xlsx")   # bundled seed workbook (MIT)
    config = os.path.join(HERE, "config.json")                    # vendored: BOTH + PARQUET

    orders = os.environ.get("CONTOSO_ORDERS", "").strip()
    cmd = [exe, config, data_xlsx, out + os.sep, cache + os.sep]
    if orders.isdigit():
        cmd.append(f"param:OrdersCount={orders}")

    t0 = time.perf_counter()
    print(f"running generator: OrdersCount={orders or 'config default'} ...", flush=True)
    subprocess.run(cmd, check=True)
    gen_secs = time.perf_counter() - t0
    print(f"generation done ({gen_secs:.1f}s) — landing to Delta ...", flush=True)

    for pq, tbl in TABLES:
        src = os.path.join(out, f"{pq}.parquet").replace("\\", "/")
        if not os.path.exists(src):
            raise SystemExit(f"generator did not produce {src}")
        con.sql(f'create or replace table contoso."{tbl}" as '
                f"select * from read_parquet('{src}')")
        n = con.sql(f'select count(*) from contoso."{tbl}"').fetchone()[0]
        print(f"  contoso.{tbl}: {n:,} rows", flush=True)

    report.merge({"tables": {"contoso_base": {"build": {
        "engine": "sqlbi_generator", "generator_version": GEN_VERSION,
        "orders": int(orders) if orders.isdigit() else None,
        "seconds": round(gen_secs, 1), "status": "rebuilt"}}}})
    print("contoso base ready.", flush=True)


if __name__ == "__main__":
    main()
