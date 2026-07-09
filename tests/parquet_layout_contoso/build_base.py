"""Generate the Contoso base with SQLBI's Contoso Data Generator V2, then land it.

This is the Contoso analog of AEMO's pre-built ``mart.fct_summary`` — except the base doesn't
pre-exist, we *make* it by running SQLBI's tool (https://github.com/sql-bi/Contoso-Data-Generator-V2,
MIT). We download the self-contained ``DatabaseGenerator`` binary for this platform, run it with the
vendored ``config.json`` (``SalesOrders: BOTH`` → the full schema, ``OutputFormat: PARQUET``).

The raw base lands entirely as parquet in the lakehouse **Files** section — the whole generator
output (all 8 tables), byte-verbatim, uploaded via obstore. Files is then the single source:

  - The SALES fact — the table under test — is read from Files by BOTH layout builds. It is never
    written by duckrun, because writing it as a duckrun Delta would hand Spark's V-Order build a
    duckrun-shaped input (V-Order preserves input row order, so it would seed Spark's clustering).
    Spark writes ``tests.sales_vorder`` from the Files parquet; duckrun writes ``tests.sales_auto_sort``
    from the same Files parquet — neither engine's layout seeds the other; only its own differs.
  - The dimensions + Orders + OrderRows are materialised as ``contoso.*`` Delta tables (the Direct
    Lake model joins to them) by reading them back from the Files parquet. Their layout is irrelevant
    to the benchmark, but they still flow from the same Files source, so nothing is special-cased.

The generator's own semantic model (``pbit-Import``) is the source of the ported Direct Lake models
under ``contoso_*.SemanticModel/`` — see README.md for full credit + source links.

Env in:
  ONELAKE_TABLES_PATH  target Tables root — abfss://… (OneLake) OR a local path (offline dry-run)
  ONELAKE_TOKEN        storage bearer token (omit for a local path)
  CONTOSO_ORDERS       OrdersCount override (default: config.json's default) — the scale knob
  FORCE_REBUILD        "true" → rebuild even if the base already exists
  CONTOSO_WORK         scratch dir for the generator download + OUT/CACHE (default: ./_contoso_work)
"""
import os
import platform
import re
import stat
import subprocess
import sys
import time
import urllib.request
import zipfile

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

# All raw generator parquet lands under this folder in the lakehouse Files section.
FILES_DIR = "Files/contoso_base"
# The sales.parquet object path — build_spark_variant.py and build_auto_sort.py read exactly this.
SALES_FILES_REL = f"{FILES_DIR}/sales.parquet"


def files_object(pq):
    """obstore object path for generator table ``pq`` in the Files section."""
    return f"{FILES_DIR}/{pq}.parquet"

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


def sales_files_urls(tables_path=None):
    """``(abfss_read_url, store_root)`` for the raw sales.parquet in the lakehouse Files section,
    derived from ONELAKE_TABLES_PATH (``abfss://{ws}@{host}/{lh}/Tables``). ``(None, None)`` for a
    local path (offline dry-run). ``abfss_read_url`` is what Spark / duckrun ``read_parquet`` consume;
    ``store_root`` is the obstore ``AzureStore`` root (its object path is then ``SALES_FILES_REL``).
    Single source of truth — build_spark_variant / build_auto_sort / check_tables all import this."""
    path = (tables_path or os.environ["ONELAKE_TABLES_PATH"]).rstrip("/")
    m = re.match(r"abfss://([^@]+)@([^/]+)/([^/]+)/Tables$", path)
    if not m:
        return None, None
    ws, host, lh = m.groups()
    root = f"abfss://{ws}@{host}/{lh}/"
    return (root + SALES_FILES_REL, root)


def _files_store(store_root, token):
    from obstore.store import AzureStore
    return AzureStore.from_url(store_root, bearer_token=token)


def _files_exists(store_root, token):
    """True if the raw sales.parquet is already in Files (obstore HEAD)."""
    if not store_root:
        return False
    import obstore
    try:
        obstore.head(_files_store(store_root, token), SALES_FILES_REL)
        return True
    except Exception:
        return False


def _upload_to_files(local_path, store_root, token, object_path):
    """Upload a (possibly multi-GB) parquet byte-verbatim to OneLake Files via obstore, which streams
    it as a multipart upload. duckrun's ``conn.copy`` reads a whole file into one BLOB, which a ~5GB
    sales.parquet would OOM — obstore chunks it and preserves the generator's exact bytes."""
    import obstore
    gb = os.path.getsize(local_path) / 1e9
    print(f"  obstore put → {object_path} ({gb:.2f} GB, multipart)...", flush=True)
    with open(local_path, "rb") as f:
        obstore.put(_files_store(store_root, token), object_path, f)


def main():
    force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
    orders = os.environ.get("CONTOSO_ORDERS", "").strip()
    token = os.environ.get("ONELAKE_TOKEN")
    _, store_root = sales_files_urls()       # store_root None on a local path (offline dry-run)
    con = _connect()
    con.sql("create schema if not exists contoso")

    # Everything except sales lands as contoso.* Delta; sales is the raw parquet in Files (see docstring).
    dim_tables = [(pq, tbl) for pq, tbl in TABLES if tbl != "sales"]

    def _sales_present():
        return _files_exists(store_root, token) if store_root else _exists(con, "sales")

    # Fast path: reuse an existing base instead of regenerating (existence-only — the generator is the
    # slow step). But guard against a SILENT scale mismatch: if the base on disk was built at a
    # different scale than the CONTOSO_ORDERS now requested, reusing it would benchmark the wrong size.
    # We can't safely auto-regenerate just the base here — the derived layout copies (sales_auto_sort /
    # sales_vorder) have their own rebuild=false skip and would stay at the old scale, desyncing the
    # chain. A scale change must rebuild the WHOLE chain, which is exactly what rebuild=true does, so
    # refuse loud and point at it. The generator emits a hair fewer orders than OrdersCount (a few %
    # drop for unassignable rows), so match the `orders` table on a tolerance, not equality.
    if not force and all(_exists(con, tbl) for _, tbl in dim_tables) and _sales_present():
        have = con.sql('select count(*) from contoso."orders"').fetchone()[0]
        if orders.isdigit() and int(orders) > 0 and abs(have - int(orders)) > 0.05 * int(orders):
            raise SystemExit(
                f"contoso base exists at orders={have:,} but OrdersCount={orders} was requested — "
                f"a scale change must rebuild the whole chain (base + both layout copies). "
                f"Re-dispatch with rebuild=true.")
        print(f"contoso base already present (orders={have:,}, raw sales.parquet in Files) — "
              "skipping (rebuild=true to regenerate)", flush=True)
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

    cmd = [exe, config, data_xlsx, out + os.sep, cache + os.sep]
    if orders.isdigit():
        cmd.append(f"param:OrdersCount={orders}")

    t0 = time.perf_counter()
    print(f"running generator: OrdersCount={orders or 'config default'} ...", flush=True)
    subprocess.run(cmd, check=True)
    gen_secs = time.perf_counter() - t0
    print(f"generation done ({gen_secs:.1f}s) — landing ...", flush=True)

    for pq, _tbl in TABLES:
        if not os.path.exists(os.path.join(out, f"{pq}.parquet")):
            raise SystemExit(f"generator did not produce {pq}.parquet")

    if store_root:
        # 1) Upload EVERY generator parquet byte-verbatim to Files — the raw base, single source.
        print("uploading all raw generator parquet to Files (obstore) ...", flush=True)
        for pq, _tbl in TABLES:
            src = os.path.join(out, f"{pq}.parquet").replace("\\", "/")
            _upload_to_files(src, store_root, token, files_object(pq))
        # 2) Materialise the dims + Orders + OrderRows as contoso.* Delta by reading them BACK from
        #    Files (the model joins to them). Sales is left as parquet-only — both layout builds read
        #    it straight from Files, so duckrun never shapes the fact under test.
        for pq, tbl in dim_tables:
            url = store_root + files_object(pq)
            con.sql(f'create or replace table contoso."{tbl}" as '
                    f"select * from read_parquet('{url}')")
            n = con.sql(f'select count(*) from contoso."{tbl}"').fetchone()[0]
            print(f"  contoso.{tbl}: {n:,} rows (from Files)", flush=True)
    else:
        # Offline dry-run (local path, no Files section): materialise everything as local Delta so the
        # readers have something.
        for pq, tbl in TABLES:
            src = os.path.join(out, f"{pq}.parquet").replace("\\", "/")
            con.sql(f'create or replace table contoso."{tbl}" as '
                    f"select * from read_parquet('{src}')")
            print(f"  contoso.{tbl}: local Delta", flush=True)

    report.merge({"tables": {"contoso_base": {"build": {
        "engine": "sqlbi_generator", "generator_version": GEN_VERSION,
        "orders": int(orders) if orders.isdigit() else None,
        "seconds": round(gen_secs, 1), "status": "rebuilt"}}}})
    print("contoso base ready (all raw parquet in Files; dims materialised as contoso.* Delta).",
          flush=True)


if __name__ == "__main__":
    main()
