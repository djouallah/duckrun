"""Local layout probe: how much of the V-Order money-column win can a plain ORDER BY buy?

On the full-scale nyc ``fct_trips`` build, V-Order lands the money columns 20x+ smaller than
duckrun's SORTED BY AUTO (benchmark history, 2026-08-11: ``total_amount`` 3.3 MB vs 66.5 MB on the
same 43.7M-row subset — run ids in the anchor tables below). The mechanism is multi-column
run-minimizing reordering; duckrun's only reordering lever is lexicographic ORDER BY. This probe
measures, locally and cheaply, how much of that gap different ORDER BY keys close:

  * ``none``            — unsorted control (the iid baseline)
  * ``auto``            — whatever the shipping recommender picks today
  * ``dims``            — a hand key of the correlated low-card dimensions
  * ``dims_money_tail`` — the same dims with the money measures appended at the TAIL of the key
                          (rows inside each dim group sort by the measure → the measure forms runs;
                          the dims above are untouched, so nothing queries filter on is scrambled)
  * ``route_tail``      — dims + route (locations, distance) then money, the "everything cheap
                          first" variant

It writes each variant as a real Delta table through duckrun's own writer (delta-rs, the shipping
encodings/pages/geometry), then reads per-column compressed bytes back from the parquet footers.
The verdict feeds the SORTED BY AUTO model: if a tail-measure key closes a real fraction of the
gap, the recommender learns to score it; if nothing moves, the gap is clustering-only and duckrun
ships nothing (DuckDB has no clustering primitive and duckrun is not growing one).

Standalone script, sibling of the aemo/contoso layout experiments — NOT a pytest suite, NOT wired
into any workflow, and carved out of cores.yml like its siblings. Needs network on first run only
(downloads public TLC months into ``_data/``, ~50 MB each, cached). Everything it writes stays
under this folder (``_data/``, ``_out/`` — both gitignored).

    python tests/parquet_layout/nyc/probe_measure_aware_key.py
    PROBE_MONTHS=2024-01 python …   # fewer/more months (comma-separated)
"""
import glob
import os
import shutil
import time
import urllib.request

import duckdb

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("PROBE_DATA_DIR") or os.path.join(HERE, "_data")
OUT = os.environ.get("PROBE_OUT_DIR") or os.path.join(HERE, "_out")
MONTHS = [m.strip() for m in (os.environ.get("PROBE_MONTHS") or "2024-01,2024-02").split(",")
          if m.strip()]
TLC = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"

# The canonical fct_trips frame — fabric-dbt-benchmark models/nyc/duckdb/marts/fct_trips.sql +
# macros/nyc_trip_columns.sql, replicated verbatim so AUTO profiles the same shape it sees in CI.
INT_COLS = ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID",
            "payment_type"]
TS_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
STR_COLS = ["store_and_fwd_flag"]
DBL_COLS = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount"]
MONEY = DBL_COLS  # every DOUBLE is a measure; trip_distance included — V-Order compresses it too

# Reference anchors, bits/value, from the benchmark history (43,734,157-row subset, same mart SQL):
#   vorder — 2026-08-11T0209Z-31450956154 (Fabric Spark, V-Order on)
#   auto   — 2026-08-11T0517Z-31460095071 (duckrun, SORTED BY AUTO, row groups pinned 2M)
# djouallah/fabric-dbt-benchmark/history/runs. MB * 2^20 * 8 / rows; different months than the
# local sample, so anchors are reference points, not same-data controls.
_ANCHOR_ROWS = 43_734_157
_MB = 1024 * 1024
ANCHORS_MB = {
    "vorder@CI": {"trip_distance": 27.71, "fare_amount": 1.57, "extra": 0.01, "mta_tax": 0.01,
                  "tip_amount": 1.79, "tolls_amount": 0.04, "improvement_surcharge": 0.0,
                  "total_amount": 3.30},
    "auto@CI": {"trip_distance": 62.73, "fare_amount": 52.30, "extra": 5.63, "mta_tax": 0.31,
                "tip_amount": 22.59, "tolls_amount": 4.10, "improvement_surcharge": 0.0,
                "total_amount": 66.46},
}
ANCHORS_BITS = {name: {c: mb * _MB * 8 / _ANCHOR_ROWS for c, mb in cols.items()}
                for name, cols in ANCHORS_MB.items()}

VARIANTS = [
    ("none", ""),
    ("auto", "sorted by auto"),
    ("dims", "sorted by (pickup_date, payment_type, RatecodeID, passenger_count)"),
    ("dims_money_tail", "sorted by (pickup_date, payment_type, RatecodeID, passenger_count, "
                        "fare_amount, tip_amount, tolls_amount)"),
    ("route_tail", "sorted by (pickup_date, RatecodeID, PULocationID, DOLocationID, "
                   "trip_distance, fare_amount, total_amount)"),
    # refinements: is total_amount near-FD of the money tail? does trip_distance respond at all?
    # and does the tail work just as well behind AUTO's own (VendorID-polluted) dim key — i.e. can
    # Phase 2 leave dim selection untouched and only add tail slots?
    ("tail_total", "sorted by (pickup_date, payment_type, RatecodeID, passenger_count, "
                   "fare_amount, tip_amount, tolls_amount, total_amount)"),
    ("tail_dist", "sorted by (pickup_date, payment_type, RatecodeID, passenger_count, "
                  "trip_distance, fare_amount, tip_amount, tolls_amount, total_amount)"),
    ("auto_tail", "sorted by (pickup_date, VendorID, payment_type, passenger_count, "
                  "fare_amount, tip_amount, tolls_amount, total_amount)"),
]


def download():
    os.makedirs(DATA, exist_ok=True)
    paths = []
    for m in MONTHS:
        p = os.path.join(DATA, f"yellow_tripdata_{m}.parquet")
        if not os.path.exists(p):
            url = TLC.format(m=m)
            print(f"downloading {url} ...", flush=True)
            urllib.request.urlretrieve(url, p + ".part")
            os.replace(p + ".part", p)
        paths.append(p)
    return paths


def frame_sql(paths):
    casts = []
    for c in INT_COLS:
        casts.append(f'CAST("{c}" AS INT) AS "{c}"')
    for c in TS_COLS:
        casts.append(f'CAST("{c}" AS TIMESTAMP) AS "{c}"')
    for c in STR_COLS:
        casts.append(f'CAST("{c}" AS VARCHAR) AS "{c}"')
    for c in DBL_COLS:
        casts.append(f'CAST("{c}" AS DOUBLE) AS "{c}"')
    # paths are passed forward-slashed, so `filename` comes back forward-slashed too — no
    # backslash handling needed, and [.] keeps the regex free of escape characters entirely.
    files = "[" + ", ".join(f"'{p.replace(chr(92), '/')}'" for p in paths) + "]"
    return (
        "SELECT " + ", ".join(casts) + ", "
        "CAST(tpep_pickup_datetime AS DATE) AS pickup_date, "
        "regexp_extract(filename, '([^/]+)[.]parquet$', 1) AS file "
        f"FROM read_parquet({files}, filename = 1, hive_partitioning = false)"
    )


def column_bytes(table_dir):
    """Per-column compressed bytes + row count, from the parquet footers of one Delta table dir."""
    con = duckdb.connect()
    pat = os.path.join(table_dir, "**", "*.parquet").replace("\\", "/")
    cols = dict(con.sql(
        f"SELECT path_in_schema, SUM(total_compressed_size) FROM parquet_metadata('{pat}') "
        "GROUP BY 1").fetchall())
    n = con.sql(f"SELECT SUM(num_values) FROM parquet_metadata('{pat}') "
                "WHERE path_in_schema = 'pickup_date'").fetchone()[0] or 0
    return cols, int(n)


def main():
    paths = download()
    if os.path.isdir(OUT):
        shutil.rmtree(OUT)  # CREATE OR REPLACE tombstones old files but leaves them on disk —
    os.makedirs(OUT)        # a stale run would double-count; start every probe from an empty root
    src = frame_sql(paths)

    con = duckrun.connect(OUT, read_only=False)
    try:
        con.sql("create schema if not exists probe")
    except Exception:
        con.con.execute("create schema if not exists probe")

    results = {}
    for name, clause in VARIANTS:
        t0 = time.perf_counter()
        print(f"\n=== building probe.fct_{name} {clause or '(unsorted)'} ===", flush=True)
        con.sql(f"create or replace table probe.fct_{name} {clause} as {src}")
        table_dir = os.path.join(OUT, "probe", f"fct_{name}")
        if not os.path.isdir(table_dir):  # tolerate a different local root layout
            hits = glob.glob(os.path.join(OUT, "**", f"fct_{name}"), recursive=True)
            if not hits:
                raise RuntimeError(f"cannot locate the written table dir for fct_{name} under {OUT}")
            table_dir = hits[0]
        cols, n = column_bytes(table_dir)
        results[name] = (cols, n)
        print(f"    {n:,} rows in {time.perf_counter() - t0:.1f}s", flush=True)

    # ---- report ----------------------------------------------------------------------------
    order = [v[0] for v in VARIANTS]
    print("\n\nmoney columns, bits/value (compressed bytes * 8 / rows) — lower is better")
    hdr = ["column"] + order + ["auto@CI", "vorder@CI"]
    print("  " + " | ".join(f"{h:>16}" for h in hdr))
    for c in MONEY:
        cells = [f"{results[v][0].get(c, 0) * 8 / results[v][1]:.3f}" for v in order]
        cells += [f"{ANCHORS_BITS['auto@CI'][c]:.3f}", f"{ANCHORS_BITS['vorder@CI'][c]:.3f}"]
        print("  " + " | ".join(f"{x:>16}" for x in [c] + cells))

    money_bits = {v: sum(results[v][0].get(c, 0) for c in MONEY) * 8 / results[v][1]
                  for v in order}
    for tag in ("auto@CI", "vorder@CI"):
        money_bits[tag] = sum(ANCHORS_BITS[tag].values())
    print("\n  money total (bits/row over the 8 measures):")
    for k, v in money_bits.items():
        print(f"    {k:>16}: {v:8.2f}")

    iid, floor = money_bits["none"], money_bits["vorder@CI"]
    print(f"\n  gap closure vs the V-Order reference (iid -> vorder = {iid:.2f} -> {floor:.2f}):")
    for v in order:
        if v == "none":
            continue
        closure = 100.0 * (iid - money_bits[v]) / (iid - floor) if iid > floor else 0.0
        print(f"    {v:>16}: {closure:6.1f}% of the money-column gap closed")

    print("\n  whole-table MB per variant (all columns):")
    for v in order:
        print(f"    {v:>16}: {sum(results[v][0].values()) / _MB:8.1f} MB")


if __name__ == "__main__":
    main()
