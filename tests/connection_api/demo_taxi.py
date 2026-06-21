"""A live NYC-taxi showcase for ``duckrun.connect`` — a **demo you run and watch**, not a test.

Run it and read the output: it reads *real, live* public data straight off the web (no vendored
CSVs, no download step) — the NYC TLC **Yellow Taxi** Parquet + the taxi-zone lookup CSV, both
served by ``d37ci6vzurychx.cloudfront.net`` and read directly by DuckDB over https — then lands it
into **Delta on OneLake** using nothing but SQL through ``conn.sql(...)``. Along the way it leans on
DuckDB's fancy SQL (QUALIFY, PIVOT, ROLLUP, LIST/STRUCT, ASOF JOIN) and closes with a results
scorecard. ``test_coffee.py`` already covers the Spark-style builder API on generated data; this is
the SQL-first, real-data counterpart.

The one deliberate non-SQL step is the finale: a **concurrent MERGE clash** staged through the
Spark ``DeltaTable.merge`` builder (``conn.sql`` rejects ``MERGE`` on purpose) — two writers pinned
to one snapshot, the stale one refused with ``CommitFailedError``, proving snapshot isolation.

Heavy read+compute runs in-engine over the *full* month (millions of rows, local CPU); only a
bounded **sample** plus compact marts are written over the network to OneLake, so the network is not
the bottleneck.

OneLake only (this is what "the sample lakehouse" means here) — set, with no Azure ids in code:

    WAREHOUSE_PATH   abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables
    ONELAKE_TOKEN    storage bearer token (resource https://storage.azure.com/)

Knobs (all env-overridable):
    DUCKRUN_TAXI_SCHEMA   schema to write into        (default: duckrun_taxi_demo)
    TAXI_MONTH            which month to read, YYYY-MM (default: 2024-01 — pinned, deterministic)
    TAXI_SAMPLE_ROWS      rows landed to Delta         (default: 50000)
"""
import os
import sys
import textwrap
import time
from contextlib import contextmanager

import duckrun

# The scenario prints box-drawing chars and ✅/❌; force utf-8 so a Windows cp1252 console doesn't
# crash when watching the demo.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CF = "https://d37ci6vzurychx.cloudfront.net"
ZONE_CSV = f"{CF}/misc/taxi_zone_lookup.csv"

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")
ONELAKE_TOKEN = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
TAXI_SCHEMA = os.environ.get("DUCKRUN_TAXI_SCHEMA", "duckrun_taxi_demo")
TAXI_MONTH = os.environ.get("TAXI_MONTH", "2024-01")
SAMPLE_ROWS = int(os.environ.get("TAXI_SAMPLE_ROWS", "50000"))


# ── narration + rendering helpers ────────────────────────────────────────────────────────────────
@contextmanager
def _step(n, label):
    """Narrate and time one stage of the demo. Run the script to watch it build, stage by stage."""
    print(f"\n[{n}] {label}", flush=True)
    t = time.perf_counter()
    yield lambda detail: print(f"      -> {detail}", flush=True)
    print(f"      ({time.perf_counter() - t:.2f}s)", flush=True)


def _table(rows, headers, title=None):
    """Render rows as a titled ASCII box. ``rows`` is an iterable of tuples/lists; values are
    stringified. Feed it small, already-LIMITed result sets."""
    cells = [[str(h) for h in headers]] + [[str(c) for c in r] for r in rows]
    w = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    edge = lambda l, m, r: l + m.join("─" * (w[i] + 2) for i in range(len(headers))) + r
    fmt = lambda row: "  │ " + " │ ".join(row[i].ljust(w[i]) for i in range(len(headers))) + " │"
    out = ([title] if title else []) + ["  " + edge("┌", "┬", "┐"), fmt(cells[0]), "  " + edge("├", "┼", "┤")]
    out += [fmt(row) for row in cells[1:]]
    out.append("  " + edge("└", "┴", "┘"))
    print("\n".join(out), flush=True)


def _row(name, before, after, expected, count_ok, values_ok, dt):
    return dict(name=name, before=before, after=after, expected=expected,
                count_ok=count_ok, values_ok=values_ok, dt=dt)


def _scorecard(results):
    """Final card — same visual vocabulary as the merge-spill card (✅/❌, timings), demo columns."""
    rows = [(
        r["name"], f"{r['after'] - r['before']:+,}", f"{r['before']:,}", f"{r['after']:,}",
        f"{r['expected']:,}", "✅" if r["count_ok"] else "❌", "✅" if r["values_ok"] else "❌",
        f"{r['dt']:.1f}s",
    ) for r in results]
    _table(rows, ["Operation", "Rows", "Before", "After", "Expected", "Count ✓", "Values ✓", "Time"],
           "\n  📊 Results — duckrun connection API on Delta (SQL-first)")
    ok = all(r["count_ok"] and r["values_ok"] for r in results)
    print("\n  ✅ all operations correct." if ok
          else "\n  ❌ one or more operations were wrong — see the ❌ cells above.", flush=True)


def _read_retry(conn, sql, attempts=3, base=3):
    """Run a ``conn.sql`` that hits the network (the two remote reads). CloudFront can hiccup on a
    runner; back off and retry rather than failing the whole demo. Pattern from the aemo model."""
    for attempt in range(attempts):
        try:
            return conn.sql(sql)
        except Exception as e:
            if attempt == attempts - 1:
                raise
            wait = base * (2 ** attempt)
            print(f"      WARN: remote read failed (attempt {attempt + 1}/{attempts}), "
                  f"retrying in {wait}s: {str(e).splitlines()[0][:120]}", flush=True)
            time.sleep(wait)


def _ddl(conn, stmt, retry=False):
    """Echo the exact SQL we're about to run (this is a demo — show the data engineer the statement,
    with literals substituted), then execute it through ``conn.sql()``. One statement per call:
    duckrun routes each to delta-rs individually and rejects ``;``-separated batches."""
    clean = textwrap.dedent(stmt).strip()
    print("      ┄┄ SQL ┄┄", flush=True)
    print("\n".join("      │ " + line for line in clean.splitlines()), flush=True)
    return _read_retry(conn, clean) if retry else conn.sql(clean)


def _month_bounds(ym):
    """'2024-01' -> ('2024-01-01', '2024-02-01') as half-open [start, end) timestamp bounds."""
    y, m = (int(x) for x in ym.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}-01", f"{ny:04d}-{nm:02d}-01"


# ── the scenario ─────────────────────────────────────────────────────────────────────────────────
def run_taxi_demo(conn, schema):
    """The whole SQL-first showcase against ``conn``. Importable so it runs the same against a local
    warehouse (de-risking) or live OneLake (the demo)."""
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731 — scalar helper
    start, end = _month_bounds(TAXI_MONTH)
    trips_parquet = f"{CF}/trip-data/yellow_tripdata_{TAXI_MONTH}.parquet"
    results = []

    print(f"\n=== NYC taxi live demo | schema='{schema}' | month={TAXI_MONTH} | "
          f"sample={SAMPLE_ROWS:,} | warehouse={getattr(conn, 'root_path', '?')} ===", flush=True)

    # 1 ── real dimension off the web → Delta ─────────────────────────────────────────────────────
    with _step(1, "real dimension off the web: taxi-zone lookup CSV (https) → Delta 'zones'") as say:
        t0 = time.perf_counter()
        _ddl(conn, f"""
            CREATE OR REPLACE TABLE zones AS
            SELECT "LocationID"::INT AS zone_id, "Borough" AS borough,
                   "Zone" AS zone, "service_zone" AS service_zone
            FROM read_csv_auto('{ZONE_CSV}')
        """, retry=True)
        n_zones = q("SELECT count(*) FROM zones")
        nulls = q("SELECT count(*) FROM zones WHERE borough IS NULL")
        results.append(_row("Load zones (CSV→Delta)", 0, n_zones, n_zones, True, nulls == 0, time.perf_counter() - t0))
        _table(conn.sql("SELECT zone_id, borough, zone FROM zones ORDER BY zone_id LIMIT 6").collect(),
               ["zone_id", "borough", "zone"], "  zones (first 6 of a real 265-zone dimension)")
        say(f"{n_zones} zones loaded")

    # 2 ── register the live month as a TEMP TABLE (full month, in-engine, read once) ──────────────
    with _step(2, f"register the live month: read_parquet yellow_tripdata_{TAXI_MONTH} (https, ~50MB) "
                  "→ TEMP TABLE 'trips_raw'") as say:
        _ddl(conn, f"""
            CREATE TEMP TABLE trips_raw AS
            SELECT
                t.tpep_pickup_datetime  AS pickup_ts,
                t."PULocationID"::INT    AS zone_id,
                z.borough                AS borough,
                z.zone                   AS zone,
                CASE t.payment_type WHEN 1 THEN 'Credit' WHEN 2 THEN 'Cash'
                     WHEN 3 THEN 'NoCharge' WHEN 4 THEN 'Dispute' ELSE 'Other' END AS payment,
                t.fare_amount::DOUBLE    AS fare,
                t.tip_amount::DOUBLE     AS tip,
                t.trip_distance::DOUBLE  AS distance,
                t.total_amount::DOUBLE   AS total,
                hour(t.tpep_pickup_datetime) AS pickup_hour
            FROM read_parquet('{trips_parquet}') t
            JOIN zones z ON z.zone_id = t."PULocationID"
            WHERE t.fare_amount > 0 AND t.trip_distance > 0
              AND t.tpep_pickup_datetime >= TIMESTAMP '{start}'
              AND t.tpep_pickup_datetime <  TIMESTAMP '{end}'
        """, retry=True)
        n_full = q("SELECT count(*) FROM trips_raw")
        say(f"{n_full:,} clean trips for {TAXI_MONTH} — all compute in-engine; nothing written to Delta yet")

    # 3 ── land a bounded sample → Delta 'trips' (CREATE TABLE AS … USING SAMPLE) ──────────────────
    with _step(3, f"land a bounded sample → Delta 'trips' (CREATE TABLE AS … USING SAMPLE {SAMPLE_ROWS:,})") as say:
        t0 = time.perf_counter()
        _ddl(conn, f"""
            CREATE OR REPLACE TABLE trips AS
            SELECT pickup_ts, zone_id, borough, zone, payment, fare, tip, distance, total, pickup_hour
            FROM trips_raw USING SAMPLE {SAMPLE_ROWS} ROWS
        """)
        v_landed = conn.delta_table("trips").version()   # the snapshot we'll time-travel back to
        n_trips = q("SELECT count(*) FROM trips")
        bad = q("SELECT count(*) FROM trips WHERE fare <= 0")
        results.append(_row("Land trips sample", 0, n_trips, SAMPLE_ROWS, n_trips == SAMPLE_ROWS, bad == 0,
                            time.perf_counter() - t0))
        say(f"{n_trips:,} rows landed as Delta version {v_landed}; fare>0 verified")

    # 4 ── fancy SQL: QUALIFY window-rank ─────────────────────────────────────────────────────────
    with _step(4, "fancy SQL — QUALIFY: top-3 pickup zones by revenue WITHIN each borough") as say:
        rows = conn.sql("""
            SELECT borough, zone, round(sum(total), 0)::BIGINT AS revenue, count(*) AS trips
            FROM trips GROUP BY borough, zone
            QUALIFY row_number() OVER (PARTITION BY borough ORDER BY sum(total) DESC) <= 3
            ORDER BY borough, revenue DESC
        """).collect()
        _table([(b, z, f"{rev:,}", f"{tr:,}") for b, z, rev, tr in rows],
               ["borough", "zone", "revenue $", "trips"], "  top-3 zones per borough (QUALIFY)")
        say(f"{len(rows)} rows — one window pass, no self-join")

    # 5 ── fancy SQL: PIVOT ───────────────────────────────────────────────────────────────────────
    with _step(5, "fancy SQL — PIVOT: total fare by payment across time-of-day bands") as say:
        df = conn.sql("""
            PIVOT (
                SELECT CASE WHEN pickup_hour < 6 THEN '0 night' WHEN pickup_hour < 12 THEN '1 morning'
                            WHEN pickup_hour < 18 THEN '2 afternoon' ELSE '3 evening' END AS band,
                       payment, fare
                FROM trips
            ) ON payment USING round(sum(fare), 0)::BIGINT GROUP BY band ORDER BY band
        """)
        _table(df.collect(), df.columns, "  fare $ by payment × time-of-day (PIVOT)")
        say("payment values turned into columns by PIVOT")

    # 6 ── fancy SQL: ROLLUP super-aggregates ─────────────────────────────────────────────────────
    with _step(6, "fancy SQL — ROLLUP: fare subtotals by (borough, payment) with super-aggregates") as say:
        rows = conn.sql("""
            SELECT coalesce(borough, '— ALL —') AS borough, coalesce(payment, '— ALL —') AS payment,
                   round(sum(fare), 0)::BIGINT AS fare, count(*) AS trips
            FROM trips GROUP BY ROLLUP (borough, payment)
            ORDER BY borough, payment
        """).collect()
        _table([(b, p, f"{fr:,}", f"{tr:,}") for b, p, fr, tr in rows],
               ["borough", "payment", "fare $", "trips"], "  ROLLUP subtotals ('— ALL —' = super-aggregate)")
        say(f"{len(rows)} rows incl. per-borough and grand totals")

    # 7 ── fancy SQL: LIST/STRUCT nested types; write a flattened mart ─────────────────────────────
    with _step(7, "fancy SQL — LIST/STRUCT: per-borough top-zone array + fare quantiles → mart") as say:
        print("      nested LIST<STRUCT> preview (DuckDB native types):", flush=True)
        conn.sql("""
            SELECT borough,
                   (list(struct_pack(zone := zone, rev := rev) ORDER BY rev DESC))[1:3] AS top3_zones
            FROM (SELECT borough, zone, round(sum(total), 0)::BIGINT AS rev FROM trips GROUP BY borough, zone)
            GROUP BY borough ORDER BY borough
        """).show()
        t0 = time.perf_counter()
        _ddl(conn, """
            CREATE OR REPLACE TABLE mart_zone_summary AS
            WITH ranked AS (
                SELECT borough, zone, round(sum(total), 0)::BIGINT AS rev,
                       row_number() OVER (PARTITION BY borough ORDER BY sum(total) DESC) AS rn
                FROM trips GROUP BY borough, zone
            ), pct AS (
                SELECT borough, round(quantile_cont(fare, 0.5), 2) AS fare_p50,
                       round(quantile_cont(fare, 0.9), 2) AS fare_p90,
                       round(quantile_cont(fare, 0.99), 2) AS fare_p99
                FROM trips GROUP BY borough
            )
            SELECT p.borough, r.zone AS top_zone, r.rev AS top_zone_rev,
                   p.fare_p50, p.fare_p90, p.fare_p99
            FROM pct p JOIN ranked r ON r.borough = p.borough AND r.rn = 1
            ORDER BY p.borough
        """)
        n_mart = q("SELECT count(*) FROM mart_zone_summary")
        results.append(_row("Write zone-summary mart", 0, n_mart, n_mart, True, n_mart > 0, time.perf_counter() - t0))
        _table(conn.sql("""SELECT borough, top_zone, top_zone_rev, fare_p50, fare_p90, fare_p99
                           FROM mart_zone_summary ORDER BY borough""").collect(),
               ["borough", "top_zone", "rev $", "p50", "p90", "p99"], "  mart_zone_summary (flattened for Delta)")
        say(f"{n_mart} borough rows written (nested types shown, mart flattened on purpose)")

    # 8 ── fancy SQL: ASOF JOIN to a surge rate-card ──────────────────────────────────────────────
    with _step(8, "fancy SQL — ASOF JOIN: snap each trip to the latest surge multiplier by pickup time") as say:
        _ddl(conn, """
            CREATE TEMP TABLE surge_card AS
            SELECT * FROM (VALUES (TIME '00:00', 1.00), (TIME '07:00', 1.25), (TIME '10:00', 1.00),
                                  (TIME '16:00', 1.40), (TIME '20:00', 1.15)) v(valid_from, multiplier)
        """)
        t0 = time.perf_counter()
        _ddl(conn, """
            CREATE OR REPLACE TABLE mart_surged AS
            SELECT t.zone_id, t.borough, t.payment, t.pickup_ts, t.fare AS base_fare,
                   s.multiplier, round(t.fare * s.multiplier, 2) AS surged_fare
            FROM trips t ASOF JOIN surge_card s ON t.pickup_ts::TIME >= s.valid_from
        """)
        n_surged = q("SELECT count(*) FROM mart_surged")
        n_now = q("SELECT count(*) FROM trips")
        results.append(_row("Write surge mart (ASOF)", 0, n_surged, n_now, n_surged == n_now, True,
                            time.perf_counter() - t0))
        _table(conn.sql("""SELECT borough, payment, base_fare, multiplier, surged_fare FROM mart_surged
                           ORDER BY multiplier DESC, surged_fare DESC LIMIT 6""").collect(),
               ["borough", "payment", "base", "×", "surged"], "  ASOF surge applied (sample of priced trips)")
        say(f"{n_surged:,} trips priced; each snapped to the latest surge band ≤ its pickup time")

    # 9 ── catalog ────────────────────────────────────────────────────────────────────────────────
    with _step(9, "Spark Catalog: the Delta tables this demo has landed") as say:
        tbls = sorted(conn.catalog.listTables())
        _table([(t,) for t in tbls], ["table"], f"  tables in '{schema}'")
        say(f"{len(tbls)} tables: {tbls}")

    # 10 ── raw-DML INSERT (append a late-arriving batch) ─────────────────────────────────────────
    with _step(10, "raw-DML INSERT: append a 100-row late-arriving batch to 'trips'") as say:
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM trips")
        _ddl(conn, """
            INSERT INTO trips
            SELECT pickup_ts, zone_id, borough, zone, payment, fare, tip, distance, total, pickup_hour
            FROM trips_raw USING SAMPLE 100 ROWS
        """)
        after = q("SELECT count(*) FROM trips")
        results.append(_row("INSERT append (+100)", before, after, before + 100, after == before + 100,
                            after - before == 100, time.perf_counter() - t0))
        say(f"{before:,} → {after:,} rows")

    # 11 ── SQL-only upsert: DELETE affected keys + INSERT (1 update, 1 insert) ────────────────────
    with _step(11, "SQL-only upsert on 'zone_stats': DELETE literal keys + INSERT (1 update, 1 insert)") as say:
        _ddl(conn, """
            CREATE OR REPLACE TABLE zone_stats AS
            SELECT zone_id, any_value(zone) AS zone, count(*) AS trips, round(avg(fare), 2) AS avg_fare
            FROM trips GROUP BY zone_id
        """)
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM zone_stats")
        target = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")  # busiest zone
        old_avg = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {target}")
        tgt_trips = q(f"SELECT trips FROM zone_stats WHERE zone_id = {target}")
        tgt_zone = q(f"SELECT zone FROM zone_stats WHERE zone_id = {target}").replace("'", "''")
        new_avg = round(old_avg + 5.0, 2)
        # delta-rs DELETE predicates take literals, not IN (SELECT …) — so we key the DELETE on literal
        # ids captured above, then INSERT the recomputed/new rows. That is the SQL-only upsert.
        _ddl(conn, f"DELETE FROM zone_stats WHERE zone_id = {target} OR zone_id = 999")
        _ddl(conn, f"INSERT INTO zone_stats VALUES "
                   f"({target}, '{tgt_zone}', {tgt_trips}, {new_avg}), (999, 'NEW Demo Zone', 1, 7.77)")
        after = q("SELECT count(*) FROM zone_stats")
        upd_ok = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {target}") == new_avg
        ins_ok = q("SELECT count(*) FROM zone_stats WHERE zone_id = 999") == 1
        results.append(_row("SQL upsert (1 upd, 1 ins)", before, after, before + 1, after == before + 1,
                            upd_ok and ins_ok, time.perf_counter() - t0))
        say(f"zone {target} avg {old_avg}→{new_avg} (updated), zone 999 inserted; {before:,} → {after:,} rows")

    # 12 ── raw-DML DELETE + time-travel read ─────────────────────────────────────────────────────
    with _step(12, "raw-DML DELETE + time travel: drop 'Cash' trips, then read the landed snapshot") as say:
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM trips")
        cash = q("SELECT count(*) FROM trips WHERE payment = 'Cash'")
        _ddl(conn, "DELETE FROM trips WHERE payment = 'Cash'")
        after = q("SELECT count(*) FROM trips")
        still = q("SELECT count(*) FROM trips WHERE payment = 'Cash'")
        results.append(_row("DELETE Cash trips", before, after, before - cash, after == before - cash, still == 0,
                            time.perf_counter() - t0))
        path = conn.table_path(schema, "trips").replace("\\", "/")
        try:   # the cool path: DuckDB delta_scan version travel
            at_landed = q(f"SELECT count(*) FROM delta_scan('{path}', version => {v_landed})")
            how = f"delta_scan(version => {v_landed})"
        except Exception:  # fall back to delta-rs if this DuckDB build lacks the version param
            from deltalake import DeltaTable as _DLT
            _dt = _DLT(path, storage_options=conn.storage_options)
            _dt.load_as_version(v_landed)
            at_landed = _dt.to_pyarrow_dataset().count_rows()
            how = f"delta-rs load_as_version({v_landed})"
        _table([(f"{before:,}", f"{cash:,}", f"{after:,}", f"{at_landed:,}")],
               ["before", "Cash deleted", "after (now)", "at landed ver"], "  time travel: current vs landed snapshot")
        say(f"deleted {cash:,} Cash trips ({before:,} → {after:,}); {how} still sees {at_landed:,}")

    # 13 ── concurrent MERGE clash — Spark builder API, snapshot isolation ─────────────────────────
    with _step(13, "concurrent MERGE clash (Spark DeltaTable.merge): two writers, one snapshot — "
                   "the stale one is refused") as say:
        from duckrun import DeltaTable
        from deltalake.exceptions import CommitFailedError
        t0 = time.perf_counter()
        busiest = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")
        old_avg = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        before = q("SELECT count(*) FROM zone_stats")
        cond = "target.zone_id = source.zone_id"
        srcA = conn.sql(f"SELECT zone_id, zone, trips, 100.00::DOUBLE AS avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        srcB = conn.sql(f"SELECT zone_id, zone, trips, 200.00::DOUBLE AS avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        # Spark API on purpose — conn.sql rejects MERGE. .merge() snapshot-pins at BUILD time, so both
        # writers capture the SAME version before either commits: exactly two concurrent writers.
        writer_A = DeltaTable.forName(conn, "zone_stats").merge(srcA, cond).whenMatchedUpdateAll().whenNotMatchedInsertAll()
        writer_B = DeltaTable.forName(conn, "zone_stats").merge(srcB, cond).whenMatchedUpdateAll().whenNotMatchedInsertAll()
        say(f"both writers built against zone {busiest} (avg {old_avg}) at the same snapshot")
        writer_A.execute()  # writer A wins the race, advancing the table version
        try:
            writer_B.execute()  # writer B is now one version behind → OCC must refuse it
            clashed, errline = False, ""
        except CommitFailedError as e:
            clashed, errline = True, str(e).splitlines()[0]
        final_avg = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        after = q("SELECT count(*) FROM zone_stats")
        _table([("A — fresh snapshot", "100.0", "committed ✅"),
                ("B — stale snapshot", "200.0", "refused ✅" if clashed else "committed ❌ (UNSAFE)")],
               ["writer", "intended avg", "outcome"], "  two concurrent MERGEs against one snapshot")
        results.append(_row("Concurrent MERGE clash", before, after, before, after == before,
                            clashed and final_avg == 100.0, time.perf_counter() - t0))
        say(f"snapshot isolation held: zone {busiest} avg = {final_avg} (writer A won); "
            f"writer B refused — {errline.split(':')[-1].strip() or 'CommitFailedError'}")

    _scorecard(results)
    print(f"\n=== demo complete: {schema} ===", flush=True)


def main():
    if not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://") and ONELAKE_TOKEN):
        print(
            "\nThis demo writes to the OneLake sample lakehouse. Set:\n"
            "  WAREHOUSE_PATH=abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables\n"
            "  ONELAKE_TOKEN=<storage bearer token for resource https://storage.azure.com/>\n"
            "then re-run:  python tests/connection_api/demo_taxi.py\n",
            flush=True,
        )
        return
    conn = duckrun.connect(WAREHOUSE_PATH, storage_options={"bearer_token": ONELAKE_TOKEN}, schema=TAXI_SCHEMA)
    run_taxi_demo(conn, TAXI_SCHEMA)


if __name__ == "__main__":
    main()
