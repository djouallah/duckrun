"""A live NYC-taxi showcase for ``duckrun.connect`` — a **demo you run and watch**, not a test.

Run it and read the output: it reads *real, live* public data straight off the web (no vendored
CSVs, no download step) — the NYC TLC **Yellow Taxi** Parquet + the taxi-zone lookup CSV, both
served by ``d37ci6vzurychx.cloudfront.net`` and read directly by DuckDB over https — then lands it
into **Delta on OneLake** using nothing but SQL through ``conn.sql(...)``. Along the way it leans on
DuckDB's fancy SQL (QUALIFY, PIVOT, ROLLUP, LIST/STRUCT, ASOF JOIN) and closes with a results
scorecard. ``test_coffee.py`` already covers the DataFrame-style builder API on generated data; this is
the SQL-first, real-data counterpart.

The demo is split in four parts. **Part 1** is SQL-first: every step is raw SQL through ``conn.sql`` — including a real ``MERGE`` upsert.
**Part 2** switches to the DataFrame API on the same connection — the ``DataFrameWriter`` writing Delta by
**path** (``.mode("overwrite"/"append").save(path)``, read back with ``conn.read.format("delta").load(path)``), closing
with a **concurrent MERGE clash** staged through the DataFrame ``DeltaTable.merge`` builder (the builder
snapshot-pins at BUILD time, so two writers can be pinned to one snapshot before either commits — a raw
``conn.sql`` MERGE pins-and-commits in one call) — the stale writer refused with ``CommitFailedError``,
proving snapshot isolation. **Part 3** is **multi-catalog**: ``conn.attach`` brings a
*second* lakehouse in as a named catalog (here a local scratch dir alongside the warehouse — they can be on
different storage), then a single ``conn.sql`` JOINs a table in the primary catalog with one in the attached
catalog, addressing them as ``catalog.schema.table``. **Part 4** shows the **full MERGE surface** — the complete
delta-rs clause set (matched update/delete, not-matched insert, not-matched-by-source delete, CASE/arbitrary
expressions, many clauses in order) applied in a single MERGE driven by a **fresh recompute over the fact**
(a subquery — the real ETL shape, no hand-typed VALUES), BOTH through ``conn.sql`` and through the
``DeltaTable.merge`` builder.

Heavy read+compute runs in-engine over the *full* month (millions of rows, local CPU); only a
bounded **sample** plus compact marts are written over the network to OneLake, so the network is not
the bottleneck.

Besides the console narration, the run can emit a **standalone HTML report** — every statement and
its actual result, top to bottom — when ``DUCKRUN_TAXI_PAGE`` is set (CI publishes it as taxi.html).

OneLake only (this is what "the sample lakehouse" means here) — set, with no Azure ids in code:

    WAREHOUSE_PATH   abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables
    ONELAKE_TOKEN    storage bearer token (resource https://storage.azure.com/)

Knobs (all env-overridable):
    DUCKRUN_TAXI_SCHEMA   schema to write into        (default: duckrun_taxi_demo)
    TAXI_MONTH            which month to read, YYYY-MM (default: 2024-01 — pinned, deterministic)
    TAXI_SAMPLE_ROWS      rows landed to Delta         (default: 50000)
    DUCKRUN_TAXI_PAGE     write the HTML report to this path (optional)
"""
import html
import os
import sys
import tempfile
import textwrap
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckrun
from duckrun import DeltaTable

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
SAMPLE_ROWS = int(os.environ.get("TAXI_SAMPLE_ROWS", "2000000"))


# ── HTML report accumulator ──────────────────────────────────────────────────────────────────────
# Every helper below narrates to the console AND appends to _DOC, so the same run that prints the
# scorecard also produces a standalone "SQL + results, top to bottom" page (written if DUCKRUN_TAXI_PAGE).
_DOC = []


def _emit(fragment):
    _DOC.append(fragment)


# ── narration + rendering helpers ────────────────────────────────────────────────────────────────
def _part(label, blurb):
    """Open a top-level part of the demo (console banner + an <h2> divider in the HTML report).
    Used to split the SQL-first steps from the DataFrame-API steps."""
    print(f"\n══ {label} ══  {blurb}", flush=True)
    _emit(f'\n<h2 class="part">{html.escape(label)}</h2>\n  <p class="lede">{html.escape(blurb)}</p>')


@contextmanager
def _step(n, label):
    """Narrate and time one stage of the demo (console), and open a <section> in the HTML report."""
    print(f"\n[{n}] {label}", flush=True)
    _emit(f'\n<section>\n  <h3><span class="n">{n}</span> {html.escape(label)}</h3>')
    t = time.perf_counter()

    def say(detail):
        print(f"      -> {detail}", flush=True)
        _emit(f'  <p class="note">{html.escape(detail)}</p>')

    yield say
    _emit("</section>")
    print(f"      ({time.perf_counter() - t:.2f}s)", flush=True)


def _sql(conn, stmt, retry=False):
    """Echo the exact SQL (console + HTML report), then run it through ``conn.sql()`` and return the
    relation. One statement per call: duckrun routes each to delta-rs individually and rejects
    ``;``-separated batches."""
    clean = textwrap.dedent(stmt).strip()
    print("      ┄┄ SQL ┄┄", flush=True)
    print("\n".join("      │ " + line for line in clean.splitlines()), flush=True)
    _emit(f'  <pre class="sql">{html.escape(clean)}</pre>')
    return _read_retry(conn, clean) if retry else conn.sql(clean)


def _table(rows, headers, title=None, echo=True):
    """Render rows as a titled ASCII box (console, when ``echo``) AND an HTML table (report). ``rows``
    is an iterable of tuples/lists; values are stringified. Feed it small, already-LIMITed sets."""
    rows = [list(r) for r in rows]
    if echo:
        cells = [[str(h) for h in headers]] + [[str(c) for c in r] for r in rows]
        w = [max(len(row[i]) for row in cells) for i in range(len(headers))]
        edge = lambda l, m, r: l + m.join("─" * (w[i] + 2) for i in range(len(headers))) + r
        fmt = lambda row: "  │ " + " │ ".join(row[i].ljust(w[i]) for i in range(len(headers))) + " │"
        out = ([title] if title else []) + ["  " + edge("┌", "┬", "┐"), fmt(cells[0]), "  " + edge("├", "┼", "┤")]
        out += [fmt(row) for row in cells[1:]]
        out.append("  " + edge("└", "┴", "┘"))
        print("\n".join(out), flush=True)
    th = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    cap = f'  <p class="tcap">{html.escape(title.strip())}</p>\n' if title else ""
    _emit(f'{cap}  <table class="data"><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>')


def _row(name, before, after, expected, count_ok, values_ok, dt):
    return dict(name=name, before=before, after=after, expected=expected,
                count_ok=count_ok, values_ok=values_ok, dt=dt)


def _scorecard(results):
    """Final card to the console — correctness + timing for every write/mutation op."""
    rows = [(
        r["name"], f"{r['after'] - r['before']:+,}", f"{r['before']:,}", f"{r['after']:,}",
        f"{r['expected']:,}", "✅" if r["count_ok"] else "❌", "✅" if r["values_ok"] else "❌",
        f"{r['dt']:.1f}s",
    ) for r in results]
    _table(rows, ["Operation", "Rows", "Before", "After", "Expected", "Count ✓", "Values ✓", "Time"],
           "\n  📊 Results — duckrun connection API on Delta (SQL-first)", echo=True)
    # _table already appended an HTML copy; drop it so the report uses the styled scorecard instead.
    _DOC.pop()
    ok = all(r["count_ok"] and r["values_ok"] for r in results)
    print("\n  ✅ all operations correct." if ok
          else "\n  ❌ one or more operations were wrong — see the ❌ cells above.", flush=True)


def _scorecard_html(results):
    """The scorecard as a styled HTML table for the report page (a REAL run's numbers)."""
    head = ("<th>Operation</th><th>Rows</th><th>Before</th><th>After</th>"
            "<th>Expected</th><th>Count&nbsp;✓</th><th>Values&nbsp;✓</th><th>Time</th>")
    body = "\n".join(
        "    <tr><td>{n}</td><td>{r:+,}</td><td>{b:,}</td><td>{a:,}</td><td>{e:,}</td>"
        "<td>{c}</td><td>{v}</td><td>{t:.1f}s</td></tr>".format(
            n=html.escape(x["name"]), r=x["after"] - x["before"], b=x["before"], a=x["after"],
            e=x["expected"], c="✅" if x["count_ok"] else "❌", v="✅" if x["values_ok"] else "❌", t=x["dt"])
        for x in results)
    return (f'<table class="data card">\n  <thead><tr>{head}</tr></thead>\n  <tbody>\n'
            f'{body}\n  </tbody>\n</table>')


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


def _trim_loc(path):
    """Trim a table location for display: keep the scheme but elide the GUID-heavy middle, e.g.
    'abfss://<ws>@onelake…/<lh>/Tables/duckrun_taxi_demo/trips' -> 'abfss://…/Tables/duckrun_taxi_demo/trips'.
    Local paths collapse to their last few segments."""
    p = str(path).replace("\\", "/")
    if "://" in p:
        scheme = p.split("://", 1)[0]
        if "/Tables/" in p:
            return f"{scheme}://…/Tables/{p.split('/Tables/', 1)[1]}"
        return f"{scheme}://…/" + "/".join(p.split("/")[-2:])
    segs = [s for s in p.split("/") if s]
    return "…/" + "/".join(segs[-3:])


def _month_bounds(ym):
    """'2024-01' -> ('2024-01-01', '2024-02-01') as half-open [start, end) timestamp bounds."""
    y, m = (int(x) for x in ym.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}-01", f"{ny:04d}-{nm:02d}-01"


_PAGE_CSS = """
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         max-width: 900px; margin: 0 auto; padding: 2.5rem 1.25rem; line-height: 1.5; }
  a.back { font-size: 0.9rem; text-decoration: none; color: #888; }
  h1 { margin: 0.4rem 0 0.2rem; }
  h1 .tag { font-size: 0.7rem; font-weight: 700; color: #fff; background: #c0392b;
            padding: 0.12rem 0.5rem; border-radius: 6px; vertical-align: middle; margin-left: 0.4rem; }
  h2.part { margin: 2rem 0 0.2rem; padding-bottom: 0.3rem; border-bottom: 2px solid #c0392b88;
            font-size: 1.25rem; }
  p.lede { color: #777; margin-top: 0; }
  section { border-top: 1px solid #8883; padding: 0.6rem 0 0.4rem; }
  h3 { margin: 0.8rem 0 0.5rem; font-size: 1.05rem; }
  h3 .n { display: inline-block; min-width: 1.4rem; color: #c0392b; font-variant-numeric: tabular-nums; }
  pre.sql, pre.py { background: #8881; border-left: 3px solid #c0392b88; border-radius: 4px;
        padding: 0.6rem 0.8rem; overflow-x: auto; font-size: 0.82rem; line-height: 1.4; margin: 0.5rem 0; }
  pre.py { border-left-color: #2d7d4688; }
  p.note { color: #555; margin: 0.4rem 0; font-size: 0.92rem; }
  p.note::before { content: "→ "; color: #888; }
  p.tcap { color: #888; font-size: 0.82rem; margin: 0.6rem 0 0.2rem; }
  table.data { border-collapse: collapse; font-size: 0.85rem; margin: 0.2rem 0 0.8rem; }
  table.data th, table.data td { border: 1px solid #8884; padding: 0.3rem 0.6rem; text-align: left; word-break: break-word; }
  table.data th { background: #8882; font-weight: 600; }
  table.card { width: 100%; }
  table.card td:not(:first-child), table.card th:not(:first-child) { text-align: right; }
  table.card td:first-child { font-weight: 600; }
  footer { margin-top: 2rem; color: #888; font-size: 0.85rem; }
  code { background: #8882; padding: 0.1rem 0.35rem; border-radius: 4px; }
"""


def _page_html(scorecard, body, caption, ok):
    verdict = "✅ all operations correct" if ok else "❌ some operations failed"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>duckrun — the connection API on Delta (SQL &amp; DataFrame)</title>
  <style>{_PAGE_CSS}</style>
</head>
<body>
  <a class="back" href="index.html">← duckrun docs</a>
  <h1>the connection API — SQL &amp; DataFrame<span class="tag">not a dbt project</span></h1>
  <p class="lede">{html.escape(caption)}. Part 1 runs raw SQL through <code>conn.sql(...)</code>; Part 2 uses
  the <code>DataFrameWriter</code>/<code>DeltaTable</code> API to write by path; Part 3 attaches a second
  lakehouse with <code>conn.attach</code> and JOINs across catalogs; Part 4 exercises the full delta-rs
  <code>MERGE</code> clause set in both SQL and DataFrame form — all against live NYC&nbsp;TLC data,
  landing real Delta, top to bottom.</p>
  <h2 style="margin-bottom:0.2rem">Run scorecard — <span style="font-weight:400">{verdict}</span></h2>
  {scorecard}
  {body}
  <footer>Generated by
    <a href="https://github.com/djouallah/duckrun/blob/main/integration_tests/taxi/demo_taxi.py">demo_taxi.py</a>
    — re-published from a live OneLake run by the integration-tests workflow.</footer>
</body>
</html>
"""


# ── the scenario ─────────────────────────────────────────────────────────────────────────────────
def run_taxi_demo(conn, schema):
    """The whole SQL-first showcase against ``conn``. Importable so it runs the same against a local
    warehouse (de-risking) or live OneLake (the demo)."""
    _DOC.clear()
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731 — scalar helper
    start, end = _month_bounds(TAXI_MONTH)
    trips_parquet = f"{CF}/trip-data/yellow_tripdata_{TAXI_MONTH}.parquet"
    results = []

    print(f"\n=== NYC taxi live demo | schema='{schema}' | month={TAXI_MONTH} | "
          f"sample={SAMPLE_ROWS:,} | warehouse={getattr(conn, 'root_path', '?')} ===", flush=True)

    _part("Part 1 · pure SQL through conn.sql",
          "Every statement in this part is raw SQL handed to conn.sql(...) — reads, CREATE TABLE AS, "
          "INSERT, DELETE, time travel — each landing real Delta via delta-rs.")

    # 1 ── real dimension off the web → Delta ─────────────────────────────────────────────────────
    with _step(1, "real dimension off the web: taxi-zone lookup CSV (https) → Delta 'zones'") as say:
        t0 = time.perf_counter()
        _sql(conn, f"""
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
        _sql(conn, f"""
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
        _sql(conn, f"""
            CREATE OR REPLACE TABLE trips AS
            SELECT pickup_ts, zone_id, borough, zone, payment, fare, tip, distance, total, pickup_hour
            FROM trips_raw USING SAMPLE {SAMPLE_ROWS} ROWS
        """)
        v_landed = DeltaTable.forName(conn, "trips").version()   # the snapshot we'll time-travel back to
        n_trips = q("SELECT count(*) FROM trips")
        bad = q("SELECT count(*) FROM trips WHERE fare <= 0")
        results.append(_row("Land trips sample", 0, n_trips, SAMPLE_ROWS, n_trips == SAMPLE_ROWS, bad == 0,
                            time.perf_counter() - t0))
        say(f"{n_trips:,} rows landed as Delta version {v_landed}; fare>0 verified")

    # 4 ── a simple transform written back as a Delta table (one mart, plain SQL) ──────────────────
    with _step(4, "transform → Delta: revenue by borough & zone → table 'mart_zone_revenue'") as say:
        t0 = time.perf_counter()
        _sql(conn, """
            CREATE OR REPLACE TABLE mart_zone_revenue AS
            SELECT borough, zone, count(*) AS trips, round(sum(total), 2) AS revenue
            FROM trips GROUP BY borough, zone
        """)
        n_mart = q("SELECT count(*) FROM mart_zone_revenue")
        results.append(_row("Write revenue mart", 0, n_mart, n_mart, True, n_mart > 0, time.perf_counter() - t0))
        _table([(b, z, f"{tr:,}", f"{rev:,.0f}") for b, z, tr, rev in conn.sql(
                    """SELECT borough, zone, trips, revenue FROM mart_zone_revenue
                       ORDER BY revenue DESC LIMIT 8""").collect()],
               ["borough", "zone", "trips", "revenue $"], "  top zones by revenue (mart_zone_revenue)")
        say(f"{n_mart} (borough, zone) rows written to Delta")

    # 5 ── catalog WITH each table's real Delta location ───────────────────────────────────────────
    with _step(5, "Catalog: the Delta tables this demo landed — with their real storage locations") as say:
        tbls = sorted(conn.catalog.listTables())
        _table([(t, _trim_loc(conn._table_path(schema, t))) for t in tbls], ["table", "location (real Delta dir)"],
               f"  tables in '{schema}'")
        say(f"{len(tbls)} Delta tables — each a real directory with a _delta_log at the location shown")

    # 6 ── raw-DML INSERT (append a late-arriving batch) ──────────────────────────────────────────
    with _step(6, "raw-DML INSERT: append a 100-row late-arriving batch to 'trips'") as say:
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM trips")
        _sql(conn, """
            INSERT INTO trips
            SELECT pickup_ts, zone_id, borough, zone, payment, fare, tip, distance, total, pickup_hour
            FROM trips_raw USING SAMPLE 100 ROWS
        """)
        after = q("SELECT count(*) FROM trips")
        results.append(_row("INSERT append (+100)", before, after, before + 100, after == before + 100,
                            after - before == 100, time.perf_counter() - t0))
        say(f"{before:,} → {after:,} rows")

    # 7 ── SQL upsert via a real MERGE (1 update, 1 insert), one statement ─────────────────────────
    with _step(7, "SQL upsert on 'zone_stats': a real MERGE through conn.sql (1 update, 1 insert)") as say:
        _sql(conn, """
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
        # A real SQL MERGE: conn.sql routes it to delta-rs (same engine + snapshot pin as the
        # DeltaTable.merge builder). Write it like standard SQL — pick your own aliases (here z/s);
        # duckrun normalizes them internally. One statement does the upsert — the busiest zone is
        # updated, zone 999 is inserted.
        _sql(conn, f"""
            MERGE INTO zone_stats z USING (VALUES
                ({target}, '{tgt_zone}', {tgt_trips}, {new_avg}),
                (999, 'NEW Demo Zone', 1, 7.77)
            ) AS s(zone_id, zone, trips, avg_fare)
            ON z.zone_id = s.zone_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        after = q("SELECT count(*) FROM zone_stats")
        upd_ok = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {target}") == new_avg
        ins_ok = q("SELECT count(*) FROM zone_stats WHERE zone_id = 999") == 1
        results.append(_row("SQL MERGE (1 upd, 1 ins)", before, after, before + 1, after == before + 1,
                            upd_ok and ins_ok, time.perf_counter() - t0))
        say(f"zone {target} avg {old_avg}→{new_avg} (updated), zone 999 inserted; {before:,} → {after:,} rows")

    # 8 ── SQL upsert via MERGE with a SUBQUERY source (the real-world recompute-and-upsert) ─────────
    with _step(8, "SQL upsert on 'zone_stats': MERGE with a SUBQUERY source — recompute fresh stats "
                  "from 'trips' and upsert (the real-world pattern, not hand-typed VALUES)") as say:
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM zone_stats")
        # The source is a full aggregation over the live 'trips' table — not hand-typed VALUES. Every
        # Manhattan zone's stats are recomputed and upserted in one statement: matched zones are
        # refreshed (this also heals the busy zone step 7 deliberately skewed by +5), any zone not yet
        # present is inserted. This is how MERGE is actually used — sync a target from a derived query.
        _sql(conn, """
            MERGE INTO zone_stats zs
            USING (
                SELECT zone_id,
                       any_value(zone)      AS zone,
                       count(*)             AS trips,
                       round(avg(fare), 2)  AS avg_fare
                FROM trips
                WHERE borough = 'Manhattan'
                GROUP BY zone_id
            ) fresh
            ON zs.zone_id = fresh.zone_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        after = q("SELECT count(*) FROM zone_stats")
        # every recomputed Manhattan zone must now match the target exactly — no stale avg left behind.
        drift = q("""
            SELECT count(*) FROM zone_stats t
            JOIN (SELECT zone_id, round(avg(fare), 2) AS avg_fare FROM trips
                  WHERE borough = 'Manhattan' GROUP BY zone_id) s USING (zone_id)
            WHERE t.avg_fare <> s.avg_fare
        """)
        results.append(_row("SQL MERGE (subquery source)", before, after, before, after == before, drift == 0,
                            time.perf_counter() - t0))
        say(f"recomputed every Manhattan zone from 'trips' and upserted in one MERGE; "
            f"{before:,} → {after:,} rows, 0 drift (all matched → refreshed)")

    # 9 ── raw-DML DELETE + time-travel read ──────────────────────────────────────────────────────
    with _step(9, "raw-DML DELETE + time travel: drop 'Cash' trips, then read the landed snapshot") as say:
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM trips")
        cash = q("SELECT count(*) FROM trips WHERE payment = 'Cash'")
        _sql(conn, "DELETE FROM trips WHERE payment = 'Cash'")
        after = q("SELECT count(*) FROM trips")
        still = q("SELECT count(*) FROM trips WHERE payment = 'Cash'")
        results.append(_row("DELETE Cash trips", before, after, before - cash, after == before - cash, still == 0,
                            time.perf_counter() - t0))
        path = conn._table_path(schema, "trips").replace("\\", "/")
        _emit('  <pre class="py"># canonical Spark time travel — read the snapshot we landed at, by version:\n'
              f'conn.read.format("delta").option("versionAsOf", {v_landed}).load(path).count()</pre>')
        at_landed = conn.read.format("delta").option("versionAsOf", v_landed).load(path).count()
        how = f'read.option("versionAsOf", {v_landed})'
        _table([(f"{before:,}", f"{cash:,}", f"{after:,}", f"{at_landed:,}")],
               ["before", "Cash deleted", "after (now)", "at landed ver"], "  time travel: current vs landed snapshot")
        say(f"deleted {cash:,} Cash trips ({before:,} → {after:,}); {how} still sees {at_landed:,}")

    _part("Part 2 · the DataFrame & Delta API — write & mutate without SQL",
          "Same connection, no SQL DML: the DataFrameWriter, the Spark write verbs "
          "(save / insertInto / replaceWhere), and the DeltaTable builder (update / delete / merge). "
          "Writes are addressed by storage PATH (local / s3:// / gs:// / abfss://) or by catalog NAME.")

    # 10 ── DataFrame write by PATH: df.write.mode('overwrite').save(path) ─────────────────────────────
    with _step(10, "DataFrame write by path: df.write.mode('overwrite').save(path) → a bare Delta dir "
                  "(no catalog name)") as say:
        t0 = time.perf_counter()
        by_path = conn._table_path(schema, "borough_hourly")     # any store works: local / s3:// / abfss://
        _emit('  <pre class="py"># DataFrameWriter — addressed by a storage PATH, not a schema.table name:\n'
              'df = conn.sql("SELECT borough, pickup_hour, count(*) AS trips "\n'
              '              "FROM trips GROUP BY borough, pickup_hour")\n'
              'df.write.format("delta").mode("overwrite").save(path)   # land Delta at the path\n'
              '# a by-path read registers nothing — name it so conn.sql can query it BY NAME:\n'
              'conn.read.format("delta").load(path).createOrReplaceTempView("borough_hourly_v")\n'
              'conn.sql("SELECT * FROM borough_hourly_v ORDER BY trips DESC LIMIT 5")</pre>')
        df = conn.sql("SELECT borough, pickup_hour, count(*) AS trips "
                      "FROM trips GROUP BY borough, pickup_hour")
        df.write.format("delta").mode("overwrite").save(by_path)
        # createOrReplaceTempView: the by-path read is a DataFrame that registers nothing in the
        # catalog, so register it as a session view to make it queryable by name (the path-read
        # counterpart to saveAsTable). The view is native/ephemeral — not Delta, not in conn.catalog.
        n_path = conn.read.format("delta").load(by_path).createOrReplaceTempView("borough_hourly_v").count()
        results.append(_row("DataFrame save by path (overwrite)", 0, n_path, n_path, n_path > 0, n_path > 0,
                            time.perf_counter() - t0))
        top = conn.sql("SELECT borough, pickup_hour, trips FROM borough_hourly_v "
                       "ORDER BY trips DESC LIMIT 5").collect()
        _table([(b, h, f"{tr:,}") for b, h, tr in top], ["borough", "pickup_hour", "trips"],
               "  read back by path — conn.read.format('delta').load(path).createOrReplaceTempView('borough_hourly_v')")
        say(f"{n_path} (borough, hour) rows written to a bare Delta path, registered as a temp view, "
            "and queried by name")

    # 11 ── DataFrame append by PATH: df.write.mode('append').save(path) — grow the same dir ──────────
    with _step(11, "DataFrame append by path: df.write.mode('append').save(path) — grow the same Delta dir") as say:
        t0 = time.perf_counter()
        before = conn.read.format("delta").load(by_path).count()
        _emit('  <pre class="py">extra = conn.sql("SELECT \'EWR\' AS borough, 25 AS pickup_hour, 1 AS trips")\n'
              'extra.write.format("delta").mode("append").save(path)   # append to the SAME path</pre>')
        extra = conn.sql("SELECT 'EWR' AS borough, 25 AS pickup_hour, 1 AS trips")
        extra.write.format("delta").mode("append").save(by_path)
        after = conn.read.format("delta").load(by_path).count()
        results.append(_row("DataFrame append by path", before, after, before + 1, after == before + 1,
                            after - before == 1, time.perf_counter() - t0))
        say(f"{before:,} → {after:,} rows (appended by path)")

    # 12 ── DeltaTable mutate API: update(condition, set) + delete(predicate) ──────────────────────────
    with _step(12, "DeltaTable mutate API: DeltaTable.forName(conn, 'zone_stats').update(...) + .delete(...)") as say:
        t0 = time.perf_counter()
        z = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")
        before = q("SELECT count(*) FROM zone_stats")
        _emit('  <pre class="py">dt = DeltaTable.forName(conn, "zone_stats")\n'
              f'dt.update(condition="zone_id = {z}", set={{"avg_fare": "42.0"}})   # set one row\n'
              'dt.delete("zone_id = 999")                                  # drop the demo zone</pre>')
        dt = DeltaTable.forName(conn, "zone_stats")
        dt.update(condition=f"zone_id = {z}", set={"avg_fare": "42.0"})
        dt.delete("zone_id = 999")
        after = q("SELECT count(*) FROM zone_stats")
        upd_ok = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {z}") == 42.0
        del_ok = q("SELECT count(*) FROM zone_stats WHERE zone_id = 999") == 0
        results.append(_row("DeltaTable update + delete", before, after, before - 1, after == before - 1,
                            upd_ok and del_ok, time.perf_counter() - t0))
        say(f"zone {z} avg → 42.0 (update), zone 999 removed (delete); {before:,} → {after:,} rows")

    # 13 ── df.write.insertInto(name): append into an EXISTING table by catalog name ────────────────────
    with _step(13, "df.write.insertInto('zone_stats'): the Spark insertInto verb — append by name") as say:
        t0 = time.perf_counter()
        dup_id = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")
        before = q("SELECT count(*) FROM zone_stats")
        # source rows are SELECT *'d straight from the table, so the Delta schema matches exactly.
        _emit('  <pre class="py"># Spark insertInto — append rows into an existing table by name:\n'
              f'dup = conn.sql("SELECT * FROM zone_stats WHERE zone_id = {dup_id}")\n'
              'dup.write.insertInto("zone_stats")   # errors if the table does not exist</pre>')
        dup = conn.sql(f"SELECT * FROM zone_stats WHERE zone_id = {dup_id}")
        dup.write.insertInto("zone_stats")
        after = q("SELECT count(*) FROM zone_stats")
        ins_ok = q(f"SELECT count(*) FROM zone_stats WHERE zone_id = {dup_id}") == 2
        results.append(_row("DataFrame insertInto (+1)", before, after, before + 1, after == before + 1,
                            ins_ok, time.perf_counter() - t0))
        say(f"appended a copy of zone {dup_id} by name; {before:,} → {after:,} rows")

    # 14 ── df.write.option('replaceWhere', …): atomic slice overwrite by name ─────────────────────────
    with _step(14, "df.write.option('replaceWhere', pred).mode('overwrite').saveAsTable('zone_stats'): "
                   "atomic slice swap") as say:
        t0 = time.perf_counter()
        z = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")
        before = q("SELECT count(*) FROM zone_stats")
        matched = q(f"SELECT count(*) FROM zone_stats WHERE zone_id = {z}")  # may be >1 after step 12
        # source = one row, same schema (one column overridden), atomically replacing the whole slice.
        _emit('  <pre class="py">new = conn.sql(f"SELECT * REPLACE (55.5::DOUBLE AS avg_fare) "\n'
              '               f"FROM zone_stats WHERE zone_id = {z} LIMIT 1")\n'
              'new.write.option("replaceWhere", f"zone_id = {z}").mode("overwrite").saveAsTable("zone_stats")</pre>')
        new = conn.sql(f"SELECT * REPLACE (55.5::DOUBLE AS avg_fare) FROM zone_stats WHERE zone_id = {z} LIMIT 1")
        new.write.option("replaceWhere", f"zone_id = {z}").mode("overwrite").saveAsTable("zone_stats")
        after = q("SELECT count(*) FROM zone_stats")
        expected = before - matched + 1   # the matched slice collapses to the single source row
        rw_ok = (q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {z}") == 55.5
                 and q(f"SELECT count(*) FROM zone_stats WHERE zone_id = {z}") == 1)
        results.append(_row("DataFrame replaceWhere (slice swap)", before, after, expected, after == expected,
                            rw_ok, time.perf_counter() - t0))
        say(f"zone {z} slice ({matched} row(s)) replaced atomically by 1 row (avg → 55.5); "
            f"{before:,} → {after:,} rows")

    # 15 ── concurrent MERGE clash — DataFrame builder API, snapshot isolation ────────────────────────
    with _step(15, "concurrent MERGE clash (DataFrame DeltaTable.merge): two writers, one snapshot — "
                   "the stale one is refused") as say:
        from deltalake.exceptions import CommitFailedError
        t0 = time.perf_counter()
        busiest = q("SELECT zone_id FROM zone_stats ORDER BY trips DESC LIMIT 1")
        old_avg = q(f"SELECT avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        before = q("SELECT count(*) FROM zone_stats")
        cond = "target.zone_id = source.zone_id"
        # DataFrame builder on purpose (step 7 already showed a SQL MERGE): .merge() snapshot-pins at
        # BUILD time, so we can stage TWO writers against the SAME version before either commits — a raw
        # conn.sql MERGE pins-and-commits in one call, so it can't stage the clash. Exactly two concurrent writers.
        _emit('  <pre class="py"># DataFrame builder API — staged so both writers pin the SAME version before committing:\n'
              f'writer_A = DeltaTable.forName(conn, "zone_stats").merge(srcA, "{cond}") \\\n'
              '    .whenMatchedUpdateAll().whenNotMatchedInsertAll()   # srcA sets avg → 100.0\n'
              f'writer_B = DeltaTable.forName(conn, "zone_stats").merge(srcB, "{cond}") \\\n'
              '    .whenMatchedUpdateAll().whenNotMatchedInsertAll()   # srcB sets avg → 200.0\n'
              'writer_A.execute()   # wins, advances the table version\n'
              'writer_B.execute()   # stale snapshot → CommitFailedError</pre>')
        srcA = conn.sql(f"SELECT zone_id, zone, trips, 100.00::DOUBLE AS avg_fare FROM zone_stats WHERE zone_id = {busiest}")
        srcB = conn.sql(f"SELECT zone_id, zone, trips, 200.00::DOUBLE AS avg_fare FROM zone_stats WHERE zone_id = {busiest}")
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

    _part("Part 3 · multi-catalog — attach a second lakehouse and query across both",
          "conn.attach(path, name=…) brings a second lakehouse in as a named catalog, addressed as "
          "catalog.schema.table. Catalogs can sit on DIFFERENT storage — here the primary warehouse "
          "plus a local scratch lakehouse — and one SQL statement can JOIN across them.")

    # 16 ── attach a second lakehouse as a named catalog ────────────────────────────────────────────
    with _step(16, "conn.attach(path, name='scratch'): bring a second lakehouse in as a catalog") as say:
        scratch_path = tempfile.mkdtemp(prefix="duckrun_scratch_")
        primary = conn.catalog.currentCatalog()
        _emit('  <pre class="py"># a second lakehouse root becomes a named catalog (could be another\n'
              '# OneLake lakehouse, an S3 bucket, … — here a local scratch dir):\n'
              'conn.attach(scratch_path, name="scratch")\n'
              'conn.catalog.listCatalogs()      # [primary, "scratch"]\n'
              'conn.catalog.currentCatalog()    # still the primary — attach does NOT switch</pre>')
        conn.attach(scratch_path, name="scratch")
        _table([(c, "primary (current)" if c == primary else "attached") for c in conn.catalog.listCatalogs()],
               ["catalog", "role"], "  attached catalogs (each is its own lakehouse root)")
        say(f"attached '{_trim_loc(scratch_path)}' as catalog 'scratch'; current catalog is still '{primary}'")

    # 17 ── cross-catalog write: saveAsTable into the SCRATCH catalog by 3-part name ─────────────────
    with _step(17, "cross-catalog write: df.write.saveAsTable('scratch.dbo.borough_targets') — lands "
                   "under the SCRATCH root, not the primary") as say:
        t0 = time.perf_counter()
        _emit('  <pre class="py"># a 3-part name routes the write to THAT catalog\'s storage root:\n'
              'targets = conn.sql("SELECT borough, round(sum(revenue) * 1.1, 0) AS target_rev "\n'
              '                   "FROM mart_zone_revenue GROUP BY borough")\n'
              'targets.write.mode("overwrite").saveAsTable("scratch.dbo.borough_targets")\n'
              'DeltaTable.forName(conn, "scratch.dbo.borough_targets").path   # under the scratch root</pre>')
        targets = conn.sql("SELECT borough, round(sum(revenue) * 1.1, 0) AS target_rev "
                           "FROM mart_zone_revenue GROUP BY borough")
        targets.write.mode("overwrite").saveAsTable("scratch.dbo.borough_targets")
        n_tgt = q("SELECT count(*) FROM scratch.dbo.borough_targets")
        landed = DeltaTable.forName(conn, "scratch.dbo.borough_targets").path.replace("\\", "/")
        in_scratch = scratch_path.replace("\\", "/") in landed
        results.append(_row("Cross-catalog write (scratch)", 0, n_tgt, n_tgt, n_tgt > 0, in_scratch,
                            time.perf_counter() - t0))
        say(f"{n_tgt} rows written under the scratch root ({_trim_loc(landed)}) — "
            f"physically separate from the primary catalog")

    # 18 ── the payoff: ONE query joining a PRIMARY table with a SCRATCH table ───────────────────────
    with _step(18, "cross-catalog JOIN: actuals (primary catalog) ⋈ targets (scratch catalog) in one conn.sql") as say:
        rows = _sql(conn, """
            SELECT m.borough,
                   round(sum(m.revenue), 0)                              AS actual_rev,
                   any_value(t.target_rev)                               AS target_rev,
                   round(100.0 * sum(m.revenue) / any_value(t.target_rev), 1) AS pct_of_target
            FROM mart_zone_revenue m
            JOIN scratch.dbo.borough_targets t ON t.borough = m.borough
            GROUP BY m.borough ORDER BY actual_rev DESC
        """).collect()
        _table([(b, f"{a:,.0f}", f"{tg:,.0f}", f"{pct}%") for b, a, tg, pct in rows],
               ["borough", "actual $", "target $", "% of target"],
               "  actuals (primary catalog) joined to targets (scratch catalog) in a single query")
        say(f"one SQL statement joined {len(rows)} boroughs across two lakehouses on different storage")

    # 19 ── switch the current catalog, and the fail-loud safety rails ───────────────────────────────
    with _step(19, "catalog.setCurrentCatalog('scratch'): unqualified names resolve there; then the guards") as say:
        _emit('  <pre class="py">conn.catalog.setCurrentCatalog("scratch")\n'
              'conn.sql("SELECT count(*) FROM borough_targets")   # unqualified → scratch.dbo\n'
              'conn.catalog.setCurrentCatalog(primary)            # back to the warehouse\n'
              '# fail-loud rails (both raise — no state change):\n'
              'conn.attach(scratch_path, name="dup")              # one URL ↔ one name\n'
              'conn.sql("INSERT INTO scratch.dbo.borough_targets VALUES (\'Z\', 0)")  # cross-catalog raw DML</pre>')
        conn.catalog.setCurrentCatalog("scratch")
        unq = q("SELECT count(*) FROM borough_targets")           # resolves in scratch.dbo now
        switched = conn.catalog.currentCatalog()
        conn.catalog.setCurrentCatalog(primary)
        guards = []
        try:
            conn.attach(scratch_path, name="dup")                # same URL → refused
        except ValueError:
            guards.append("re-attaching the same URL → refused")
        try:
            conn.sql("INSERT INTO scratch.dbo.borough_targets VALUES ('Z', 0)")  # other catalog → refused
        except ValueError:
            guards.append("raw DML against another catalog → refused")
        _table([(g,) for g in guards], ["fail-loud guard (raised as designed)"],
               "  one URL ↔ one name; raw DML stays in the current catalog")
        say(f"unqualified name resolved in '{switched}' ({unq} rows); switched back to '{primary}'; "
            f"{len(guards)} safety rails held")

    _part("Part 4 · the full MERGE surface — a real recompute, applied in one MERGE (SQL & DataFrame)",
          "duckrun's MERGE is the COMPLETE delta-rs TableMerger — matched UPDATE (arbitrary expressions, "
          "CASE) / matched DELETE / NOT MATCHED INSERT / NOT MATCHED BY SOURCE delete, any number of "
          "clauses applied in order. Both steps drive ONE MERGE off a fresh recompute over the 'trips' "
          "fact (a subquery — the real ETL shape, no hand-typed VALUES): one through conn.sql, one "
          "through the DeltaTable.merge builder.")

    # 20 ── SQL: refresh a dimension from the fact in ONE merge — recompute source, all clause kinds ──
    with _step(20, "SQL full-surface MERGE (real ETL): a fresh recompute over 'trips', applied to a "
                  "step-behind dimension in one statement — retire (DELETE), refresh (UPDATE + CASE tier), "
                  "onboard (INSERT)") as say:
        # the dimension AS OF the last load: a recompute that's a step behind — the two busiest zones
        # were added to the network since then and aren't onboarded yet, and every tier still reads
        # 'standard'. (The MERGE source below recomputes the CURRENT truth straight off the fact.)
        _sql(conn, """
            CREATE OR REPLACE TABLE zone_scorecard AS
            SELECT zone_id, any_value(zone) AS zone, any_value(borough) AS borough,
                   count(*) AS trips, round(avg(fare), 2)::DOUBLE AS avg_fare, 'standard' AS tier
            FROM trips
            WHERE zone_id NOT IN (SELECT zone_id FROM trips GROUP BY zone_id ORDER BY count(*) DESC LIMIT 2)
            GROUP BY zone_id
        """)
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM zone_scorecard")
        # data-driven thresholds, no magic numbers: retire the smallest zone(s), gold = top quartile.
        floor = q("SELECT min(c) + 1 FROM (SELECT count(*) c FROM trips GROUP BY zone_id)")
        gold = q("SELECT approx_quantile(c, 0.75) FROM (SELECT count(*) c FROM trips GROUP BY zone_id)")
        # the MERGE source is a SUBQUERY recomputing fresh per-zone stats off the fact — the real ETL
        # shape, not hand-typed VALUES. One statement does retire + refresh + onboard, top-to-bottom.
        _sql(conn, f"""
            MERGE INTO zone_scorecard t
            USING (
                SELECT zone_id, any_value(zone) AS zone, any_value(borough) AS borough,
                       count(*) AS trips, round(avg(fare), 2)::DOUBLE AS avg_fare
                FROM trips
                GROUP BY zone_id
            ) s
            ON t.zone_id = s.zone_id
            WHEN MATCHED AND s.trips < {floor} THEN DELETE
            WHEN MATCHED THEN UPDATE SET trips    = s.trips,
                                         avg_fare = s.avg_fare,
                                         tier     = CASE WHEN s.trips >= {gold} THEN 'gold' ELSE 'standard' END
            WHEN NOT MATCHED THEN INSERT (zone_id, zone, borough, trips, avg_fare, tier)
                 VALUES (s.zone_id, s.zone, s.borough, s.trips, s.avg_fare,
                         CASE WHEN s.trips >= {gold} THEN 'gold' ELSE 'standard' END)
        """)
        after = q("SELECT count(*) FROM zone_scorecard")
        n_zones = q("SELECT count(DISTINCT zone_id) FROM trips")
        retired = q(f"SELECT count(*) FROM (SELECT count(*) c FROM trips GROUP BY zone_id) WHERE c < {floor}")
        onboarded = q("SELECT count(*) FROM (SELECT zone_id FROM trips GROUP BY zone_id ORDER BY count(*) DESC LIMIT 2)")
        # every surviving zone must equal the fresh recompute (no stale metric left behind), the
        # retired ones must be gone, and DELETE + INSERT must have actually fired (not no-ops).
        drift = q("""
            SELECT count(*) FROM zone_scorecard t
            JOIN (SELECT zone_id, count(*) AS trips, round(avg(fare), 2)::DOUBLE AS avg_fare
                  FROM trips GROUP BY zone_id) s USING (zone_id)
            WHERE t.trips <> s.trips OR t.avg_fare <> s.avg_fare
        """)
        ok = (
            after == n_zones - retired                                              # final = all zones minus retired
            and q(f"SELECT count(*) FROM zone_scorecard WHERE trips < {floor}") == 0  # retired ones gone
            and retired >= 1 and onboarded == 2                                     # DELETE and INSERT really fired
            and drift == 0                                                          # every survivor matches fresh
            and q("SELECT count(*) FROM zone_scorecard WHERE tier = 'gold'") > 0    # CASE tier applied
        )
        results.append(_row("SQL MERGE (retire+refresh+onboard)", before, after, n_zones - retired,
                            after == n_zones - retired, ok, time.perf_counter() - t0))
        _table([(t, f"{z:,}", f"{mn:,}", f"{mx:,}") for t, z, mn, mx in conn.sql(
                    """SELECT tier, count(*) AS zones, min(trips) AS mn, max(trips) AS mx
                       FROM zone_scorecard GROUP BY tier ORDER BY tier""").collect()],
               ["tier", "zones", "min trips", "max trips"],
               "  zone_scorecard after the merge — tiers (re)assigned by CASE, low-volume zones retired, new zones onboarded")
        say(f"one MERGE off a recompute subquery: retired {retired}, refreshed {before - retired}, "
            f"onboarded {onboarded}; {before:,} → {after:,} rows, 0 drift")

    # 21 ── DataFrame: full-sync a dimension from a recompute — refresh matched, purge departed keys ──
    with _step(21, "DataFrame full-surface merge (real ETL): full-sync 'fare_watch' from a fresh "
                   "recompute over 'trips' via the builder — whenMatchedUpdate(CASE) + whenNotMatchedBySourceDelete") as say:
        # the dimension carries two decommissioned zones (901, 902) that no longer appear in the fact —
        # a full sync must refresh the live zones AND purge the departed ones.
        _sql(conn, """
            CREATE OR REPLACE TABLE fare_watch AS
            SELECT zone_id, zone, round(avg_fare, 2)::DOUBLE AS avg_fare, 'active' AS status FROM (
                SELECT zone_id, any_value(zone) AS zone, avg(fare) AS avg_fare FROM trips GROUP BY zone_id)
            UNION ALL SELECT 901, 'Retired North', 0.0::DOUBLE, 'active'
            UNION ALL SELECT 902, 'Retired South', 0.0::DOUBLE, 'active'
        """)
        t0 = time.perf_counter()
        before = q("SELECT count(*) FROM fare_watch")
        n_zones = q("SELECT count(DISTINCT zone_id) FROM trips")
        hi = q("SELECT approx_quantile(avg_fare, 0.75) FROM (SELECT avg(fare) AS avg_fare FROM trips GROUP BY zone_id)")
        # the merge SOURCE is a recompute over the fact (real ETL). The builder refreshes every matched
        # zone — re-banding 'status' with a CASE expression — and DELETEs every target zone the source
        # no longer carries, so the two decommissioned zones are purged. No VALUES in the merge.
        _emit('  <pre class="py"># builder full-sync: source is a recompute over the fact, not VALUES\n'
              'src = conn.sql("SELECT zone_id, any_value(zone) AS zone, "\n'
              '               "round(avg(fare),2)::DOUBLE AS avg_fare FROM trips GROUP BY zone_id")\n'
              'DeltaTable.forName(conn, "fare_watch").merge(src, "target.zone_id = source.zone_id") \\\n'
              '    .whenMatchedUpdate(set={"avg_fare": "source.avg_fare",\n'
              '        "status": "case when source.avg_fare >= <p75> then \'premium\' else \'active\' end"}) \\\n'
              '    .whenNotMatchedBySourceDelete() \\\n'
              '    .execute()</pre>')
        src = conn.sql("SELECT zone_id, any_value(zone) AS zone, round(avg(fare), 2)::DOUBLE AS avg_fare "
                       "FROM trips GROUP BY zone_id")
        DeltaTable.forName(conn, "fare_watch").merge(src, "target.zone_id = source.zone_id") \
            .whenMatchedUpdate(set={"avg_fare": "source.avg_fare",
                                    "status": f"case when source.avg_fare >= {hi} then 'premium' else 'active' end"}) \
            .whenNotMatchedBySourceDelete() \
            .execute()
        after = q("SELECT count(*) FROM fare_watch")
        purged = q("SELECT count(*) FROM fare_watch WHERE zone_id IN (901, 902)")
        drift = q("""
            SELECT count(*) FROM fare_watch t
            JOIN (SELECT zone_id, round(avg(fare), 2)::DOUBLE AS avg_fare FROM trips GROUP BY zone_id) s
            USING (zone_id) WHERE t.avg_fare <> s.avg_fare
        """)
        ok = (
            after == n_zones                       # every live zone kept, both decommissioned zones gone
            and purged == 0                        # by-source DELETE fired
            and drift == 0                         # matched UPDATE refreshed every zone from source
            and q("SELECT count(*) FROM fare_watch WHERE status = 'premium'") > 0   # CASE re-band applied
        )
        results.append(_row("DataFrame merge (refresh+purge)", before, after, n_zones, after == n_zones, ok,
                            time.perf_counter() - t0))
        _table([(s, f"{z:,}", f"{mn:,.2f}", f"{mx:,.2f}") for s, z, mn, mx in conn.sql(
                    """SELECT status, count(*) AS zones, min(avg_fare) AS mn, max(avg_fare) AS mx
                       FROM fare_watch GROUP BY status ORDER BY status""").collect()],
               ["status", "zones", "min fare", "max fare"],
               "  fare_watch after the full-sync — every live zone refreshed & re-banded by CASE, decommissioned zones purged")
        say(f"builder full-sync off a recompute: refreshed {n_zones} live zones (re-banded via CASE), "
            f"purged {before - after} departed; {before:,} → {after:,} rows, 0 drift")

    _scorecard(results)

    # Emit the standalone HTML report when asked (CI sets DUCKRUN_TAXI_PAGE → published as taxi.html).
    page_path = os.environ.get("DUCKRUN_TAXI_PAGE")
    if page_path:
        target = "OneLake" if str(getattr(conn, "root_path", "")).startswith(("abfss://", "az://")) else "local Delta"
        caption = (f"Actual run · {SAMPLE_ROWS:,}-row sample of live NYC TLC Yellow-Taxi {TAXI_MONTH} "
                   f"→ {target} · {datetime.now(timezone.utc):%Y-%m-%d} UTC")
        ok = all(r["count_ok"] and r["values_ok"] for r in results)
        with open(page_path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(_page_html(_scorecard_html(results), "\n".join(_DOC), caption, ok))
        print(f"  wrote HTML report → {page_path}", flush=True)

    print(f"\n=== demo complete: {schema} ===", flush=True)


def main():
    if not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://") and ONELAKE_TOKEN):
        print(
            "\nThis demo writes to the OneLake sample lakehouse. Set:\n"
            "  WAREHOUSE_PATH=abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables\n"
            "  ONELAKE_TOKEN=<storage bearer token for resource https://storage.azure.com/>\n"
            "then re-run:  python integration_tests/taxi/demo_taxi.py\n",
            flush=True,
        )
        return
    conn = duckrun.connect(WAREHOUSE_PATH, storage_options={"bearer_token": ONELAKE_TOKEN},
                           schema=TAXI_SCHEMA, read_only=False)
    run_taxi_demo(conn, TAXI_SCHEMA)


if __name__ == "__main__":
    main()
