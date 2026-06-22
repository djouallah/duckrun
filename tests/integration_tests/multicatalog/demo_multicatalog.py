"""A live multi-catalog showcase for ``duckrun.connect`` + ``conn.attach`` — a **demo you run and
watch**, not a gate.

It binds three catalogs of *different kinds* to one session and queries across them:

  * **aemo** — the writable OneLake lakehouse (the session primary, ``read_only=False``); the demo
    writes a new mart back here.
  * **warehouse** — a Microsoft **Fabric Warehouse**, attached **read-only** (``read_only=True``):
    its OneLake ``Tables`` are queryable as Delta, but writes are fenced (a warehouse is managed by
    the SQL engine, not writable through OneLake). The demo proves the fence fails loud *before*
    touching storage.
  * **scratch** — a plain **local** directory attached as a writable catalog, for intermediate data.

The point: ``catalog.schema.table`` resolves across all three, a single ``conn.sql`` JOINs a table in
the writable lakehouse with one in the local scratch catalog (and, when present, the read-only
warehouse), and the result is written **back to the aemo lakehouse** as a new Delta table. Per-catalog
``read_only`` is what lets a read-only reference store sit next to a writable lakehouse in one session.

Like the taxi demo, it emits a standalone HTML report (every step + its real result) when
``DUCKRUN_MULTICAT_PAGE`` is set (CI publishes it as multicatalog.html).

OneLake, with no Azure ids in code:

    WAREHOUSE_PATH       abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables   (writable aemo)
    WAREHOUSE_RO_PATH    abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>/Tables   (read-only warehouse)
    ONELAKE_TOKEN        storage bearer token (resource https://storage.azure.com/)

Knobs:
    DUCKRUN_MULTICAT_SCHEMA   schema to write the demo's tables into (default: multicatalog_demo)
    DUCKRUN_MULTICAT_PAGE     write the HTML report to this path (optional)
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

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")              # the writable aemo lakehouse
WAREHOUSE_RO_PATH = os.environ.get("WAREHOUSE_RO_PATH")        # the read-only Fabric Warehouse
ONELAKE_TOKEN = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
SCHEMA = os.environ.get("DUCKRUN_MULTICAT_SCHEMA", "multicatalog_demo")

# Synthetic, deterministic source data — no network, so the demo's correctness doesn't depend on what
# already lives in the lakehouse. Five NEM-style regions × 24 hours of demand.
_REGIONS = [("A", "New South Wales", "NSW"), ("B", "Victoria", "VIC"), ("C", "Queensland", "QLD"),
            ("D", "South Australia", "SA"), ("E", "Tasmania", "TAS")]


# ── HTML report accumulator + narration helpers (self-contained; mirrors demo_taxi's style) ────────
_DOC = []


def _emit(fragment):
    _DOC.append(fragment)


def _part(label, blurb):
    print(f"\n══ {label} ══  {blurb}", flush=True)
    _emit(f'\n<h2 class="part">{html.escape(label)}</h2>\n  <p class="lede">{html.escape(blurb)}</p>')


@contextmanager
def _step(n, label):
    print(f"\n[{n}] {label}", flush=True)
    _emit(f'\n<section>\n  <h3><span class="n">{n}</span> {html.escape(label)}</h3>')
    t = time.perf_counter()

    def say(detail):
        print(f"      -> {detail}", flush=True)
        _emit(f'  <p class="note">{html.escape(detail)}</p>')

    yield say
    _emit("</section>")
    print(f"      ({time.perf_counter() - t:.2f}s)", flush=True)


def _py(code):
    """Echo a Python snippet to the console and the HTML report (the API being demonstrated)."""
    print("\n".join("      ┊ " + line for line in code.splitlines()), flush=True)
    _emit(f'  <pre class="py">{html.escape(code)}</pre>')


def _sql(conn, stmt):
    clean = textwrap.dedent(stmt).strip()
    print("      ┄┄ SQL ┄┄", flush=True)
    print("\n".join("      │ " + line for line in clean.splitlines()), flush=True)
    _emit(f'  <pre class="sql">{html.escape(clean)}</pre>')
    return conn.sql(clean)


def _table(rows, headers, title=None):
    rows = [list(r) for r in rows]
    cells = [[str(h) for h in headers]] + [[str(c) for c in r] for r in rows]
    w = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    edge = lambda l, m, r: l + m.join("─" * (w[i] + 2) for i in range(len(headers))) + r
    fmt = lambda row: "  │ " + " │ ".join(row[i].ljust(w[i]) for i in range(len(headers))) + " │"
    out = ([title] if title else []) + ["  " + edge("┌", "┬", "┐"), fmt(cells[0]), "  " + edge("├", "┼", "┤")]
    out += [fmt(row) for row in cells[1:]] + ["  " + edge("└", "┴", "┘")]
    print("\n".join(out), flush=True)
    th = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    cap = f'  <p class="tcap">{html.escape(title.strip())}</p>\n' if title else ""
    _emit(f'{cap}  <table class="data"><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>')


def _row(name, before, after, expected, count_ok, values_ok, dt):
    return dict(name=name, before=before, after=after, expected=expected,
                count_ok=count_ok, values_ok=values_ok, dt=dt)


def _scorecard(results):
    rows = [(r["name"], f"{r['after'] - r['before']:+,}", f"{r['before']:,}", f"{r['after']:,}",
             f"{r['expected']:,}", "✅" if r["count_ok"] else "❌", "✅" if r["values_ok"] else "❌",
             f"{r['dt']:.1f}s") for r in results]
    _table(rows, ["Operation", "Rows", "Before", "After", "Expected", "Count ✓", "Values ✓", "Time"],
           "\n  📊 Results — multi-catalog writes across lakehouse + warehouse + local")
    _DOC.pop()  # drop the plain HTML copy; the report uses the styled scorecard below
    ok = all(r["count_ok"] and r["values_ok"] for r in results)
    print("\n  ✅ all operations correct." if ok
          else "\n  ❌ one or more operations were wrong — see the ❌ cells above.", flush=True)
    return ok


def _scorecard_html(results):
    head = ("<th>Operation</th><th>Rows</th><th>Before</th><th>After</th><th>Expected</th>"
            "<th>Count&nbsp;✓</th><th>Values&nbsp;✓</th><th>Time</th>")
    body = "\n".join(
        "    <tr><td>{n}</td><td>{r:+,}</td><td>{b:,}</td><td>{a:,}</td><td>{e:,}</td>"
        "<td>{c}</td><td>{v}</td><td>{t:.1f}s</td></tr>".format(
            n=html.escape(x["name"]), r=x["after"] - x["before"], b=x["before"], a=x["after"],
            e=x["expected"], c="✅" if x["count_ok"] else "❌", v="✅" if x["values_ok"] else "❌", t=x["dt"])
        for x in results)
    return (f'<table class="data card">\n  <thead><tr>{head}</tr></thead>\n  <tbody>\n{body}\n  </tbody>\n</table>')


def _trim_loc(path):
    p = str(path).replace("\\", "/")
    if "://" in p:
        scheme = p.split("://", 1)[0]
        if "/Tables/" in p:
            return f"{scheme}://…/Tables/{p.split('/Tables/', 1)[1]}"
        return f"{scheme}://…/" + "/".join(p.split("/")[-2:])
    return "…/" + "/".join(s for s in p.split("/") if s)[-40:]


def _kind(root):
    return "OneLake (abfss)" if str(root).startswith(("abfss://", "az://")) else "local fs"


_PAGE_CSS = """
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         max-width: 900px; margin: 0 auto; padding: 2.5rem 1.25rem; line-height: 1.5; }
  a.back { font-size: 0.9rem; text-decoration: none; color: #888; }
  h1 { margin: 0.4rem 0 0.2rem; }
  h1 .tag { font-size: 0.7rem; font-weight: 700; color: #fff; background: #6c3483;
            padding: 0.12rem 0.5rem; border-radius: 6px; vertical-align: middle; margin-left: 0.4rem; }
  h2.part { margin: 2rem 0 0.2rem; padding-bottom: 0.3rem; border-bottom: 2px solid #6c348388;
            font-size: 1.25rem; }
  p.lede { color: #777; margin-top: 0; }
  section { border-top: 1px solid #8883; padding: 0.6rem 0 0.4rem; }
  h3 { margin: 0.8rem 0 0.5rem; font-size: 1.05rem; }
  h3 .n { display: inline-block; min-width: 1.4rem; color: #6c3483; font-variant-numeric: tabular-nums; }
  pre.sql, pre.py { background: #8881; border-left: 3px solid #6c348388; border-radius: 4px;
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
  <title>duckrun — multi-catalog (lakehouse + warehouse + local)</title>
  <style>{_PAGE_CSS}</style>
</head>
<body>
  <a class="back" href="index.html">← duckrun docs</a>
  <h1>multi-catalog — one session, three lakehouses<span class="tag">not a dbt project</span></h1>
  <p class="lede">{html.escape(caption)}. <code>conn.attach(...)</code> binds a writable OneLake lakehouse,
  a <strong>read-only</strong> Fabric Warehouse, and a local scratch dir as three catalogs; a single
  <code>conn.sql</code> JOINs across them as <code>catalog.schema.table</code>, and the mart is written
  back to the lakehouse. Per-catalog <code>read_only</code> fences the warehouse.</p>
  <h2 style="margin-bottom:0.2rem">Run scorecard — <span style="font-weight:400">{verdict}</span></h2>
  {scorecard}
  {body}
  <footer>Generated by
    <a href="https://github.com/djouallah/duckrun/blob/main/tests/integration_tests/multicatalog/demo_multicatalog.py">demo_multicatalog.py</a>
    — re-published from a live run by the integration-tests workflow.</footer>
</body>
</html>
"""


# ── the scenario ───────────────────────────────────────────────────────────────────────────────
def run_multicatalog_demo(conn, warehouse_path, warehouse_so, scratch_path, schema):
    """The whole showcase against ``conn`` (the writable primary lakehouse). ``warehouse_path`` is
    attached read-only; ``scratch_path`` (local) is attached writable. Storage-neutral, so it runs the
    same against three local dirs (de-risk) or live OneLake (the demo). Returns the results list."""
    _DOC.clear()
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731
    results = []
    primary = conn.catalog.currentCatalog()

    print(f"\n=== duckrun multi-catalog demo | primary(aemo)={_trim_loc(conn.root_path)} | "
          f"warehouse(ro)={_trim_loc(warehouse_path)} | scratch={_trim_loc(scratch_path)} ===", flush=True)

    _part("Part 1 · attach three catalogs of different kinds to one session",
          "A writable OneLake lakehouse (the primary), a read-only Fabric Warehouse, and a local "
          "scratch dir — bound as catalogs, addressed as catalog.schema.table.")

    # 1 ── attach the warehouse READ-ONLY and a local scratch catalog ──────────────────────────────
    with _step(1, "attach: a read-only Fabric Warehouse + a writable local scratch catalog") as say:
        _py('# the warehouse is read-only (managed by the SQL engine — not writable via OneLake):\n'
            'conn.attach(warehouse_path, name="warehouse", storage_options={"bearer_token": tok},\n'
            '            read_only=True)\n'
            '# a local scratch dir, writable (inherits the session default):\n'
            'conn.attach(scratch_path, name="scratch")')
        conn.attach(warehouse_path, name="warehouse", storage_options=warehouse_so, read_only=True)
        conn.attach(scratch_path, name="scratch")
        cat_rows = []
        for name in conn.catalog.listCatalogs():
            e = conn._catalogs[name]
            mode = "read-only" if e.read_only else "writable"
            role = "primary (aemo)" if name == primary else ("warehouse" if name == "warehouse" else "scratch")
            cat_rows.append((name, role, _kind(e.root_path), mode))
        _table(cat_rows, ["catalog", "role", "storage", "mode"], "  three catalogs, one session")
        say(f"current catalog is '{primary}' (writable); 'warehouse' attached read-only; "
            f"'scratch' on local fs")

    # 2 ── per-catalog read-only: writing to the warehouse fails loud (before touching storage) ─────
    with _step(2, "per-catalog read-only: a write into the warehouse catalog is refused") as say:
        _py('# the session is writable, but the warehouse catalog is read_only=True:\n'
            'conn.sql("select 1 as x").write.mode("overwrite").saveAsTable("warehouse.dbo.nope")\n'
            '#  -> PermissionError (fenced before any Delta write is attempted)')
        try:
            conn.sql("select 1 as x").write.mode("overwrite").saveAsTable("warehouse.dbo.nope")
            fenced, errline = False, ""
        except PermissionError as e:
            fenced, errline = True, str(e).splitlines()[0]
        _table([("warehouse (read-only)", "saveAsTable", "refused ✅" if fenced else "WROTE ❌ (UNSAFE)")],
               ["catalog", "attempted", "outcome"], "  the read-only fence")
        results.append(_row("Read-only fence (warehouse)", 0, 0, 0, True, fenced, 0.0))
        say(f"write refused: {errline[:90] or 'PermissionError'}")

    # 3 ── read the warehouse (read-only): list its tables, sample one if present ───────────────────
    with _step(3, "read the warehouse (read-only): list its Delta tables, sample a row count") as say:
        conn.catalog.setCurrentCatalog("warehouse")
        wh_dbs = conn.catalog.listDatabases()
        wh_tables = [(db, t) for db in wh_dbs for t in conn.catalog.listTables(db)]
        sampled = ""
        if wh_tables:
            db, t = wh_tables[0]
            n = q(f'select count(*) from {_q(conn.catalog.currentCatalog())}.{_q(db)}.{_q(t)}')
            sampled = f"{db}.{t} = {n:,} rows"
        conn.catalog.setCurrentCatalog(primary)  # back to the writable lakehouse
        _table([(db, t) for db, t in wh_tables[:8]] or [("—", "(no Delta tables discovered)")],
               ["schema", "table"], "  warehouse catalog contents (read-only)")
        say(f"warehouse exposes {len(wh_tables)} Delta table(s)"
            + (f"; sampled {sampled}" if sampled else "") + "; switched back to the lakehouse")

    _part("Part 2 · write across catalogs, then JOIN and write the mart back to the lakehouse",
          "Seed the writable lakehouse and the local scratch catalog, JOIN across them in one "
          "conn.sql, and write the result BACK to the aemo lakehouse as a new Delta table.")

    # 4 ── seed the writable lakehouse: regional demand (CTAS via conn.sql) ──────────────────────────
    with _step(4, f"seed the aemo lakehouse: '{schema}.regional_demand' (5 regions × 24 hours)") as say:
        t0 = time.perf_counter()
        _sql(conn, f"""
            CREATE OR REPLACE TABLE {schema}.regional_demand AS
            SELECT r.region, h.hour,
                   round(500 + 300 * sin((h.hour + ascii(r.region)) / 3.0) + 40 * ascii(r.region), 1) AS demand_mw
            FROM (VALUES ('A'), ('B'), ('C'), ('D'), ('E')) r(region)
            CROSS JOIN range(0, 24) h(hour)
        """)
        n_dem = q(f"SELECT count(*) FROM {schema}.regional_demand")
        results.append(_row("Seed regional_demand (aemo)", 0, n_dem, 120, n_dem == 120, n_dem == 120,
                            time.perf_counter() - t0))
        say(f"{n_dem} rows written to the aemo lakehouse")

    # 5 ── write a lookup INTO the local scratch catalog (DataFrame saveAsTable by 3-part name) ─────
    with _step(5, "cross-catalog write: df.write.saveAsTable('scratch.dbo.region_lookup') — local fs") as say:
        t0 = time.perf_counter()
        values = ", ".join(f"('{r}', '{n}', '{s}')" for r, n, s in _REGIONS)
        _py('lookup = conn.sql("SELECT * FROM (VALUES …) t(region, region_name, state)")\n'
            'lookup.write.mode("overwrite").saveAsTable("scratch.dbo.region_lookup")')
        lookup = conn.sql(f"SELECT * FROM (VALUES {values}) t(region, region_name, state)")
        lookup.write.mode("overwrite").saveAsTable("scratch.dbo.region_lookup")
        n_lk = q("SELECT count(*) FROM scratch.dbo.region_lookup")
        landed = DeltaTable.forName(conn, "scratch.dbo.region_lookup").path.replace("\\", "/")
        in_scratch = scratch_path.replace("\\", "/") in landed
        results.append(_row("Write lookup (scratch)", 0, n_lk, len(_REGIONS), n_lk == len(_REGIONS),
                            in_scratch, time.perf_counter() - t0))
        say(f"{n_lk} rows written under the scratch root ({_trim_loc(landed)})")

    # 6 ── JOIN aemo ⋈ scratch in one query, write the mart BACK to the aemo lakehouse ──────────────
    with _step(6, f"cross-catalog JOIN → write back: '{schema}.mart_region_demand' in the aemo lakehouse") as say:
        t0 = time.perf_counter()
        _py('# one query spans the writable lakehouse (regional_demand) and the local scratch\n'
            '# catalog (region_lookup); the result is written back to the lakehouse by name:\n'
            'mart = conn.sql("""\n'
            '    SELECT k.state, k.region_name,\n'
            '           round(sum(d.demand_mw), 1) AS total_mw,\n'
            '           round(avg(d.demand_mw), 1) AS avg_mw\n'
            f'    FROM {schema}.regional_demand d\n'
            '    JOIN scratch.dbo.region_lookup k ON k.region = d.region\n'
            '    GROUP BY k.state, k.region_name""")\n'
            f'mart.write.mode("overwrite").saveAsTable("{schema}.mart_region_demand")')
        mart = conn.sql(f"""
            SELECT k.state, k.region_name,
                   round(sum(d.demand_mw), 1) AS total_mw,
                   round(avg(d.demand_mw), 1)  AS avg_mw
            FROM {schema}.regional_demand d
            JOIN scratch.dbo.region_lookup k ON k.region = d.region
            GROUP BY k.state, k.region_name
        """)
        mart.write.mode("overwrite").saveAsTable(f"{schema}.mart_region_demand")
        n_mart = q(f"SELECT count(*) FROM {schema}.mart_region_demand")
        landed = DeltaTable.forName(conn, f"{schema}.mart_region_demand").path.replace("\\", "/")
        in_aemo = conn.root_path.replace("\\", "/") in landed
        results.append(_row("Write mart back (aemo)", 0, n_mart, len(_REGIONS), n_mart == len(_REGIONS),
                            in_aemo, time.perf_counter() - t0))
        _table([(s, rn, f"{tot:,.0f}", f"{avg:,.0f}") for s, rn, tot, avg in conn.sql(
                    f"SELECT state, region_name, total_mw, avg_mw FROM {schema}.mart_region_demand "
                    "ORDER BY total_mw DESC").collect()],
               ["state", "region", "total MW", "avg MW"],
               "  mart_region_demand — lakehouse ⋈ local scratch, written back to the lakehouse")
        say(f"{n_mart} rows written back to the aemo lakehouse ({_trim_loc(landed)})")

    ok = _scorecard(results)

    page_path = os.environ.get("DUCKRUN_MULTICAT_PAGE")
    if page_path:
        tgt = "OneLake" if str(conn.root_path).startswith(("abfss://", "az://")) else "local Delta"
        caption = (f"Actual run · writable {tgt} lakehouse + read-only Fabric Warehouse + local scratch "
                   f"· {datetime.now(timezone.utc):%Y-%m-%d} UTC")
        with open(page_path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(_page_html(_scorecard_html(results), "\n".join(_DOC), caption, ok))
        print(f"  wrote HTML report → {page_path}", flush=True)

    print(f"\n=== demo complete: {schema} ===", flush=True)
    return results


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


def main():
    if not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://") and ONELAKE_TOKEN
            and WAREHOUSE_RO_PATH and WAREHOUSE_RO_PATH.startswith("abfss://")):
        print(
            "\nThis demo binds three OneLake catalogs. Set:\n"
            "  WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables   (writable aemo)\n"
            "  WAREHOUSE_RO_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>/Tables (read-only warehouse)\n"
            "  ONELAKE_TOKEN=<storage bearer token for resource https://storage.azure.com/>\n"
            "then re-run:  python tests/integration_tests/multicatalog/demo_multicatalog.py\n",
            flush=True,
        )
        return
    so = {"bearer_token": ONELAKE_TOKEN}
    conn = duckrun.connect(WAREHOUSE_PATH, storage_options=so, schema=SCHEMA, read_only=False)
    scratch = tempfile.mkdtemp(prefix="duckrun_scratch_")
    run_multicatalog_demo(conn, WAREHOUSE_RO_PATH, so, scratch, SCHEMA)


if __name__ == "__main__":
    main()
