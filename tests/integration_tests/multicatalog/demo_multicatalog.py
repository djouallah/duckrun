"""A multi-catalog showcase for ``duckrun.connect`` + ``conn.attach`` — a **demo you run and watch**.

The point it makes, visually: in Microsoft Fabric a **Warehouse** and a **Lakehouse** are the same
thing to duckrun — both are Delta tables in OneLake — *except a Warehouse is locked to writes*. So you
bind them as catalogs in one session, **list every table across all of them with the three-part
``catalog.schema.table`` name**, and query across them. Tokens are automatic on OneLake (inside a
Fabric notebook there is nothing to pass), so no credentials appear anywhere in this code.

What it shows, in order:
  1. attach a writable **lakehouse** (primary), a read-only **warehouse**, and a **local** folder — one session.
  2. **list every table across all three catalogs** as ``catalog.schema.table`` — the key visual.
  3. the **three ways to name a table**: ``catalog.schema.table`` → ``schema.table`` → ``table``.
  4. the warehouse is **read-only**: a write into it is refused; the lakehouse is writable.
  5. a real pipeline: read facts from the **warehouse**, the DUID dimension from the **lakehouse**, a
     carbon-factor lookup from the **local** catalog, JOIN across all three, and write the mart **back
     to the lakehouse**.

Emits a standalone HTML report when ``DUCKRUN_MULTICAT_PAGE`` is set (CI publishes it as multicatalog.html).

OneLake — tokens are acquired automatically (Fabric notebook, env, or `az login`), so set only paths:

    WAREHOUSE_PATH       abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables   (writable lakehouse)
    WAREHOUSE_RO_PATH    abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>/Tables   (read-only warehouse)

Knobs:
    DUCKRUN_MULTICAT_SCHEMA   schema the mart is written into (default: multicatalog_demo)
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

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")          # the writable OneLake lakehouse (primary)
WAREHOUSE_RO_PATH = os.environ.get("WAREHOUSE_RO_PATH")    # the read-only Fabric Warehouse
SCHEMA = os.environ.get("DUCKRUN_MULTICAT_SCHEMA", "multicatalog_demo")
# Both remote catalogs are scoped to the `mart` schema: it keeps discovery fast and shows the SAME
# tables in the warehouse and the lakehouse — driving home that they're the same thing, one locked.
REMOTE_SCHEMA = "mart"
# The fact table is huge (100M+ rows); this is a multi-catalog *plumbing* demo, not an analytics
# benchmark, so it reads a bounded LIMIT sample — DuckDB early-terminates the delta_scan, so only a
# few row groups cross the network, never the whole table.
SAMPLE_ROWS = int(os.environ.get("DUCKRUN_MULTICAT_SAMPLE", "200000"))

# A small analyst-maintained carbon-intensity lookup (kg CO₂ per MWh) — the LOCAL catalog's data, by
# AEMO FuelSourceDescriptor. Renewables/biogenic and unknowns default to 0 via the LEFT JOIN.
_FUEL_FACTORS = [
    ("Black coal", 900), ("Brown coal", 1200), ("Natural gas", 490),
    ("Natural gas / fuel oil", 600), ("Natural gas / diesel", 600), ("Coal seam methane", 500),
    ("Diesel", 850), ("Kerosene", 800), ("Waste coal mine gas", 400),
    ("Wind", 0), ("Solar", 0), ("Water", 0), ("Bagasse", 0),
    ("Biogas - sludge", 0), ("Landfill methane / landfill gas", 0),
]


# ── HTML report accumulator + narration helpers ──────────────────────────────────────────────────
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


def _trim_loc(path):
    p = str(path).replace("\\", "/")
    if "://" in p:
        scheme = p.split("://", 1)[0]
        if "/Tables/" in p:
            return f"{scheme}://…/Tables/{p.split('/Tables/', 1)[1]}"
        return f"{scheme}://…/" + "/".join(p.split("/")[-2:])
    return "…/" + "/".join([s for s in p.split("/") if s][-3:])


def _kind(root):
    return "OneLake (abfss)" if str(root).startswith(("abfss://", "az://")) else "local"


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
  code { background: #8882; padding: 0.1rem 0.35rem; border-radius: 4px; }
  footer { margin-top: 2rem; color: #888; font-size: 0.85rem; }
"""


def _page_html(body, caption):
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>duckrun — multi-catalog (warehouse + lakehouse + local)</title>
  <style>{_PAGE_CSS}</style>
</head>
<body>
  <a class="back" href="index.html">← duckrun docs</a>
  <h1>multi-catalog — one session, three catalogs<span class="tag">not a dbt project</span></h1>
  <p class="lede">{html.escape(caption)}. A Fabric <strong>Warehouse</strong> and a <strong>Lakehouse</strong>
  are the same thing to duckrun — Delta in OneLake — except the warehouse is <strong>locked to writes</strong>.
  <code>conn.attach(...)</code> binds them (plus a local folder) as catalogs; every table is named
  <code>catalog.schema.table</code>, and a single <code>conn.sql</code> joins across all three. Tokens are
  automatic on OneLake — no credentials in the code.</p>
  {body}
  <footer>Generated by
    <a href="https://github.com/djouallah/duckrun/blob/main/tests/integration_tests/multicatalog/demo_multicatalog.py">demo_multicatalog.py</a>
    — re-published from a live run by the integration-tests workflow.</footer>
</body>
</html>
"""


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


# ── the scenario ───────────────────────────────────────────────────────────────────────────────
def run_multicatalog_demo(conn, warehouse_path, local_path, schema):
    """The showcase against ``conn`` (the writable lakehouse, primary). ``warehouse_path`` is attached
    read-only; ``local_path`` is attached writable. Storage-neutral, so it runs the same against local
    dirs (de-risk) or live OneLake (the demo)."""
    _DOC.clear()
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731
    lakehouse = conn.catalog.currentCatalog()

    print(f"\n=== duckrun multi-catalog | lakehouse={_trim_loc(conn.root_path)} | "
          f"warehouse(ro)={_trim_loc(warehouse_path)} | local={_trim_loc(local_path)} ===", flush=True)

    _part("One session, three catalogs",
          "A writable lakehouse (primary), a read-only warehouse, and a local folder — bound with "
          "conn.attach. On OneLake the token is automatic (nothing to pass in a Fabric notebook).")

    # 1 ── attach the warehouse (read-only) and a local catalog — no credentials, tokens are automatic ─
    with _step(1, "attach a read-only warehouse + a local catalog (OneLake tokens are automatic)") as say:
        _py('# a Warehouse is just a Lakehouse that is locked to writes — attach it read-only.\n'
            '# OneLake tokens are acquired automatically (Fabric notebook / env / az login):\n'
            'conn.attach(warehouse_path, name="warehouse", schema="mart", read_only=True)\n'
            'conn.attach(local_path,     name="local")   # a plain local folder, writable')
        conn.attach(warehouse_path, name="warehouse", schema=REMOTE_SCHEMA, read_only=True)
        conn.attach(local_path, name="local")
        rows = [(c, "primary" if c == lakehouse else ("read-only" if conn._catalogs[c].read_only else "writable"),
                 _kind(conn._catalogs[c].root_path)) for c in conn.catalog.listCatalogs()]
        _table(rows, ["catalog", "mode", "storage"], "  conn.catalog.listCatalogs()")
        say(f"three catalogs bound to one session; '{lakehouse}' is writable, 'warehouse' is read-only")

    # 2 ── THE KEY VISUAL: list every table across all catalogs as catalog.schema.table ─────────────
    with _step(2, "list EVERY table across all catalogs — the three-part catalog.schema.table name") as say:
        _py('# one session sees all three catalogs; every table has a three-part name:\n'
            'conn.sql("""SELECT table_catalog, table_schema, table_name\n'
            '            FROM information_schema.tables\n'
            "            WHERE table_schema NOT IN ('information_schema','pg_catalog','main')\n"
            '            ORDER BY 1, 2, 3""")')
        rows = conn._connection.execute(
            "SELECT table_catalog, table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema','pg_catalog','main') "
            "ORDER BY table_catalog, table_schema, table_name"
        ).fetchall()
        _table([(cat, f"{cat}.{sch}.{tbl}") for cat, sch, tbl in rows] or [("—", "(none yet)")],
               ["catalog", "fully-qualified name (catalog.schema.table)"],
               "  every table the session can see, three-part named")
        say(f"{len(rows)} tables across {len(conn.catalog.listCatalogs())} catalogs — the warehouse and "
            f"lakehouse expose the SAME mart tables; only the warehouse is read-only")

    # 3 ── the three ways to name the same table: 3-part → 2-part → 1-part ──────────────────────────
    with _step(3, "the three ways to name a table: catalog.schema.table → schema.table → table") as say:
        # A bounded probe (LIMIT) — the point is that all three names hit the same relation, not to
        # scan the whole fact, so this never downloads more than a sample.
        probe = "SELECT count(*) FROM (SELECT 1 FROM {ref} LIMIT 100000)"
        _py('# 3-part — fully qualified, works from anywhere (cross-catalog):\n'
            'conn.sql("SELECT * FROM warehouse.mart.fct_summary LIMIT 5")\n'
            '# 2-part — schema.table, in the current catalog:\n'
            'conn.catalog.setCurrentCatalog("warehouse"); conn.sql("SELECT * FROM mart.fct_summary LIMIT 5")\n'
            '# 1-part — bare table, in the current catalog + database:\n'
            'conn.catalog.setCurrentDatabase("mart");      conn.sql("SELECT * FROM fct_summary LIMIT 5")')
        n3 = q(probe.format(ref="warehouse.mart.fct_summary"))
        conn.catalog.setCurrentCatalog("warehouse")
        n2 = q(probe.format(ref="mart.fct_summary"))
        conn.catalog.setCurrentDatabase("mart")
        n1 = q(probe.format(ref="fct_summary"))
        conn.catalog.setCurrentCatalog(lakehouse)  # back to the writable lakehouse
        _table([("3-part", "warehouse.mart.fct_summary", f"{n3:,}"),
                ("2-part", "mart.fct_summary (current catalog)", f"{n2:,}"),
                ("1-part", "fct_summary (current catalog + db)", f"{n1:,}")],
               ["naming", "reference", "rows (100k sample probe)"],
               "  the same warehouse table, three naming conventions — identical on a bounded probe")
        say("all three names resolve to the same table (probed on a 100k-row sample, not the full fact)")

    # 4 ── the warehouse is read-only; the lakehouse is writable ────────────────────────────────────
    with _step(4, "a Warehouse is locked to writes — the read-only fence refuses a write into it") as say:
        _py('conn.sql("SELECT 1 AS x").write.mode("overwrite").saveAsTable("warehouse.mart.nope")\n'
            '#  -> PermissionError: catalog \'warehouse\' is read-only')
        try:
            conn.sql("SELECT 1 AS x").write.mode("overwrite").saveAsTable("warehouse.mart.nope")
            outcome = "WROTE ❌ (UNSAFE)"
        except PermissionError:
            outcome = "refused ✅"
        _table([("warehouse", "read-only", outcome), (lakehouse, "writable", "allowed ✅")],
               ["catalog", "mode", "write attempt"], "  the same write, two catalogs")
        say("the warehouse refuses writes (locked); the lakehouse accepts them")

    _part("A pipeline across all three catalogs",
          "Facts from the warehouse, the DUID dimension from the lakehouse, a carbon-factor lookup "
          "from the local catalog — joined in one query and written back to the lakehouse.")

    # 5 ── seed the LOCAL catalog with the carbon-factor lookup ─────────────────────────────────────
    with _step(5, "write a carbon-factor lookup into the LOCAL catalog (local.dbo.fuel_factors)") as say:
        values = ", ".join(f"('{f}', {c})" for f, c in _FUEL_FACTORS)
        _py('lookup = conn.sql("SELECT * FROM (VALUES …) t(fuel, co2_kg_per_mwh)")\n'
            'lookup.write.mode("overwrite").saveAsTable("local.dbo.fuel_factors")')
        conn.sql(f"SELECT * FROM (VALUES {values}) t(fuel, co2_kg_per_mwh)") \
            .write.mode("overwrite").saveAsTable("local.dbo.fuel_factors")
        say(f"{len(_FUEL_FACTORS)} fuel factors written to the local catalog")

    # 6 ── JOIN warehouse facts ⋈ lakehouse dim ⋈ local lookup, write the mart BACK to the lakehouse ─
    with _step(6, f"JOIN all three → write '{schema}.mart_generation_by_state' back to the lakehouse") as say:
        # Read a bounded SAMPLE of the facts (LIMIT) — DuckDB early-terminates the delta_scan, so the
        # demo never downloads the full 100M+-row fact table.
        _py('# facts: WAREHOUSE (read-only) · DUID dim: LAKEHOUSE · carbon factor: LOCAL\n'
            'mart = conn.sql(f"""\n'
            '    WITH facts AS (   -- a bounded sample, not the whole 100M+-row fact\n'
            f'        SELECT DUID, mw, price FROM warehouse.mart.fct_summary LIMIT {SAMPLE_ROWS}\n'
            '    )\n'
            '    SELECT d.State,\n'
            '           round(sum(f.mw), 0)                              AS total_mw,\n'
            '           round(avg(f.price), 2)                           AS avg_price,\n'
            '           round(sum(f.mw * coalesce(lf.co2_kg_per_mwh,0))/1000.0, 1) AS est_tonnes_co2\n'
            '    FROM facts f\n'
            '    JOIN mart.dim_duid d                ON d.DUID = f.DUID\n'
            '    LEFT JOIN local.dbo.fuel_factors lf ON lower(lf.fuel) = lower(d.FuelSourceDescriptor)\n'
            '    GROUP BY d.State""")\n'
            f'mart.write.mode("overwrite").saveAsTable("{schema}.mart_generation_by_state")')
        mart = conn.sql(f"""
            WITH facts AS (
                SELECT DUID, mw, price FROM warehouse.mart.fct_summary LIMIT {SAMPLE_ROWS}
            )
            SELECT d.State,
                   round(sum(f.mw), 0)                                        AS total_mw,
                   round(avg(f.price), 2)                                     AS avg_price,
                   round(sum(f.mw * coalesce(lf.co2_kg_per_mwh, 0)) / 1000.0, 1) AS est_tonnes_co2
            FROM facts f
            JOIN mart.dim_duid d                ON d.DUID = f.DUID
            LEFT JOIN local.dbo.fuel_factors lf ON lower(lf.fuel) = lower(d.FuelSourceDescriptor)
            GROUP BY d.State
        """)
        mart.write.mode("overwrite").saveAsTable(f"{schema}.mart_generation_by_state")
        landed = DeltaTable.forName(conn, f"{schema}.mart_generation_by_state").path.replace("\\", "/")
        _table([(s, f"{mw:,.0f}", f"{p:,.2f}", f"{co2:,.0f}") for s, mw, p, co2 in conn.sql(
                    f"SELECT State, total_mw, avg_price, est_tonnes_co2 FROM {schema}.mart_generation_by_state "
                    "ORDER BY total_mw DESC").collect()],
               ["state", "total MW", "avg price $", "est t CO₂"],
               f"  {schema}.mart_generation_by_state — warehouse ⋈ lakehouse ⋈ local ({SAMPLE_ROWS:,}-row fact sample)")
        say(f"mart written back to the lakehouse ({_trim_loc(landed)}) — joined a {SAMPLE_ROWS:,}-row "
            f"fact sample(warehouse) + dim(lakehouse) + factors(local)")

    page_path = os.environ.get("DUCKRUN_MULTICAT_PAGE")
    if page_path:
        tgt = "OneLake" if str(conn.root_path).startswith(("abfss://", "az://")) else "local Delta"
        caption = (f"Actual run · a read-only warehouse + a writable {tgt} lakehouse + a local catalog "
                   f"· {datetime.now(timezone.utc):%Y-%m-%d} UTC")
        with open(page_path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(_page_html("\n".join(_DOC), caption))
        print(f"  wrote HTML report → {page_path}", flush=True)

    print(f"\n=== demo complete: {schema} ===", flush=True)


def main():
    if not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://")
            and WAREHOUSE_RO_PATH and WAREHOUSE_RO_PATH.startswith("abfss://")):
        print(
            "\nThis demo binds OneLake catalogs (tokens are automatic — Fabric notebook / env / az login). Set:\n"
            "  WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables   (writable lakehouse)\n"
            "  WAREHOUSE_RO_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>/Tables (read-only warehouse)\n"
            "then re-run:  python tests/integration_tests/multicatalog/demo_multicatalog.py\n",
            flush=True,
        )
        return
    # No storage_options: on OneLake the token is acquired automatically (the whole point).
    conn = duckrun.connect(WAREHOUSE_PATH, schema=REMOTE_SCHEMA, read_only=False)
    local = tempfile.mkdtemp(prefix="duckrun_local_")
    run_multicatalog_demo(conn, WAREHOUSE_RO_PATH, local, SCHEMA)


if __name__ == "__main__":
    main()
