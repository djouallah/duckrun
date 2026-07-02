"""Multi-lakehouse **dbt** showcase — a Bronze/Silver/Gold medallion, each layer materialized into a
SEPARATE Fabric Lakehouse via the duckrun `catalogs:` feature, then read back and rendered to HTML.

The companion to `demo_multicatalog.py` (the connection-API showcase). Where that one binds catalogs
in one live session, this one shows the **dbt** path: `catalogs:` in the profile + `+database: <alias>`
on a model routes each layer to its own Lakehouse, and `ref()`/joins resolve across them.

Runs `dbt run` against `integration_tests/multicatalog/dbt`, then reads each layer back from its own
Lakehouse root and writes a standalone report when `DUCKRUN_MULTICAT_DBT_PAGE` is set (CI publishes it
as multicatalog_dbt.html).

Env — three Lakehouse roots (three `abfss://` on OneLake, or three folders locally):
    LH_BRONZE_PATH   raw layer          LH_SILVER_PATH   cleaned (default catalog)   LH_GOLD_PATH  serving
    ONELAKE_TOKEN    OneLake bearer token (blank for local)   DBT_SCHEMA  schema to write into (default main)
    DUCKRUN_MULTICAT_DBT_PAGE   write the HTML report here (optional)
"""
import html
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckrun
from dbt.cli.main import dbtRunner

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PROJECT_DIR = str(Path(__file__).parent / "dbt")
SCHEMA = os.environ.get("DBT_SCHEMA", "main")
TOKEN = os.environ.get("ONELAKE_TOKEN") or ""
LAYERS = [  # (layer, catalog, table, root env var)
    ("Bronze", "lh_bronze", "raw_generation", "LH_BRONZE_PATH"),
    ("Silver", "(default)", "clean_generation", "LH_SILVER_PATH"),
    ("Gold", "lh_gold", "generation_by_fuel", "LH_GOLD_PATH"),
]


def _so():
    return {"bearer_token": TOKEN} if TOKEN else None


def _trim(path):
    p = str(path).replace("\\", "/")
    if "/Tables" in p and "://" in p:
        return f"{p.split('://', 1)[0]}://…/Tables/{p.split('/Tables', 1)[1].lstrip('/')}".rstrip("/")
    return "…/" + "/".join([s for s in p.split("/") if s][-2:])


def _readback(root, table):
    con = duckrun.connect(root, schema=SCHEMA, storage_options=_so(), read_only=True)
    try:
        cols = [c[0] for c in con.sql(f"select * from {table} limit 0").description]
        rows = con.sql(f"select * from {table}").fetchall()
        return cols, rows
    finally:
        con.stop()


def main():
    roots = {env: os.environ.get(env) for _, _, _, env in LAYERS}
    missing = [e for e, v in roots.items() if not v]
    if missing:
        # Local convenience: default any unset root to a temp folder so the demo still runs offline.
        base = tempfile.mkdtemp(prefix="duckrun_mc_dbt_")
        for e in missing:
            roots[e] = str(Path(base) / e.split("_")[1].lower())
            os.environ[e] = roots[e]
        print(f"(local mode — unset roots defaulted under {base})", flush=True)

    kind = "OneLake" if str(roots["LH_SILVER_PATH"]).startswith(("abfss://", "az://")) else "local"
    print(f"\n=== duckrun multi-lakehouse dbt demo | {kind} | schema={SCHEMA} ===", flush=True)
    for layer, cat, table, env in LAYERS:
        print(f"  {layer:<7} → {cat:<11} {table:<20} @ {_trim(roots[env])}", flush=True)

    res = dbtRunner().invoke(["run", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR])
    if not res.success:
        print("dbt run FAILED", flush=True)
        raise SystemExit(1)

    sections = []
    for layer, cat, table, env in LAYERS:
        cols, rows = _readback(roots[env], table)
        print(f"\n[{layer}] {cat}.{SCHEMA}.{table} — {len(rows)} rows", flush=True)
        print("   " + " | ".join(cols), flush=True)
        for r in rows:
            print("   " + " | ".join(str(c) for c in r), flush=True)
        sections.append((layer, cat, table, roots[env], cols, rows))

    page = os.environ.get("DUCKRUN_MULTICAT_DBT_PAGE")
    if page:
        _write_html(page, kind, sections)
        print(f"\n  wrote HTML report → {page}", flush=True)
    print(f"\n=== demo complete: {SCHEMA} across {len(LAYERS)} lakehouses ===", flush=True)


_CSS = """
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         max-width: 900px; margin: 0 auto; padding: 2.5rem 1.25rem; line-height: 1.5; }
  a.back { font-size: 0.9rem; text-decoration: none; color: #888; }
  h1 { margin: 0.4rem 0 0.2rem; }
  h1 .tag { font-size: 0.7rem; font-weight: 700; color: #fff; background: #2d7d46;
            padding: 0.12rem 0.5rem; border-radius: 6px; vertical-align: middle; margin-left: 0.4rem; }
  p.lede { color: #777; margin-top: 0; }
  section { border-top: 1px solid #8883; padding: 0.6rem 0 0.4rem; }
  h3 { margin: 0.9rem 0 0.3rem; font-size: 1.05rem; }
  h3 .cat { color: #2d7d46; font-variant-numeric: tabular-nums; }
  p.loc { color: #888; font-size: 0.82rem; margin: 0 0 0.5rem; }
  pre.yaml { background: #8881; border-left: 3px solid #2d7d4688; border-radius: 4px;
        padding: 0.6rem 0.8rem; overflow-x: auto; font-size: 0.82rem; line-height: 1.4; margin: 0.5rem 0; }
  table.data { border-collapse: collapse; font-size: 0.85rem; margin: 0.2rem 0 0.8rem; }
  table.data th, table.data td { border: 1px solid #8884; padding: 0.3rem 0.6rem; text-align: left; }
  table.data th { background: #8882; font-weight: 600; }
  code { background: #8882; padding: 0.1rem 0.35rem; border-radius: 4px; }
  footer { margin-top: 2rem; color: #888; font-size: 0.85rem; }
"""

_PROFILE = """outputs:
  dev:
    type: duckrun
    root_path: "<Silver lakehouse>"      # the default catalog
    catalogs:
      lh_bronze: { root_path: "<Bronze lakehouse>" }
      lh_gold:   { root_path: "<Gold lakehouse>" }"""


def _tbl(cols, rows):
    th = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="data"><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>'


def _write_html(path, kind, sections):
    parts = []
    for layer, cat, table, root, cols, rows in sections:
        db = "" if cat == "(default)" else f", database='{cat}'"
        parts.append(
            f'<section><h3>{html.escape(layer)} — <span class="cat">{html.escape(cat)}</span>.'
            f'{html.escape(SCHEMA)}.{html.escape(table)}</h3>'
            f'<p class="loc">{html.escape(_trim(root))} · <code>{{{{ config(materialized=…{html.escape(db)}) }}}}</code></p>'
            f'{_tbl(cols, rows)}</section>'
        )
    caption = (f"Actual {kind} run · one dbt project, a Bronze/Silver/Gold medallion across three "
               f"Lakehouses · {datetime.now(timezone.utc):%Y-%m-%d} UTC")
    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>duckrun — multi-lakehouse dbt (Bronze/Silver/Gold)</title>
<style>{_CSS}</style></head>
<body>
  <a class="back" href="index.html">← duckrun docs</a>
  <h1>multi-lakehouse — one dbt project, three Lakehouses<span class="tag">dbt</span></h1>
  <p class="lede">{html.escape(caption)}. Extra write roots are declared once as named
  <code>catalogs:</code> in the profile; a model picks one with the standard dbt
  <code>+database: &lt;alias&gt;</code>. <code>ref()</code> and joins resolve across Lakehouses.</p>
  <pre class="yaml">{html.escape(_PROFILE)}</pre>
  {''.join(parts)}
  <footer>Generated by
    <a href="https://github.com/djouallah/duckrun/blob/main/integration_tests/multicatalog/demo_multicatalog_dbt.py">demo_multicatalog_dbt.py</a>
    — re-published from a live run by the integration-tests workflow.</footer>
</body></html>
"""
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(doc)


if __name__ == "__main__":
    main()
