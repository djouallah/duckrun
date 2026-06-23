#!/usr/bin/env python3
"""Build the duckrun GitHub Pages site (https://djouallah.github.io/duckrun/).

Renders the markdown docs (docs/*.md) into self-contained, navigable HTML pages
under docs/site/ and regenerates the homepage (docs/site/index.html). The
markdown is the single source of truth — the generated *.html are git-ignored
and rebuilt on every deploy (and can be rebuilt locally to preview).

The per-project dbt DAGs (aemo.html, coffee.html, sde_dbt_tutorial.html,
merge_spill.html) and the live demo reports (taxi.html, multicatalog.html) are
produced by the integration workflow, NOT here — this script never touches them.

Usage:
    python docs/build_site.py              # writes docs/site/*.html
    python docs/build_site.py --serve      # build, then serve docs/site/ on :8000
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent
SITE = DOCS / "site"
REPO = "https://github.com/djouallah/duckrun"
REPO_BLOB = f"{REPO}/blob/main"
RAW = "https://raw.githubusercontent.com/djouallah/duckrun/main"

# Ordered doc pages, grouped for the sidebar: (markdown, output html, label).
GROUPS = [
    ("Guides", [
        ("connection-api.md",     "connection-api.html",     "Connection API"),
        ("dbt-adapter.md",        "dbt-adapter.html",        "dbt adapter"),
        ("spark-delta-parity.md", "spark-delta-parity.html", "Spark / Delta coverage"),
    ]),
    ("Internals", [
        ("design_document.md",    "design_document.html",    "Design document"),
        ("snapshot-isolation.md", "snapshot-isolation.html", "Snapshot isolation"),
        ("snapshot-pin.md",       "snapshot-pin.html",       "Snapshot pin walkthrough"),
    ]),
    ("Benchmarks", [
        ("conformance.md",        "conformance.html",        "dbt conformance"),
        ("merge-benchmark.md",    "merge-benchmark.html",    "MERGE benchmark"),
    ]),
]
PAGES = [p for _, pages in GROUPS for p in pages]
MD_TO_HTML = {src: out for src, out, _ in PAGES}

# Short blurbs for the homepage documentation cards (keyed by output html).
DOC_BLURBS = {
    "connection-api.html":     "The duckrun.connect() notebook API — sql(), DataFrame writes, "
                               "DeltaTable, multi-catalog, raw DML — plus the live method scorecard.",
    "dbt-adapter.html":        "Profiles, Delta-backed materializations, incremental strategies, "
                               "sources, automatic compaction/vacuum, and the design trade-offs.",
    "spark-delta-parity.html": "What the connect() surface maps to in PySpark / Delta-on-Spark — "
                               "what's 1:1, what's duckrun-flavored, what's deliberately out of scope.",
    "design_document.html":    "Why delta-rs (not DuckDB's native Delta writer), why Delta (not "
                               "Iceberg), why a separate adapter — and how the pieces fit.",
    "snapshot-isolation.html": "How a read-modify-write is fenced to the version you read, and how "
                               "it compares to delta-rs, Spark/Delta, and an RDBMS.",
    "snapshot-pin.html":       "The dbt MERGE snapshot pin proved version by version through a real "
                               "dbt run.",
    "conformance.html":        "Official dbt-tests-adapter results, regenerated on every push to main.",
    "merge-benchmark.html":    "The ~60M-row TPCH merge / append / overwrite scorecard — the "
                               "release gate.",
}

# The example / demo gallery (rendered by the integration workflow; linked here).
EXAMPLES = [
    {
        "href": "https://djouallah.github.io/dbt_fabric_python_delta/#!/model/model.aemo_electricity.fct_scada",
        "title": "aemo",
        "desc": "The AEMO dbt project built against live Microsoft Fabric OneLake "
                "(<code>abfss://</code>). Full catalog with column metadata over the real Delta tables.",
        "src_label": "djouallah/dbt_fabric_python_delta",
        "src_href": "https://github.com/djouallah/dbt_fabric_python_delta",
    },
    {
        "href": "merge_spill.html",
        "title": "merge_spill",
        "desc": "The incremental-MERGE spill benchmark (<code>merge_spill_bench</code>): a chain of "
                "merge / append / overwrite ops on a TPCH <code>lineitem</code>. Built for real on a "
                "tiny scale factor, so the catalog carries Delta stats. The heavy 60M-row spill run "
                "is a separate release gate.",
        "src_label": "djouallah/duckrun",
        "src_href": "https://github.com/djouallah/duckrun/tree/main/tests/integration_tests/merge_spill",
    },
    {
        "href": "coffee.html",
        "title": "coffee",
        "desc": "The coffee-shop scenario (<code>coffee_shop</code>): ingest two dimension CSVs over "
                "https, dedup the SCD2 product dim, generate an N-row fact partitioned by region, and "
                "a revenue mart. Built for real on OneLake, so the catalog carries Delta stats.",
        "src_label": "JosueBogran/coffeeshopdatageneratorv2",
        "src_href": "https://github.com/JosueBogran/coffeeshopdatageneratorv2",
    },
    {
        "href": "sde_dbt_tutorial.html",
        "title": "sde_dbt_tutorial",
        "desc": "The <code>josephmachado/simple_dbt_project</code> port: raw tables → bronze typing → a "
                "Delta-backed SCD2 snapshot of the customer dim → a merge-incremental clickstream fact "
                "→ the <code>orders_obt</code> gold mart joined through the SCD2 validity window.",
        "src_label": "josephmachado/simple_dbt_project",
        "src_href": "https://github.com/josephmachado/simple_dbt_project",
    },
    {
        "href": "taxi.html",
        "title": "pure SQL — the connection API",
        "tag": "not a dbt project",
        "desc": "A runnable showcase of <code>duckrun.connect()</code> (<em>not</em> dbt): reads "
                "<strong>live</strong> NYC TLC Yellow-Taxi data straight off <code>https</code> and "
                "lands it into Delta on OneLake using nothing but <code>conn.sql(...)</code> — QUALIFY, "
                "PIVOT, ROLLUP, ASOF JOIN, a SQL-only upsert, time travel, and a concurrent-MERGE "
                "clash. Click through for every statement and its actual output from a live run.",
        "src_label": "demo_taxi.py",
        "src_href": "https://github.com/djouallah/duckrun/blob/main/tests/integration_tests/taxi/demo_taxi.py",
    },
    {
        "href": "multicatalog.html",
        "title": "multi-catalog — lakehouse + warehouse + local",
        "tag": "not a dbt project",
        "desc": "One <code>duckrun.connect()</code> session binding three catalogs: a "
                "<strong>writable</strong> OneLake lakehouse, a <strong>read-only</strong> Fabric "
                "Warehouse (<code>attach(..., read_only=True)</code>), and a local scratch dir. A "
                "single <code>conn.sql</code> JOINs across them as <code>catalog.schema.table</code>, "
                "the read-only fence refuses a warehouse write, and the mart is written back to the "
                "lakehouse. Click through for every step and its actual output.",
        "src_label": "demo_multicatalog.py",
        "src_href": "https://github.com/djouallah/duckrun/blob/main/tests/integration_tests/multicatalog/demo_multicatalog.py",
    },
]


# --------------------------------------------------------------------------- #
# Markdown -> HTML, with repo-relative links rewritten so they resolve on Pages
# --------------------------------------------------------------------------- #
def _rewrite_href(href: str) -> str:
    """Make a markdown link work on the published site.

    - links to a sibling doc we render (foo.md[#a]) -> foo.html[#a]
    - any other repo-relative path (../tests/..., ../.github/...) -> GitHub blob
    - absolute / anchor / mailto links are left untouched
    """
    if href.startswith(("http://", "https://", "#", "mailto:")):
        return href
    path, _, anchor = href.partition("#")
    anchor = f"#{anchor}" if anchor else ""
    if path in MD_TO_HTML:
        return MD_TO_HTML[path] + anchor
    # resolve relative to docs/ and point at the source on GitHub
    repo_rel = os.path.normpath(os.path.join("docs", path)).replace("\\", "/")
    return f"{REPO_BLOB}/{repo_rel}{anchor}"


def md_to_html(text: str) -> str:
    html = markdown.markdown(
        text,
        extensions=["fenced_code", "tables", "toc", "sane_lists", "attr_list"],
    )
    return re.sub(
        r'href="([^"]+)"',
        lambda m: f'href="{_rewrite_href(m.group(1))}"',
        html,
    )


# --------------------------------------------------------------------------- #
# Shared chrome
# --------------------------------------------------------------------------- #
GH_ICON = (
    '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 '
    '3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53'
    '-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 '
    '1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 '
    '0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 '
    '2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 '
    '1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 '
    '2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>'
)

STYLE = """
:root {
  --bg: #ffffff; --fg: #1b1f24; --muted: #5b6573; --faint: #8a93a0;
  --border: #e3e7ec; --soft: #f5f7fa; --accent: #f2c200; --accent-ink: #3a2f00;
  --link: #b58900; --code-bg: #f3f5f8; --maxw: 1180px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0f1216; --fg: #e6e9ee; --muted: #9aa4b2; --faint: #6b7480;
    --border: #262c34; --soft: #161b21; --accent: #f2c200; --accent-ink: #1a1500;
    --link: #f2c200; --code-bg: #161b21;
  }
}
* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0; background: var(--bg); color: var(--fg); line-height: 1.6;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
code {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  background: var(--code-bg); padding: 0.12em 0.38em; border-radius: 5px; font-size: 0.88em;
}
pre {
  background: var(--code-bg); border: 1px solid var(--border); border-radius: 10px;
  padding: 1rem 1.1rem; overflow: auto; line-height: 1.5; font-size: 0.86rem;
}
pre code { background: none; padding: 0; font-size: inherit; }

/* top bar */
.topbar {
  position: sticky; top: 0; z-index: 20; background: color-mix(in srgb, var(--bg) 88%, transparent);
  backdrop-filter: blur(8px); border-bottom: 1px solid var(--border);
}
.topbar-inner {
  max-width: var(--maxw); margin: 0 auto; padding: 0.7rem 1.25rem;
  display: flex; align-items: center; gap: 1.25rem;
}
.brand { font-weight: 800; font-size: 1.1rem; color: var(--fg); letter-spacing: -0.01em; }
.brand:hover { text-decoration: none; }
.brand .dot { color: var(--accent); }
.topnav { display: flex; gap: 1.1rem; margin-left: auto; align-items: center; flex-wrap: wrap; }
.topnav a { color: var(--muted); font-size: 0.92rem; font-weight: 500; }
.topnav a:hover { color: var(--fg); text-decoration: none; }
.gh {
  display: inline-flex; align-items: center; gap: 0.4rem; color: var(--fg);
  border: 1px solid var(--border); border-radius: 8px; padding: 0.35rem 0.7rem; font-weight: 600;
}
.gh:hover { background: var(--soft); text-decoration: none; }
.gh svg { width: 18px; height: 18px; fill: currentColor; }

/* doc layout: sidebar + content */
.layout { max-width: var(--maxw); margin: 0 auto; display: grid; grid-template-columns: 250px 1fr; gap: 2.5rem; padding: 0 1.25rem; }
.sidebar { position: sticky; top: 64px; align-self: start; height: calc(100vh - 64px); overflow-y: auto; padding: 1.75rem 0; }
.sidebar h4 { margin: 1.25rem 0 0.5rem; font-size: 0.72rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--faint); }
.sidebar ul { list-style: none; margin: 0; padding: 0; }
.sidebar li a { display: block; padding: 0.32rem 0.6rem; border-radius: 7px; color: var(--muted); font-size: 0.92rem; }
.sidebar li a:hover { background: var(--soft); color: var(--fg); text-decoration: none; }
.sidebar li a.active { background: color-mix(in srgb, var(--accent) 22%, transparent); color: var(--fg); font-weight: 600; }
.content { min-width: 0; padding: 2.25rem 0 4rem; }

/* prose (rendered markdown) */
.prose { max-width: 800px; }
.prose h1 { font-size: 2rem; line-height: 1.2; margin: 0 0 1rem; letter-spacing: -0.02em; }
.prose h2 { font-size: 1.4rem; margin: 2.4rem 0 0.8rem; padding-bottom: 0.3rem; border-bottom: 1px solid var(--border); }
.prose h3 { font-size: 1.12rem; margin: 1.8rem 0 0.6rem; }
.prose h4 { font-size: 1rem; margin: 1.4rem 0 0.5rem; }
.prose p, .prose li { color: var(--fg); }
.prose a { font-weight: 500; }
.prose table { border-collapse: collapse; width: 100%; margin: 1.2rem 0; font-size: 0.9rem; display: block; overflow-x: auto; }
.prose th, .prose td { border: 1px solid var(--border); padding: 0.5rem 0.7rem; text-align: left; vertical-align: top; }
.prose th { background: var(--soft); font-weight: 600; }
.prose blockquote { margin: 1.2rem 0; padding: 0.3rem 1rem; border-left: 3px solid var(--accent); color: var(--muted); background: var(--soft); border-radius: 0 8px 8px 0; }
.prose img { max-width: 100%; border: 1px solid var(--border); border-radius: 10px; }
.prose hr { border: none; border-top: 1px solid var(--border); margin: 2rem 0; }

/* homepage */
.home { max-width: var(--maxw); margin: 0 auto; padding: 0 1.25rem; }
.hero { text-align: center; padding: 3.5rem 1rem 2.5rem; }
.hero img.logo { width: 260px; max-width: 70%; }
.hero p.tagline { font-size: 1.25rem; color: var(--muted); max-width: 680px; margin: 1rem auto 0; }
.hero p.tagline strong { color: var(--fg); }
.badges { margin: 1.4rem 0 0; display: flex; gap: 0.4rem; justify-content: center; flex-wrap: wrap; }
.badges img { height: 20px; }
.cta { margin-top: 1.8rem; display: flex; gap: 0.75rem; justify-content: center; flex-wrap: wrap; }
.btn { display: inline-block; padding: 0.6rem 1.3rem; border-radius: 9px; font-weight: 600; border: 1px solid var(--border); }
.btn:hover { text-decoration: none; }
.btn.primary { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); }
.btn.primary:hover { filter: brightness(1.05); }
.btn.ghost { color: var(--fg); }
.btn.ghost:hover { background: var(--soft); }

.section { padding: 2.5rem 0; border-top: 1px solid var(--border); }
.section > h2 { font-size: 1.6rem; margin: 0 0 0.4rem; letter-spacing: -0.01em; }
.section > p.lede { color: var(--muted); margin: 0 0 1.5rem; max-width: 720px; }

.split { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; align-items: start; }
.split .prose { max-width: none; }

.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 1rem; }
.card { border: 1px solid var(--border); border-radius: 12px; padding: 1.1rem 1.25rem; background: var(--soft); }
.card h3 { margin: 0 0 0.4rem; font-size: 1.05rem; }
.card p { margin: 0; color: var(--muted); font-size: 0.92rem; }
.card.link:hover { border-color: var(--accent); }
.card.link a { color: var(--fg); }

.gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 1rem; }
.tile { border: 1px solid var(--border); border-radius: 12px; padding: 1.1rem 1.25rem; }
.tile > a.title { font-size: 1.1rem; font-weight: 700; color: var(--fg); }
.tile .desc { display: block; color: var(--muted); font-size: 0.9rem; margin-top: 0.4rem; }
.tile .src { display: block; margin-top: 0.6rem; font-size: 0.82rem; color: var(--faint); }
.tag { font-size: 0.6rem; font-weight: 700; color: #fff; background: #c0392b; padding: 0.12rem 0.45rem; border-radius: 6px; vertical-align: middle; margin-left: 0.4rem; }
figure.shot { margin: 0; }
figure.shot img { width: 100%; display: block; border: 1px solid var(--border); border-radius: 10px; }
figure.shot figcaption { color: var(--muted); font-size: 0.88rem; margin-top: 0.5rem; }

footer { border-top: 1px solid var(--border); margin-top: 3rem; }
footer .inner { max-width: var(--maxw); margin: 0 auto; padding: 1.75rem 1.25rem; color: var(--faint); font-size: 0.85rem; display: flex; gap: 1rem; flex-wrap: wrap; justify-content: space-between; }
footer a { color: var(--muted); }

@media (max-width: 820px) {
  .layout { grid-template-columns: 1fr; gap: 0; }
  .sidebar { position: static; height: auto; border-bottom: 1px solid var(--border); padding: 1rem 0; }
  .split { grid-template-columns: 1fr; }
}
"""


def topbar() -> str:
    links = (
        '<a href="connection-api.html">Connection API</a>'
        '<a href="dbt-adapter.html">dbt adapter</a>'
        '<a href="design_document.html">Design</a>'
        '<a href="index.html#examples">Examples</a>'
    )
    return f"""<header class="topbar"><div class="topbar-inner">
  <a class="brand" href="index.html">duck<span class="dot">·</span>run</a>
  <nav class="topnav">{links}
    <a class="gh" href="{REPO}">{GH_ICON}GitHub</a>
  </nav>
</div></header>"""


def sidebar(active: str) -> str:
    out = ['<aside class="sidebar"><ul>',
           '<li><a href="index.html">Home</a></li>',
           '<li><a href="index.html#examples">Examples</a></li></ul>']
    for group, pages in GROUPS:
        out.append(f"<h4>{group}</h4><ul>")
        for _, html, label in pages:
            cls = ' class="active"' if html == active else ""
            out.append(f'<li><a href="{html}"{cls}>{label}</a></li>')
        out.append("</ul>")
    out.append("</aside>")
    return "".join(out)


def page(title: str, body: str, *, description: str = "") -> str:
    desc = f'<meta name="description" content="{description}">' if description else ""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
{desc}
<style>{STYLE}</style>
</head>
<body>
{topbar()}
{body}
{footer()}
</body>
</html>
"""


def footer() -> str:
    return f"""<footer><div class="inner">
  <span>duckrun — DuckDB executes · delta-rs materializes · Arrow bridges · dbt orchestrates.</span>
  <span><a href="{REPO}">GitHub</a> · <a href="https://pypi.org/project/duckrun/">PyPI</a> ·
  <a href="{REPO}/blob/main/LICENSE">MIT</a></span>
</div></footer>"""


# --------------------------------------------------------------------------- #
# Page builders
# --------------------------------------------------------------------------- #
def build_doc_page(src: str, html_name: str, label: str) -> None:
    md = (DOCS / src).read_text(encoding="utf-8")
    article = md_to_html(md)
    body = f"""<div class="layout">
{sidebar(html_name)}
<main class="content"><article class="prose">{article}</article></main>
</div>"""
    (SITE / html_name).write_text(
        page(f"{label} — duckrun", body, description=DOC_BLURBS.get(html_name, "")),
        encoding="utf-8",
    )


# Markdown for the homepage's narrative middle (install / quickstart / how it works),
# rendered through the same converter as the docs so the code blocks match.
HOME_NARRATIVE = f"""
### Install

```bash
pip install duckrun
```

In a **Microsoft Fabric** notebook, upgrade and restart the kernel (duckrun needs
`duckdb` >= 1.5.4, newer than the bundled stable build):

```python
!pip install duckrun --upgrade
notebookutils.session.restartPython()
```

### Read a lakehouse (read-only by default)

```python
import duckrun

# Read-only by default — explore safely, no chance of an accidental write.
conn = duckrun.connect("abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/dbo")

conn.sql("show tables").show()
conn.sql("select status, count(*) from orders group by status").show()
df = conn.table("orders").toPandas()
```

### Write Delta straight from SQL (opt in)

```python
conn = duckrun.connect(".../Tables/dbo", read_only=False)

conn.sql("select * from orders where amount > 0") \\
    .write.mode("overwrite").saveAsTable("clean_orders")

# raw DML routes to delta-rs (insert / update / delete / create table as / alter / drop)
conn.sql("delete from clean_orders where amount = 0")

# upsert — snapshot-pinned automatically, nothing extra to pass
from duckrun import DeltaTable
src = conn.sql("select * from updates")
DeltaTable.forName(conn, "clean_orders").merge(src, "target.id = source.id") \\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Or run dbt models as Delta tables

```yaml
# ~/.dbt/profiles.yml
my_project:
  outputs:
    dev:
      type: duckrun
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables"
```
"""


def build_home() -> None:
    narrative = md_to_html(HOME_NARRATIVE)

    feature_cards = [
        ("Snapshot-pinned writes", "Every read-modify-write is fenced to the version it read. A "
         "concurrent commit is rejected with <code>CommitFailedError</code> instead of silently "
         "overwriting a lost update."),
        ("Two engines, split cleanly", "DuckDB runs every query and reads Delta via "
         "<code>delta_scan</code>; delta-rs handles every write; an Arrow C-stream bridges them."),
        ("A real dbt adapter", "A thin wrapper around <code>dbt-duckdb</code> that adds Delta-backed "
         "<code>table</code> / <code>incremental</code> materializations — everything else is inherited."),
        ("Multiple catalogs", "Attach more lakehouses (or a read-only Fabric Warehouse) and "
         "read / join across them by three-part <code>catalog.schema.table</code> name."),
        ("Automatic maintenance", "Compaction, vacuum, and log cleanup run inline on every write — "
         "no <code>OPTIMIZE</code>/<code>VACUUM</code> job to schedule."),
        ("SQL-first DML", "<code>conn.sql</code> applies <code>insert</code>/<code>update</code>/"
         "<code>delete</code>/<code>merge</code> through delta-rs — identical locally and on OneLake."),
    ]
    features = "".join(
        f'<div class="card"><h3>{t}</h3><p>{d}</p></div>' for t, d in feature_cards
    )

    doc_cards = "".join(
        f'<div class="card link"><h3><a href="{html}">{label}</a></h3>'
        f'<p>{DOC_BLURBS.get(html, "")}</p></div>'
        for _, html, label in PAGES
    )

    tiles = []
    for e in EXAMPLES:
        tag = f'<span class="tag">{e["tag"]}</span>' if e.get("tag") else ""
        tiles.append(
            f'<div class="tile"><a class="title" href="{e["href"]}">{e["title"]}</a>{tag}'
            f'<span class="desc">{e["desc"]}</span>'
            f'<span class="src">source: <a href="{e["src_href"]}">{e["src_label"]}</a></span></div>'
        )
    gallery = "".join(tiles)

    body = f"""<main class="home">

  <section class="hero">
    <img class="logo" src="{RAW}/duckrun.png" alt="duckrun">
    <p class="tagline">Run SQL in <strong>DuckDB</strong>, read &amp; write
      <strong>Delta Lake</strong> via delta-rs — locally or on OneLake / S3 / GCS / ADLS.
      It's just glue: <strong>DuckDB executes · delta-rs materializes · Arrow bridges · dbt orchestrates.</strong></p>
    <div class="badges">
      <a href="https://pypi.org/project/duckrun/"><img src="https://img.shields.io/pypi/v/duckrun?color=blue&label=PyPI&logo=pypi&logoColor=white" alt="PyPI"></a>
      <a href="https://pepy.tech/project/duckrun"><img src="https://static.pepy.tech/badge/duckrun" alt="Downloads"></a>
      <a href="https://pypi.org/project/duckrun/"><img src="https://img.shields.io/pypi/pyversions/duckrun?logo=python&logoColor=white" alt="Python"></a>
      <a href="{REPO}/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/duckrun?color=lightgrey" alt="License"></a>
    </div>
    <div class="cta">
      <a class="btn primary" href="#install">Get started</a>
      <a class="btn ghost" href="connection-api.html">Connection API</a>
      <a class="btn ghost" href="dbt-adapter.html">dbt adapter</a>
      <a class="btn ghost" href="{REPO}">{GH_ICON}GitHub</a>
    </div>
  </section>

  <section class="section" id="install">
    <h2>Quickstart</h2>
    <p class="lede">Two ways to use it: a notebook <code>connect()</code> helper for ad-hoc SQL over
      Delta, and a dbt adapter that materializes models as Delta tables. Concurrent writers are
      first-class — every write is snapshot-pinned and fails loud rather than silently interleaving.</p>
    <article class="prose">{narrative}</article>
  </section>

  <section class="section" id="how">
    <h2>How it works</h2>
    <p class="lede">Two engines, split cleanly: DuckDB runs every query and reads Delta through
      <code>delta_scan</code> views, delta-rs handles every write, an Arrow C-stream bridges them,
      and dbt orchestrates on top.</p>
    <figure class="shot">
      <img src="{RAW}/docs/architecture.png" alt="duckrun architecture: DuckDB executes SQL and reads Delta via delta_scan; an Arrow C-stream bridges to delta-rs, which handles every write and commits against the read version (OCC); dbt orchestrates on top">
    </figure>
    <p class="lede" style="margin-top:1.5rem">Writes are snapshot-pinned: the read is fixed at
      <code>delta_scan(…, version =&gt; N)</code> and the write commits against <code>N</code>, so a
      concurrent commit is rejected with <code>CommitFailedError</code> instead of silently
      overwriting a lost update.</p>
    <figure class="shot">
      <img src="{RAW}/docs/snapshot-timeline.png" alt="Two writers race on one table: Writer A reads v5 and computes; Writer B commits v6 in between; A's commit against v5 is rejected with CommitFailedError instead of silently overwriting B">
    </figure>
  </section>

  <section class="section">
    <h2>Why duckrun</h2>
    <div class="cards">{features}</div>
  </section>

  <section class="section" id="docs">
    <h2>Documentation</h2>
    <p class="lede">The full reference for both surfaces, the design rationale, and the benchmarks.</p>
    <div class="cards">{doc_cards}</div>
  </section>

  <section class="section" id="examples">
    <h2>Examples</h2>
    <p class="lede">Real dbt projects materialized as Delta tables, plus runnable connection-API
      showcases — every one rebuilt against live Microsoft Fabric OneLake on each push to
      <code>main</code>.</p>
    <figure class="shot" style="margin-bottom:1.5rem">
      <img src="duckrun_dbt_output.png" alt="dbt models materialized by duckrun as Delta tables in a Microsoft Fabric OneLake lakehouse">
      <figcaption>The DAGs below, materialized — duckrun writes each dbt model as a Delta table,
        here in a Microsoft Fabric OneLake lakehouse.</figcaption>
    </figure>
    <div class="gallery">{gallery}</div>
  </section>

</main>"""
    (SITE / "index.html").write_text(
        page("duckrun — DuckDB + Delta Lake, locally or on OneLake", body,
             description="duckrun runs SQL in DuckDB and reads/writes Delta Lake via delta-rs — "
                         "a notebook connect() helper and a dbt adapter, locally or on OneLake / "
                         "S3 / GCS / ADLS."),
        encoding="utf-8",
    )


def main() -> None:
    SITE.mkdir(parents=True, exist_ok=True)
    for src, html_name, label in PAGES:
        build_doc_page(src, html_name, label)
    build_home()
    built = ["index.html"] + [h for _, h, _ in PAGES]
    print(f"Built {len(built)} pages into {SITE}:")
    for name in built:
        print(f"  - {name}")

    if "--serve" in sys.argv:
        import http.server
        import socketserver
        os.chdir(SITE)
        with socketserver.TCPServer(("", 8000), http.server.SimpleHTTPRequestHandler) as httpd:
            print("Serving docs/site/ at http://localhost:8000  (Ctrl-C to stop)")
            httpd.serve_forever()


if __name__ == "__main__":
    main()
