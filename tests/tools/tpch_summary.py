"""Render a duckrun TPC-H benchmark run (ingestion + 22 query timings) as a Markdown "card" for
the docs and the GitHub job summary.

Pure formatting: ``render_card(timings)`` takes the dict that
``tests/integration_tests/tpch/test_tpch.py::run_tpch_benchmark`` fills via its ``timings_out``
argument and returns the card markdown. The driver that actually runs the benchmark and writes the
card is ``tests/integration_tests/tpch/tpch_card.py``.
"""
import sys

# The card contains box-drawing; force UTF-8 so it prints on a Windows cp1252 console too.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _human(n: int) -> str:
    return f"{n / 1e6:.1f}M" if n >= 1_000_000 else f"{n:,}"


def _box(lines):
    """An at-a-glance monospaced box (fenced so it renders fixed-width)."""
    width = max(len(s) for s in lines) + 1
    out = ["```", "┌" + "─" * (width + 1) + "┐"]
    out += ["│ " + s.ljust(width) + "│" for s in lines]
    out += ["└" + "─" * (width + 1) + "┘", "```"]
    return out


def render_card(t: dict) -> str:
    ingestion = t["ingestion"]
    queries = t["queries"]
    ing_total = sum(r["dur"] for r in ingestion)
    qry_total = sum(r["dur"] for r in queries)
    rows_total = sum(r["rows"] for r in ingestion)

    L = ["## 🐤 TPC-H benchmark — duckrun on Delta Lake", ""]
    L.append(
        "**What this checks:** duckrun ingests the full TPC-H schema (8 tables) from Parquet into "
        "Delta through its write path (`conn.read.parquet(...).write.saveAsTable(...)`), then runs "
        "the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is "
        "duckrun's write path; the **query** times are DuckDB reading Delta — there is no second "
        "engine to race here, so read them as \"the whole schema loads and all 22 queries run at "
        "this scale\", not a *duckrun is fast* claim."
    )
    L.append("")
    L += _box([
        f"ingest {len(ingestion)} tables {ing_total:9.1f}s",
        f"run 22 queries {qry_total:9.1f}s",
        f"SF {t['sf']}  -  {_human(rows_total)} rows  -  {t['cpu']} cores",
    ])
    L.append("")

    L += ["### Setup", "| | |", "|---|---|"]
    L.append(f"| Engine | duckrun &middot; DuckDB {t.get('duckdb', '?')} &middot; "
             f"delta_rs {t.get('deltalake', '?')} |")
    L.append(f"| Scale factor | **{t['sf']}** |")
    L.append(f"| Runner | GitHub-hosted &middot; {t['cpu']} cores |")
    L.append("")

    L += ["### Ingestion — Parquet → Delta (duckrun write path)",
          "| Table | Rows | Write (s) |", "|---|---:|---:|"]
    for r in ingestion:
        L.append(f"| `{r['table']}` | {r['rows']:,} | {r['dur']:.2f} |")
    L.append(f"| **Total** | **{rows_total:,}** | **{ing_total:.2f}** |")
    L.append("")

    L += ["### Queries — 22 TPC-H over `delta_scan`",
          "| Query | Duration (s) |", "|:---|---:|"]
    for r in queries:
        L.append(f"| Q{r['query']:02d} | {r['dur']:.3f} |")
    L.append(f"| **Total** | **{qry_total:.2f}** |")
    L.append("")

    return "\n".join(L) + "\n"
