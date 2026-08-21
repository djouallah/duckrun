"""Side-by-side parquet layout of the two writers, everything else held equal.

Reads the pair `writer_ab.yml` just built — `tests.fct_summary_ab_deltars` (delta-rs CTAS) and
`tests.fct_summary_ab_duckdb` (DuckDB COPY + delta-rs commit-only) — and prints what the layout
actually looks like: table geometry, per-column bytes and encodings, and how well each writer kept
the sort key clustered across files.

No DAX, no semantic model, no V-Order: the question here is purely "given the same source, the same
sort key and the same geometry knobs, what parquet does each writer emit?".

Env: ONELAKE_TABLES_PATH (resolve_env), AB_PREFIX (table-name prefix, default 'fct_summary_ab'),
OPT_SORT (to know which column clustering is measured on; default 'date, time'), RUN_REPORT.
"""
import os
import sys

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

PREFIX = os.environ.get("AB_PREFIX") or "fct_summary_ab"
SORT_COL = ((os.environ.get("OPT_SORT") or "date, time").split(",")[0]).strip()

con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"], schema="tests")

stats = con.get_stats(f"{PREFIX}_*").fetchall()          # catalog, schema, table, rows, files, ...
if not stats:
    raise SystemExit(f"compare_writers: no tables matched tests.{PREFIX}_* — did the builds run?")
tables = sorted(r[2] for r in stats)

# build seconds / engine come from whatever the two builders merged into the run report
_rep = {}
_path = os.environ.get("RUN_REPORT", "run_report.json")
if os.path.exists(_path):
    import json
    with open(_path, encoding="utf-8") as f:
        _rep = json.load(f).get("tables", {})


def _build(t, field, default=None):
    return (_rep.get(t) or {}).get("build", {}).get(field, default)


# ---- per-column bytes + encodings, and the per-file sort-key ranges, in one detailed pass
detail = con.get_stats(f"{PREFIX}_*", detailed=True)
cols = detail.aggregate("""
    "table", path_in_schema, any_value(type) as type,
    round(sum(total_compressed_size)/1048576.0, 1) as comp_mb,
    round(sum(total_uncompressed_size)/1048576.0, 1) as uncomp_mb,
    string_agg(distinct encodings, ' | ') as encodings
""").fetchall()

# Clustering: adjacent files must not overlap on the sort key. Footer stats come back as strings,
# so cast to the live column type — lexicographic order is not numeric order.
coltype = dict(con.con.execute(
    f'select column_name, column_type from (describe select * from tests.{tables[0]})').fetchall()
)[SORT_COL]
ranges = detail.filter(f"path_in_schema = '{SORT_COL}'").aggregate(f"""
    "table", file_name,
    min(cast(stats_min_value as {coltype})) as mn,
    max(cast(stats_max_value as {coltype})) as mx
""").order('"table", mn').fetchall()

overlap = {}
for t in tables:
    r = [(a, b) for tbl, _f, a, b in ranges if tbl == t]
    overlap[t] = (sum(1 for x, y in zip(r, r[1:]) if y[0] < x[1]), max(len(r) - 1, 1))

out = [f"## Writer A/B — parquet layout ({', '.join(tables)})", ""]
out.append("| metric | " + " | ".join(tables) + " |")
out.append("|---|" + "---|" * len(tables))


def _row(label, fn):
    out.append(f"| {label} | " + " | ".join(str(fn(t)) for t in tables) + " |")


by_table = {r[2]: r for r in stats}
_row("writer", lambda t: _build(t, "engine", "?"))
_row("sort", lambda t: _build(t, "sort", "?"))
_row("build seconds", lambda t: _build(t, "seconds", "?"))
_row("rows", lambda t: f"{by_table[t][3]:,}")
_row("size (MB)", lambda t: by_table[t][7])
_row("files", lambda t: by_table[t][4])
_row("row groups", lambda t: by_table[t][5])
_row("avg row group", lambda t: f"{by_table[t][6]:,.0f}")
_row("compression", lambda t: by_table[t][9])   # 8 is vorder, 9 is compression
_row(f"{SORT_COL} overlap across files", lambda t: f"{overlap[t][0]}/{overlap[t][1]}")

out += ["", f"### Per column (sorted by size in {tables[0]})", ""]
out.append("| column | type | " + " | ".join(f"{t} comp / uncomp MB" for t in tables)
           + " | encodings |")
out.append("|---|---|" + "---|" * len(tables) + "---|")
names = [c[1] for c in cols if c[0] == tables[0]]
names.sort(key=lambda n: -next(c[3] for c in cols if c[0] == tables[0] and c[1] == n))
for n in names:
    cells, encs = [], []
    for t in tables:
        c = next((c for c in cols if c[0] == t and c[1] == n), None)
        cells.append(f"{c[3]} / {c[4]}" if c else "—")
        if c:
            encs.append(f"{t.rsplit('_', 1)[-1]}: {c[5]}")
    typ = next(c[2] for c in cols if c[0] == tables[0] and c[1] == n)
    out.append(f"| {n} | {typ} | " + " | ".join(cells) + " | " + "<br>".join(encs) + " |")

text = "\n".join(out)
print("\n" + text, flush=True)
summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    with open(summary, "a", encoding="utf-8") as f:
        f.write(text + "\n\n")

report.merge({"writer_ab": {t: {
    "size_mb": by_table[t][7], "files": by_table[t][4], "row_groups": by_table[t][5],
    "avg_row_group": by_table[t][6], "engine": _build(t, "engine"),
    "seconds": _build(t, "seconds"),
    "sort_key_overlap": f"{overlap[t][0]}/{overlap[t][1]}",
} for t in tables}})
