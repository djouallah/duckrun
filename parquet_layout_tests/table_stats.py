"""duckrun get_stats for the summary tables — BOTH views:
  1. per-TABLE summary  (get_stats('fct_summary*'))
  2. per-ROW-GROUP detail (get_stats('fct_summary*', detailed=True)) — the raw parquet row-group
     metadata, where the vorder vs optimized layout difference actually shows (how many rows / MB
     sit in each parquet row group).
Console tables + markdown sections in the GitHub Actions job summary.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN (minted in the workflow).
"""
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Right-align these (numeric); everything else left-aligns.
_NUM = {"total_rows", "num_files", "num_row_groups", "avg_row_group", "size_mb",
        "uncompressed_mb", "compressed_mb", "ratio"}
_FLOAT = {"avg_row_group", "size_mb", "uncompressed_mb", "compressed_mb", "ratio"}


def _fmt(col, v):
    if v is None:
        return ""
    if col in _NUM:
        try:
            return f"{float(v):,.1f}" if col in _FLOAT else f"{int(v):,}"
        except (TypeError, ValueError):
            return str(v)
    return str(v)


def _markdown(title, note, cols, rows):
    align = ["--:" if c in _NUM else ":--" for c in cols]
    out = [title, "",
           "| " + " | ".join(cols) + " |",
           "| " + " | ".join(align) + " |"]
    for r in rows:
        out.append("| " + " | ".join(_fmt(c, v) for c, v in zip(cols, r)) + " |")
    if note:
        out += ["", note, ""]
    return "\n".join(out)


def _write_summary(md):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")


def main():
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/mart",
                          storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})

    # --- 1) Per-table summary (feeds the report card) ---
    summ = con.get_stats("fct_summary*")
    print("\n=== Per-table summary — get_stats('fct_summary*') ===")
    try:
        summ.show(max_width=100000, max_col_width=1000)
    except TypeError:
        summ.show()
    _write_summary(_markdown(
        "## 📊 Table layout — duckrun `get_stats('fct_summary*')`",
        "_`vorder` = Fabric V-Order flag; `avg_row_group` = rows per row group "
        "(smaller ⇒ finer granularity, usually faster Direct Lake cold transcode)._",
        summ.columns, summ.fetchall()))

    # --- 2) Full detail (detailed=True) — raw parquet_metadata -> CSV only (no report-card noise).
    # Uploaded as the 'table-stats-detailed' artifact; download into Excel / pandas.
    det = con.get_stats("fct_summary*", detailed=True)
    det_cols, det_rows = det.columns, det.fetchall()
    import csv as _csv
    csv_path = os.environ.get("STATS_CSV", "stats_detailed.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(det_cols)
        w.writerows(det_rows)
    print(f"\nwrote detailed stats CSV -> {os.path.abspath(csv_path)} "
          f"({len(det_rows)} rows × {len(det_cols)} cols)")

    # --- 3) RLE / encoding proxy — per column, which encoding it landed on (RLE_DICTIONARY vs PLAIN)
    # and its compression ratio. Parquet stores no run *count*, but the encoding + uncompressed/compressed
    # ratio is the observable signal of how well RLE/dictionary found runs (what a sort manufactures);
    # compare the two layouts column by column. Best-effort — parquet_metadata column names can shift
    # between DuckDB versions, so any failure just skips this section.
    try:
        enc = det.query("d", """
            SELECT "table" AS tbl, path_in_schema AS column,
                   string_agg(DISTINCT encodings, ' ') AS encodings,
                   ROUND(SUM(total_uncompressed_size) / 1e6, 1) AS uncompressed_mb,
                   ROUND(SUM(total_compressed_size) / 1e6, 1) AS compressed_mb,
                   ROUND(SUM(total_uncompressed_size)::DOUBLE
                         / NULLIF(SUM(total_compressed_size), 0), 2) AS ratio
            FROM d
            WHERE "table" IN ('fct_summary_optimized', 'fct_summary_vorder')
              AND path_in_schema NOT IN ('', 'schema')
            GROUP BY "table", path_in_schema
            ORDER BY column, "table"
        """)
        print("\n=== Per-column encoding & compression (RLE proxy) ===")
        try:
            enc.show(max_width=100000, max_col_width=1000)
        except TypeError:
            enc.show()
        _write_summary(_markdown(
            "## 🧬 Per-column encoding & compression (RLE proxy)",
            "_`encodings` = the Parquet encodings the column landed on (`RLE_DICTIONARY` vs `PLAIN`); "
            "`ratio` = uncompressed / compressed — higher ⇒ RLE/dictionary found longer runs, which is "
            "what a global sort manufactures. Compare `fct_summary_optimized` (duckrun) vs "
            "`fct_summary_vorder` row by row._",
            enc.columns, enc.fetchall()))
    except Exception as e:
        print(f"(encoding / RLE summary skipped: {str(e).splitlines()[0][:140]})")


if __name__ == "__main__":
    main()
