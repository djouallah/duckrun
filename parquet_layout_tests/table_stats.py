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
_NUM = {"total_rows", "num_files", "num_row_groups", "avg_row_group", "size_mb"}
_FLOAT = {"avg_row_group", "size_mb"}


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


if __name__ == "__main__":
    main()
