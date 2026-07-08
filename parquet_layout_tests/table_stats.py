"""duckrun get_stats for the summary tables → run_report.json (tables.<t>.parquet)
plus a stats_detailed.csv of the raw per-row-group parquet_metadata.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, RUN_REPORT.
"""
import csv as _csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


def main():
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/tests",
                          storage_options={"bearer_token": os.environ["ONELAKE_TOKEN"]})

    # --- per-table summary -> tables.<table>.parquet ---
    summ = con.get_stats("fct_summary*")
    print("\n=== Per-table summary — get_stats('fct_summary*') ===")
    try:
        summ.show(max_width=100000, max_col_width=1000)
    except TypeError:
        summ.show()
    scols = summ.columns
    for row in summ.fetchall():
        d = dict(zip(scols, row))
        report.merge({"tables": {d["table"]: {"parquet": {
            "rows": d.get("total_rows"),
            "files": d.get("num_files"),
            "row_groups": d.get("num_row_groups"),
            "avg_row_group": d.get("avg_row_group"),
            "size_mb": d.get("size_mb"),
            "vorder": d.get("vorder"),
            "compression": d.get("compression"),
        }}}})

    # --- per-row-group detail: CSV (raw) + row_group_detail into the report ---
    det = con.get_stats("fct_summary*", detailed=True)
    det_cols, det_rows = det.columns, det.fetchall()
    csv_path = os.environ.get("STATS_CSV", "stats_detailed.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(det_cols)
        w.writerows(det_rows)
    print(f"\nwrote detailed stats CSV -> {os.path.abspath(csv_path)} "
          f"({len(det_rows)} rows × {len(det_cols)} cols)")

    try:
        i = {c: n for n, c in enumerate(det_cols)}
        tc, fc, rgc = i["table"], i["file_name"], i["row_group_id"]
        rc, sc = i["row_group_num_rows"], i["total_compressed_size"]
        agg = {}
        for r in det_rows:
            key = (r[tc], r[fc], r[rgc])
            a = agg.setdefault(key, {"rows": r[rc], "bytes": 0})
            a["bytes"] += r[sc] or 0
        detail = defaultdict(list)
        for (t, fn, rg), a in agg.items():
            detail[t].append({"file": fn, "rg": rg, "rows": a["rows"],
                              "mb": round(a["bytes"] / 1e6, 1)})
        for t, lst in detail.items():
            lst.sort(key=lambda x: (str(x["file"]), x["rg"]))
            report.merge({"tables": {t: {"parquet": {"row_group_detail": lst}}}})
    except Exception as e:
        print(f"(row_group_detail skipped: {str(e).splitlines()[0][:120]})")


if __name__ == "__main__":
    main()
