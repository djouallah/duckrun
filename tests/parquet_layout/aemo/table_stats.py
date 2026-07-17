"""duckrun get_stats for the summary tables → run_report.json (tables.<t>.parquet):
per-table summary, row_group_detail, and the raw per-column-chunk parquet_metadata
(encodings, dictionary, sizes, stats) under column_chunks. One file, everything in it.

Env in: ONELAKE_TABLES_PATH, ONELAKE_TOKEN, RUN_REPORT.
"""
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


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return str(v)


def main():
    import duckrun
    con = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"] + "/tests")

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

    # --- raw per-column-chunk parquet_metadata -> column_chunks + row_group_detail rollup ---
    det = con.get_stats("fct_summary*", detailed=True)
    det_cols, det_rows = det.columns, det.fetchall()
    print(f"\ndetailed parquet_metadata: {len(det_rows)} rows × {len(det_cols)} cols "
          f"-> tables.<t>.parquet.column_chunks")

    chunks = defaultdict(list)
    for r in det_rows:
        d = {c: _jsonable(v) for c, v in zip(det_cols, r)}
        chunks[d["table"]].append(d)
    for t, lst in chunks.items():
        report.merge({"tables": {t: {"parquet": {"column_chunks": lst}}}})

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
