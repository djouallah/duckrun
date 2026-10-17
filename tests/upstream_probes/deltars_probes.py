"""Probe the installed deltalake for the upstream bugs duckrun works around.

Not a test suite and not a gate: a report. Each probe exercises bare ``deltalake`` (no duckrun code
path) for one behaviour a duckrun workaround exists for, and says whether that behaviour is still
broken on the installed wheel. Run it whenever the deltalake floor moves; a workaround may go only
when its probe reads FIXED on the floor version.

    python tests/upstream_probes/deltars_probes.py          # all probes, Markdown to stdout
                                                            # (and to $GITHUB_STEP_SUMMARY in CI)

Each probe runs in its own subprocess: several of these bugs are Rust panics, and a panic that
aborts takes the interpreter with it. A probe that dies without reporting reads CRASH.

Statuses: FIXED (the workaround is no longer needed for this behaviour), BROKEN (still needed),
OK / EXPOSED (an exposure check for a bug duckrun has no workaround for), INFO (observation only).
"""
import json
import os
import subprocess
import sys
import tempfile
import traceback

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, WriterProperties, write_deltalake


def _tmp():
    return tempfile.mkdtemp(prefix="deltars_probe_")


def _rows(p):
    return DeltaTable(p).to_pyarrow_table()


def _sorted(p, col):
    return sorted(_rows(p)[col].to_pylist())


def _added_files(p, before):
    return [u for u in DeltaTable(p).file_uris() if u not in before]


# ------------------------------------------------------------------------------------------ probes
# Each returns (status, detail). Raising reads ERROR. Keep them small: one behaviour each.

def p1_predicateless_update():
    """delta_dml.py UPDATE: a predicate-less update on a multi-file table (seen on 1.5.0) updated only
    SOME rows, so every no-WHERE UPDATE is a DuckDB-evaluated full overwrite."""
    p = _tmp()
    for k in range(60):                                   # 60 single-row commits -> 60 files
        write_deltalake(p, pa.table({"i": pa.array([k], pa.int32())}), mode="append")
    DeltaTable(p).update(updates={"i": "i + 1"})
    got_none = _sorted(p, "i")
    DeltaTable(p).update(updates={"i": "i + 1"}, predicate="1 = 1")
    got_const = _sorted(p, "i")
    ok = got_none == list(range(1, 61)) and got_const == list(range(2, 62))
    return ("FIXED" if ok else "BROKEN",
            f"predicate=None updated {sum(got_none) - sum(range(60))}/60 rows; "
            f"predicate='1 = 1' -> sum {sum(got_const)} (want {sum(range(2, 62))})")


def p4_int32_delete_literal():
    """delta_plugin.py delete+insert notes the kernel rejecting an Int32 column vs an Int64 literal in
    dt.delete. The raw-SQL DELETE fast path still calls dt.delete(predicate) on such columns."""
    p = _tmp()
    for k in range(4):
        write_deltalake(p, pa.table({"k": pa.array([k * 2, k * 2 + 1], pa.int32())}), mode="append")
    DeltaTable(p).delete("k = 3")
    DeltaTable(p).delete("k IN (5, 6)")
    got = _sorted(p, "k")
    return ("OK" if got == [0, 1, 2, 4, 7] else "EXPOSED", f"rows left {got}")


def p5_update_unknown_column():
    """delta_dml.py / engine.update_rows: dt.update silently accepted an unknown SET column and wrote
    a no-op commit, so duckrun validates SET targets first."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": [1, 2]}))
    v0 = DeltaTable(p).version()
    try:
        DeltaTable(p).update(updates={"nope": "1"})
    except Exception as e:
        return "FIXED", f"raises: {type(e).__name__}: {str(e)[:120]}"
    return "BROKEN", f"accepted; version {v0} -> {DeltaTable(p).version()}"


def p6_dml_writer_properties():
    """engine.delete_rows / update_rows pass no writer_properties, so DML-rewritten files miss the read
    layout. Upstream fixed delete() writing string_view and dropping writer_properties.compression;
    if delete AND update now honor writer_properties, duckrun can pass its own."""
    out = []
    ok = True
    for op in ("delete", "update"):
        p = _tmp()
        n = 300_000
        write_deltalake(p, pa.table({"k": pa.array(range(n), pa.int64()),
                                     "s": pa.array([f"v{i % 1000}" for i in range(n)])}))
        before = set(DeltaTable(p).file_uris())
        wp = WriterProperties(compression="ZSTD", max_row_group_size=50_000)
        dt = DeltaTable(p)
        if op == "delete":
            dt.delete("k = 7", writer_properties=wp)
        else:
            dt.update(updates={"s": "'x'"}, predicate="k = 7", writer_properties=wp)
        new = _added_files(p, before)
        if not new:
            return "ERROR", f"{op}: no rewritten file"
        md = pq.ParquetFile(new[0].replace("file://", "")).metadata
        codecs = {md.row_group(g).column(c).compression
                  for g in range(md.num_row_groups) for c in range(md.num_columns)}
        max_rg = max(md.row_group(g).num_rows for g in range(md.num_row_groups))
        # The Arrow label (string vs string_view) is informational: plain writes carry the same
        # label, and the parquet column is BYTE_ARRAY/UTF8 either way.
        s_type = str(pq.read_schema(new[0].replace("file://", "")).field("s").type)
        good = codecs == {"ZSTD"} and max_rg <= 50_000
        ok &= good
        out.append(f"{op}: codecs={sorted(codecs)} max_rg={max_rg} s={s_type}")
    return ("FIXED" if ok else "BROKEN"), "; ".join(out)


def p7_empty_append():
    """engine.insert path: a 0-row append committed a new version, so duckrun probes LIMIT 1 first."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": pa.array([1], pa.int64())}))
    v0 = DeltaTable(p).version()
    write_deltalake(p, pa.table({"k": pa.array([], pa.int64())}), mode="append")
    v1 = DeltaTable(p).version()
    return ("FIXED" if v1 == v0 else "BROKEN", f"version {v0} -> {v1}")


def p8_merge_duplicate_source_keys():
    """engine._merge_cardinality_guard: delta-rs silently duplicated target rows on a duplicate-key
    source (Spark/Snowflake raise), so duckrun probes the source for duplicate keys first."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": [1, 2], "v": [0, 0]}))
    src = pa.table({"k": [1, 1], "v": [10, 11]})
    try:
        (DeltaTable(p).merge(src, "target.k = source.k", source_alias="source",
                             target_alias="target")
         .when_matched_update_all().when_not_matched_insert_all().execute())
    except Exception as e:
        return "FIXED", f"raises: {type(e).__name__}: {str(e)[:120]}"
    return "BROKEN", f"silently succeeded; target rows {sorted(_rows(p).to_pylist(), key=str)}"


def p9_multiple_when_matched():
    """engine.resolve_do_nothing emits several WHEN MATCHED clauses guarded by predicates; upstream
    had an error with duplicate WHEN MATCHED clauses. First matching clause must win."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": [1, 2, 3], "v": [0, 0, 0]}))
    src = pa.table({"k": [1, 2], "v": [50, 5]})
    (DeltaTable(p).merge(src, "target.k = source.k", source_alias="source", target_alias="target")
     .when_matched_update({"v": "source.v"}, predicate="source.v > 10")
     .when_matched_update({"v": "-1"})
     .execute())
    got = sorted(_rows(p).to_pylist(), key=lambda r: r["k"])
    want = [{"k": 1, "v": 50}, {"k": 2, "v": -1}, {"k": 3, "v": 0}]
    return ("OK" if got == want else "EXPOSED", f"got {got}")


def p10_merge_list_column():
    """No duckrun workaround: upstream fixed when_matched_update writing NULL into list columns."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": [1, 2], "v": [0, 0],
                                 "l": pa.array([[1, 2], [3]], pa.list_(pa.int64()))}))
    src = pa.table({"k": [1, 2], "v": [9, 9], "l": pa.array([[7, 8], [9]], pa.list_(pa.int64()))})
    (DeltaTable(p).merge(src, "target.k = source.k", source_alias="source", target_alias="target")
     .when_matched_update({"v": "source.v"}, predicate="target.k = 1")
     .when_matched_update({"l": "source.l"}, predicate="target.k = 2")
     .execute())
    got = {r["k"]: r["l"] for r in _rows(p).to_pylist()}
    return ("OK" if got == {1: [1, 2], 2: [9]} else "EXPOSED", f"l after merge {got}")


def p11_replacewhere_cast():
    """engine.replace_where: delta-rs couldn't serialize a CAST in a replaceWhere predicate
    ('Unable to convert expression to string'), so duckrun builds CAST-free predicates."""
    p = _tmp()
    import datetime as dtm
    d = [dtm.date(2024, 1, 1), dtm.date(2024, 1, 2)]
    write_deltalake(p, pa.table({"d": d, "v": [1, 2]}))
    try:
        write_deltalake(p, pa.table({"d": [dtm.date(2024, 1, 2)], "v": [20]}), mode="overwrite",
                        predicate="d >= CAST('2024-01-02' AS DATE)")
    except Exception as e:
        return "BROKEN", f"{type(e).__name__}: {str(e)[:160]}"
    got = sorted(r["v"] for r in _rows(p).to_pylist())
    return ("FIXED" if got == [1, 20] else "BROKEN", f"rows v={got}")


def p12_create_non_utc_tz():
    """delta_dml.py CREATE TABLE coldefs: DeltaTable.create accepted only UTC tz labels, so duckrun
    relabels a session-zone TIMESTAMPTZ to UTC."""
    p = _tmp()
    schema = pa.schema([pa.field("ts", pa.timestamp("us", tz="Europe/Paris"))])
    try:
        DeltaTable.create(p, schema=schema)
    except Exception as e:
        return "BROKEN", f"{type(e).__name__}: {str(e)[:160]}"
    return "FIXED", f"created; schema {DeltaTable(p).schema().to_arrow()}"


def p13_alter_drop_rename():
    """delta_dml.py ALTER DROP/RENAME COLUMN is a full fenced rewrite because delta-rs had no
    in-place drop/rename. Column-mapping writes landed in 1.6.1 — is there an API for it now?"""
    p = _tmp()
    write_deltalake(p, pa.table({"a": [1], "b": [2]}))
    alter = DeltaTable(p).alter
    methods = sorted(m for m in dir(alter) if not m.startswith("_"))
    hit = [m for m in methods if m in ("drop_column", "drop_columns", "rename_column",
                                       "rename_columns")]
    return ("FIXED" if hit else "BROKEN", f"alter API: {methods}")


def p14_merge_correlated_subquery():
    """delta_dml.py rejects a subquery in a MERGE value up front: delta-rs hit a datafusion
    'not implemented' panic that killed the process. A CRASH here means still broken."""
    p = _tmp()
    write_deltalake(p, pa.table({"k": [1], "v": [0]}))
    src = pa.table({"k": [1], "v": [5]})
    try:
        (DeltaTable(p).merge(src, "target.k = source.k", source_alias="source",
                             target_alias="target")
         .when_matched_update({"v": "(SELECT source.v + 1)"}).execute())
    except BaseException as e:  # pyo3 PanicException is a BaseException
        return "BROKEN", f"{type(e).__name__}: {str(e)[:160]}"
    return ("FIXED" if _rows(p)["v"].to_pylist() == [6] else "BROKEN",
            f"v={_rows(p)['v'].to_pylist()}")


PROBES = [
    ("P1", "predicate-less UPDATE on 60 files touches every row", p1_predicateless_update),
    ("P4", "DELETE on INT32 column with int literal", p4_int32_delete_literal),
    ("P5", "UPDATE unknown SET column fails loud", p5_update_unknown_column),
    ("P6", "delete/update honor writer_properties", p6_dml_writer_properties),
    ("P7", "0-row append writes no commit", p7_empty_append),
    ("P8", "MERGE duplicate source keys raises", p8_merge_duplicate_source_keys),
    ("P9", "MERGE several guarded WHEN MATCHED clauses", p9_multiple_when_matched),
    ("P10", "MERGE update keeps / sets list columns", p10_merge_list_column),
    ("P11", "replaceWhere predicate with CAST", p11_replacewhere_cast),
    ("P12", "DeltaTable.create with non-UTC tz", p12_create_non_utc_tz),
    ("P13", "in-place DROP/RENAME COLUMN API", p13_alter_drop_rename),
    ("P14", "MERGE value with correlated subquery", p14_merge_correlated_subquery),
]


def _run_one(pid):
    fn = {p[0]: p[2] for p in PROBES}[pid]
    try:
        status, detail = fn()
    except Exception as e:
        status, detail = "ERROR", f"{type(e).__name__}: {e}\n{traceback.format_exc()[-600:]}"
    print("@@RESULT@@" + json.dumps({"status": status, "detail": detail}), flush=True)


def main():
    import deltalake
    import duckdb
    lines = [f"### delta-rs probes — deltalake {deltalake.__version__}, pyarrow {pa.__version__}, "
             f"duckdb {duckdb.__version__}", "", "| # | behaviour | status | detail |",
             "|---|---|---|---|"]
    for pid, title, _ in PROBES:
        try:
            r = subprocess.run([sys.executable, __file__, "--one", pid], capture_output=True,
                               text=True, timeout=600)
            res = next((json.loads(ln[len("@@RESULT@@"):]) for ln in r.stdout.splitlines()
                        if ln.startswith("@@RESULT@@")), None)
            if res is None:
                tail = (r.stderr or r.stdout).strip().splitlines()[-3:]
                res = {"status": "CRASH", "detail": f"exit {r.returncode}: {' / '.join(tail)}"}
        except subprocess.TimeoutExpired:
            res = {"status": "CRASH", "detail": "timed out after 600s"}
        detail = res["detail"].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {pid} | {title} | **{res['status']}** | {detail} |")
        print(f"{pid:4} {res['status']:8} {title}: {res['detail']}", file=sys.stderr, flush=True)
    report = "\n".join(lines) + "\n"
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--one":
        _run_one(sys.argv[2])
    else:
        main()
