"""Write both writers' files on the runner and compare the column chunk BYTE FOR BYTE. No Fabric.

Ground truth for the question every Fabric run has been circling: at matched page geometry, are
DuckDB's page bytes actually equal to parquet-cpp's, or not? Locally that came back byte-identical
(header, body and payload, at bit widths 6, 8 and 9) — but locally means DuckDB v1.5.5, because the
pinned nightly has no Windows wheel. CI writes with v2.0.0-alpha38615, and nothing has ever
compared ITS bytes. That gap is the whole reason this exists.

Costs nothing but a runner: it reads the source read-only through the same delta_scan expression
the benchmark uses, writes both files to local disk, and never creates a Fabric item.

Geometry is chosen so the comparison is meaningful rather than approximate. ROW_GROUP_SIZE is a
multiple of DuckDB's 2048-row vector AND below parquet-cpp's 20,000-row page cap, so both writers
emit exactly one data page per row group and pages correspond one-to-one. DuckDB's run framing and
its missing is_sorted are normalised first, because those are measured no-ops and leaving them in
would bury a real difference under two known ones.

Env: ONELAKE_TABLES_PATH, BENCH_SOURCE, BYTES_ROWS (default 655360), BYTES_RG (default 16384),
BYTES_COLUMN (default DUID), BYTES_SKIP (rows to skip, to reach a wider-dictionary regime).
"""
import os
import struct
import sys
import tempfile

import numpy as np
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from fastparquet.cencoding import NumpyIO, from_buffer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import page_reframe as PR  # noqa: E402
import payload_diff as PD  # noqa: E402

COLUMN = os.environ.get("BYTES_COLUMN") or "DUID"
RG = int(os.environ.get("BYTES_RG") or 16384)
ROWS = int(os.environ.get("BYTES_ROWS") or 655_360)
SKIP = int(os.environ.get("BYTES_SKIP") or 0)
COLS = (os.environ.get("OPT_COLUMNS") or "date, time, DUID").strip()
SORT = (os.environ.get("OPT_SORT") or "date, time, DUID").strip()

if RG % 2048:
    raise SystemExit(f"BYTES_RG must be a multiple of 2048 (DuckDB's vector), got {RG}")
if RG > 20000:
    raise SystemExit(f"BYTES_RG must be <= 20000 so parquet-cpp emits one page per row group, "
                     f"got {RG}")


def chunks(blob, column):
    """Every page of `column`, per row group: header bytes, body bytes, decompressed payload."""
    flen = struct.unpack("<I", blob[-8:-4])[0]
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[len(blob) - 8 - flen:-8], dtype="uint8")),
                      "FileMetaData")
    out = []
    for rg in fmd.row_groups:
        cc = next((c for c in rg.columns
                   if ".".join(x.decode() if isinstance(x, bytes) else x
                               for x in c.meta_data.path_in_schema) == column), None)
        if cc is None:
            continue
        m = cc.meta_data
        start = m.dictionary_page_offset or m.data_page_offset
        chunk = bytes(blob[start:start + m.total_compressed_size])
        io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
        pages = []
        while io.tell() < len(chunk) - 4:
            p0 = io.tell()
            ph = from_buffer(io, "PageHeader")
            hdr = chunk[p0:io.tell()]
            body = chunk[io.tell():io.tell() + ph.compressed_page_size]
            io.seek(ph.compressed_page_size, 1)
            raw = bytes(PD._decompress(body, m.codec, ph.uncompressed_page_size))
            pages.append({"type": ph.type, "hdr": bytes(hdr), "body": bytes(body), "raw": raw,
                          "num_values": getattr(getattr(ph, "data_page_header", None)
                                                or getattr(ph, "dictionary_page_header", None),
                                                "num_values", None)})
        out.append({"num_rows": rg.num_rows, "chunk": chunk, "pages": pages})
    return out


def report(where, a, b, label_a, label_b, limit=64):
    if a == b:
        return False
    print(f"\n  *** {where}: DIFFER ({len(a)} vs {len(b)} bytes)")
    if len(a) == len(b):
        d = [i for i in range(len(a)) if a[i] != b[i]]
        print(f"      {len(d)} of {len(a)} bytes differ; first at offset {d[0]}")
        i = d[0]
        print(f"      {label_a:<10} {a[max(0, i - 4):i + limit].hex()}")
        print(f"      {label_b:<10} {b[max(0, i - 4):i + limit].hex()}")
    else:
        print(f"      {label_a:<10} {a[:limit].hex()}")
        print(f"      {label_b:<10} {b[:limit].hex()}")
    return True


def main():
    src = (os.environ.get("BENCH_SOURCE") or "mart.fct_summary").strip()
    d = tempfile.mkdtemp(prefix="byte_verify_")
    duck = os.path.join(d, "duck.parquet").replace("\\", "/")
    arrow = os.path.join(d, "arrow.parquet").replace("\\", "/")

    # duckrun.connect wires the OIDC token into DuckDB's azure secret; a bare duckdb.connect()
    # has no OneLake credentials and dies on the delta_scan with "Identity not found".
    import duckrun
    _c = duckrun.connect(os.environ["ONELAKE_TABLES_PATH"].rstrip("/"), read_only=True)
    con = _c.con
    con.execute("SET threads=1")
    print(f"duckdb {duckdb.__version__} | pyarrow {pa.__version__}", flush=True)
    # Deterministic slice: order first, then skip/limit, so both writers get identical rows.
    sub = (f"(select {COLS} from {src} order by {SORT} "
           f"limit {ROWS} offset {SKIP})")
    con.execute(f"""
        COPY (select {COLS} from {sub} order by {SORT})
        TO '{duck}' (FORMAT parquet, ROW_GROUP_SIZE {RG}, COMPRESSION snappy,
                     PARQUET_VERSION V1, DICTIONARY_SIZE_LIMIT 16000000)
    """)
    tbl = con.execute(f"select {COLS} from {sub} order by {SORT}").arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    w = pq.ParquetWriter(arrow, tbl.schema, compression="snappy", use_dictionary=True,
                         version="1.0", write_statistics=True)
    for off in range(0, tbl.num_rows, RG):
        w.write_table(tbl.slice(off, RG), row_group_size=RG)
    w.close()
    print(f"{tbl.num_rows:,} rows, RG={RG:,} -> duckdb {os.path.getsize(duck):,}B, "
          f"pyarrow {os.path.getsize(arrow):,}B", flush=True)

    db = open(duck, "rb").read()
    ab = open(arrow, "rb").read()
    # Normalise the two measured no-ops so a real difference is not buried under known ones.
    db, s1 = PR.reframe_bytes(db, column=COLUMN)
    db, s2 = PR.mark_dict_sorted_bytes(db, column=COLUMN)
    db, s3 = PR.set_dict_encoding_bytes(db, column=COLUMN, encoding=2)
    print(f"normalised duckdb: reframe {s1['pages']} pages, is_sorted {s2['dictionary_pages_marked']}"
          f", retag {s3['dictionary_pages_retagged']}", flush=True)

    ca, cb = chunks(db, COLUMN), chunks(ab, COLUMN)
    print(f"row groups: duckdb {len(ca)}, pyarrow {len(cb)}", flush=True)
    if len(ca) != len(cb):
        raise SystemExit("row group counts differ — geometry did not line up")

    n_diff = 0
    for i, (x, y) in enumerate(zip(ca, cb)):
        if x["num_rows"] != y["num_rows"]:
            raise SystemExit(f"row group {i}: {x['num_rows']} vs {y['num_rows']} rows")
        if len(x["pages"]) != len(y["pages"]):
            print(f"\n  *** rg{i}: page COUNT differs — {len(x['pages'])} vs {len(y['pages'])}")
            n_diff += 1
            continue
        for pi, (p, q) in enumerate(zip(x["pages"], y["pages"])):
            kind = {2: "DICT", 0: "DATA"}.get(p["type"], p["type"])
            if p["raw"] != q["raw"]:
                n_diff += report(f"rg{i}.page{pi}[{kind}] PAYLOAD (uncompressed)",
                                 p["raw"], q["raw"], "duckdb", "pyarrow")
            elif p["body"] != q["body"]:
                n_diff += report(f"rg{i}.page{pi}[{kind}] BODY (compressed, payload equal)",
                                 p["body"], q["body"], "duckdb", "pyarrow")
            if p["hdr"] != q["hdr"]:
                n_diff += report(f"rg{i}.page{pi}[{kind}] HEADER",
                                 p["hdr"], q["hdr"], "duckdb", "pyarrow")
        if n_diff and i >= 2:
            print(f"\n  (stopping after row group {i}; the pattern repeats)")
            break

    print(f"\n{'=' * 90}")
    if n_diff:
        print(f"VERDICT: the two writers' {COLUMN} bytes DIFFER at matched geometry "
              f"({n_diff} difference(s) shown)")
    else:
        print(f"VERDICT: {COLUMN} chunk bytes are IDENTICAL across all {len(ca)} row groups "
              f"once run framing, is_sorted and the dictionary tag are normalised")
    print(f"{'=' * 90}")


if __name__ == "__main__":
    main()
