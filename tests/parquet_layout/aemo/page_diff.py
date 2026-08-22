"""Byte-level diff of ONE column between the two writers' real files. No hypotheses, just a dump.

Five plausible causes of the DuckDB arm's slow cold read have now been eliminated by single-variable
runs (bloom filters, the V1/V2 encoding tag, page geometry, nullability, LogicalType). It is parquet
either way, so whatever delta-rs does differently is IN THE FILE. This walks the actual page headers
of one column chunk from each writer and prints every field, so the difference can be read off
rather than guessed at.

Only the bytes needed are fetched: the footer, then the target column's chunk in the first N row
groups, via range reads. `parquet_metadata()` cannot answer this — it exposes no page headers and no
PageEncodingStats.

Env: ONELAKE_TABLES_PATH (resolve_env), AB_PREFIX (default 'fct_summary_ab'),
PAGE_DIFF_COLUMN (default 'DUID'), PAGE_DIFF_ROW_GROUPS (default 1), PAGE_DIFF_PAGES (default 6).
PAGE_DIFF_URIS (comma-separated full table URIs) overrides the AB_PREFIX discovery for tables that
live in separate throwaway lakehouses — the cold benchmark's files, the ones actually timed.
"""
import os
import struct
import sys

import numpy as np
import duckrun
from fastparquet.cencoding import NumpyIO, from_buffer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PREFIX = os.environ.get("AB_PREFIX") or "fct_summary_ab"
COLUMN = os.environ.get("PAGE_DIFF_COLUMN") or "DUID"
N_RG = int(os.environ.get("PAGE_DIFF_ROW_GROUPS") or 1)
N_PAGES = int(os.environ.get("PAGE_DIFF_PAGES") or 6)
URIS = [u.strip() for u in (os.environ.get("PAGE_DIFF_URIS") or "").split(",") if u.strip()]

PAGE_TYPE = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
ENCODING = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED",
            5: "DELTA_BINARY_PACKED", 6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY",
            8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}


def _enc(v):
    return ENCODING.get(v, str(v))


def _sha(b):
    import hashlib
    return hashlib.sha256(b).hexdigest()[:16]


def read_footer(store, key, size):
    import obstore
    tail = bytes(obstore.get_range(store, key, start=max(0, size - 65536), end=size))
    flen = struct.unpack("<I", tail[-8:-4])[0]
    if flen + 8 > len(tail):
        tail = bytes(obstore.get_range(store, key, start=size - flen - 8, end=size))
    raw = tail[len(tail) - 8 - flen:len(tail) - 8]
    return from_buffer(NumpyIO(np.frombuffer(raw, dtype="uint8")), "FileMetaData")


def walk_pages(blob):
    """Every page header in a column chunk, in order, with the fields a reader branches on."""
    io = NumpyIO(np.frombuffer(blob, dtype="uint8"))
    pages = []
    while io.tell() < len(blob) - 4:
        pos = io.tell()
        try:
            ph = from_buffer(io, "PageHeader")
        except Exception as e:
            pages.append({"error": f"{type(e).__name__} at byte {pos}"})
            break
        hdr_bytes = io.tell() - pos
        rec = {"page_type": PAGE_TYPE.get(ph.type, ph.type), "offset": pos,
               "header_bytes": hdr_bytes, "comp": ph.compressed_page_size,
               "uncomp": ph.uncompressed_page_size, "crc": ph.crc}
        d = getattr(ph, "data_page_header", None)
        v2 = getattr(ph, "data_page_header_v2", None)
        dic = getattr(ph, "dictionary_page_header", None)
        if d is not None:
            rec.update(num_values=d.num_values, encoding=_enc(d.encoding),
                       def_enc=_enc(d.definition_level_encoding),
                       rep_enc=_enc(d.repetition_level_encoding),
                       page_stats=d.statistics is not None)
        elif v2 is not None:
            rec.update(num_values=v2.num_values, encoding=_enc(v2.encoding),
                       num_nulls=v2.num_nulls, num_rows=v2.num_rows,
                       def_bytes=v2.definition_levels_byte_length,
                       rep_bytes=v2.repetition_levels_byte_length,
                       compressed=v2.is_compressed, page_stats=v2.statistics is not None)
        elif dic is not None:
            rec.update(num_values=dic.num_values, encoding=_enc(dic.encoding),
                       is_sorted=getattr(dic, "is_sorted", None))
        pages.append(rec)
        io.seek(ph.compressed_page_size, 1)
        if len(pages) > 20000:
            break
    return pages


def main():
    import obstore
    from dbt.adapters.duckrun.objectstore import build_store

    root = os.environ["ONELAKE_TABLES_PATH"].rstrip("/")
    con = duckrun.connect(root, schema="tests")
    if URIS:
        targets = [(u.rstrip("/").rsplit("/", 1)[-1], u.rstrip("/")) for u in URIS]
    else:
        names = sorted(r[2] for r in con.get_stats(f"{PREFIX}_*").fetchall())
        if not names:
            raise SystemExit(f"page_diff: no tables matched tests.{PREFIX}_*")
        targets = [(n, f"{root}/tests/{n}") for n in names]

    out = []
    for tbl, uri in targets:
        store = build_store(uri, con.storage_options)
        entries = []
        for batch in obstore.list(store):
            for obj in batch:
                name = obj["path"].rsplit("/", 1)[-1]
                if name.endswith(".parquet"):
                    entries.append((name, obj["size"]))
        entries.sort()
        key, size = entries[0]
        fmd = read_footer(store, key, size)

        print(f"\n{'=' * 104}\n{tbl}  —  {key}  ({size / 1048576:.1f} MB, "
              f"{len(entries)} file(s), {len(fmd.row_groups)} row groups)\n{'=' * 104}")
        cb = fmd.created_by
        kv = [(k.key, k.value) for k in (fmd.key_value_metadata or [])]
        print(f"created_by         : {cb.decode() if isinstance(cb, bytes) else cb}")
        # Values, not just keys: ARROW:schema is a large blob, so length + hash is the
        # comparable form across writers.
        if kv:
            for k, v in kv:
                kk = k.decode() if isinstance(k, bytes) else k
                vb = v if isinstance(v, bytes) else str(v or "").encode()
                print(f"footer key/value   : {kk!r} = {len(vb)} bytes, "
                      f"sha {_sha(vb)}, head {vb[:48]!r}")
        else:
            print("footer key/value   : NONE")
        so = fmd.schema[0]
        rname = so.name.decode() if isinstance(so.name, bytes) else so.name
        print(f"format version     : {fmd.version}")
        print(f"root schema element: name={rname!r}, num_children={getattr(so, 'num_children', None)}")
        print(f"column_orders      : {fmd.column_orders!r}")

        for ri in range(min(N_RG, len(fmd.row_groups))):
            rg = fmd.row_groups[ri]
            cc = next((c for c in rg.columns
                       if ".".join(x.decode() if isinstance(x, bytes) else x
                                   for x in c.meta_data.path_in_schema) == COLUMN), None)
            if cc is None:
                print(f"  row group {ri}: no column {COLUMN!r}")
                continue
            m = cc.meta_data
            start = m.dictionary_page_offset or m.data_page_offset
            print(f"\n-- row group {ri}: {rg.num_rows:,} rows, "
                  f"sorting_columns={getattr(rg, 'sorting_columns', None)}")
            print(f"   rg fields     : file_offset={getattr(rg, 'file_offset', None)}, "
                  f"total_byte_size={getattr(rg, 'total_byte_size', None)}, "
                  f"total_compressed_size={getattr(rg, 'total_compressed_size', None)}, "
                  f"ordinal={getattr(rg, 'ordinal', None)}")
            print(f"   chunk         : {m.total_compressed_size:,} comp / "
                  f"{m.total_uncompressed_size:,} raw bytes, {m.num_values:,} values")
            print(f"   chunk fields  : file_path={getattr(cc, 'file_path', None)!r}, "
                  f"file_offset={getattr(cc, 'file_offset', None)}, codec={m.codec}, "
                  f"index_page_offset={getattr(m, 'index_page_offset', None)}")
            print(f"   encodings     : {[_enc(e) for e in (m.encodings or [])]}")
            es = m.encoding_stats
            print(f"   encoding_stats: " + (
                ", ".join(f"{PAGE_TYPE.get(s.page_type, s.page_type)}/{_enc(s.encoding)}x{s.count}"
                          for s in es) if es else "ABSENT"))
            st = m.statistics
            print(f"   statistics    : null_count={getattr(st, 'null_count', None)}, "
                  f"distinct_count={getattr(st, 'distinct_count', None)}")
            # Which Statistics fields are populated — a writer on deprecated min/max and one
            # on min_value/max_value look identical in every other readout here.
            shape = ("ABSENT" if st is None else
                     ", ".join(f"{f}={'set' if getattr(st, f, None) is not None else '-'}"
                               for f in ("min", "max", "min_value", "max_value",
                                         "is_min_value_exact", "is_max_value_exact")))
            print(f"   stats shape   : {shape}")
            print(f"   index offsets : column_index={cc.column_index_offset}, "
                  f"offset_index={cc.offset_index_offset}, bloom={m.bloom_filter_offset}")

            blob = bytes(obstore.get_range(store, key, start=start,
                                           end=start + m.total_compressed_size))
            pages = walk_pages(blob)
            n_data = sum(1 for p in pages if p["page_type"].startswith("DATA"))
            # Dictionary page BYTES, compressed and decompressed — the payload decoder only
            # compares parsed values, which cannot see codec or serialization differences.
            for p in pages:
                if p["page_type"] == "DICTIONARY_PAGE" and "error" not in p:
                    body = blob[p["offset"] + p["header_bytes"]:
                                p["offset"] + p["header_bytes"] + p["comp"]]
                    from payload_diff import _decompress
                    try:
                        raw_sha = _sha(_decompress(body, m.codec, p["uncomp"]))
                    except SystemExit as e:
                        raw_sha = f"undecodable ({e})"
                    print(f"   dict page raw : comp {p['comp']:,}B sha {_sha(body)} | "
                          f"uncomp {p['uncomp']:,}B sha {raw_sha} | "
                          f"stored_uncompressed={p['comp'] == p['uncomp']}")
                    break
            print(f"   pages walked  : {len(pages)} ({n_data} data), showing first {N_PAGES}")
            for p in pages[:N_PAGES]:
                extra = " ".join(f"{k}={v}" for k, v in p.items()
                                 if k not in ("page_type", "offset"))
                print(f"     {p['page_type']:<16} {extra}")
            out.append((tbl, ri, len(pages), n_data))

    print(f"\n{'=' * 104}")
    print(f"{'table':<32} {'rg':>4} {'pages':>8} {'data pages':>12}")
    for tbl, ri, n, nd in out:
        print(f"{tbl:<32} {ri:>4} {n:>8} {nd:>12}")


if __name__ == "__main__":
    main()
