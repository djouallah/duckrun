"""Two tables in, every difference out. One column, every level, no rewrites.

Fifteen measured rewrites have each fixed one property and left the read time flat, which only
narrows things if the list of properties is complete. This does not assume it is. It walks both
files and compares EVERYTHING for one column - footer, row group, chunk, page header, page body,
the definition-level block, the bit-packed padding, the dictionary and the decoded index stream -
and prints what differs. Nothing is rewritten and nothing is measured; it only answers "how do
these two files actually differ".

The parts no existing tool reads: the definition-level block (page_reframe copies it verbatim,
page_diff never decompresses, payload_diff skips past it), the trailing padding of the final
bit-packed run, and whether uncompressed_page_size matches the real decompressed length.

Env: ONELAKE_TABLES_PATH, FULL_DIFF_URIS (comma-separated table URIs, 2+),
FULL_DIFF_COLUMN (default DUID), FULL_DIFF_ROW_GROUPS (default 2), FULL_DIFF_PAGES (default 3).
"""
import os
import struct
import sys

import numpy as np
import duckrun
from fastparquet.cencoding import NumpyIO, from_buffer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import payload_diff as PD  # noqa: E402
from page_diff import read_footer  # noqa: E402

COLUMN = os.environ.get("FULL_DIFF_COLUMN") or "DUID"
N_RG = int(os.environ.get("FULL_DIFF_ROW_GROUPS") or 2)
N_PAGES = int(os.environ.get("FULL_DIFF_PAGES") or 3)
URIS = [u.strip() for u in (os.environ.get("FULL_DIFF_URIS") or "").split(",") if u.strip()]

PAGE_TYPE = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
ENCODING = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED",
            5: "DELTA_BINARY_PACKED", 6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY",
            8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}


def _sha(b):
    import hashlib
    return hashlib.sha256(bytes(b)).hexdigest()[:16]


def _txt(v):
    return v.decode() if isinstance(v, bytes) else v


def describe_deflevels(raw, optional):
    """The bytes every tool skips: the definition-level block at the head of a v1 data page."""
    if not optional:
        return {"present": False}
    (dl,) = struct.unpack_from("<I", raw, 0)
    block = bytes(raw[4:4 + dl])
    out = {"present": True, "declared_len": dl, "bytes": block.hex(), "sha": _sha(block)}
    # RLE/bit-packed hybrid: decode the run headers so a second run, or bit-packed levels,
    # or a value other than 1 is visible rather than assumed.
    runs, pos = [], 0
    try:
        while pos < len(block) and len(runs) < 8:
            hdr, pos = PD._varint(block, pos)
            if hdr & 1:                                   # bit-packed run
                groups = hdr >> 1
                runs.append(f"bitpacked[{groups * 8} values]")
                pos += groups            # levels are 1 bit wide -> 1 byte per group of 8
            else:                                         # RLE run
                n = hdr >> 1
                val = block[pos] if pos < len(block) else None
                runs.append(f"rle[{n} x {val}]")
                pos += 1
    except Exception as e:
        runs.append(f"<parse error {type(e).__name__}>")
    out["runs"] = runs
    return out


def describe_page(blob, off, ph, hdr_len, codec, optional):
    body = bytes(blob[off + hdr_len:off + hdr_len + ph.compressed_page_size])
    raw = bytes(PD._decompress(body, codec, ph.uncompressed_page_size))
    rec = {
        "type": PAGE_TYPE.get(ph.type, ph.type),
        "header_bytes": hdr_len,
        "header_hex": bytes(blob[off:off + hdr_len]).hex(),
        "crc": ph.crc,
        "compressed_page_size": ph.compressed_page_size,
        "uncompressed_page_size": ph.uncompressed_page_size,
        "real_decompressed_len": len(raw),
        "size_field_honest": len(raw) == ph.uncompressed_page_size,
        "body_sha": _sha(body),
        "payload_sha": _sha(raw),
    }
    d = getattr(ph, "data_page_header", None)
    dic = getattr(ph, "dictionary_page_header", None)
    if dic is not None:
        rec.update(num_values=dic.num_values, encoding=ENCODING.get(dic.encoding, dic.encoding),
                   is_sorted=getattr(dic, "is_sorted", None))
    elif d is not None:
        rec.update(num_values=d.num_values, encoding=ENCODING.get(d.encoding, d.encoding),
                   def_enc=ENCODING.get(d.definition_level_encoding),
                   rep_enc=ENCODING.get(d.repetition_level_encoding),
                   page_stats=d.statistics is not None)
        rec["deflevels"] = describe_deflevels(raw, optional)
        pos = 4 + struct.unpack_from("<I", raw, 0)[0] if optional else 0
        rec["bit_width"] = raw[pos]
        idx, st = PD.decode_hybrid(raw[pos + 1:], raw[pos], d.num_values)
        rec.update(rle_runs=st["rle_runs"], bitpacked_runs=st["bitpacked_runs"],
                   mean_bitpacked_len=st["mean_bitpacked_len"])
        rec["indices_sha"] = _sha(np.asarray(idx, dtype="<u4").tobytes())
        # trailing slots of the final bit-packed run: a partial run still packs a whole
        # multiple of 8, so the unused slots are writer's choice and nobody has looked.
        packed = raw[pos + 1:]
        rec["packed_tail_hex"] = bytes(packed[-16:]).hex()
        rec["packed_sha"] = _sha(packed)
    return rec, raw


def read_table(name, uri, storage_options):
    import obstore
    from dbt.adapters.duckrun.objectstore import build_store

    store = build_store(uri, storage_options)
    entries = []
    for batch in obstore.list(store):
        for obj in batch:
            nm = obj["path"].rsplit("/", 1)[-1]
            if nm.endswith(".parquet"):
                entries.append((nm, obj["size"]))
    entries.sort()
    key, size = entries[0]
    fmd = read_footer(store, key, size)

    se_by_name = {}
    for se in fmd.schema:
        se_by_name[_txt(se.name)] = se
    leaf = se_by_name.get(COLUMN)
    optional = leaf is not None and leaf.repetition_type == 1

    out = {
        "name": name, "file": key, "size": size, "files": len(entries),
        "created_by": _txt(fmd.created_by), "version": fmd.version,
        "root_schema_name": _txt(fmd.schema[0].name),
        "kv": {_txt(k.key): (len(k.value or b""), _sha(k.value or b""))
               for k in (fmd.key_value_metadata or [])},
        "column_orders": repr(fmd.column_orders),
        "n_row_groups": len(fmd.row_groups),
        "leaf": {f: getattr(leaf, f, None) for f in
                 ("type", "type_length", "repetition_type", "converted_type", "scale",
                  "precision", "field_id")} if leaf is not None else None,
        "leaf_logical": repr(getattr(leaf, "logicalType", None)) if leaf is not None else None,
        "row_groups": [],
    }

    for ri in range(min(N_RG, len(fmd.row_groups))):
        rg = fmd.row_groups[ri]
        cc = next((c for c in rg.columns
                   if ".".join(_txt(x) for x in c.meta_data.path_in_schema) == COLUMN), None)
        if cc is None:
            continue
        m = cc.meta_data
        st = m.statistics
        start = m.dictionary_page_offset or m.data_page_offset
        blob = bytes(obstore.get_range(store, key, start=start,
                                       end=start + m.total_compressed_size))
        io = NumpyIO(np.frombuffer(blob, dtype="uint8"))
        pages, dict_vals, idx_all = [], None, []
        while io.tell() < len(blob) - 4:
            p0 = io.tell()
            ph = from_buffer(io, "PageHeader")
            hdr_len = io.tell() - p0
            rec, raw = describe_page(blob, p0, ph, hdr_len, m.codec, optional)
            io.seek(ph.compressed_page_size, 1)
            if ph.type == 2:
                dict_vals = [v.decode("utf8", "replace") for v in
                             PD.parse_plain_byte_array(raw, ph.dictionary_page_header.num_values)]
            elif ph.type == 0 and len(idx_all) < 400_000:
                pos = 4 + struct.unpack_from("<I", raw, 0)[0] if optional else 0
                vals, _ = PD.decode_hybrid(raw[pos + 1:], raw[pos],
                                           ph.data_page_header.num_values)
                idx_all.extend(list(vals)[:400_000 - len(idx_all)])
            if len(pages) < N_PAGES + 1:
                pages.append(rec)
        vals = [dict_vals[i] for i in idx_all] if dict_vals else []
        out["row_groups"].append({
            "num_rows": rg.num_rows, "rg_file_offset": getattr(rg, "file_offset", None),
            "rg_total_byte_size": getattr(rg, "total_byte_size", None),
            "rg_total_compressed_size": getattr(rg, "total_compressed_size", None),
            "rg_ordinal": getattr(rg, "ordinal", None),
            "sorting_columns": repr(getattr(rg, "sorting_columns", None)),
            "chunk_file_path": getattr(cc, "file_path", None),
            "chunk_file_offset": getattr(cc, "file_offset", None),
            "codec": m.codec, "num_values": m.num_values,
            "encodings": [ENCODING.get(e, e) for e in (m.encodings or [])],
            "encoding_stats": "ABSENT" if not m.encoding_stats else ", ".join(
                f"{PAGE_TYPE.get(s.page_type)}/{ENCODING.get(s.encoding)}x{s.count}"
                for s in m.encoding_stats),
            "total_compressed_size": m.total_compressed_size,
            "total_uncompressed_size": m.total_uncompressed_size,
            "dictionary_page_offset": m.dictionary_page_offset,
            "data_page_offset": m.data_page_offset,
            "index_page_offset": getattr(m, "index_page_offset", None),
            "offset_index_offset": cc.offset_index_offset,
            "column_index_offset": cc.column_index_offset,
            "bloom_filter_offset": m.bloom_filter_offset,
            "stats_shape": "ABSENT" if st is None else ",".join(
                f for f in ("min", "max", "min_value", "max_value",
                            "is_min_value_exact", "is_max_value_exact")
                if getattr(st, f, None) is not None),
            "stats_null_count": getattr(st, "null_count", None),
            "stats_distinct_count": getattr(st, "distinct_count", None),
            "n_pages": len(pages), "pages": pages,
            "dict_len": len(dict_vals or []),
            "dict_sha_order": _sha("|".join(dict_vals or []).encode()),
            "dict_sha_set": _sha("|".join(sorted(dict_vals or [])).encode()),
            "dict_head": (dict_vals or [])[:5],
            "values_sha": _sha("|".join(vals).encode()),
            "values_head": vals[:6],
            "values_counted": len(vals),
        })
    return out


def read_delta_log(uri, storage_options):
    """The Delta commit both arms wrote. Direct Lake plans from this, not from the parquet.

    The duckdb and pyarrow arms share one AddAction code path, which is why this was assumed
    equal - but the stats inside come from parquet_metadata() over each writer's own footers,
    and DuckDB's chunks carry distinct_count and the deprecated min/max that pyarrow's do not.
    Nothing has ever diffed the two logs.
    """
    import json

    import obstore
    from dbt.adapters.duckrun.objectstore import build_store

    store = build_store(uri + "/_delta_log", storage_options)
    names = []
    for batch in obstore.list(store):
        for obj in batch:
            nm = obj["path"].rsplit("/", 1)[-1]
            if nm.endswith(".json"):
                names.append(nm)
    out = {"commits": len(names), "adds": [], "metadata": None, "protocol": None}
    for nm in sorted(names):
        raw = bytes(obstore.get(store, nm).bytes()).decode("utf8", "replace")
        for line in raw.splitlines():
            if not line.strip():
                continue
            act = json.loads(line)
            if "add" in act:
                add = dict(act["add"])
                st = add.get("stats")
                add["stats_parsed"] = json.loads(st) if isinstance(st, str) else st
                add.pop("stats", None)
                add.pop("modificationTime", None)     # wall clock, never comparable
                add["path"] = "<file>"                # uuid per run, never comparable
                out["adds"].append(add)
            elif "metaData" in act:
                md = dict(act["metaData"])
                md.pop("id", None)
                md.pop("createdTime", None)
                out["metadata"] = md
            elif "protocol" in act:
                out["protocol"] = act["protocol"]
    return out


def diff_delta_log(a_name, a_log, b_name, b_log):
    print(f"\n{'=' * 100}\nDELTA LOG\n{'=' * 100}")
    shown = 0

    def cmp(path, va, vb):
        nonlocal shown
        if va == vb:
            return
        shown += 1
        print(f"  {path}")
        print(f"      {a_name:<26} {va}")
        print(f"      {b_name:<26} {vb}")

    cmp("commits", a_log["commits"], b_log["commits"])
    cmp("protocol", a_log["protocol"], b_log["protocol"])
    for k in sorted(set(a_log["metadata"] or {}) | set(b_log["metadata"] or {})):
        cmp(f"metaData.{k}", (a_log["metadata"] or {}).get(k), (b_log["metadata"] or {}).get(k))
    cmp("n_add_actions", len(a_log["adds"]), len(b_log["adds"]))
    for i, (aa, bb) in enumerate(zip(a_log["adds"], b_log["adds"])):
        for k in sorted(set(aa) | set(bb)):
            if k == "stats_parsed":
                sa, sb = aa.get(k) or {}, bb.get(k) or {}
                for sk in sorted(set(sa) | set(sb)):
                    cmp(f"add[{i}].stats.{sk}", sa.get(sk), sb.get(sk))
            else:
                cmp(f"add[{i}].{k}", aa.get(k), bb.get(k))
    print(f"  {shown} Delta-log field(s) differ" if shown else "  Delta logs are IDENTICAL")


def diff(a, b):
    """Print every field that differs. Same-valued fields are summarised, not listed."""
    same, shown = 0, 0

    def cmp(path, va, vb):
        nonlocal same, shown
        if va == vb:
            same += 1
            return
        shown += 1
        print(f"  {path}")
        print(f"      {a['name']:<26} {va}")
        print(f"      {b['name']:<26} {vb}")

    print(f"\n{'=' * 100}\nFILE LEVEL\n{'=' * 100}")
    for f in ("files", "created_by", "version", "root_schema_name", "kv", "column_orders",
              "n_row_groups", "leaf", "leaf_logical"):
        cmp(f, a.get(f), b.get(f))
    cmp("size_bytes", a["size"], b["size"])

    for ri, (ra, rb) in enumerate(zip(a["row_groups"], b["row_groups"])):
        print(f"\n{'=' * 100}\nROW GROUP {ri} — column {COLUMN}\n{'=' * 100}")
        for f in ("num_rows", "rg_file_offset", "rg_total_byte_size", "rg_total_compressed_size",
                  "rg_ordinal", "sorting_columns", "chunk_file_path", "chunk_file_offset",
                  "codec", "num_values", "encodings", "encoding_stats", "total_compressed_size",
                  "total_uncompressed_size", "dictionary_page_offset", "data_page_offset",
                  "index_page_offset", "offset_index_offset", "column_index_offset",
                  "bloom_filter_offset", "stats_shape", "stats_null_count",
                  "stats_distinct_count", "n_pages", "dict_len", "dict_sha_order",
                  "dict_sha_set", "dict_head", "values_sha", "values_head", "values_counted"):
            cmp(f"rg{ri}.{f}", ra.get(f), rb.get(f))
        for pi, (pa, pb) in enumerate(zip(ra["pages"], rb["pages"])):
            if pi > N_PAGES:
                break
            keys = sorted(set(pa) | set(pb))
            for f in keys:
                cmp(f"rg{ri}.page{pi}[{pa.get('type')}].{f}", pa.get(f), pb.get(f))

    print(f"\n{'=' * 100}")
    print(f"{shown} field(s) DIFFER, {same} identical, between {a['name']} and {b['name']}")
    print(f"{'=' * 100}")


def main():
    root = os.environ["ONELAKE_TABLES_PATH"].rstrip("/")
    con = duckrun.connect(root, schema="tests")
    if len(URIS) < 2:
        raise SystemExit("full_diff: FULL_DIFF_URIS needs at least two table URIs")
    tables = [read_table(u.rstrip("/").rsplit("/", 1)[-1], u.rstrip("/"), con.storage_options)
              for u in URIS]
    for t in tables:
        print(f"{t['name']:<28} {t['file']}  ({t['size'] / 1048576:.1f} MB, "
              f"{t['n_row_groups']} row groups, created_by={t['created_by']})")
    logs = []
    for u in URIS:
        try:
            logs.append(read_delta_log(u.rstrip("/"), con.storage_options))
        except Exception as ex:                       # never let the log read hide the byte diff
            print(f"delta log unreadable for {u}: {type(ex).__name__}: {ex}")
            logs.append(None)
    for i, other in enumerate(tables[1:], start=1):
        diff(tables[0], other)
        if logs[0] and logs[i]:
            diff_delta_log(tables[0]["name"], logs[0], other["name"], logs[i])


if __name__ == "__main__":
    main()
