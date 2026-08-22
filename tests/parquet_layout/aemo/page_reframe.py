"""Rewrite a DuckDB parquet's data pages at 63-group bit-packed runs. Diagnostic only.

The three-writer run left exactly one page-internal property separating the slow file from the two
fast ones. Same rows, same sort, same geometry:

    delta-rs (parquet-rs)  502 ms   504 values per bit-packed run
    pyarrow  (parquet-cpp) 553 ms   500 values per bit-packed run
    DuckDB                11196 ms  256 values per bit-packed run

504 is 63 groups of 8, the largest run a canonical single-byte header expresses; both fast writers
emit maximal runs. Page SIZE is ruled out — the two fast writers differ from each other by 46x on
values per page and 43x on pages per chunk — and a local sweep of every DuckDB COPY option (V1/V2,
page limits from 64KB to 16MB, uncompressed/snappy/zstd, dictionary limits, row group sizes) leaves
the run length at 256 in every single one. It is fixed in the encoder.

So change it directly and re-measure. This re-encodes the dictionary indices at `GROUPS` groups per
run and changes NOTHING else: same values, same bit width, same dictionary page, same definition
levels (copied through verbatim), same page boundaries, same row groups. Page sizes shift, so the
whole file is rewritten and every offset recomputed — that is the only reason this is more than a
footer patch.

Correctness is enforced, not assumed: every re-encoded page is decoded again and compared to the
values that went in, and the run refuses to write a file if any page fails.
"""
import struct

MARKER = b"PAR1"
GROUPS = 63                     # 63 * 8 = 504 values, what parquet-rs and parquet-cpp both emit

DICTIONARY_PAGE, DATA_PAGE = 2, 0

PH_FIELDS = ["type", "uncompressed_page_size", "compressed_page_size", "crc",
             "data_page_header", "index_page_header", "dictionary_page_header",
             "data_page_header_v2"]
PH_I32 = [1, 2, 3, 4]
CMD_FIELDS = ["type", "encodings", "path_in_schema", "codec", "num_values",
              "total_uncompressed_size", "total_compressed_size", "key_value_metadata",
              "data_page_offset", "index_page_offset", "dictionary_page_offset", "statistics",
              "encoding_stats", "bloom_filter_offset", "bloom_filter_length", "size_statistics",
              "geospatial_statistics"]
CMD_I32 = [1, 4, 15]
CC_FIELDS = ["file_path", "file_offset", "meta_data", "offset_index_offset",
             "offset_index_length", "column_index_offset", "column_index_length",
             "crypto_metadata", "encrypted_column_metadata"]
CC_I32 = [5, 7]
RG_FIELDS = ["columns", "total_byte_size", "num_rows", "sorting_columns", "file_offset",
             "total_compressed_size", "ordinal"]
RG_I32 = [7]
FMD_FIELDS = ["version", "schema", "num_rows", "row_groups", "key_value_metadata", "created_by",
              "column_orders", "encryption_algorithm", "footer_signing_key_metadata"]


def _obj(name, i32, src, fields, **override):
    from fastparquet.cencoding import ThriftObject
    kw = {f: getattr(src, f) for f in fields if getattr(src, f, None) is not None}
    kw.update({k: v for k, v in override.items() if v is not None})
    return ThriftObject.from_fields(name, i32list=i32, **kw)


def _compress(raw, codec):
    if codec == 0:
        return raw
    import cramjam
    if codec == 1:
        return bytes(cramjam.snappy.compress_raw(raw))
    if codec == 2:
        return bytes(cramjam.gzip.compress(raw))
    if codec == 6:
        return bytes(cramjam.zstd.compress(raw))
    raise SystemExit(f"page_reframe: unsupported codec {codec}")


def _varint_bytes(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def encode_hybrid(vals, bit_width, groups=GROUPS):
    """Bit-pack `vals` in runs of `groups` groups of 8 — the inverse of payload_diff.decode_hybrid."""
    import numpy as np

    out = bytearray()
    per_run = groups * 8
    i, n = 0, len(vals)
    while i < n:
        take = min(per_run, n - i)
        chunk = np.asarray(vals[i:i + take], dtype=np.int64)
        g = (take + 7) // 8                      # pad the final run up to a whole group
        if take < g * 8:
            chunk = np.concatenate([chunk, np.zeros(g * 8 - take, dtype=np.int64)])
        out += _varint_bytes((g << 1) | 1)
        bits = ((chunk[:, None] >> np.arange(bit_width, dtype=np.int64)) & 1).astype(np.uint8)
        out += np.packbits(bits.ravel(), bitorder="little").tobytes()
        i += take
    return bytes(out)


def reframe_bytes(blob, column=None, groups=GROUPS):
    """Return (new_blob, stats). `column` limits the rewrite; None rewrites every column."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    import payload_diff as PD

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    by_name = {}
    for se in fmd.schema:
        nm = se.name.decode() if isinstance(se.name, bytes) else se.name
        by_name[nm] = se

    out = bytearray(MARKER)
    n_pages = n_cols = 0
    row_groups = []
    for rg in fmd.row_groups:
        rg_start = len(out)
        columns = []
        for cc in rg.columns:
            m = cc.meta_data
            name = ".".join(x.decode() if isinstance(x, bytes) else x for x in m.path_in_schema)
            se = by_name.get(name)
            optional = se is not None and se.repetition_type == 1
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))

            col_start = len(out)
            dict_off = data_off = None
            raw_total = 0
            touched = column is None or name == column
            while io.tell() < len(chunk) - 4:
                ph = from_buffer(io, "PageHeader")
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type == DICTIONARY_PAGE:
                    dict_off = len(out)
                    out += bytes(ph.to_bytes()) + body
                    raw_total += ph.uncompressed_page_size
                    continue
                if data_off is None:
                    data_off = len(out)
                if ph.type != DATA_PAGE or not touched:
                    out += bytes(ph.to_bytes()) + body
                    raw_total += ph.uncompressed_page_size
                    continue

                raw = PD._decompress(body, m.codec, ph.uncompressed_page_size)
                pos = 0
                if optional:
                    (dl,) = struct.unpack_from("<I", raw, 0)
                    pos = 4 + dl
                levels = raw[:pos]
                bit_width = raw[pos]
                vals, st0 = PD.decode_hybrid(raw[pos + 1:], bit_width,
                                             ph.data_page_header.num_values)
                # This encoder emits bit-packed runs only. If the source page used RLE runs, a
                # rewrite would change TWO things at once and the measurement would be worthless.
                # The column under test has zero RLE runs in every row group; fail loudly if that
                # ever stops being true rather than quietly converting them.
                if st0["rle_runs"]:
                    raise SystemExit(
                        f"page_reframe: {name} has {st0['rle_runs']} RLE run(s) in a page — "
                        "re-framing would convert them to bit-packed and stop being a "
                        "single-variable change. Refusing.")
                new_raw = levels + bytes([bit_width]) + encode_hybrid(vals, bit_width, groups)

                check, st2 = PD.decode_hybrid(new_raw[pos + 1:], bit_width, len(vals))
                if not np.array_equal(check, vals):
                    raise SystemExit(f"page_reframe: re-encoded page does not decode back to the "
                                     f"same values ({name}) — refusing to write")
                if st2["mean_bitpacked_len"] < groups * 8 * 0.5 and len(vals) > groups * 8:
                    raise SystemExit(f"page_reframe: re-encode produced runs of "
                                     f"{st2['mean_bitpacked_len']} ({name}) — expected {groups * 8}")
                comp = _compress(new_raw, m.codec)
                new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS,
                              uncompressed_page_size=len(new_raw), compressed_page_size=len(comp))
                out += bytes(new_ph.to_bytes()) + comp
                raw_total += len(new_raw)
                n_pages += 1
            if touched:
                n_cols += 1
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=dict_off,
                        data_page_offset=data_off if data_off is not None else dict_off,
                        total_compressed_size=len(out) - col_start,
                        total_uncompressed_size=raw_total)
            columns.append(_obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_byte_size=len(out) - rg_start,
                               total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"pages": n_pages, "columns": n_cols,
                        "bytes_before": len(blob), "bytes_after": len(out)}


def reframe_remote(store, keys, column=None, groups=GROUPS):
    """Download, reframe and replace each key in place (single PUT — OneLake rejects multipart)."""
    import obstore

    stats = {}
    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, st = reframe_bytes(blob, column=column, groups=groups)
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: {st['pages']:,} pages re-framed to {groups} groups "
              f"({groups * 8} values), {st['bytes_before']:,} -> {st['bytes_after']:,} bytes",
              flush=True)
        stats = st
    return stats
