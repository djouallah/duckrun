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
DPH_FIELDS = ["num_values", "encoding", "is_sorted"]
DPH_I32 = [1, 2]
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


def mark_dict_sorted_bytes(blob, column=None):
    """Write the explicit ``is_sorted=False`` on every dictionary page that DuckDB omits.

    The last page-internal difference between the slow writer and the two fast ones, and the one
    every previous experiment structurally could not reach: footer injection never touches page
    headers, and ``reframe_bytes`` copies dictionary page headers through verbatim. Both fast
    writers emit the field (17-byte header); DuckDB leaves it unset (16 bytes). Absent and false
    ought to mean the same thing to a reader, which is exactly why it is worth measuring rather
    than assuming.

    Only that one field changes. Page bodies, data pages, levels, dictionary values and row groups
    are all copied byte for byte; the header grows by a byte, so the file is rebuilt and every
    offset recomputed.
    """
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    out = bytearray(MARKER)
    n_marked = 0
    row_groups = []
    for rg in fmd.row_groups:
        rg_start = len(out)
        columns = []
        for cc in rg.columns:
            m = cc.meta_data
            name = ".".join(x.decode() if isinstance(x, bytes) else x for x in m.path_in_schema)
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            col_start = len(out)
            dict_off = data_off = None
            touched = column is None or name == column
            while io.tell() < len(chunk) - 4:
                ph = from_buffer(io, "PageHeader")
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type != DICTIONARY_PAGE:
                    if data_off is None:
                        data_off = len(out)
                    out += bytes(ph.to_bytes()) + body
                    continue
                dict_off = len(out)
                dph = ph.dictionary_page_header
                if not touched or getattr(dph, "is_sorted", None) is not None:
                    out += bytes(ph.to_bytes()) + body
                    continue
                new_dph = ThriftObject.from_fields("DictionaryPageHeader", i32list=DPH_I32,
                                                   num_values=dph.num_values,
                                                   encoding=dph.encoding, is_sorted=False)
                new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS, dictionary_page_header=new_dph)
                # Round-trip it: a header that no longer parses, or that changed anything other
                # than is_sorted, would be a silently different file rather than a measurement.
                raw = bytes(new_ph.to_bytes())
                back = from_buffer(NumpyIO(np.frombuffer(raw + body, dtype="uint8")), "PageHeader")
                bd = back.dictionary_page_header
                if (back.type != ph.type or back.compressed_page_size != ph.compressed_page_size
                        or back.uncompressed_page_size != ph.uncompressed_page_size
                        or bd.num_values != dph.num_values or bd.encoding != dph.encoding
                        or bd.is_sorted is not False):
                    raise SystemExit(f"page_reframe: dictionary page header rebuild changed more "
                                     f"than is_sorted ({name}) — refusing to write")
                out += raw + body
                n_marked += 1
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=dict_off,
                        data_page_offset=data_off if data_off is not None else dict_off,
                        total_compressed_size=len(out) - col_start)
            columns.append(_obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"dictionary_pages_marked": n_marked,
                        "bytes_before": len(blob), "bytes_after": len(out)}


def repack_bitwidth_bytes(blob, column=None, mode="uniform", groups=GROUPS):
    """Re-encode a column's dictionary indices at a different bit width. Diagnostic only.

    The width is not a free choice for the writer: it must cover the row group's dictionary, and
    DuckDB picks one width per chunk while parquet-cpp picks one per page. On the real fact that
    makes DuckDB flip 8 -> 9 at row group 5, exactly where the dictionary crosses 256 (251 values
    in rg4, 264 in rg5), while the engine's resident segments are 9 bits for all 24. So for the
    first five row groups the stored width disagrees with the resident width, and a consumer that
    sizes a global dictionary from what it saw first would have to widen what it already built.

    No COPY option reaches this, which is why it has never been measured. Two modes:

      uniform  one width for the whole column - the widest any row group needs - so the width
               never changes down the file (tests the "keep it consistent" reading)
      min      the narrowest width each page can carry, which is what parquet-cpp does

    Values, dictionary, definition levels and page boundaries are all preserved; only the width
    byte and the packing change. Every page is decoded again and refused on mismatch.
    """
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    import payload_diff as PD

    if mode not in ("uniform", "min"):
        raise SystemExit(f"page_reframe: repack mode must be uniform or min, got {mode!r}")
    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    by_name = {}
    for se in fmd.schema:
        by_name[se.name.decode() if isinstance(se.name, bytes) else se.name] = se

    def _chunks():
        for rg in fmd.row_groups:
            for cc in rg.columns:
                nm = ".".join(x.decode() if isinstance(x, bytes) else x
                              for x in cc.meta_data.path_in_schema)
                if column is None or nm == column:
                    yield nm, cc.meta_data

    # Pass one: the widest width the column needs anywhere, so `uniform` never truncates.
    global_width = 0
    if mode == "uniform":
        for _nm, m in _chunks():
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            while io.tell() < len(chunk) - 4:
                ph = from_buffer(io, "PageHeader")
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type != DATA_PAGE:
                    continue
                raw = PD._decompress(body, m.codec, ph.uncompressed_page_size)
                global_width = max(global_width, raw[_levels_end(raw, blob, fmd, by_name, _nm)])
        if not global_width:
            raise SystemExit("page_reframe: found no data pages to repack")

    out = bytearray(MARKER)
    n_pages, widths = 0, {}
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
                levels, src_width = raw[:pos], raw[pos]
                vals, st0 = PD.decode_hybrid(raw[pos + 1:], src_width,
                                             ph.data_page_header.num_values)
                if st0["rle_runs"]:
                    raise SystemExit(f"page_reframe: {name} has RLE runs — repacking would change "
                                     "two things at once. Refusing.")
                hi = int(max(vals)) if len(vals) else 0
                need = max(1, hi.bit_length())
                width = global_width if mode == "uniform" else need
                if width < need:
                    raise SystemExit(f"page_reframe: width {width} cannot hold index {hi} ({name})")
                new_raw = levels + bytes([width]) + encode_hybrid(vals, width, groups)
                check, _ = PD.decode_hybrid(new_raw[pos + 1:], width, len(vals))
                if not np.array_equal(check, vals):
                    raise SystemExit(f"page_reframe: repacked page does not decode back to the "
                                     f"same values ({name}) — refusing to write")
                comp = _compress(new_raw, m.codec)
                new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS,
                              uncompressed_page_size=len(new_raw), compressed_page_size=len(comp))
                out += bytes(new_ph.to_bytes()) + comp
                raw_total += len(new_raw)
                widths[(src_width, width)] = widths.get((src_width, width), 0) + 1
                n_pages += 1
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=dict_off,
                        data_page_offset=data_off if data_off is not None else dict_off,
                        total_compressed_size=len(out) - col_start,
                        total_uncompressed_size=raw_total)
            columns.append(_obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"pages": n_pages, "mode": mode, "uniform_width": global_width or None,
                        "transitions": {f"{a}->{b}": c for (a, b), c in sorted(widths.items())},
                        "bytes_before": len(blob), "bytes_after": len(out)}


def _levels_end(raw, blob, fmd, by_name, name):
    """Byte offset of the bit-width marker inside an uncompressed v1 data page."""
    se = by_name.get(name)
    if se is not None and se.repetition_type == 1:
        (dl,) = struct.unpack_from("<I", raw, 0)
        return 4 + dl
    return 0


def repack_bitwidth_remote(store, keys, column=None, mode="uniform"):
    """Download, repack and replace each key in place (single PUT — OneLake rejects multipart)."""
    import obstore

    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, stats = repack_bitwidth_bytes(blob, column=column, mode=mode)
        if not stats["pages"]:
            raise SystemExit(f"OPT_BITWIDTH was set but {key} had no data pages to repack.")
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: repacked {stats['pages']} page(s) mode={stats['mode']} "
              f"width={stats['uniform_width']} transitions={stats['transitions']}, "
              f"{stats['bytes_before']:,} -> {stats['bytes_after']:,} bytes", flush=True)


def recompress_dict_bytes(blob, column=None):
    """Re-compress dictionary pages from the identical uncompressed values.

    The other half of the same blind spot as is_sorted. Every rewrite so far leaves the dictionary
    page body exactly as DuckDB's compressor emitted it: reframe copies it through verbatim, the
    transplant moves the whole chunk, footer injection never touches pages. So DuckDB's own snappy
    output for that page is present in every slow measurement and in none of the fast ones, and
    nothing has separated "DuckDB's bytes" from "DuckDB's compressor" for it.

    The values are provably unchanged - the page is decompressed, re-compressed, and decompressed
    again to confirm it round-trips to the same bytes - so the only thing that moves is which
    implementation produced the compressed stream.

    MEASURED AND RULED OUT, kept so the null result stays reproducible: re-compressing through
    cramjam reproduces DuckDB's dictionary page byte for byte (878 bytes on the local repro,
    bytes_delta 0), so DuckDB's compressor is not doing anything unusual - parquet-cpp's 895 is
    the outlier of the three. On the real fact the same page lands at 1462 (DuckDB), 1464
    (parquet-rs) and 1469 (parquet-cpp) bytes, one fast and one slow within seven bytes of each
    other, so the compressed dictionary bytes cannot be what the consumer reacts to.
    """
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    import payload_diff as PD

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    out = bytearray(MARKER)
    n_done, saved = 0, 0
    row_groups = []
    for rg in fmd.row_groups:
        rg_start = len(out)
        columns = []
        for cc in rg.columns:
            m = cc.meta_data
            name = ".".join(x.decode() if isinstance(x, bytes) else x for x in m.path_in_schema)
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            col_start = len(out)
            dict_off = data_off = None
            touched = column is None or name == column
            while io.tell() < len(chunk) - 4:
                ph = from_buffer(io, "PageHeader")
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type != DICTIONARY_PAGE:
                    if data_off is None:
                        data_off = len(out)
                    out += bytes(ph.to_bytes()) + body
                    continue
                dict_off = len(out)
                if not touched or m.codec == 0:
                    out += bytes(ph.to_bytes()) + body
                    continue
                raw = PD._decompress(body, m.codec, ph.uncompressed_page_size)
                comp = _compress(raw, m.codec)
                if PD._decompress(comp, m.codec, len(raw)) != raw:
                    raise SystemExit(f"page_reframe: re-compressed dictionary page does not "
                                     f"round-trip ({name}) — refusing to write")
                new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS,
                              uncompressed_page_size=len(raw), compressed_page_size=len(comp))
                out += bytes(new_ph.to_bytes()) + comp
                saved += len(body) - len(comp)
                n_done += 1
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=dict_off,
                        data_page_offset=data_off if data_off is not None else dict_off,
                        total_compressed_size=len(out) - col_start)
            columns.append(_obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"dictionary_pages_recompressed": n_done, "bytes_delta": -saved,
                        "bytes_before": len(blob), "bytes_after": len(out)}


def recompress_dict_remote(store, keys, column=None):
    """Download, re-compress dictionary pages and replace each key in place (single PUT)."""
    import obstore

    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, stats = recompress_dict_bytes(blob, column=column)
        if not stats["dictionary_pages_recompressed"]:
            raise SystemExit(f"OPT_DICT_RECOMPRESS was set but {key} had no compressed dictionary "
                             "page to re-compress — that would be a silent no-op control.")
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: re-compressed "
              f"{stats['dictionary_pages_recompressed']} dictionary page(s), "
              f"{stats['bytes_before']:,} -> {stats['bytes_after']:,} bytes", flush=True)


def set_dict_encoding_bytes(blob, column=None, encoding=2):
    """Rewrite the dictionary page's ``encoding`` tag. Diagnostic only.

    The other survivor alongside page statistics. DuckDB tags its dictionary page PLAIN (0) and
    its data pages PLAIN_DICTIONARY (2); parquet-cpp tags BOTH PLAIN_DICTIONARY; parquet-rs tags
    the dictionary PLAIN and the data RLE_DICTIONARY (8). So DuckDB is the only one of the three
    that mixes the modern spelling on the dictionary page with the deprecated spelling on the data
    pages, and a reader pairing the two to decide "does this chunk have a usable dictionary" would
    see a combination neither fast writer produces.

    It was set aside because parquet-rs also writes PLAIN and is fast - the same "both fast writers
    disagree" filter that also discarded page statistics, which assumes one rule makes both fast.
    Only the tag changes: page bodies, dictionary values, data pages and row groups are copied byte
    for byte, and every rebuilt header is parsed back and rejected if anything else moved.
    """
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    out = bytearray(MARKER)
    n_set = 0
    row_groups = []
    for rg in fmd.row_groups:
        rg_start = len(out)
        columns = []
        for cc in rg.columns:
            m = cc.meta_data
            name = ".".join(x.decode() if isinstance(x, bytes) else x for x in m.path_in_schema)
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            col_start = len(out)
            dict_off = data_off = None
            touched = column is None or name == column
            while io.tell() < len(chunk) - 4:
                ph = from_buffer(io, "PageHeader")
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type != DICTIONARY_PAGE:
                    if data_off is None:
                        data_off = len(out)
                    out += bytes(ph.to_bytes()) + body
                    continue
                dict_off = len(out)
                dph = ph.dictionary_page_header
                if not touched or dph.encoding == encoding:
                    out += bytes(ph.to_bytes()) + body
                    continue
                kw = {"num_values": dph.num_values, "encoding": encoding}
                if getattr(dph, "is_sorted", None) is not None:
                    kw["is_sorted"] = dph.is_sorted
                new_dph = ThriftObject.from_fields("DictionaryPageHeader", i32list=DPH_I32, **kw)
                new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS, dictionary_page_header=new_dph)
                raw = bytes(new_ph.to_bytes())
                back = from_buffer(NumpyIO(np.frombuffer(raw + body, dtype="uint8")), "PageHeader")
                bd = back.dictionary_page_header
                if (back.type != ph.type or back.compressed_page_size != ph.compressed_page_size
                        or back.uncompressed_page_size != ph.uncompressed_page_size
                        or bd.num_values != dph.num_values or bd.encoding != encoding
                        or getattr(bd, "is_sorted", None) != getattr(dph, "is_sorted", None)):
                    raise SystemExit(f"page_reframe: dictionary header rebuild changed more than "
                                     f"the encoding tag ({name}) — refusing to write")
                out += raw + body
                n_set += 1
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=dict_off,
                        data_page_offset=data_off if data_off is not None else dict_off,
                        total_compressed_size=len(out) - col_start)
            columns.append(_obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"dictionary_pages_retagged": n_set, "encoding": encoding,
                        "bytes_before": len(blob), "bytes_after": len(out)}


def set_dict_encoding_remote(store, keys, column=None, encoding=2):
    """Download, retag and replace each key in place (single PUT — OneLake rejects multipart)."""
    import obstore

    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, stats = set_dict_encoding_bytes(blob, column=column, encoding=encoding)
        if not stats["dictionary_pages_retagged"]:
            raise SystemExit(f"OPT_DICT_ENCODING was set but {key} had nothing to retag — the "
                             "writer may already use that tag, a silent no-op control.")
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: dictionary page encoding -> {encoding} on "
              f"{stats['dictionary_pages_retagged']} page(s), "
              f"{stats['bytes_before']:,} -> {stats['bytes_after']:,} bytes", flush=True)


def mark_dict_sorted_remote(store, keys, column=None):
    """Download, mark and replace each key in place (single PUT — OneLake rejects multipart)."""
    import obstore

    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, stats = mark_dict_sorted_bytes(blob, column=column)
        if not stats["dictionary_pages_marked"]:
            raise SystemExit(f"OPT_DICT_SORTED was set but {key} had nothing to mark — the writer "
                             "may already emit is_sorted, which would make this a no-op control.")
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: is_sorted=False on "
              f"{stats['dictionary_pages_marked']} dictionary page(s), "
              f"{stats['bytes_before']:,} -> {stats['bytes_after']:,} bytes", flush=True)


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
