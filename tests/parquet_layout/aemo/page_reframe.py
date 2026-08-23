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
import os
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
DATAPH_FIELDS = ["num_values", "encoding", "definition_level_encoding",
                 "repetition_level_encoding", "statistics"]
DATAPH_I32 = [1, 2, 3, 4]
# Statistics takes NO i32list: null_count and distinct_count are both I64. Getting that wrong
# re-emits them one width too wide, which still parses and is not what the writer wrote.
STAT_FIELDS = ["max", "min", "null_count", "distinct_count", "max_value", "min_value",
               "is_max_value_exact", "is_min_value_exact"]
# What parquet-cpp actually puts in a DATA PAGE's Statistics — measured, not assumed. The rg0 DUID
# page from writer_bytes run 32619755844 (pyarrow 25.0.1) decodes as:
#
#   1c              field 5 of DataPageHeader, struct  -> statistics
#     36 00         field 3, I64 zigzag                -> null_count = 0  (written even when zero)
#     28 05 ...     field 5, binary                    -> max_value = b'YWPS3'
#     18 05 ...     field 6, binary                    -> min_value = b'ARWF1'
#     11            field 7, bool-true                 -> is_max_value_exact
#     11            field 8, bool-true                 -> is_min_value_exact
#     00            stop
#
# The deprecated min/max (1, 2) and distinct_count (4) are absent. This constant is the expected
# answer, not the trusted one: calibrate_page_stats re-derives the set from whatever pyarrow is
# installed and refuses to run if the rebuild is not byte-identical, so a version bump fails loudly
# instead of quietly stamping a header parquet-cpp would never have written.
PAGE_STAT_FIELDS = ("null_count", "max_value", "min_value",
                    "is_max_value_exact", "is_min_value_exact")
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
    """Compress a page body. WHICH snappy matters, so it is a knob rather than an assumption.

    Two valid snappy encoders can emit different bytes for the same input, and they do here: on the
    real fact cramjam and parquet-cpp disagree by a byte or two per page (on synthetic data they
    agree exactly, which is how this went unnoticed). Every re-framing run to date recompressed
    through cramjam, so none of them ever produced parquet-cpp's bytes — which makes "the framing
    is a no-op" a claim about cramjam's output, not about the framing.

    pyarrow's compressor IS the one parquet-cpp writes through, so `pyarrow` reproduces the fast
    writer's bytes exactly; `cramjam` is kept to reproduce the earlier null results.
    """
    if codec == 0:
        return raw
    impl = (os.environ.get("PAGE_COMPRESSOR") or "pyarrow").strip().lower()
    if codec == 1 and impl == "pyarrow":
        import pyarrow as pa
        return bytes(pa.compress(raw, codec="snappy", asbytes=True))
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


def _stats_obj(fields, lo, hi, null_count):
    """Build a page-level Statistics carrying exactly `fields`, in parquet-cpp's shape.

    Only the fields fastparquet knows about. The exactness flags are spliced on separately — see
    _exact_flag_bytes for why.
    """
    from fastparquet.cencoding import ThriftObject

    kw = {}
    if "null_count" in fields:
        kw["null_count"] = null_count
    if lo is not None and "min_value" in fields:
        kw["min_value"] = lo
    if hi is not None and "max_value" in fields:
        kw["max_value"] = hi
    return ThriftObject.from_fields("Statistics", **kw)


def _exact_flag_bytes(fields, lo, hi):
    """Serialize is_max_value_exact / is_min_value_exact by hand.

    fastparquet's vendored parquet.thrift stops at field 6 (`min_value`) — it has never heard of
    `is_max_value_exact` (7) or `is_min_value_exact` (8), so it can neither parse them nor write
    them. That is the same class of blind spot as §9.2: a tool that cannot see a field reports
    every file as if the field did not exist, and parquet-cpp writes BOTH of them on every data
    page. Without these two bytes the stamped header is 20 bytes of statistics that still is not
    what the fast writer emits, and the whole experiment would be measuring a near-miss.

    They are cheap to emit correctly: in thrift's compact protocol a boolean field IS its type
    nibble, so each is one byte of (delta << 4) | BOOLEAN_TRUE.
    """
    last = 0
    if "null_count" in fields:
        last = 3
    if hi is not None and "max_value" in fields:
        last = 5
    if lo is not None and "min_value" in fields:
        last = 6
    out = bytearray()
    for fid, want in ((7, hi is not None and "is_max_value_exact" in fields),
                      (8, lo is not None and "is_min_value_exact" in fields)):
        if not want:
            continue
        delta = fid - last
        if not 1 <= delta <= 15:
            raise SystemExit(f"page_reframe: cannot encode statistics field {fid} at delta {delta}")
        out.append((delta << 4) | 1)                 # 1 = BOOLEAN_TRUE, no value bytes follow
        last = fid
    return bytes(out)


def _header_bytes(ph, dph, fields, lo, hi, nulls):
    """The full PageHeader for a data page, with page statistics stamped in."""
    st = _stats_obj(fields, lo, hi, nulls)
    new_ph = _obj("PageHeader", PH_I32, ph, PH_FIELDS,
                  data_page_header=_obj("DataPageHeader", DATAPH_I32, dph, DATAPH_FIELDS,
                                        statistics=st))
    raw = bytes(new_ph.to_bytes())
    flags = _exact_flag_bytes(fields, lo, hi)
    if not flags:
        return raw
    # statistics is the last field of DataPageHeader, which is the last field of PageHeader, so the
    # serialization ends with exactly three stop bytes: Statistics, DataPageHeader, PageHeader. The
    # flags belong inside Statistics, i.e. immediately before the first of them. Assert the shape
    # rather than assume it — a splice into the wrong structure would still parse.
    if not raw.endswith(b"\x00\x00\x00"):
        raise SystemExit("page_reframe: page header does not end in the three expected stop bytes "
                         f"({raw[-6:].hex()}) — refusing to splice the exactness flags")
    return raw[:-3] + flags + b"\x00\x00\x00"


def _page_minmax(raw, optional, num_values, dictionary, name):
    """(min, max, null_count) for one uncompressed v1 dictionary-encoded data page.

    DECODE ONLY. Nothing here re-encodes a payload, which is why this carries no RLE-run refusal:
    reframe_bytes and repack_bitwidth_bytes refuse RLE runs because converting them to bit-packed
    would change two things at once, but reading values out of them changes nothing at all.
    """
    import numpy as np

    import payload_diff as PD

    pos, null_count = 0, 0
    if optional:
        (dl,) = struct.unpack_from("<I", raw, 0)
        levels, _ = PD.decode_hybrid(raw[4:4 + dl], 1, num_values)
        null_count = int(num_values) - int(levels.sum())
        pos = 4 + dl
    # num_values counts nulls, the index stream does not. DUID has none, but a column that did
    # would decode one value short forever if this used num_values directly.
    n_present = int(num_values) - null_count
    if n_present == 0:
        return None, None, null_count
    bit_width = raw[pos]
    idx, _ = PD.decode_hybrid(raw[pos + 1:], bit_width, n_present)
    if len(idx) != n_present:
        raise SystemExit(f"page_reframe: {name} page decoded {len(idx)} of {n_present} indices — "
                         "refusing to compute statistics from a short read")
    uniq = np.unique(idx)
    if int(uniq[-1]) >= len(dictionary):
        raise SystemExit(f"page_reframe: {name} index {int(uniq[-1])} exceeds its "
                         f"{len(dictionary)}-value dictionary — refusing")
    vals = [dictionary[int(i)] for i in uniq]
    # bytes compare unsigned lexicographically in Python, which is parquet's UNSIGNED order for
    # a UTF8 BYTE_ARRAY — the order parquet-cpp writes these in.
    return min(vals), max(vals), null_count


def _chunk_dictionary(chunk, m, name):
    """The chunk's dictionary values, decoded once, or None if it has no dictionary page."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, from_buffer

    import payload_diff as PD

    io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
    while io.tell() < len(chunk) - 4:
        ph = from_buffer(io, "PageHeader")
        body = chunk[io.tell():io.tell() + ph.compressed_page_size]
        io.seek(ph.compressed_page_size, 1)
        if ph.type != DICTIONARY_PAGE:
            continue
        raw = PD._decompress(body, m.codec, ph.uncompressed_page_size)
        return PD.parse_plain_byte_array(raw, ph.dictionary_page_header.num_values)
    raise SystemExit(f"page_reframe: {name} has no dictionary page — page statistics here would "
                     "mean decoding PLAIN values, which this tool does not do. Refusing.")


def _walk_stats(blob, column, fields, verify_only):
    """Shared engine: rebuild every data page header of `column` with Statistics stamped in.

    `verify_only` is the calibration mode — the source already HAS page statistics, so the rebuild
    must reproduce the original header bytes exactly rather than replace them.
    """
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
        by_name[se.name.decode() if isinstance(se.name, bytes) else se.name] = se

    out = bytearray(MARKER)
    n_pages, n_cols, seen_fields = 0, 0, set()
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
            touched = column is None or name == column
            # A rebuild copies chunks and moves every offset, but a bloom filter lives OUTSIDE the
            # chunk and would be left behind with its footer offset still pointing into the old
            # file. The benchmark writes WRITE_BLOOM_FILTER false, so this should never fire; if it
            # does, the alternative is a silently corrupt file.
            if getattr(m, "bloom_filter_offset", None) is not None:
                raise SystemExit(f"page_reframe: {name} carries a bloom filter, which a rebuild "
                                 "would strand. Write with WRITE_BLOOM_FILTER false. Refusing.")
            if touched and se is not None and se.type != 6:
                raise SystemExit(f"page_reframe: {name} is physical type {se.type}, not BYTE_ARRAY."
                                 " Signed/unsigned min-max order is type-dependent and this tool "
                                 "will not guess it. Refusing.")
            dictionary = _chunk_dictionary(chunk, m, name) if touched else None

            io = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            col_start = len(out)
            dict_off = data_off = None
            lo_all = hi_all = None
            nulls_all = 0
            while io.tell() < len(chunk) - 4:
                p0 = io.tell()
                ph = from_buffer(io, "PageHeader")
                hdr = bytes(chunk[p0:io.tell()])
                body = chunk[io.tell():io.tell() + ph.compressed_page_size]
                io.seek(ph.compressed_page_size, 1)
                if ph.type == DICTIONARY_PAGE:
                    dict_off = len(out)
                    out += bytes(ph.to_bytes()) + body
                    continue
                if data_off is None:
                    data_off = len(out)
                if ph.type != DATA_PAGE or not touched:
                    out += bytes(ph.to_bytes()) + body
                    continue

                dph = ph.data_page_header
                if dph.encoding not in (2, 8):
                    raise SystemExit(f"page_reframe: {name} data page is encoding {dph.encoding}, "
                                     "not dictionary-encoded — refusing")
                # GUARD 1. Rebuild the header WITHOUT touching statistics and require it to be
                # byte-identical to what the writer emitted. An i32 re-emitted as i64, or a field
                # order that disagrees with the spec, still parses and is silently a different
                # file; this is the only thing that catches that mechanically.
                probe = _obj("PageHeader", PH_I32, ph, PH_FIELDS,
                             data_page_header=_obj("DataPageHeader", DATAPH_I32, dph,
                                                   DATAPH_FIELDS))
                if bytes(probe.to_bytes()) != hdr:
                    raise SystemExit(
                        f"page_reframe: data page header rebuild is not byte-identical ({name}) — "
                        f"an i32/i64 width or field-order bug. Refusing to write.\n"
                        f"  writer   {hdr.hex()}\n  rebuilt  {bytes(probe.to_bytes()).hex()}")

                src_st = getattr(dph, "statistics", None)
                if verify_only and src_st is None:
                    raise SystemExit(f"page_reframe: calibration file has no page statistics "
                                     f"on {name} — nothing to calibrate against")
                if not verify_only and src_st is not None:
                    out += hdr + body
                    continue

                raw = PD._decompress(body, m.codec, ph.uncompressed_page_size)
                lo, hi, nulls = _page_minmax(raw, optional, dph.num_values, dictionary, name)

                if verify_only:
                    # THE GATE. The synthesizer does not go near the slow writer's file until it
                    # reproduces parquet-cpp's own bytes exactly. The field set is DERIVED here
                    # rather than assumed: fastparquet cannot see fields 7 and 8 at all, so the
                    # only honest way to learn whether this pyarrow writes them is to build each
                    # candidate and compare bytes.
                    base = tuple(f for f in STAT_FIELDS if getattr(src_st, f, None) is not None)
                    use = None
                    for extra in (("is_max_value_exact", "is_min_value_exact"), (),
                                  ("is_max_value_exact",), ("is_min_value_exact",)):
                        if _header_bytes(ph, dph, base + extra, lo, hi, nulls) == hdr:
                            use = base + extra
                            break
                    if use is None:
                        raise SystemExit(
                            f"page_reframe: calibration FAILED on {name} — the synthesized page "
                            f"statistics are not the bytes pyarrow wrote, under any exactness-flag "
                            f"combination. Refusing.\n"
                            f"  pyarrow      {hdr.hex()}\n"
                            f"  synthesized  "
                            f"{_header_bytes(ph, dph, base + PAGE_STAT_FIELDS[3:], lo, hi, nulls).hex()}")
                    seen_fields.add(use)
                    out += hdr + body
                    n_pages += 1
                    continue

                use = fields
                new_hdr = _header_bytes(ph, dph, use, lo, hi, nulls)

                # GUARD 3. Parse it back: nothing but statistics may have moved.
                back = from_buffer(NumpyIO(np.frombuffer(new_hdr + body, dtype="uint8")),
                                   "PageHeader")
                bd = back.data_page_header
                bs = getattr(bd, "statistics", None)
                if (back.type != ph.type or back.compressed_page_size != ph.compressed_page_size
                        or back.uncompressed_page_size != ph.uncompressed_page_size
                        or bd.num_values != dph.num_values or bd.encoding != dph.encoding
                        or bd.definition_level_encoding != dph.definition_level_encoding
                        or bd.repetition_level_encoding != dph.repetition_level_encoding
                        or bs is None
                        or ("min_value" in use and bs.min_value != lo)
                        or ("max_value" in use and bs.max_value != hi)
                        or ("null_count" in use and bs.null_count != nulls)):
                    raise SystemExit(f"page_reframe: stamped data page header changed more than "
                                     f"statistics ({name}) — refusing to write")
                out += new_hdr + body
                lo_all = lo if lo_all is None else min(lo_all, lo)
                hi_all = hi if hi_all is None else max(hi_all, hi)
                nulls_all += nulls
                n_pages += 1

            if touched and not verify_only and lo_all is not None:
                # GUARD 2. Cross-check the whole chunk against the footer statistics the WRITER
                # produced. A decoder bug would manufacture plausible wrong values; DuckDB's own
                # footer is ground truth it cannot have agreed with our decoder on.
                fs = m.statistics
                f_lo = getattr(fs, "min_value", None) or getattr(fs, "min", None)
                f_hi = getattr(fs, "max_value", None) or getattr(fs, "max", None)
                if f_lo is not None and bytes(f_lo) != lo_all:
                    raise SystemExit(f"page_reframe: {name} pages say min={lo_all!r} but the "
                                     f"footer says {bytes(f_lo)!r} — decoder disagrees with the "
                                     "writer. Refusing.")
                if f_hi is not None and bytes(f_hi) != hi_all:
                    raise SystemExit(f"page_reframe: {name} pages say max={hi_all!r} but the "
                                     f"footer says {bytes(f_hi)!r} — decoder disagrees with the "
                                     "writer. Refusing.")
                f_nulls = getattr(fs, "null_count", None)
                if f_nulls is not None and int(f_nulls) != nulls_all:
                    raise SystemExit(f"page_reframe: {name} pages say {nulls_all} nulls but the "
                                     f"footer says {int(f_nulls)} — refusing")
                n_cols += 1

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
    return bytes(out), {"pages_stamped": n_pages, "columns": n_cols, "field_sets": seen_fields,
                        "bytes_before": len(blob), "bytes_after": len(out)}


def calibrate_page_stats(template_blob, column=None):
    """Prove the synthesizer reproduces parquet-cpp's page statistics byte for byte.

    The gate on everything below. Page statistics are the one property findings §9.6 names as never
    tested by ADDITION — removal from pyarrow was measured (443.8 ms, still fast) but nothing has
    ever put them into DuckDB's file, because no tool could write a DataPageHeader. Writing one by
    hand is exactly where this goes wrong quietly, so the synthesizer is run first against a file
    parquet-cpp itself wrote and is required to reproduce its headers exactly. If it cannot, the
    field set, the field order, the exactness flags or an integer width is wrong, and nothing it
    stamps into the slow writer's file would mean anything.

    Returns the field set pyarrow actually writes, which is then what gets stamped.
    """
    import pyarrow as pa

    _new, st = _walk_stats(template_blob, column, None, verify_only=True)
    if not st["pages_stamped"]:
        raise SystemExit("page_reframe: calibration file had no data pages to check")
    if len(st["field_sets"]) != 1:
        raise SystemExit(f"page_reframe: calibration file writes inconsistent statistics field "
                         f"sets {st['field_sets']} — refusing to pick one")
    fields = next(iter(st["field_sets"]))
    print(f"  [ok] page-stats calibration: {st['pages_stamped']} page(s) reproduced byte for byte "
          f"against pyarrow {pa.__version__}; fields {fields}", flush=True)
    return fields


def calibration_blob(values, rows=40960, page_bytes=20480):
    """A small parquet-cpp file to calibrate against when no pyarrow arm exists in the run.

    The Fabric build writes one file, DuckDB's, so there is nothing to calibrate against unless one
    is made. Same role as payload_diff's selftest: the tool proves itself against a real
    parquet-cpp file before any result it produces is trusted.
    """
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    vals = [values[i % len(values)] for i in range(rows)]
    tbl = pa.table({"c": pa.array(vals, type=pa.string())})
    buf = io.BytesIO()
    w = pq.ParquetWriter(buf, tbl.schema, compression="snappy", use_dictionary=True,
                         version="1.0", write_statistics=True, data_page_size=page_bytes)
    w.write_table(tbl, row_group_size=rows)
    w.close()
    return buf.getvalue()


def add_page_stats_bytes(blob, column=None, fields=PAGE_STAT_FIELDS):
    """Stamp per-page Statistics into every data page header. Diagnostic only.

    The last structural difference standing. parquet-cpp writes min_value/max_value/null_count into
    every DataPageHeader; DuckDB writes none, and no COPY option produces them. Findings §9.6 named
    this the weakest link in the whole record: it was only ever tested by REMOVAL from the fast
    writer, never by ADDITION to the slow one, and until now no tool could construct a
    DataPageHeader at all.

    Only the header changes. Page bodies, levels, dictionary pages, values and row groups are copied
    byte for byte — the values are read out to compute min/max and never re-encoded — but the header
    grows ~20 bytes per page, so the file is rebuilt and every offset recomputed.
    """
    return _walk_stats(blob, column, tuple(fields), verify_only=False)


def _first_dictionary(blob, column):
    """The first dictionary this column carries, so calibration uses realistic values."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, from_buffer

    flen = struct.unpack("<I", blob[-8:-4])[0]
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[len(blob) - 8 - flen:-8], dtype="uint8")),
                      "FileMetaData")
    for rg in fmd.row_groups:
        for cc in rg.columns:
            m = cc.meta_data
            name = ".".join(x.decode() if isinstance(x, bytes) else x for x in m.path_in_schema)
            if column is not None and name != column:
                continue
            start = m.dictionary_page_offset or m.data_page_offset
            return _chunk_dictionary(blob[start:start + m.total_compressed_size], m, name)
    raise SystemExit(f"page_reframe: no chunk found for column {column!r}")


def add_page_stats_remote(store, keys, column=None):
    """Download, stamp and replace each key in place (single PUT — OneLake rejects multipart).

    Calibration happens HERE, per file, rather than at the call site: the Fabric build writes only
    DuckDB's file, so unless one is made there is no parquet-cpp output to check against, and an
    uncalibrated synthesizer is exactly the failure mode this whole campaign is trying to avoid.
    """
    import obstore

    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        vals = [v.decode("utf-8", "replace") for v in _first_dictionary(blob, column)]
        fields = calibrate_page_stats(calibration_blob(vals), "c")
        new, stats = add_page_stats_bytes(blob, column=column, fields=fields)
        if not stats["pages_stamped"]:
            raise SystemExit(f"OPT_PAGE_STATS was set but {key} had no data page to stamp — the "
                             "writer may already emit page statistics, which would make this a "
                             "silent no-op control.")
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: page statistics on {stats['pages_stamped']} page(s) across "
              f"{stats['columns']} chunk(s), {stats['bytes_before']:,} -> "
              f"{stats['bytes_after']:,} bytes", flush=True)


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
