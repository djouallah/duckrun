"""Add to a DuckDB parquet footer the metadata delta-rs writes and DuckDB does not. Diagnostic only.

A page-level diff of the real 142M-row tables (page_diff.py) showed the DATA is the same: for the
string column both writers produce a 217-value dictionary, RLE_DICTIONARY data pages, RLE levels,
no page statistics, and chunk sizes 0.2% apart. Yet delta-rs's file cold-reads that column in
~0.5s and DuckDB's in ~12.5s. Whatever costs the 12 seconds is therefore in the METADATA, and after
the diff exactly three differences remain:

  encstats     `encoding_stats` (PageEncodingStats) - delta-rs writes
               DICTIONARY_PAGE/PLAINx1 + DATA_PAGE/RLE_DICTIONARYxN on every chunk, DuckDB writes
               nothing (duckdb#12892, closed as not planned). It is the only way to prove a chunk
               is 100% dictionary-encoded without cracking pages open.
  offsetindex  `OffsetIndex` - the byte offset, size and first row of every page. delta-rs writes
               it, DuckDB has no page-index code at all (a code search for OffsetIndex across
               duckdb/duckdb returns one hit, in the Swift bindings). Without it a reader cannot
               locate page N without walking pages 0..N-1.
  encodings    `ColumnMetaData.encodings` - the spec requires every encoding used in the chunk.
               The page walk proves DuckDB uses three (dictionary page PLAIN, levels RLE, data
               RLE_DICTIONARY) and declares one: `['RLE_DICTIONARY']`, against delta-rs's
               `['PLAIN', 'RLE', 'RLE_DICTIONARY']`. Neither PLAIN nor PLAIN_DICTIONARY appears,
               so a reader testing that list for a dictionary page finds none.
  nodistinct   `Statistics.distinct_count` - DuckDB writes it, and BOTH fast writers omit it
               entirely (parquet-rs and parquet-cpp both leave it null). After run framing was
               re-encoded and measured as a no-op, this is the only known property the slow writer
               has that neither fast writer has. The field is widely treated as unreliable, so a
               consumer that trusts it - to pre-size a dictionary, say - would be sized from a
               PER-ROW-GROUP count for a column with more values globally.
  createdby    `created_by` - the writer's signature. Everything else has been eliminated: the
               data is identical, every metadata difference has been injected with no effect,
               running order is ruled out, and NEUTRAL READERS SHOW NO GAP AT ALL (pyarrow and
               DuckDB decode the DuckDB-written file 0.85-1.04x, i.e. marginally FASTER). A file
               that is fine by every normal measure but slow in one engine points at that engine
               recognising the writer, and this is the only input left untested.
  logical      `LogicalType` - already MEASURED AND RULED OUT (probe_duid 12585ms with it,
               12312-13091ms without). Kept so the null result stays reproducible.
  nodeprecstats
               deprecated `Statistics.min`/`max`. DuckDB writes BOTH the deprecated pair and the
               modern `min_value`/`max_value`; parquet-cpp writes only the modern pair. These
               fields were deprecated BECAUSE their ordering semantics are type-dependent and were
               unspecified for BYTE_ARRAY, so a reader that finds them set on a string column has
               reason to distrust or re-derive. That shape matches the measured penalty gradient
               (no cost on INT32, ~1.5-2.9x on decimals, ~20x on the string) better than any
               property ruled out so far. Drops the deprecated pair, keeps everything else.

Everything here rewrites metadata only. Page data is never touched, every offset in a parquet
footer is absolute from the start of the file, and new OffsetIndex structures are appended in the
gap the old footer occupied - so no column chunk ever moves.

FIDELITY IS ENFORCED, NOT ASSUMED. Rebuilding thrift by hand is where this goes wrong quietly: an
integer re-emitted as I64 instead of I32, or `_asdict()` handing back `name` as the repr of a bytes
object, both produce a footer that still parses and is subtly not what the writer wrote. So every
structure is rebuilt and compared against the original serialization first, and the whole patch
aborts unless the untouched rebuild is byte-identical.
"""
import struct

MARKER = b"PAR1"
FEATURES = ("logical", "encstats", "offsetindex", "encodings", "createdby", "nodistinct",
            "nodeprecstats", "arrowschema")

# Statistics field order, for rebuilding one WITHOUT a chosen field. None of these are I32:
# null_count and distinct_count are both I64, so no i32list is needed.
STAT_FIELDS = ["max", "min", "null_count", "distinct_count", "max_value", "min_value",
               "is_max_value_exact", "is_min_value_exact"]

# What `createdby` rewrites the footer's created_by string to. Readers legitimately
# branch on this: parquet-mr distrusts statistics from known-buggy writer versions, and
# arrow does the same, so a writer allowlist for a fast path is entirely plausible.
CREATED_BY = "parquet-rs version 57.3.0"

# ConvertedType ordinal -> the LogicalType union field that means the same thing.
PROMOTE = {0: "STRING", 6: "DATE"}

# Thrift field numbers that are I32 (or I16) rather than I64. Without these every integer is
# re-emitted one width too wide and the rebuild stops matching the writer byte for byte.
SE_I32 = [1, 2, 3, 5, 6, 7, 8, 9]                     # SchemaElement, all but name + logicalType
CMD_I32 = [1, 4, 15]                                  # ColumnMetaData: type, codec, bloom len
CC_I32 = [5, 7]                                       # ColumnChunk: offset/column index lengths
RG_I32 = [7]                                          # RowGroup: ordinal
PL_I32 = [2]                                          # PageLocation: compressed_page_size
PES_I32 = [1, 2, 3]                                   # PageEncodingStats: all three

SE_FIELDS = ["type", "type_length", "repetition_type", "name", "num_children",
             "converted_type", "scale", "precision", "field_id", "logicalType"]
CMD_FIELDS = ["type", "encodings", "path_in_schema", "codec", "num_values",
              "total_uncompressed_size", "total_compressed_size", "key_value_metadata",
              "data_page_offset", "index_page_offset", "dictionary_page_offset", "statistics",
              "encoding_stats", "bloom_filter_offset", "bloom_filter_length", "size_statistics",
              "geospatial_statistics"]
CC_FIELDS = ["file_path", "file_offset", "meta_data", "offset_index_offset", "offset_index_length",
             "column_index_offset", "column_index_length", "crypto_metadata",
             "encrypted_column_metadata"]
RG_FIELDS = ["columns", "total_byte_size", "num_rows", "sorting_columns", "file_offset",
             "total_compressed_size", "ordinal"]
FMD_FIELDS = ["version", "schema", "num_rows", "row_groups", "key_value_metadata", "created_by",
              "column_orders", "encryption_algorithm", "footer_signing_key_metadata"]

DICTIONARY_PAGE, DATA_PAGE, DATA_PAGE_V2 = 2, 0, 3


def _arrow_schema_value(blob):
    """The ARROW:schema blob parquet-cpp would have written for THIS file's schema.

    Derived rather than invented: read the parquet schema back, hand it to a real
    ParquetWriter with store_schema on, and lift the key/value it produces. Both fast writers
    carry this key and DuckDB carries none, so injecting it asks whether the consumer's fast
    path is gated on an Arrow-specific footer entry rather than on anything in the spec.
    """
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pq.read_schema(io.BytesIO(blob))
    sink = pa.BufferOutputStream()
    pq.ParquetWriter(sink, schema, store_schema=True).close()
    md = pq.ParquetFile(pa.BufferReader(sink.getvalue())).metadata.metadata or {}
    value = md.get(b"ARROW:schema")
    if not value:
        raise SystemExit("footer_inject: pyarrow produced no ARROW:schema to copy")
    return value


def _obj(name, i32, src, fields, **override):
    """Rebuild a thrift struct from a PARSED object, never from `_asdict()`."""
    from fastparquet.cencoding import ThriftObject

    kw = {f: getattr(src, f) for f in fields if getattr(src, f, None) is not None}
    kw.update({k: v for k, v in override.items() if v is not None})
    return ThriftObject.from_fields(name, i32list=i32, **kw)


def _walk(blob, start, length):
    """Page headers of one column chunk: (page_type, encoding, offset, comp_size, num_values)."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, from_buffer

    io = NumpyIO(np.frombuffer(blob[start:start + length], dtype="uint8"))
    pages = []
    while io.tell() < length - 4:
        pos = io.tell()
        try:
            ph = from_buffer(io, "PageHeader")
        except Exception:
            break
        h = (getattr(ph, "data_page_header", None) or getattr(ph, "data_page_header_v2", None)
             or getattr(ph, "dictionary_page_header", None))
        levels = [getattr(h, "definition_level_encoding", None),
                  getattr(h, "repetition_level_encoding", None)]
        pages.append((ph.type, getattr(h, "encoding", None), start + pos,
                      ph.compressed_page_size, getattr(h, "num_values", 0),
                      [x for x in levels if x is not None]))
        io.seek(ph.compressed_page_size, 1)
    return pages


def patch_bytes(blob, features):
    """Return ``(new_blob, notes)``. ``features`` is any subset of FEATURES."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    bad = set(features) - set(FEATURES)
    if bad:
        raise SystemExit(f"footer_inject: unknown feature(s) {sorted(bad)}; known: {FEATURES}")
    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file (missing PAR1 marker)")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")
    notes = []

    # ---- schema: LogicalType
    schema, promoted = [], []
    for se in fmd.schema:
        promo = (PROMOTE.get(se.converted_type)
                 if "logical" in features and se.logicalType is None else None)
        lt = ThriftObject.from_fields("LogicalType", **{promo: {}}) if promo else None
        new = _obj("SchemaElement", SE_I32, se, SE_FIELDS, logicalType=lt)
        if promo:
            nm = se.name
            promoted.append(nm.decode() if isinstance(nm, bytes) else str(nm))
        elif bytes(new.to_bytes()) != bytes(se.to_bytes()):
            raise SystemExit(f"footer_inject: SchemaElement rebuild is not byte-identical ({se.name})"
                             " — refusing to write a footer that differs from the writer's")
        schema.append(new)
    if promoted:
        notes.append(f"LogicalType on {', '.join(promoted)}")

    # ---- row groups: encoding_stats on the chunk, OffsetIndex appended after the data
    indexes, n_es, n_oi, n_enc, n_nd, n_dep = [], 0, 0, 0, 0, 0
    cursor = foot_start                       # where appended OffsetIndex structures will land
    row_groups = []
    for rg in fmd.row_groups:
        columns = []
        for cc in rg.columns:
            m = cc.meta_data
            need_pages = ("encstats" in features and not m.encoding_stats) or \
                         ("offsetindex" in features and cc.offset_index_offset is None) or \
                         ("encodings" in features)
            pages = _walk(blob, m.dictionary_page_offset or m.data_page_offset,
                          m.total_compressed_size) if need_pages else []

            es = None
            if "encstats" in features and not m.encoding_stats and pages:
                counts = {}
                for ptype, enc, _o, _s, _n, _lv in pages:
                    counts[(ptype, enc)] = counts.get((ptype, enc), 0) + 1
                es = [ThriftObject.from_fields("PageEncodingStats", i32list=PES_I32,
                                               page_type=p, encoding=e, count=c)
                      for (p, e), c in sorted(counts.items())]
                n_es += 1

            # The spec requires `encodings` to list EVERY encoding used in the chunk. DuckDB lists
            # only the data-page encoding, omitting the dictionary page's PLAIN and the levels'
            # RLE — so a reader asking "is there a dictionary here?" finds neither PLAIN nor
            # PLAIN_DICTIONARY. Rebuild the list from what the pages actually use.
            stats = None
            drop = set()
            if "nodistinct" in features:
                drop.add("distinct_count")
            if "nodeprecstats" in features:
                drop.update(("min", "max"))
            src_st = m.statistics
            if drop and src_st is not None                     and any(getattr(src_st, f, None) is not None for f in drop):
                kw_st = {f: getattr(src_st, f) for f in STAT_FIELDS
                         if f not in drop and getattr(src_st, f, None) is not None}
                stats = ThriftObject.from_fields("Statistics", **kw_st)
                if getattr(src_st, "distinct_count", None) is not None and "distinct_count" in drop:
                    n_nd += 1
                if "min" in drop and (getattr(src_st, "min", None) is not None
                                      or getattr(src_st, "max", None) is not None):
                    n_dep += 1

            encs = None
            if "encodings" in features and pages:
                seen = set()
                for _p, enc, _o, _s, _n, levels in pages:
                    if enc is not None:
                        seen.add(enc)
                    seen.update(levels)
                seen = sorted(seen)
                if seen != sorted(m.encodings or []):
                    encs = seen
                    n_enc += 1

            oi_off = oi_len = None
            if "offsetindex" in features and cc.offset_index_offset is None and pages:
                locs, row = [], 0
                for ptype, _e, off, size, nvals, _lv in pages:
                    if ptype == DICTIONARY_PAGE:
                        continue
                    locs.append(ThriftObject.from_fields(
                        "PageLocation", i32list=PL_I32,
                        offset=off, compressed_page_size=size, first_row_index=row))
                    row += nvals
                raw = bytes(ThriftObject.from_fields("OffsetIndex",
                                                     page_locations=locs).to_bytes())
                indexes.append(raw)
                oi_off, oi_len = cursor, len(raw)
                cursor += len(raw)
                n_oi += 1

            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS, encoding_stats=es,
                        encodings=encs, statistics=stats)
            if es is None and encs is None and stats is None                     and bytes(meta.to_bytes()) != bytes(m.to_bytes()):
                raise SystemExit("footer_inject: ColumnMetaData rebuild is not byte-identical "
                                 f"({m.path_in_schema}) — refusing to write it")
            new_cc = _obj("ColumnChunk", CC_I32, cc, CC_FIELDS, meta_data=meta,
                          offset_index_offset=oi_off, offset_index_length=oi_len)
            columns.append(new_cc)
        row_groups.append(_obj("RowGroup", RG_I32, rg, RG_FIELDS, columns=columns))
    if n_es:
        notes.append(f"encoding_stats on {n_es} chunk(s)")
    if n_oi:
        notes.append(f"OffsetIndex on {n_oi} chunk(s)")
    if n_enc:
        notes.append(f"completed encodings on {n_enc} chunk(s)")
    if n_nd:
        notes.append(f"dropped distinct_count on {n_nd} chunk(s)")
    if n_dep:
        notes.append(f"dropped deprecated min/max on {n_dep} chunk(s)")
    # created_by is decided BEFORE the early return: it is a whole-footer field, so it is the one
    # feature that can be the only thing a run changes.
    new_created_by = None
    if "createdby" in features:
        was = fmd.created_by
        was = was.decode() if isinstance(was, bytes) else str(was)
        if was != CREATED_BY:
            new_created_by = CREATED_BY.encode()
            notes.append(f"created_by {was!r} -> {CREATED_BY!r}")
    new_kv = None
    if "arrowschema" in features:
        have = {(k.key if isinstance(k.key, bytes) else str(k.key).encode())
                for k in (fmd.key_value_metadata or [])}
        if b"ARROW:schema" not in have:
            new_kv = list(fmd.key_value_metadata or [])
            new_kv.append(ThriftObject.from_fields("KeyValue", key=b"ARROW:schema",
                                                   value=_arrow_schema_value(blob)))
            notes.append("added ARROW:schema key/value")
    if not notes:
        return blob, []

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f, None) is not None}
    kw["schema"], kw["row_groups"] = schema, row_groups
    if new_created_by is not None:
        kw["created_by"] = new_created_by
    if new_kv is not None:
        kw["key_value_metadata"] = new_kv
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    return (blob[:foot_start] + b"".join(indexes) + footer
            + struct.pack("<I", len(footer)) + MARKER), notes


def patch_remote(store, keys, features):
    """Download, patch and replace each key in place. Returns the notes from the last file.

    The replace goes back through the same single-PUT path duckrun uses for OneLake overwrites —
    OneLake rejects a multipart commit over a committed blob.
    """
    import obstore

    notes = []
    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, notes = patch_bytes(blob, features)
        if not notes:
            print(f"  [skip] {key}: nothing to inject", flush=True)
            continue
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: +{len(new) - len(blob):,} bytes — {'; '.join(notes)}", flush=True)
    return notes
