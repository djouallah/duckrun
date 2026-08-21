"""Inject the modern parquet LogicalType into a DuckDB-written footer. Diagnostic only.

DuckDB annotates a VARCHAR with the DEPRECATED ConvertedType UTF8 and nothing else; delta-rs
writes that AND `LogicalType.STRING`. Same for DATE. That is the last surviving difference between
the two writers' footers, and on the cold benchmark it tracks the gap column for column: the two
columns where DuckDB omits a LogicalType read 7-11x slower, the two where both writers agree read
~2x slower (the baseline), and the one column where NEITHER writes one is the only column DuckDB
wins. This module exists to turn that correlation into a causal test.

It rewrites ONLY the footer. Page data is untouched, and every offset in a parquet footer is
absolute from the start of the file, so truncating at the footer and appending a slightly longer
one leaves every column chunk exactly where it was. Verified locally on a 2M-row file: data region
byte-identical, footer 4 bytes longer, and both DuckDB and pyarrow re-read it with zero row-level
differences.

Fidelity notes, all learned the hard way:
  * fastparquet's thrift objects will not take a nested struct by assignment from Python, and
    `_asdict()` round-trips `name` as the repr of a bytes object. So each SchemaElement is rebuilt
    from its PARSED attributes and the rest of the footer is reused as already-parsed objects.
  * `i32list` is load-bearing: without it every integer is re-emitted as I64, and an unpatched
    footer stops being byte-identical to what the writer produced.
"""
import struct

MARKER = b"PAR1"
# ConvertedType ordinal -> the LogicalType union field that means the same thing.
PROMOTE = {0: "STRING", 6: "DATE"}
SE_FIELDS = ["type", "type_length", "repetition_type", "name", "num_children",
             "converted_type", "scale", "precision", "field_id", "logicalType"]
SE_I32 = [1, 2, 3, 5, 6, 7, 8, 9]          # every SchemaElement field except name and logicalType
FMD_FIELDS = ["version", "schema", "num_rows", "row_groups", "key_value_metadata",
              "created_by", "column_orders", "encryption_algorithm",
              "footer_signing_key_metadata"]


def patch_bytes(blob):
    """Return ``(new_blob, [column names patched])``. ``blob`` is a whole parquet file."""
    import numpy as np
    from fastparquet.cencoding import NumpyIO, ThriftObject, from_buffer

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file (missing PAR1 marker)")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    foot_start = len(blob) - 8 - flen
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[foot_start:-8], dtype="uint8")), "FileMetaData")

    schema, patched = [], []
    for se in fmd.schema:
        kw = {f: getattr(se, f) for f in SE_FIELDS if getattr(se, f) is not None}
        promo = PROMOTE.get(se.converted_type) if se.logicalType is None else None
        if promo:
            kw["logicalType"] = ThriftObject.from_fields("LogicalType", **{promo: {}})
            name = se.name
            patched.append(name.decode() if isinstance(name, bytes) else str(name))
        schema.append(ThriftObject.from_fields("SchemaElement", i32list=SE_I32, **kw))
    if not patched:
        return blob, []

    kw = {f: getattr(fmd, f) for f in FMD_FIELDS if getattr(fmd, f) is not None}
    kw["schema"] = schema
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    return blob[:foot_start] + footer + struct.pack("<I", len(footer)) + MARKER, patched


def patch_remote(store, keys):
    """Download, patch and replace each key in place. Returns the columns patched on the last file.

    The replace goes back through the same single-PUT path duckrun uses for OneLake overwrites —
    OneLake rejects a multipart commit over a committed blob.
    """
    import obstore

    cols = []
    for key in keys:
        blob = bytes(obstore.get(store, key).bytes())
        new, cols = patch_bytes(blob)
        if not cols:
            print(f"  [skip] {key}: nothing to promote", flush=True)
            continue
        obstore.put(store, key, new, use_multipart=False)
        print(f"  [ok] {key}: +{len(new) - len(blob)} footer bytes, promoted {', '.join(cols)}",
              flush=True)
    return cols
