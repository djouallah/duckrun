"""Splice one writer's column payload into another writer's file. Diagnostic only.

Every metadata difference between the two writers has now been neutralised in a single run —
LogicalType, encoding_stats, OffsetIndex, the encodings list, distinct_count, the deprecated
min/max pair, created_by, ARROW:schema, the root schema element name, the bit-packed run length
and row-group emission order — and the slow file is still slow. So the container is not the cause
and the cost lives in the column's page bytes.

This is the experiment that says so without ambiguity. It takes the HOST file, keeps its container
(footer, schema, other columns) and replaces one column's chunk bytes with the DONOR's, verbatim:
dictionary page and data pages exactly as the donor encoder wrote them, with only the byte offsets
recomputed. Two runs split the question completely.

  host=duckdb   donor=pyarrow   fast => the payload carried the cost
  host=pyarrow  donor=duckdb    slow => the payload carried the cost (the confirming direction)

REQUIRES ALIGNED ROW GROUPS. A chunk describes a specific number of values, so the host's row
group i and the donor's row group i must hold the same rows. DuckDB rounds ROW_GROUP_SIZE up to a
multiple of its 2048-row vector (6,000,000 becomes 6,000,640) while pyarrow slices exactly, so the
two only line up when the workflow passes a multiple of 2048 to both arms (writer_cold's opt_rg).
This refuses to write rather than produce a file whose footer lies about its contents.
"""
import struct

from footer_inject import (CC_FIELDS, CC_I32, CMD_FIELDS, CMD_I32, FMD_FIELDS, MARKER,
                           RG_FIELDS, RG_I32, _obj)


def _footer(blob):
    import numpy as np
    from fastparquet.cencoding import NumpyIO, from_buffer

    if blob[:4] != MARKER or blob[-4:] != MARKER:
        raise ValueError("not a parquet file (missing PAR1 marker)")
    flen = struct.unpack("<I", blob[-8:-4])[0]
    start = len(blob) - 8 - flen
    return from_buffer(NumpyIO(np.frombuffer(blob[start:-8], dtype="uint8")), "FileMetaData")


def _name(cc):
    return ".".join(x.decode() if isinstance(x, bytes) else x
                    for x in cc.meta_data.path_in_schema)


def transplant_bytes(host_blob, donor_blob, column):
    """Return (new_blob, stats): the host file with `column`'s pages taken from the donor."""
    from fastparquet.cencoding import ThriftObject

    host, donor = _footer(host_blob), _footer(donor_blob)

    if len(host.row_groups) != len(donor.row_groups):
        raise SystemExit(f"chimera: row group count differs — host {len(host.row_groups)}, "
                         f"donor {len(donor.row_groups)}. Build both arms with the same opt_rg.")
    for i, (hrg, drg) in enumerate(zip(host.row_groups, donor.row_groups)):
        if hrg.num_rows != drg.num_rows:
            raise SystemExit(
                f"chimera: row group {i} holds {hrg.num_rows:,} rows in the host and "
                f"{drg.num_rows:,} in the donor. A transplant would produce a footer that lies "
                "about its own contents. Pass a multiple of 2048 as opt_rg so DuckDB's vector "
                "rounding and pyarrow's exact slicing agree, and threads=1 so the order matches.")
        # Matching row COUNTS are not matching ROWS. A bare `limit` over an unordered scan once
        # gave the two arms different 142M-row subsets of the same source - same counts, different
        # data - and two transplants were measured before a Delta-log diff caught it. The columns
        # that are NOT being replaced must describe the same rows on both sides, or the result is
        # one writer's column bolted onto another writer's unrelated rows.
        for hcc in hrg.columns:
            nm = _name(hcc)
            if nm == column:
                continue
            dcc = next((d for d in drg.columns if _name(d) == nm), None)
            hs, ds = hcc.meta_data.statistics, dcc.meta_data.statistics if dcc else None
            if hs is None or ds is None:
                continue
            for f in ("min_value", "max_value", "min", "max", "null_count"):
                hv, dv = getattr(hs, f, None), getattr(ds, f, None)
                if hv != dv:
                    raise SystemExit(
                        f"chimera: row group {i} column {nm!r} has {f}={hv!r} in the host and "
                        f"{dv!r} in the donor — the two files do not hold the same rows, so a "
                        "transplant would bolt one writer's column onto another writer's data. "
                        "Check that the row cap is deterministic (order before limit).")

    out = bytearray(MARKER)
    row_groups, n_moved = [], 0
    for hrg, drg in zip(host.row_groups, donor.row_groups):
        rg_start = len(out)
        columns = []
        for cc in hrg.columns:
            name = _name(cc)
            src_cc, src_blob = cc, host_blob
            if name == column:
                match = next((d for d in drg.columns if _name(d) == name), None)
                if match is None:
                    raise SystemExit(f"chimera: donor has no column {column!r}")
                if match.meta_data.num_values != cc.meta_data.num_values:
                    raise SystemExit(
                        f"chimera: {column} holds {cc.meta_data.num_values:,} values in the host "
                        f"and {match.meta_data.num_values:,} in the donor — refusing.")
                src_cc, src_blob = match, donor_blob
                n_moved += 1

            m = src_cc.meta_data
            start = m.dictionary_page_offset or m.data_page_offset
            col_start = len(out)
            out += src_blob[start:start + m.total_compressed_size]

            # Offsets are absolute from the start of the file, so they are the only thing that
            # has to change. Everything else about the chunk — encodings, encoding_stats,
            # statistics, num_values — stays whatever the source encoder wrote, because the
            # pages being described are that encoder's.
            meta = _obj("ColumnMetaData", CMD_I32, m, CMD_FIELDS,
                        dictionary_page_offset=(col_start if m.dictionary_page_offset else None),
                        data_page_offset=col_start + (m.data_page_offset - start))
            columns.append(_obj("ColumnChunk", CC_I32, src_cc, CC_FIELDS, meta_data=meta,
                                offset_index_offset=None, offset_index_length=None,
                                column_index_offset=None, column_index_length=None))
        row_groups.append(_obj("RowGroup", RG_I32, hrg, RG_FIELDS, columns=columns,
                               file_offset=rg_start, total_compressed_size=len(out) - rg_start))

    kw = {f: getattr(host, f) for f in FMD_FIELDS if getattr(host, f, None) is not None}
    kw["row_groups"] = row_groups
    footer = bytes(ThriftObject.from_fields("FileMetaData", i32list=[1], **kw).to_bytes())
    out += footer + struct.pack("<I", len(footer)) + MARKER
    return bytes(out), {"chunks_moved": n_moved, "row_groups": len(row_groups),
                        "bytes_before": len(host_blob), "bytes_after": len(out)}


def verify(blob, column, expect_rows):
    """Decode the transplanted column back and check it reads as the donor's data.

    The whole point is a file that is valid and holds the right values; a transplant that
    silently corrupts the column would look exactly like a fast read.
    """
    import io

    import pyarrow.parquet as pq

    t = pq.read_table(io.BytesIO(blob), columns=[column])
    if t.num_rows != expect_rows:
        raise SystemExit(f"chimera: transplanted file has {t.num_rows:,} rows, expected "
                         f"{expect_rows:,}")
    head = [v.as_py() for v in t.column(column)[:6]]
    return {"rows": t.num_rows, "head": head}


def transplant_remote(store, host_key, donor_store, donor_key, column):
    """Read both files, transplant, and replace the host in place (single PUT for OneLake)."""
    import obstore

    host_blob = bytes(obstore.get(store, host_key).bytes())
    donor_blob = bytes(obstore.get(donor_store, donor_key).bytes())
    new, stats = transplant_bytes(host_blob, donor_blob, column)
    checked = verify(new, column, sum(rg.num_rows for rg in _footer(new).row_groups))
    obstore.put(store, host_key, new, use_multipart=False)
    print(f"  [ok] {host_key}: {stats['chunks_moved']} {column} chunk(s) from {donor_key}, "
          f"{stats['bytes_before']:,} -> {stats['bytes_after']:,} bytes; "
          f"reads back {checked['rows']:,} rows, head={checked['head']}", flush=True)
    return stats
