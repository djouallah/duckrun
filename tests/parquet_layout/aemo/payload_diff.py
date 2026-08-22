"""Decode and compare the PAYLOAD of one column between the two writers. Not the headers — the bytes.

Ten cold runs compared metadata and never decoded a value. Every "the data is identical" claim so
far rests on chunk byte SIZES (6,013,968 vs 6,026,119) and on row group 0 of a synthetic file.
`page_diff.py` walks page headers and stops; it never decompresses a page. Row groups 1..23 have
never been looked at by anything.

Direct Lake reads parquet and has no idea which writer produced it, so a 13x gap on one string
column has to be in the bytes. This decodes them:

  * DICTIONARY per row group — the values, with an ORDER-SENSITIVE hash and a SORTED-SET hash. If a
    writer's local dictionary permutes or changes content from row group to row group while the
    other's is stable, that is a real per-segment cost for a consumer that maps local dictionary IDs
    into one global dictionary, and nothing has looked at it.
  * RUN FRAMING per data page — the RLE/bit-packing hybrid is a sequence of runs, each either an RLE
    run (one value, a repeat count) or a bit-packed run (a multiple of 8 values). Two chunks can be
    the same size with completely different run framing, and run framing is what a decoder's inner
    loop scales with. Reports bit width, run counts and mean run length.
  * VALUE SEQUENCE — a hash of the decoded index sequence plus the first values, because if the two
    writers did not lay the same rows down in the same order then the tables are not comparable and
    that reframes everything measured so far.

Env: ONELAKE_TABLES_PATH (resolve_env), AB_PREFIX (default 'fct_summary_ab'),
PAYLOAD_DIFF_COLUMN (default 'DUID'), PAYLOAD_DIFF_PAGES (data pages decoded per row group,
default 2 — decoding is the expensive part, and run framing is visible in the first page).

Run `python payload_diff.py selftest <duckdb.parquet> <deltars.parquet>` to verify the decoder
against pyarrow on local files. A decoder that silently mis-parses would MANUFACTURE a difference,
so that check gates every remote result.
"""
import hashlib
import os
import struct
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PREFIX = os.environ.get("AB_PREFIX") or "fct_summary_ab"
COLUMN = os.environ.get("PAYLOAD_DIFF_COLUMN") or "DUID"
N_PAGES = int(os.environ.get("PAYLOAD_DIFF_PAGES") or 2)
# Comma-separated full table URIs, for when the two tables do NOT live in one lakehouse — the cold
# benchmark puts each writer in its own throwaway lakehouse, and those are the files whose timing we
# actually measured, so they are the ones worth decoding.
URIS = [u.strip() for u in (os.environ.get("PAYLOAD_DIFF_URIS") or "").split(",") if u.strip()]
# Which file to decode. Picking the alphabetically-first one compares delta-rs's part-00000 (the
# first ROWS) against a random DuckDB part-<tag>-<uuid>, so the two sides describe different slices
# of the table and every hash "differs" for no reason. Both tables are sorted, so the file holding
# the first rows is the one with the smallest minimum on the leading sort column.
ORDER_COL = os.environ.get("PAYLOAD_DIFF_ORDER_COL") or "date"

DICTIONARY_PAGE, DATA_PAGE, DATA_PAGE_V2 = 2, 0, 3


def _decompress(raw, codec, n):
    if codec == 0:
        return raw
    import cramjam
    if codec == 1:                       # SNAPPY — raw block format, NOT the framed one
        return bytes(cramjam.snappy.decompress_raw(raw))
    if codec == 2:
        return bytes(cramjam.gzip.decompress(raw))
    if codec == 6:
        return bytes(cramjam.zstd.decompress(raw))
    raise SystemExit(f"payload_diff: unsupported compression codec {codec}")


def _varint(buf, pos):
    out = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        out |= (b & 0x7F) << shift
        if not b & 0x80:
            return out, pos
        shift += 7


def decode_hybrid(buf, bit_width, max_values):
    """RLE / bit-packing hybrid -> (values, run stats). The run stats ARE the point of this file."""
    vals = np.empty(max_values, dtype=np.int64)
    n = pos = 0
    rle_runs = bp_runs = 0
    rle_len, bp_len = [], []
    byte_w = (bit_width + 7) // 8
    while n < max_values and pos < len(buf):
        header, pos = _varint(buf, pos)
        if header & 1:                                   # bit-packed: (header>>1) groups of 8
            groups = header >> 1
            count = groups * 8
            nbytes = groups * bit_width
            chunk = buf[pos:pos + nbytes]
            pos += nbytes
            if bit_width:
                bits = np.unpackbits(np.frombuffer(chunk, dtype=np.uint8), bitorder="little")
                usable = (len(bits) // bit_width) * bit_width
                v = bits[:usable].reshape(-1, bit_width)
                weights = (1 << np.arange(bit_width, dtype=np.int64))
                dec = (v * weights).sum(axis=1)
            else:
                dec = np.zeros(count, dtype=np.int64)
            take = min(count, max_values - n)
            vals[n:n + take] = dec[:take]
            n += take
            bp_runs += 1
            bp_len.append(count)
        else:                                            # RLE: one value repeated header>>1 times
            count = header >> 1
            v = int.from_bytes(buf[pos:pos + byte_w], "little") if byte_w else 0
            pos += byte_w
            take = min(count, max_values - n)
            vals[n:n + take] = v
            n += take
            rle_runs += 1
            rle_len.append(count)
    stats = {"bit_width": bit_width, "rle_runs": rle_runs, "bitpacked_runs": bp_runs,
             "mean_rle_len": round(float(np.mean(rle_len)), 1) if rle_len else 0.0,
             "mean_bitpacked_len": round(float(np.mean(bp_len)), 1) if bp_len else 0.0}
    return vals[:n], stats


def parse_plain_byte_array(buf, count):
    """PLAIN dictionary layout for BYTE_ARRAY: (4-byte LE length + bytes) repeated."""
    out, pos = [], 0
    for _ in range(count):
        (ln,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        out.append(bytes(buf[pos:pos + ln]))
        pos += ln
    return out


def decode_chunk(read, meta, optional, phys_type, n_pages):
    """Decode one column chunk. `read(offset, length) -> bytes` so this works local or remote."""
    from fastparquet.cencoding import NumpyIO, from_buffer

    start = meta.dictionary_page_offset or meta.data_page_offset
    blob = read(start, meta.total_compressed_size)
    io = NumpyIO(np.frombuffer(blob, dtype="uint8"))
    dictionary, pages, indices = None, [], []
    while io.tell() < len(blob) - 4 and len(pages) < n_pages:
        ph = from_buffer(io, "PageHeader")
        body = blob[io.tell():io.tell() + ph.compressed_page_size]
        io.seek(ph.compressed_page_size, 1)
        raw = _decompress(body, meta.codec, ph.uncompressed_page_size)
        if ph.type == DICTIONARY_PAGE:
            cnt = ph.dictionary_page_header.num_values
            dictionary = (parse_plain_byte_array(raw, cnt) if phys_type == 6
                          else [raw[i:i + 8] for i in range(0, min(len(raw), cnt * 8), 8)])
            continue
        if ph.type != DATA_PAGE:                         # V2 not produced by either writer here
            continue
        h = ph.data_page_header
        pos = 0
        if optional:                                     # def levels: 4-byte LE length, then RLE
            (dl,) = struct.unpack_from("<I", raw, 0)
            pos = 4 + dl
        bit_width = raw[pos]
        vals, st = decode_hybrid(raw[pos + 1:], bit_width, h.num_values)
        st["num_values"] = h.num_values
        st["decoded"] = len(vals)
        pages.append(st)
        indices.append(vals)
    return dictionary, pages, (np.concatenate(indices) if indices else np.empty(0, dtype=np.int64))


def _stat_min(fmd, column):
    """Row group 0's minimum for `column`, decoded by physical type — NOT compared as raw bytes."""
    rg = fmd.row_groups[0]
    cc = next((c for c in rg.columns
               if ".".join(x.decode() if isinstance(x, bytes) else x
                           for x in c.meta_data.path_in_schema) == column), None)
    if cc is None or cc.meta_data.statistics is None:
        return None
    st = cc.meta_data.statistics
    raw = getattr(st, "min_value", None) or getattr(st, "min", None)
    if raw is None:
        return None
    t = cc.meta_data.type
    if t in (1, 2):                                  # INT32 / INT64 — little-endian, signed
        return int.from_bytes(raw, "little", signed=True)
    if t == 4:
        return struct.unpack("<f", raw)[0]
    if t == 5:
        return struct.unpack("<d", raw)[0]
    return bytes(raw)


def _hash(items):
    h = hashlib.sha256()
    for x in items:
        h.update(x if isinstance(x, bytes) else str(x).encode())
        h.update(b"\x1f")
    return h.hexdigest()[:16]


def analyse(label, files, column, n_pages):
    """files: list of (name, size, read_fn, footer). Prints per-row-group detail, returns summary."""
    rows = []
    for name, _size, read, fmd in files:
        se = next((s for s in fmd.schema
                   if (s.name.decode() if isinstance(s.name, bytes) else s.name) == column), None)
        if se is None:
            raise SystemExit(f"payload_diff: no column {column!r} in {name}")
        optional = se.repetition_type == 1
        for ri, rg in enumerate(fmd.row_groups):
            cc = next((c for c in rg.columns
                       if ".".join(x.decode() if isinstance(x, bytes) else x
                                   for x in c.meta_data.path_in_schema) == column), None)
            if cc is None:
                continue
            d, pages, idx = decode_chunk(read, cc.meta_data, optional, se.type, n_pages)
            rows.append({
                "file": name, "rg": ri, "dict_n": len(d) if d else 0,
                "dict_order_hash": _hash(d) if d else "-",
                "dict_set_hash": _hash(sorted(d)) if d else "-",
                "first": [x.decode(errors="replace") for x in (d or [])[:3]],
                "pages": pages, "idx_hash": _hash(idx[:100_000].tolist()) if len(idx) else "-",
                "idx_head": idx[:12].tolist(),
                # The INDEX sequence is not evidence of anything on its own: both writers assign
                # dictionary IDs by first appearance, so both start 0,1,2,3... no matter which
                # values those are. Only the resolved VALUES say whether the rows match.
                "val_hash": _hash([d[i] for i in idx[:100_000]]) if (d is not None and len(idx))
                            else "-",
                "val_head": [d[i].decode(errors="replace") for i in idx[:6]] if (
                    d is not None and len(idx)) else [],
            })
    print(f"\n{'=' * 108}\n{label}\n{'=' * 108}")
    print(f"{'rg':>3} {'dict':>5} {'dict order hash':>17} {'dict set hash':>15} {'bw':>3} "
          f"{'rle':>6} {'bitpk':>6} {'mean bp':>9}  first dict values")
    for r in rows:
        p = r["pages"][0] if r["pages"] else {}
        print(f"{r['rg']:>3} {r['dict_n']:>5} {r['dict_order_hash']:>17} {r['dict_set_hash']:>15} "
              f"{p.get('bit_width', '-'):>3} {p.get('rle_runs', '-'):>6} "
              f"{p.get('bitpacked_runs', '-'):>6} {p.get('mean_bitpacked_len', '-'):>9}  "
              f"{','.join(r['first'])}")
    order_hashes = {r["dict_order_hash"] for r in rows}
    set_hashes = {r["dict_set_hash"] for r in rows}
    print(f"  dictionary IDENTICAL across all {len(rows)} row groups (order-sensitive): "
          f"{len(order_hashes) == 1}")
    print(f"  dictionary identical as a SET: {len(set_hashes) == 1}")
    print(f"  rg0 index sequence hash: {rows[0]['idx_hash'] if rows else '-'}  "
          f"head={rows[0]['idx_head'] if rows else '-'}")
    print(f"  rg0 VALUE sequence hash: {rows[0]['val_hash'] if rows else '-'}  "
          f"head={rows[0]['val_head'] if rows else '-'}")
    return rows


def main():
    import obstore
    import duckrun
    from dbt.adapters.duckrun.objectstore import build_store
    from page_diff import read_footer

    root = os.environ["ONELAKE_TABLES_PATH"].rstrip("/")
    con = duckrun.connect(root, schema="tests")
    if URIS:
        targets = [(u.rstrip("/").rsplit("/", 1)[-1], u.rstrip("/")) for u in URIS]
    else:
        names = sorted(r[2] for r in con.get_stats(f"{PREFIX}_*").fetchall())
        if not names:
            raise SystemExit(f"payload_diff: no tables matched tests.{PREFIX}_*")
        targets = [(n, f"{root}/tests/{n}") for n in names]

    summary = {}
    for tbl, uri in targets:
        store = build_store(uri, con.storage_options)
        entries = []
        for batch in obstore.list(store):
            for obj in batch:
                nm = obj["path"].rsplit("/", 1)[-1]
                if nm.endswith(".parquet"):
                    entries.append((nm, obj["size"]))
        entries.sort()
        if len(entries) > 1:                             # pick the file holding the FIRST rows
            keyed = []
            for nm, sz in entries:
                try:
                    mn = _stat_min(read_footer(store, nm, sz), ORDER_COL)
                except Exception:
                    mn = None
                keyed.append((mn is None, mn, nm, sz))
            keyed.sort(key=lambda k: (k[0], k[1] if k[1] is not None else 0, k[2]))
            _, mn, name, size = keyed[0]
            print(f"  {tbl}: {len(entries)} files, decoding the one starting at "
                  f"{ORDER_COL}={mn!r} ({name})", flush=True)
        else:
            name, size = entries[0]

        def read(off, ln, _k=name, _s=store):
            return bytes(obstore.get_range(_s, _k, start=off, end=off + ln))

        fmd = read_footer(store, name, size)
        summary[tbl] = analyse(f"{tbl}  —  {name}", [(name, size, read, fmd)], COLUMN, N_PAGES)

    if len(summary) >= 2:
        names = sorted(summary)
        base = names[0]
        rb0 = summary[base][0]
        bar = "=" * 108
        print(f"\n{bar}\nDIFFERENCES vs {base} — row group 0, column {COLUMN}\n{bar}")
        for n in names:
            print(f"first rows  {n:<24}: {summary[n][0]['val_head']}")
        for key, desc in (("dict_n", "dictionary size"),
                          ("dict_order_hash", "dictionary VALUES IN ORDER"),
                          ("dict_set_hash", "dictionary as a set"),
                          ("val_hash", "DECODED VALUES (the actual rows)")):
            cells = " | ".join(
                f"{n}={'same' if summary[n][0][key] == rb0[key] else summary[n][0][key]}"
                for n in names)
            print(f"{desc:<34}: {cells}")
        print("\nrun framing (first data page)")
        print(f"   {'':<25} " + " ".join(f"{n[-18:]:>20}" for n in names))
        for k in ("bit_width", "num_values", "rle_runs", "bitpacked_runs",
                  "mean_rle_len", "mean_bitpacked_len"):
            vals = []
            for n in names:
                pg = summary[n][0]["pages"][0] if summary[n][0]["pages"] else {}
                vals.append(f"{pg.get(k)}")
            print(f"   {k:<25} " + " ".join(f"{v:>20}" for v in vals))
        # A property only ONE writer has is the interesting one now: two of these three read fast.
        print("\nper-writer, across every row group decoded:")
        for n in names:
            rows = summary[n]
            bws = sorted({r["pages"][0]["bit_width"] for r in rows if r["pages"]})
            mbp = sorted({r["pages"][0]["mean_bitpacked_len"] for r in rows if r["pages"]})
            rle = sum(r["pages"][0]["rle_runs"] for r in rows if r["pages"])
            print(f"   {n:<26} row_groups={len(rows)} bit_widths={bws} "
                  f"mean_bitpacked={mbp} total_rle_runs={rle}")


def selftest(*paths):
    """The decoder must reproduce pyarrow exactly on EVERY writer's file, or nothing it says counts.

    This is the gate on the whole tool: a decoder that mis-parses would MANUFACTURE a difference
    between writers, which is precisely the kind of finding this is meant to produce.
    """
    import fastparquet
    import pyarrow.parquet as pq

    ok = True
    for path in paths:
        blob = open(path, "rb").read()
        fmd = fastparquet.ParquetFile(path).fmd
        se = next(s for s in fmd.schema
                  if (s.name.decode() if isinstance(s.name, bytes) else s.name) == COLUMN)
        cc = next(c for c in fmd.row_groups[0].columns
                  if ".".join(x.decode() if isinstance(x, bytes) else x
                              for x in c.meta_data.path_in_schema) == COLUMN)
        d, pages, idx = decode_chunk(lambda o, ln: blob[o:o + ln], cc.meta_data,
                                     se.repetition_type == 1, se.type, 1)
        mine = [d[i].decode() for i in idx[:5000]]
        theirs = pq.read_table(path, columns=[COLUMN]).column(0).to_pylist()[:5000]
        match = mine == theirs
        ok &= match
        label = os.path.basename(path)[:38]
        print(f"{label:<40} dict={len(d)} bit_width={pages[0]['bit_width']} "
              f"decoded={len(idx):,} round-trip vs pyarrow: {match}")
        if not match:
            bad = next(i for i in range(len(mine)) if mine[i] != theirs[i])
            print(f"    first mismatch at {bad}: mine={mine[bad]!r} pyarrow={theirs[bad]!r}")
    print("\nDECODER VERIFIED" if ok else "\nDECODER IS WRONG — fix it before trusting any output")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "selftest":
        sys.exit(selftest(*sys.argv[2:]))
    main()
