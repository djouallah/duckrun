"""Offline guards for the page-statistics synthesizer. No Fabric, no OneLake, no network.

The synthesizer writes a DataPageHeader by hand, which is precisely where this investigation has
gone wrong quietly before: an integer re-emitted one width too wide still parses and is silently not
what the writer wrote. These tests are the cheap version of the calibration gate that guards every
real run — they fail on a laptop in a second rather than 30 minutes into a Fabric benchmark.

Run by writer_bytes.yml before it compares anything, and standalone with:

    python -m pytest tests/parquet_layout/aemo/test_page_stats.py -q
"""
import io
import os
import struct
import sys

import numpy as np
import pytest
from fastparquet.cencoding import NumpyIO, from_buffer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import page_reframe as PR  # noqa: E402

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROWS = 40960
RG = 8192
# Deliberately not sorted and not uniform-length, so min/max are a real computation rather than the
# first and last value, and so the dictionary indices span more than one bit width.
VALUES = [f"{chr(65 + (i * 7) % 26)}{'X' * (i % 5)}{i % 313}" for i in range(313)]


def _pyarrow_file(values=VALUES, rows=ROWS, nulls=False):
    vals = [values[i % len(values)] for i in range(rows)]
    if nulls:
        vals = [None if i % 97 == 0 else v for i, v in enumerate(vals)]
    tbl = pa.table({"c": pa.array(vals, type=pa.string())})
    buf = io.BytesIO()
    w = pq.ParquetWriter(buf, tbl.schema, compression="snappy", use_dictionary=True,
                         version="1.0", write_statistics=True)
    for off in range(0, rows, RG):
        w.write_table(tbl.slice(off, RG), row_group_size=RG)
    w.close()
    return buf.getvalue(), tbl


def _duckdb_file(tmp_path, tbl):
    duckdb = pytest.importorskip("duckdb")
    out = str(tmp_path / "duck.parquet").replace("\\", "/")
    con = duckdb.connect()
    con.register("t", tbl)
    # WRITE_BLOOM_FILTER false matches the benchmark (OPT_BLOOM defaults false) and is required:
    # a bloom filter lives outside the column chunk, so any rebuild would strand it.
    con.execute(f"COPY (select * from t) TO '{out}' (FORMAT parquet, ROW_GROUP_SIZE {RG}, "
                f"COMPRESSION snappy, PARQUET_VERSION V1, DICTIONARY_SIZE_LIMIT 16000000, "
                f"WRITE_BLOOM_FILTER false)")
    con.close()
    return open(out, "rb").read()


def _page_stats(blob, column="c"):
    """Every data page's parsed Statistics, in file order."""
    flen = struct.unpack("<I", blob[-8:-4])[0]
    fmd = from_buffer(NumpyIO(np.frombuffer(blob[len(blob) - 8 - flen:-8], dtype="uint8")),
                      "FileMetaData")
    out = []
    for rg in fmd.row_groups:
        for cc in rg.columns:
            name = ".".join(x.decode() if isinstance(x, bytes) else x
                            for x in cc.meta_data.path_in_schema)
            if name != column:
                continue
            m = cc.meta_data
            start = m.dictionary_page_offset or m.data_page_offset
            chunk = blob[start:start + m.total_compressed_size]
            io_ = NumpyIO(np.frombuffer(chunk, dtype="uint8"))
            while io_.tell() < len(chunk) - 4:
                ph = from_buffer(io_, "PageHeader")
                io_.seek(ph.compressed_page_size, 1)
                if ph.type == PR.DATA_PAGE:
                    out.append(getattr(ph.data_page_header, "statistics", None))
    return out


def test_calibration_reproduces_parquet_cpp_bytes():
    """THE GATE: the synthesizer must reproduce pyarrow's own page-stats headers exactly."""
    blob, _ = _pyarrow_file()
    fields = PR.calibrate_page_stats(blob, "c")
    assert "min_value" in fields and "max_value" in fields
    # The measured template from writer_bytes run 32619755844 (pyarrow 25.0.1). If a pyarrow bump
    # changes it, the calibration above still passes and THIS fails — which is the signal to record
    # the new template in the findings rather than to relax the constant.
    assert fields == PR.PAGE_STAT_FIELDS, f"pyarrow {pa.__version__} writes {fields}"


def test_calibration_with_nulls():
    blob, _ = _pyarrow_file(nulls=True)
    assert PR.calibrate_page_stats(blob, "c")


def test_duckdb_file_has_no_page_stats():
    """The premise. If this ever fails, the whole experiment is a no-op control."""
    _blob, tbl = _pyarrow_file()
    db = _duckdb_file(_tmp(), tbl)
    assert _page_stats(db), "no data pages found"
    assert all(s is None for s in _page_stats(db))


def test_stamped_stats_match_what_parquet_cpp_computes():
    """Stamping DuckDB's file yields the same min/max/null_count parquet-cpp writes."""
    ab, tbl = _pyarrow_file()
    db = _duckdb_file(_tmp(), tbl)
    new, st = PR.add_page_stats_bytes(db, column="c")
    assert st["pages_stamped"] == len(_page_stats(db))
    assert st["bytes_after"] > st["bytes_before"]

    got, want = _page_stats(new), _page_stats(ab)
    assert len(got) == len(want), "geometry did not line up; the comparison would be meaningless"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g is not None, f"page {i} was not stamped"
        assert bytes(g.min_value) == bytes(w.min_value), f"page {i} min"
        assert bytes(g.max_value) == bytes(w.max_value), f"page {i} max"
        assert int(g.null_count) == int(w.null_count), f"page {i} null_count"


def test_stamped_file_still_reads():
    """A header that parses is not enough — the values must survive it."""
    _ab, tbl = _pyarrow_file()
    db = _duckdb_file(_tmp(), tbl)
    new, _ = PR.add_page_stats_bytes(db, column="c")
    back = pq.read_table(io.BytesIO(new))
    assert back.num_rows == tbl.num_rows
    assert back.column("c").to_pylist() == tbl.column("c").to_pylist()


def test_restamping_is_a_detected_no_op():
    """A run that silently changes nothing is worse than one that fails."""
    _ab, tbl = _pyarrow_file()
    db = _duckdb_file(_tmp(), tbl)
    once, _ = PR.add_page_stats_bytes(db, column="c")
    _twice, st = PR.add_page_stats_bytes(once, column="c")
    assert st["pages_stamped"] == 0


def test_non_byte_array_is_refused():
    """Signed vs unsigned min-max order is type-dependent; the tool refuses rather than guesses."""
    tbl = pa.table({"c": pa.array(list(range(ROWS)), type=pa.int32())})
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="snappy", use_dictionary=True, version="1.0")
    with pytest.raises(SystemExit, match="BYTE_ARRAY"):
        PR.add_page_stats_bytes(buf.getvalue(), column="c")


_TMP = []


def _tmp():
    import pathlib
    import tempfile
    d = tempfile.mkdtemp(prefix="page_stats_test_")
    _TMP.append(d)
    return pathlib.Path(d)
