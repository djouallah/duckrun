"""Build tests.fct_summary_auto_sort with DUCKDB'S OWN PARQUET WRITER; delta-rs only COMMITS.

Same table slot as build_auto_sort.py, so deploy_vorder / table_stats / xmla_compare /
vertipaq_dmv all run unchanged — the `writer` workflow input picks which builder fills it.

Why this exists: delta-rs's writer cannot produce N files of exactly one clean row group (its
byte cap truncates the in-flight group at the roll — measured on 1.5.0), and it exposes no
per-column encoding control. DuckDB COPY has both levers. Target layout = spark V-Order's
physical shape minus the V-Order encoding itself: K files x ONE full row group of ~OPT_RG rows,
globally sorted, dictionary-encoded everywhere (Direct Lake's cheap transcode is the dictionary
ID remap; anything plain gets re-encoded).

Shape mechanics, each measured in a local probe before this was written:
  * ONE sorted COPY — no temp table, no manual slicing (the sort spills instead of materializing
    the whole fact). Files rotate on FILE_SIZE_BYTES (~256MB, duckrun's shipping target) along
    the sorted stream. Runs at 2 threads, purely so the runner does not OOM (each writer buffers
    a full uncompressed row group); adjacent files therefore DO overlap on the sort key, which is
    accepted — that clustering was measured worth 0.05% of the table and delta-rs overlaps too.
    PER_THREAD_OUTPUT + ROW_GROUPS_PER_FILE 1 also measured and REJECTED — one RG per file buys
    nothing, and ROW_GROUPS_PER_FILE is only exact under PER_THREAD_OUTPUT anyway.
  * AddAction stats come from the parquet footers CAST to each column's DuckDB type, then
    re-serialized in Delta JSON spelling — the raw footer stat strings used as-is poisoned
    delta_scan's pruning to zero rows.
  * DICTIONARY_SIZE_LIMIT is raised so no column falls out of dictionary even at 16M-row groups —
    DuckDB's default is ROW_GROUP_SIZE/5, and 1.5.5 dictionary-encodes every type at any NDV
    under the limit. (Exception: DECIMAL(p>18) writes FLBA, never dictionary — the mart already
    stores decimal(18,4), and the post-commit WARN names any column that slips out.)
  * DATA_PAGE_SIZE_LIMIT caps data pages (duckdb#24645, nightly-only until the next stable). This
    arm's first run died on the old hardcoded 100MB split: ONE giant page per 16M-row column
    chunk, which the consumer transcodes 5-26x slower (duckdb#24507). A startup probe fails the
    build loudly on any duckdb without the option — a stable build would silently write the
    broken layout and corrupt the A/B.
    The default 1048576 is NOT parity with delta-rs, despite matching its byte cap: arrow-rs also
    caps a page at 20,000 ROWS, while DuckDB caps rows only at 524,288 (a power-of-two vector
    count, so the effective limit steps 2^19 -> 2^17 -> 2^15 as the byte cap falls). Measured on a
    6M-row group: DuckDB writes 12 pages/chunk at 1MB and 184 at 64KB, against delta-rs's 294.
    64KB therefore buys near-parity page geometry for no extra bytes — see OPT_PAGE_BYTES.
  * encoding_stats (PageEncodingStats) is the one footer field we CANNOT match: DuckDB writes
    none (duckdb#12892, closed as not planned), delta-rs writes
    DICTIONARY_PAGE/PLAINx1 + DATA_PAGE/RLE_DICTIONARYxN. A consumer that wants to prove a chunk
    is 100% dictionary-encoded before choosing a transcode path can read that for free from
    delta-rs and must crack pages open for DuckDB.

Env: ONELAKE_TABLES_PATH (resolve_env), OPT_SORT (explicit columns, default 'date, time' —
'auto' is the delta-rs builder's spelling and is rejected here), OPT_RG (rows per row group,
default 6000000 = duckrun's fixed write geometry; the nightly OOM'd the 12.4GiB runner at 4
threads), OPT_DICT_LIMIT (default 16000000), OPT_PAGE_BYTES (DATA_PAGE_SIZE_LIMIT, default 1048576;
65536 is the value that lands page geometry near delta-rs's),
OPT_TFS_MB (file rotation size, default 256 = duckrun's target_file_size_mb),
OPT_PARQUET_VERSION (V1/V2, byte-identical), OPT_BLOOM (default false — delta-rs writes none,
and they cost 29.2 MB here), OPT_FOOTER_INJECT (comma-separated, default empty — add footer
metadata DuckDB omits: encstats, offsetindex, logical), OPT_PAGE_STATS (column or 'all' — stamp the
per-page Statistics parquet-cpp writes and DuckDB does not), BENCH_ROW_LIMIT, FORCE_REBUILD.
"""
import datetime as _dt
import decimal as _dec
import json
import os
import sys
import tempfile
import time
import uuid

import duckdb
import duckrun
from deltalake import DeltaTable, Schema
from deltalake.transaction import AddAction, create_table_with_add_actions

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

# OPT_TABLE lets a caller aim this builder at its own slot (writer_ab.yml builds both writers
# side by side); the default keeps parquet_layout.yml's shared-slot behaviour unchanged.
TABLE = os.environ.get("OPT_TABLE") or "fct_summary_auto_sort"

RG = int(os.environ.get("OPT_RG") or 6_000_000)
# 16M (>= any row group) means NO column ever falls out of dictionary. KEEP IT THAT WAY: the
# consuming engine's cheap path is a dictionary-ID remap, so plain values would have to be
# re-encoded on load. delta-rs instead caps its dictionary PAGE at ~1MB of bytes and spills the
# rest of the column to PLAIN, which is ~36 MB of why it writes a smaller file — a trade we are
# deliberately NOT making. OPT_DICT_LIMIT=0 (DuckDB's own value-count default) exists to
# reproduce that measurement, not as a configuration to ship.
_dl = (os.environ.get("OPT_DICT_LIMIT") or "").strip()
DICT_LIMIT = None if _dl == "0" else int(_dl or 16_000_000)
PAGE_BYTES = int(os.environ.get("OPT_PAGE_BYTES") or 1_048_576)
FILE_BYTES = int(float(os.environ.get("OPT_TFS_MB") or 256) * 1024 * 1024)
# V1 writes PLAIN_DICTIONARY, V2 writes RLE_DICTIONARY — the SAME physical encoding under two
# spec names (V1's is the deprecated alias), measured byte-identical locally at both low and high
# cardinality. Knob exists so that claim is testable on the real fact, not to tune anything.
PQ_VERSION = (os.environ.get("OPT_PARQUET_VERSION") or "V1").strip().upper()
if PQ_VERSION not in ("V1", "V2"):
    raise SystemExit(f"build_duckdb_native: OPT_PARQUET_VERSION must be V1 or V2, got {PQ_VERSION!r}")
# DuckDB writes bloom filters by default; delta-rs does not. They live BETWEEN row groups and are
# not counted in total_compressed_size, so a per-column footer sum cannot see them — on the real
# fact they are ~32 MB of the 65 MB delta-rs wins by. Default off, because the reader here is
# Direct Lake (which does not use parquet bloom filters) and because leaving them on makes the
# writer A/B measure a feature delta-rs simply doesn't ship.
BLOOM = (os.environ.get("OPT_BLOOM") or "false").strip().lower() in ("true", "1", "yes")
# DIAGNOSTIC. The page compression codec. The byte verify showed the two writers' UNCOMPRESSED page
# payloads are byte-identical at matched geometry and only the compressed representation differs,
# so the cleanest way to ask whether compression matters at all is to remove it: an uncompressed
# file has no snappy stream for the reader to react to. Costs ~3.5x the bytes, which is itself
# informative - a much larger file reading FAST would be unambiguous.
COMPRESSION = (os.environ.get("OPT_COMPRESSION") or "snappy").strip().lower()
_PA_CODEC = "none" if COMPRESSION in ("uncompressed", "none") else COMPRESSION
_DD_CODEC = "uncompressed" if COMPRESSION in ("uncompressed", "none") else COMPRESSION
# OPT_WRITER picks which library encodes the parquet. Everything downstream — the listing, the
# typed footer stats, the AddActions, the Delta commit — is shared, so this isolates the ENCODER.
# 'pyarrow' is parquet-cpp, a third implementation independent of both DuckDB and the parquet-rs
# that delta-rs writes through. It is the experiment that splits "DuckDB encodes something the
# consumer dislikes" from "the consumer has a fast path only parquet-rs output hits".
WRITER = (os.environ.get("OPT_WRITER") or "duckdb").strip().lower()
if WRITER not in ("duckdb", "pyarrow"):
    raise SystemExit(f"build_duckdb_native: OPT_WRITER must be duckdb or pyarrow, got {WRITER!r}")
# DIAGNOSTIC. A page-level diff of the real tables showed the two writers emit the SAME data for
# the slow column — same dictionary, same encoding, chunk sizes 0.2% apart — so what is left is
# metadata. Comma-separated subset of footer_inject.FEATURES ('encstats,offsetindex,logical'),
# empty to write DuckDB's footer untouched. A measurement knob, not a shipping feature: if one of
# these is the cold-read gap, the fix belongs upstream in DuckDB, not in a footer rewrite.
FOOTER_INJECT = [f.strip() for f in (os.environ.get("OPT_FOOTER_INJECT") or "").split(",")
                 if f.strip()]
# DIAGNOSTIC. Re-encode the dictionary indices at 63 groups per bit-packed run (504 values), which
# is what parquet-rs and parquet-cpp both emit and DuckDB never does at any COPY setting. Set to a
# column name to rewrite that column, or 'all'. Same values, same bit width, same dictionary, same
# page boundaries — only the run boundaries move. See page_reframe.py.
REFRAME = (os.environ.get("OPT_REFRAME") or "").strip()
# DIAGNOSTIC (pyarrow arm). Both fast writers carry an ARROW:schema key/value in the footer;
# DuckDB carries none — the one footer property the fast writers SHARE and DuckDB lacks.
# store_schema=False strips it from the pyarrow file: if that file then cold-reads slow, the
# consumer's fast path is gated on ARROW:schema, not on anything parquet-spec.
STORE_SCHEMA = (os.environ.get("OPT_STORE_SCHEMA") or "true").strip().lower() in ("true", "1", "yes")
# DIAGNOSTIC (pyarrow arm). parquet-cpp writes per-PAGE statistics into every data page header;
# DuckDB writes none. It was set aside because delta-rs writes none either and is fast - but that
# filter assumes ONE rule makes both fast writers fast, and DuckDB is the only writer with neither
# page statistics NOR the RLE_DICTIONARY tag. Taking them away from pyarrow is the same shape of
# test that cleared ARROW:schema: if the stripped file then reads slow, this is the trigger.
WRITE_STATS = (os.environ.get("OPT_WRITE_STATISTICS") or "true").strip().lower() in ("true", "1", "yes")
# DIAGNOSTIC. Replace this column's page bytes with the ones another writer produced, keeping this
# file's container. Every metadata difference has now been neutralised at once and the slow file is
# still slow, so the cost is in the payload; this is what proves it. Needs OPT_TRANSPLANT_FROM (the
# donor table URI), and both tables built at the same opt_rg with threads=1 so the row groups line
# up — chimera refuses rather than write a footer that lies. See chimera.py.
# DIAGNOSTIC. Write the explicit is_sorted=False on dictionary page headers. Both fast writers
# emit it (17-byte header), DuckDB omits it (16 bytes), and NO previous experiment could reach it:
# footer injection never touches page headers and the reframe copies dictionary headers verbatim.
# Column name, or 'all'. See page_reframe.mark_dict_sorted_bytes.
# DIAGNOSTIC. Re-encode the dictionary indices at a different bit width. DuckDB picks one width
# per chunk, so on the real fact it flips 8 -> 9 at row group 5, exactly where the dictionary
# crosses 256 - while the engine's resident segments are 9 bits for all 24 row groups. No COPY
# option reaches this. 'uniform' holds one width for the whole column so it never changes down the
# file; 'min' gives each page the narrowest width it can carry, which is what parquet-cpp does.
# Format: '<mode>:<column>' e.g. 'uniform:DUID'. See page_reframe.repack_bitwidth_bytes.
BITWIDTH = (os.environ.get("OPT_BITWIDTH") or "").strip()
# DIAGNOSTIC. Retag the dictionary page encoding. DuckDB tags its dictionary page PLAIN and its
# data pages PLAIN_DICTIONARY - the only one of the three writers that mixes the modern spelling
# on one with the deprecated spelling on the other. Column name, or 'all'.
DICT_ENCODING = (os.environ.get("OPT_DICT_ENCODING") or "").strip()
DICT_SORTED = (os.environ.get("OPT_DICT_SORTED") or "").strip()
# DIAGNOSTIC (duckdb arm). Stamp per-page Statistics into every data page header. parquet-cpp writes
# min_value/max_value/null_count on every page; DuckDB writes none and no COPY option produces them.
# Findings §9.6 named this the weakest link in the whole record: it was only ever tested by REMOVAL
# from pyarrow (443.8 ms, still fast), never by ADDITION to DuckDB, because until now nothing could
# construct a DataPageHeader. Column name, or 'all'. The synthesizer refuses to run unless it first
# reproduces parquet-cpp's own page-stats headers byte for byte. See page_reframe.add_page_stats_bytes.
PAGE_STATS = (os.environ.get("OPT_PAGE_STATS") or "").strip()
# DIAGNOSTIC (pyarrow arm). parquet-cpp caps a data page at 20,000 rows; DuckDB caps by bytes and
# lands on 1,048,576. Raising parquet-cpp's cap to DuckDB's value is the only way to match page
# boundaries WITHOUT shrinking the row group, and the row group has to stay large or the run
# measures per-segment overhead instead of the per-value penalty.
_mrp = (os.environ.get("OPT_MAX_ROWS_PER_PAGE") or "").strip()
MAX_ROWS_PER_PAGE = int(_mrp) if _mrp.isdigit() and int(_mrp) > 0 else None
TRANSPLANT = (os.environ.get("OPT_TRANSPLANT") or "").strip()
TRANSPLANT_FROM = (os.environ.get("OPT_TRANSPLANT_FROM") or "").strip().rstrip("/")
# DIAGNOSTIC (duckdb arm). COPY thread count — 2 is the memory-fit default (see the SET threads
# comment at the call site). 1 additionally makes row-group emission deterministic: the parallel
# carve emits row groups OUT of stream order (rg0 holds mid-stream rows; payload_diff shows it),
# which is a real difference from the single-stream fast writers.
THREADS = int(os.environ.get("OPT_THREADS") or 2)
sort = (os.environ.get("OPT_SORT") or "date, time").strip()
if sort.lower() == "auto":
    raise SystemExit("build_duckdb_native: OPT_SORT must be explicit columns (e.g. 'date, time'); "
                     "'auto' belongs to the delta-rs builder (build_auto_sort.py).")
sort_cols = [c.strip() for c in sort.split(",") if c.strip()]
force = os.environ.get("FORCE_REBUILD", "false").strip().lower() == "true"
_lim = os.environ.get("BENCH_ROW_LIMIT", "").strip()
N_CAP = int(_lim) if _lim.isdigit() and int(_lim) > 0 else None

tables_root = os.environ["ONELAKE_TABLES_PATH"].rstrip("/")
table_uri = f"{tables_root}/tests/{TABLE}"

con = duckrun.connect(tables_root, read_only=False)
dd = con.con

# Let the sort spill. BENCH_ROW_LIMIT caps with `select * from ... order by all limit N`, so the
# top-N carries EVERY column of the fact, not the three OPT_COLUMNS projects — and with no temp
# directory configured DuckDB cannot spill it and simply dies. Run 32627183898 OOM'd here at
# 8.3M rows ("failed to allocate 768.0 MiB, 11.9/12.4 GiB used") on a build that survives 142M,
# because a cap well below the source size is a real top-N heap while a cap at the source size
# degenerates to a stream. Spilling changes nothing about what is written.
_spill = tempfile.mkdtemp(prefix="duckdb_spill_")
dd.execute(f"SET temp_directory='{_spill.replace(chr(92), '/')}'")

# Probe DATA_PAGE_SIZE_LIMIT before any expensive work: a duckdb without it (any stable <= 1.5.5)
# would silently write the old broken layout — one ~100MB page per column chunk — and corrupt the A/B.
_probe = os.path.join(tempfile.mkdtemp(prefix="duckrun_page_probe_"), "probe.parquet").replace("\\", "/")
try:
    dd.execute(f"COPY (select 1 as x) TO '{_probe}' (FORMAT parquet, DATA_PAGE_SIZE_LIMIT {PAGE_BYTES})")
except Exception as ex:
    raise SystemExit(
        f"build_duckdb_native: this duckdb ({duckdb.__version__}) rejected DATA_PAGE_SIZE_LIMIT "
        f"(duckdb#24645, nightlies only until the next stable; the workflow pins "
        f"duckdb==1.6.0.dev365 for writer=duckdb_native): {ex}")
try:
    con.sql("create schema if not exists tests")
except Exception:
    dd.execute("create schema if not exists tests")


def _exists():
    try:
        con.sql(f"select 1 from tests.{TABLE} limit 1").fetchone()
        return True
    except Exception:
        return False


def _write_pyarrow(dd, collist, src, order, table_uri, run_tag, con):
    """Same rows, same order, same geometry — encoded by parquet-cpp instead of DuckDB.

    Written locally then uploaded, because pyarrow has no OneLake filesystem here and the file is
    small enough that a round trip through the runner's disk is cheaper than the plumbing.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dbt.adapters.duckrun.objectstore import build_store, upload

    local_dir = tempfile.mkdtemp(prefix="pyarrow_write_")
    local = os.path.join(local_dir, f"part-{run_tag}-{uuid.uuid4()}.parquet")
    rel = dd.sql(f"select {collist} from {src} order by {order}")
    reader = rel.to_arrow_reader(65536) if hasattr(rel, "to_arrow_reader")         else rel.fetch_arrow_reader(65536)
    t_s = time.perf_counter()
    written, writer, buf, nbuf = 0, None, [], 0

    def _flush(tbl, w):
        # Slice to EXACTLY RG rows per group. Flushing on ">= RG" leaves a ragged tail row group
        # behind every write, which is a geometry difference the comparison must not introduce.
        while tbl.num_rows >= RG:
            if w is None:
                _kw = {}
                if MAX_ROWS_PER_PAGE:
                    # Make parquet-cpp write DuckDB's page geometry instead of the other way round.
                    # Byte identity needs the two writers to agree on page boundaries, and every
                    # attempt so far did that by shrinking DuckDB to parquet-cpp's 20,000-row pages
                    # — which forces a row group small enough that thousands of them are needed for
                    # the full fact, and per-segment overhead then swamps the very signal being
                    # measured (509 row groups put ~800ms of fixed cost on BOTH arms). Going the
                    # other way keeps 24 row groups and the 20x signal intact.
                    _kw["max_rows_per_page"] = MAX_ROWS_PER_PAGE
                w = pq.ParquetWriter(local, tbl.schema, compression=_PA_CODEC, use_dictionary=True,
                                     data_page_size=PAGE_BYTES, version="1.0",
                                     write_statistics=WRITE_STATS, store_schema=STORE_SCHEMA, **_kw)
            w.write_table(tbl.slice(0, RG), row_group_size=RG)
            tbl = tbl.slice(RG)
        return tbl, w

    try:
        for batch in reader:
            buf.append(batch)
            nbuf += batch.num_rows
            if nbuf >= RG:
                tbl, writer = _flush(pa.Table.from_batches(buf), writer)
                written += nbuf - tbl.num_rows
                buf = tbl.to_batches() if tbl.num_rows else []
                nbuf = tbl.num_rows
        if nbuf:
            tbl = pa.Table.from_batches(buf)
            if writer is None:
                writer = pq.ParquetWriter(local, tbl.schema, compression=_PA_CODEC,
                                          use_dictionary=True, data_page_size=PAGE_BYTES,
                                          version="1.0", write_statistics=WRITE_STATS,
                                          store_schema=STORE_SCHEMA)
            writer.write_table(tbl, row_group_size=RG)
            written += nbuf
    finally:
        if writer is not None:
            writer.close()
    size = os.path.getsize(local)
    print(f"{written:,} rows -> pyarrow (parquet-cpp) {pa.__version__}, ROW_GROUP_SIZE {RG:,} "
          f"x data_page_size {PAGE_BYTES:,} x {_PA_CODEC} x v1.0 x store_schema={STORE_SCHEMA} "
          f"x write_statistics={WRITE_STATS} "
          f"-> {size / 1048576:.1f} MB in {time.perf_counter() - t_s:.1f}s", flush=True)
    store = build_store(table_uri, con.storage_options)
    upload(store, os.path.basename(local), local, single_shot=True)
    print(f"uploaded {os.path.basename(local)}", flush=True)


def _delta_json(v):
    if isinstance(v, _dt.datetime):
        return v.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(v, (_dt.date, _dt.time)):
        return str(v)
    if isinstance(v, _dec.Decimal):  # Delta JSON stats spell decimals as numbers
        return float(v)
    return v


_t0 = time.perf_counter()
if not force and _exists():
    rows = con.sql(f"select count(*) from tests.{TABLE}").fetchone()[0]
    print(f"tests.{TABLE} already exists ({rows:,} rows) — skipping (rebuild=true to rebuild)",
          flush=True)
    status, k, n = "skipped", None, rows
else:
    # BENCH_SOURCE lets a caller read one lakehouse while writing into another (a full expression
    # such as delta_scan('abfss://.../mart/fct_summary')); the cold benchmark needs that because
    # its throwaway lakehouse has no mart schema. Same override the delta-rs builder honours.
    _src_tbl = (os.environ.get("BENCH_SOURCE") or "mart.fct_summary").strip()
    # `order by all` before the cap, NOT a bare limit. A bare limit takes whichever rows the scan
    # produces first, so two arms asking for the same 142M rows get DIFFERENT rows — which was
    # silently true of every writer comparison here until a Delta-log diff caught the two arms
    # disagreeing on maxValues.date by a fortnight while their row counts matched exactly. Ordering
    # first makes the subset the FIRST n rows and identical for every arm; DuckDB runs it as a
    # top-N rather than a full sort.
    #
    # The projection goes INSIDE the cap, which is a memory fix, not a cosmetic one. DuckDB's
    # top-N is not a spillable operator, so `select *` makes it hold N rows of EVERY column of the
    # fact in memory — 8.3M of them OOM'd a 12.4GiB runner twice (runs 32627183898, 32627622202)
    # on a harness that survives 142M, because a cap at roughly the source size degenerates to a
    # stream while a cap well below it is a real heap. Projecting first is also what byte_verify
    # does, so the Fabric arms now slice the source the same way the off-Fabric byte comparison
    # does. Determinism is unaffected: rows tied on the projected columns are by definition
    # identical in them, so which of them the cap takes cannot change the bytes written.
    coltypes = dict(dd.execute(
        f"select column_name, column_type from (describe select * from {_src_tbl})").fetchall())
    cols = list(coltypes)
    # OPT_COLUMNS narrows the table to a subset — a minimal reproducer writes far fewer bytes and
    # measures the same thing, since probing one column only transcodes that column.
    _want = [c.strip() for c in (os.environ.get("OPT_COLUMNS") or "").split(",") if c.strip()]
    if _want:
        missing = [c for c in _want if c not in coltypes]
        if missing:
            raise SystemExit(f"build_duckdb_native: OPT_COLUMNS names unknown column(s) {missing}; "
                             f"source has {cols}")
        cols = _want
        coltypes = {c: coltypes[c] for c in cols}
    collist = ", ".join(f'"{c}"' for c in cols)

    # The cap is a CUTOFF on the leading sort column, not `order by all limit N`. DuckDB's top-N is
    # not a spillable operator, so a top-N carrying whole rows OOM'd a 12.4GiB runner four times
    # (32627183898, 32627622202, 32628685053, 32628987366) at an 8.3M cap on a harness that
    # survives 142M — a cap at roughly the source size degenerates to a stream, a cap well below it
    # is a real heap. Taking the top-N over ONE column costs 4 bytes a row instead of a whole row,
    # and the resulting cutoff is a plain predicate.
    #
    # It keeps what §9.1 actually needs — both arms evaluating ONE deterministic expression, so
    # they read identical rows — and gives up only exactness of the count, since the cutoff rounds
    # out to a whole value of the leading column. That is why n_src is printed rather than assumed.
    if N_CAP is None:
        src = _src_tbl
    else:
        _lead = f'"{sort_cols[0]}"'
        _cut = dd.execute(f"select max(k) from (select {_lead} as k from {_src_tbl} "
                          f"order by k limit {N_CAP})").fetchone()[0]
        src = f"(select {collist} from {_src_tbl} where {_lead} <= '{_cut}')"
    n_src = dd.execute(f"select count(*) from {src}").fetchone()[0]
    if N_CAP is not None:
        _total = dd.execute(f"select count(*) from {_src_tbl}").fetchone()[0]
        print(f"source has {_total:,} rows; capped to {n_src:,} at {sort_cols[0]} <= {_cut}"
              + ("  (cap is a no-op)" if _total <= N_CAP else ""), flush=True)
    order = ", ".join(f'"{c}"' for c in sort_cols)
    run_tag = uuid.uuid4().hex[:8]

    # threads=1 is the whole ballgame for cross-file clustering, and it is NOT about
    # threads=2 is a MEMORY guard, not a layout one. Each writer thread buffers a full
    # uncompressed row group, so the runner's default 4 threads OOMs a 12.4GiB box on this fact —
    # measured three times now: at 16M rows/group, at 6M, and again with no pin at all
    # (run 32475734960, "failed to allocate 512.0 MiB (12.4/12.4 GiB used)"). 2 threads fits.
    # It is deliberately NOT 1. Pinning to one writer is what makes rotated files disjoint on
    # the sort key (locally 1 thread -> 0/6 overlapping pairs, 2 or 4 -> 6/6, because a
    # multi-threaded COPY writes several files at once and the writers carve the sorted stream
    # in parallel), but that clustering was measured worth 0.05% of the table here
    # (19/23 -> 0/2 overlapping pairs moved it 835.08 -> 834.66 MB) and delta-rs, the writer
    # this is compared against, ships 2/3 overlapping files itself. So overlap is accepted.
    # OPT_THREADS=1 exists as a diagnostic: it restores stream-order row-group emission.
    dd.execute(f"SET threads={THREADS}")
    if WRITER == "pyarrow":
        _write_pyarrow(dd, collist, src, order, table_uri, run_tag, con)
    else:
        print(f"{n_src:,} rows -> one sorted single-stream COPY, "
              f"ROW_GROUP_SIZE {RG:,} x FILE_SIZE_BYTES {FILE_BYTES:,} "
              f"x DATA_PAGE_SIZE_LIMIT {PAGE_BYTES:,} x {PQ_VERSION} x DICTIONARY_SIZE_LIMIT "
              f"{f'{DICT_LIMIT:,}' if DICT_LIMIT else 'duckdb default'} x bloom={BLOOM}",
              flush=True)
        _dict_opt = f"DICTIONARY_SIZE_LIMIT {DICT_LIMIT}," if DICT_LIMIT else ""
        t_s = time.perf_counter()
        dd.execute(f"""
            COPY (select {collist} from {src} order by {order})
            TO '{table_uri}'
            (FORMAT parquet, ROW_GROUP_SIZE {RG}, FILE_SIZE_BYTES {FILE_BYTES},
             {_dict_opt} DATA_PAGE_SIZE_LIMIT {PAGE_BYTES},
             COMPRESSION {_DD_CODEC}, PARQUET_VERSION {PQ_VERSION},
             WRITE_BLOOM_FILTER {str(BLOOM).lower()},
             FILENAME_PATTERN 'part-{run_tag}-{{uuid}}', APPEND)
        """)
        print(f"copied in {time.perf_counter() - t_s:.1f}s", flush=True)

    # Sizes from a real listing (the AddAction size field should be true, not guessed).
    from dbt.adapters.duckrun.objectstore import build_store
    import obstore
    store = build_store(table_uri, con.storage_options)

    def _listing():
        out = {}
        for batch in obstore.list(store):
            for obj in batch:
                p = obj["path"].rsplit("/", 1)[-1]
                if p.startswith(f"part-{run_tag}-"):
                    out[p] = obj["size"]
        return out

    sizes = _listing()
    if not sizes:
        raise SystemExit(f"listing returned no part-{run_tag}-* files")

    # DIAGNOSTIC rewrites. Both must happen HERE — after the COPY, before the listing that feeds
    # AddAction.size — because each rewrite changes file length, and a Delta size field that
    # disagrees with the blob is a worse bug than the one being investigated. REFRAME runs FIRST:
    # page_reframe rebuilds every ColumnChunk with NULLed index offsets (page_reframe.py), so an
    # OffsetIndex injected before it would be silently stripped; footer_inject walks the pages
    # itself, so injecting after the reframe describes the file as it now is.
    # Transplant first: it swaps whole chunks, so any footer surgery must describe the result.
    if TRANSPLANT:
        import chimera
        if not TRANSPLANT_FROM:
            raise SystemExit("OPT_TRANSPLANT needs OPT_TRANSPLANT_FROM (the donor table URI)")
        donor_store = build_store(TRANSPLANT_FROM, con.storage_options)
        donor_keys = sorted(
            obj["path"].rsplit("/", 1)[-1]
            for batch in obstore.list(donor_store) for obj in batch
            if obj["path"].rsplit("/", 1)[-1].endswith(".parquet"))
        if len(donor_keys) != 1 or len(sizes) != 1:
            raise SystemExit(
                f"transplant needs exactly one file on each side — host has {len(sizes)}, "
                f"donor has {len(donor_keys)}. Raise OPT_TFS_MB so neither rotates.")
        print(f"transplanting {TRANSPLANT} pages from {TRANSPLANT_FROM.rsplit('/', 1)[-1]}",
              flush=True)
        chimera.transplant_remote(store, sorted(sizes)[0], donor_store, donor_keys[0], TRANSPLANT)
        sizes = _listing()          # the chunk swap changed the length; re-read the true size

    if DICT_ENCODING:
        import page_reframe
        col = None if DICT_ENCODING.lower() == "all" else DICT_ENCODING
        print(f"retagging dictionary page encoding -> PLAIN_DICTIONARY in {len(sizes)} file(s), "
              f"column={col or 'ALL'}", flush=True)
        page_reframe.set_dict_encoding_remote(store, sorted(sizes), column=col, encoding=2)
        sizes = _listing()          # header width changed; re-read the true sizes

    if DICT_SORTED:
        import page_reframe
        col = None if DICT_SORTED.lower() == "all" else DICT_SORTED
        print(f"marking is_sorted=False on dictionary pages in {len(sizes)} file(s), "
              f"column={col or 'ALL'}", flush=True)
        page_reframe.mark_dict_sorted_remote(store, sorted(sizes), column=col)
        sizes = _listing()          # each dictionary header grew a byte; re-read the true sizes

    if BITWIDTH:
        import page_reframe
        _mode, _, _col = BITWIDTH.partition(":")
        col = _col.strip() or None
        print(f"repacking bit width mode={_mode.strip()} in {len(sizes)} file(s), "
              f"column={col or 'ALL'}", flush=True)
        page_reframe.repack_bitwidth_remote(store, sorted(sizes), column=col,
                                            mode=_mode.strip())
        sizes = _listing()          # widths changed the packing; re-read the true sizes

    if REFRAME:
        import page_reframe
        col = None if REFRAME.lower() == "all" else REFRAME
        print(f"re-framing bit-packed runs to {page_reframe.GROUPS} groups "
              f"({page_reframe.GROUPS * 8} values) in {len(sizes)} file(s), "
              f"column={col or 'ALL'}", flush=True)
        page_reframe.reframe_remote(store, sorted(sizes), column=col)
        sizes = _listing()          # every page changed size; re-read the true sizes

    # PAGE_STATS runs AFTER the payload rewrites and BEFORE footer_inject. After, because the
    # stamped header carries uncompressed/compressed_page_size and must describe the FINAL bodies —
    # the statistics values themselves are invariant under a re-framing, but the size fields are
    # not. Before, because every stamped header grows ~20 bytes and moves every offset after it, so
    # any footer surgery has to describe the file as it ends up.
    if PAGE_STATS:
        import page_reframe
        col = None if PAGE_STATS.lower() == "all" else PAGE_STATS
        print(f"stamping per-page statistics in {len(sizes)} file(s), "
              f"column={col or 'ALL'}", flush=True)
        page_reframe.add_page_stats_remote(store, sorted(sizes), column=col)
        sizes = _listing()          # every data page header grew; re-read the true sizes

    # DIAGNOSTIC (OPT_FOOTER_INJECT): add the metadata delta-rs writes and DuckDB does not.
    if FOOTER_INJECT:
        import footer_inject
        print(f"injecting {', '.join(FOOTER_INJECT)} into {len(sizes)} footer(s)", flush=True)
        notes = footer_inject.patch_remote(store, sorted(sizes), FOOTER_INJECT)
        if not notes:
            raise SystemExit(f"OPT_FOOTER_INJECT={','.join(FOOTER_INJECT)} was set but nothing "
                             "needed injecting — duckdb may have started writing it, which would "
                             "silently turn this run into a no-op control.")
        sizes = _listing()          # files grew; re-read the true sizes

    files = sorted(sizes)
    k = len(files)

    # Explicit file list, no glob: globbing abfss:// needs a LIST the azure extension can't
    # parse against OneLake (json.exception.type_error.302); direct GETs work fine.
    flist = "[" + ", ".join(f"'{table_uri}/{f}'" for f in files) + "]"

    nrows = {f.rsplit("/", 1)[-1]: r for f, r in dd.execute(
        f"select file_name, num_rows from parquet_file_metadata({flist})").fetchall()}
    n = sum(nrows.values())
    if n != n_src:
        raise SystemExit(f"row count mismatch after COPY: files {n:,} vs source {n_src:,}")

    # Typed per-file stats from the footers: CAST each stat string to the column's type, then
    # spell it Delta-JSON. cast (not try_cast): an unparseable stat should kill the build loudly.
    mins: dict = {f: {} for f in files}
    maxs: dict = {f: {} for f in files}
    nulls: dict = {f: {} for f in files}
    for c in cols:
        t = coltypes[c]
        for f, mn, mx, nc in dd.execute(f"""
            select file_name, cast(stats_min_value as {t}), cast(stats_max_value as {t}),
                   coalesce(stats_null_count, 0)
            from parquet_metadata({flist}) where path_in_schema = '{c}'
        """).fetchall():
            b = f.rsplit("/", 1)[-1]
            mins[b][c], maxs[b][c], nulls[b][c] = mn, mx, int(nc)

    now_ms = int(time.time() * 1000)
    actions = []
    for f in files:
        stats = json.dumps({
            "numRecords": nrows[f],
            "minValues": {c: _delta_json(mins[f][c]) for c in cols},
            "maxValues": {c: _delta_json(maxs[f][c]) for c in cols},
            "nullCount": {c: nulls[f][c] for c in cols},
        })
        actions.append(AddAction(path=f, size=sizes[f], partition_values={},
                                 modification_time=now_ms, data_change=True, stats=stats))

    # arro3 (a deltalake dep) reads the relation's Arrow C-stream — CI has no pyarrow (test-only dep)
    from arro3.core import RecordBatchReader
    schema = Schema.from_arrow(
        RecordBatchReader.from_stream(dd.sql(f"select {collist} from {src} limit 0")).schema)

    so = con.storage_options
    try:
        dt = DeltaTable(table_uri, storage_options=so)
        # existing table (either builder's output): overwrite emits the removes for the old files
        dt.create_write_transaction(actions, mode="overwrite", schema=schema)
    except Exception as ex:
        if "not a delta table" not in str(ex).lower() and type(ex).__name__ != "TableNotFoundError":
            raise
        create_table_with_add_actions(table_uri, schema, actions, mode="error",
                                      storage_options=so)
    print(f"committed {k} add action(s) -> v{DeltaTable(table_uri, storage_options=so).version()}",
          flush=True)

    # ---- verify the layout on the REAL files: a silently wrong shape would corrupt the A/B.
    meta = dd.execute(f"""
        select file_name, count(distinct row_group_id),
               min(case when path_in_schema = '{sort_cols[0]}' then stats_min_value end),
               max(case when path_in_schema = '{sort_cols[0]}' then stats_max_value end)
        from parquet_metadata({flist})
        group by 1 order by 3
    """).fetchall()
    rgs = sum(m[1] for m in meta)
    rng = [(m[2], m[3]) for m in meta]
    overlaps = sum(1 for a, b in zip(rng, rng[1:]) if b[0] < a[1])
    # THE metric for the single-stream shape: files rotate along the sorted stream, so adjacent
    # files should NOT overlap on the sort key. The PER_THREAD_OUTPUT variant scored 19/23.
    print(f"{sort_cols[0]} range overlap across files: {overlaps}/{max(len(rng) - 1, 1)} "
          f"adjacent pairs", flush=True)
    non_dict = dd.execute(f"""
        select distinct path_in_schema, encodings
        from parquet_metadata({flist})
        where not (contains(encodings, 'RLE_DICTIONARY') or contains(encodings, 'PLAIN_DICTIONARY'))
    """).fetchall()
    if non_dict:  # warn only: a data property (e.g. >DICT_LIMIT distinct doubles), not a bug
        print(f"WARN: columns not fully dictionary-encoded: {non_dict}", flush=True)
    n_back = dd.execute(f"select count(*) from delta_scan('{table_uri}')").fetchone()[0]
    if n_back != n_src:
        raise SystemExit(f"row count mismatch after commit: delta_scan {n_back:,} vs source {n_src:,}")
    print(f"verified: {len(meta)} file(s) / {rgs} row group(s), {n_back:,} rows readable",
          flush=True)
    status = "rebuilt"

report.merge({"tables": {TABLE: {"build": {
    "engine": "duckdb_copy+delta_commit", "sort": f"sorted by ({sort})", "vorder": False,
    "row_group_ceiling": RG, "dictionary_size_limit": DICT_LIMIT,
    "data_page_size_limit": PAGE_BYTES, "file_size_bytes": FILE_BYTES,
    "parquet_version": PQ_VERSION, "bloom_filter": BLOOM,
    "files": k, "rows": n,
    "seconds": round(time.perf_counter() - _t0, 1), "status": status}}}})
