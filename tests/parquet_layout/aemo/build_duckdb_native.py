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
and they cost 29.2 MB here), BENCH_ROW_LIMIT, FORCE_REBUILD.
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
    src = _src_tbl if N_CAP is None else f"(select * from {_src_tbl} limit {N_CAP})"
    n_src = dd.execute(f"select count(*) from {src}").fetchone()[0]

    # column types drive the typed stats casts below; the mart already stores decimal(18,4)
    # (duckrun's write path narrows wide decimals), so everything here dictionary-encodes —
    # a DECIMAL(p>18) source would write FLBA, which never gets a dictionary (the WARN catches it)
    coltypes = dict(dd.execute(f"select column_name, column_type from (describe {src})").fetchall())
    cols = list(coltypes)
    collist = ", ".join(f'"{c}"' for c in cols)
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
    dd.execute("SET threads=2")
    print(f"{n_src:,} rows -> one sorted single-stream COPY, "
          f"ROW_GROUP_SIZE {RG:,} x FILE_SIZE_BYTES {FILE_BYTES:,} "
          f"x DATA_PAGE_SIZE_LIMIT {PAGE_BYTES:,} x {PQ_VERSION} x DICTIONARY_SIZE_LIMIT "
          f"{f'{DICT_LIMIT:,}' if DICT_LIMIT else 'duckdb default'} x bloom={BLOOM}", flush=True)
    _dict_opt = f"DICTIONARY_SIZE_LIMIT {DICT_LIMIT}," if DICT_LIMIT else ""
    t_s = time.perf_counter()
    dd.execute(f"""
        COPY (select {collist} from {src} order by {order})
        TO '{table_uri}'
        (FORMAT parquet, ROW_GROUP_SIZE {RG}, FILE_SIZE_BYTES {FILE_BYTES},
         {_dict_opt} DATA_PAGE_SIZE_LIMIT {PAGE_BYTES},
         COMPRESSION snappy, PARQUET_VERSION {PQ_VERSION},
         WRITE_BLOOM_FILTER {str(BLOOM).lower()},
         FILENAME_PATTERN 'part-{run_tag}-{{uuid}}', APPEND)
    """)
    print(f"copied in {time.perf_counter() - t_s:.1f}s", flush=True)

    # Sizes from a real listing (the AddAction size field should be true, not guessed).
    from dbt.adapters.duckrun.objectstore import build_store
    import obstore
    store = build_store(table_uri, con.storage_options)
    sizes = {}
    for batch in obstore.list(store):
        for obj in batch:
            p = obj["path"]
            if p.rsplit("/", 1)[-1].startswith(f"part-{run_tag}-"):
                sizes[p.rsplit("/", 1)[-1]] = obj["size"]
    if not sizes:
        raise SystemExit(f"listing returned no part-{run_tag}-* files")
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
