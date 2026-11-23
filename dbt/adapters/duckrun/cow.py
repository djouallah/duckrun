"""Copy-on-write rewrites: DuckDB writes the parquet, delta-rs only commits.

For an operation that rewrites existing data (MERGE, UPDATE, DELETE), DuckDB does the expensive
work — the join, the rewrite of the touched files and the parquet encoding, spilling to disk as it
needs — and delta-rs commits nothing but the file actions: the new files (``AddAction``) and the
files they replace (``RemoveAction``), in ONE transaction pinned to the version the statement read
(``create_write_transaction``, ``max_commit_retries=0``), so a concurrent commit fails it loudly.

Internal module, not public API. :func:`unsupported_reason` names the table shapes this writer
cannot commit correctly — deletion vectors and change data feed on a rewrite (the commit API has no
action for their metadata), identity columns (their high-water mark lives in the schema), append-only
tables, and column-mapped tables that are also partitioned or nested (DuckDB's own Delta writer
refuses those too) — and the statement is refused with :class:`Unsupported` before anything is
written; there is no fallback writer. CHECK constraints and invariants are evaluated, and generated
columns recomputed, in DuckDB (:func:`apply_table_rules`); an expression DuckDB cannot evaluate is
refused the same way.
"""
import datetime as _dt
import json
import math
import os
import time
import uuid
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Sequence

from deltalake import CommitProperties, DeltaTable
from deltalake.exceptions import CommitFailedError
from deltalake.transaction import AddAction, RemoveAction

from .policy import DATA_PAGE_SIZE_LIMIT, DEFAULT_TARGET_FILE_SIZE, ROW_GROUP_DEFAULT_ROWS

# The fixed read layout (Direct Lake / VertiPaq), measured on the writer_cold / writer_ab benchmarks:
#   * DATA_PAGE_SIZE_LIMIT 1 MB — DuckDB's default page is far larger (duckdb#24507);
#   * DICTIONARY_SIZE_LIMIT up to the row-group size, so every column stays dictionary-encoded;
#   * PARQUET_VERSION V1 — a column that overflows its dictionary falls back to PLAIN;
#   * no bloom filters — the delta-rs writer never wrote them (~30 MB on the aemo mart).
DICTIONARY_SIZE_LIMIT = 16_000_000
STATS_TRUNCATE_LENGTH = 64          # longest string stat kept (a longer max is dropped, min is cut)
DEFAULT_NUM_INDEXED_COLS = 32       # delta.dataSkippingNumIndexedCols default (Delta protocol)


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


# The scan's own columns. The file is named explicitly: a table may carry a column called
# `filename` (a file-ingest table often does), and `filename = true` then fails to bind. The
# (file, row) pair is the row's physical position, the order a rewrite keeps when no sort key is
# given (the session runs with preserve_insertion_order=false, so nothing else would keep it).
FILE_COL = "__duckrun_file"
ROW_COL = "__duckrun_row"
_HIDDEN = (FILE_COL, ROW_COL)


def scan(path: str, version: int, alias: str = "") -> str:
    """``delta_scan`` of ``path`` at ``version`` with the file column (:data:`FILE_COL`)."""
    return (f"delta_scan({_lit(path)}, version => {int(version)}, filename = {_lit(FILE_COL)})"
            + (f" AS {_q(alias)}" if alias else ""))


def with_position(qualifier: str = "") -> str:
    """Select-list tail that keeps a scanned row's physical position (:data:`ROW_COL`)."""
    return f"{qualifier}file_row_number AS {_q(ROW_COL)}"


# Writer features a plain add/remove commit leaves intact. Anything else (row tracking, domain
# metadata, in-commit timestamps, clustering, …) asks the writer for more, so delta-rs writes it.
_WRITABLE_FEATURES = {"appendOnly", "invariants", "checkConstraints", "changeDataFeed",
                      "generatedColumns", "columnMapping", "identityColumns", "deletionVectors",
                      "timestampNtz", "vacuumProtocolCheck"}


PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName"
FIELD_ID_KEY = "delta.columnMapping.id"


def mapping_mode(configuration) -> Optional[str]:
    """The table's ``delta.columnMapping.mode`` (lower-cased), or None when mapping is off."""
    mode = str((configuration or {}).get("delta.columnMapping.mode") or "").strip().lower()
    return None if mode in ("", "none") else mode


def column_mapping(dt: DeltaTable):
    """``{logical name: (physical name, field id)}`` for a column-mapped table (Spark/Fabric
    ``delta.columnMapping.mode`` name / id), or None when mapping is off. Its readers resolve a
    column by the physical name (or the parquet field id), so the files are written under those."""
    if mapping_mode(dt.metadata().configuration) is None:
        return None
    out = {}
    for f in dt.schema().fields:
        md = f.metadata or {}
        fid = md.get(FIELD_ID_KEY)
        out[f.name] = (str(md.get(PHYSICAL_NAME_KEY) or f.name), int(fid) if fid is not None else None)
    return out


def unsupported_reason(dt: DeltaTable, op: str = "rewrite") -> Optional[str]:
    """Why DuckDB-written files committed through delta-rs's commit API cannot ``op`` ``dt`` (None =
    they can): each case needs Delta metadata or checks the commit API does not produce. ``op`` is
    ``rewrite`` (MERGE / UPDATE / DELETE / REPLACE WHERE), ``append``, ``overwrite``, ``truncate``
    (every file removed, none added) or ``compact``.

    * change data feed stops only a rewrite: a commit that only adds or removes WHOLE files (append,
      overwrite, truncate) needs no change files — readers derive the changes from the actions —
      but a rewrite carries rows over, which would read as deleted and re-inserted;
    * append-only stops anything that removes data; a compaction changes none (dataChange=false);
    * deletion vectors stop a rewrite and a compaction (their removes would need the vector); an
      overwrite / truncate lets delta-rs derive the removes itself;
    * identity columns stop every write of rows (their high-water mark lives in the schema)."""
    md = dt.metadata()
    conf = md.configuration or {}
    if (op == "rewrite"
            and str(conf.get("delta.enableChangeDataFeed", "false")).lower() == "true"):
        return "change data feed (the commit API cannot write the change files)"
    if (op in ("rewrite", "overwrite", "truncate")
            and str(conf.get("delta.appendOnly", "false")).lower() == "true"):
        return "append-only table"
    mode = mapping_mode(conf)
    if mode is not None:
        if mode not in ("name", "id"):
            return f"column mapping mode {mode}"
        if md.partition_columns:
            return "column mapping on a partitioned table"
        if any(getattr(f.type, "type", None) in ("struct", "array", "map") for f in dt.schema().fields):
            return "column mapping with nested columns"
        if any(PHYSICAL_NAME_KEY not in (f.metadata or {}) for f in dt.schema().fields):
            return "column mapping without physical names"
    unknown = sorted(set(dt.protocol().writer_features or []) - _WRITABLE_FEATURES)
    if unknown:
        return f"table feature(s) {', '.join(unknown)}"
    if (op in ("rewrite", "compact")
            and "deletionVectors" in (dt.protocol().reader_features or [])):
        return "deletion vectors (the commit API cannot remove a file that has one)"
    if op not in ("compact", "truncate") and '"delta.identity.' in dt.schema().to_json():
        return "identity columns"
    return None


class Unsupported(ValueError):
    """duckrun cannot write this statement or table shape (DuckDB writes the files and delta-rs's
    commit API commits them; some Delta features need actions that API does not have). Raised
    before anything is written."""


def _table_rules(dt: DeltaTable):
    """``(checks, generated)``: the CHECK constraints and invariants as ``(what, expr)``, and the
    generated columns as ``(name, expr)``."""
    conf = dt.metadata().configuration or {}
    checks = [(f"CHECK constraint {k.split('.', 2)[2]}", v) for k, v in conf.items()
              if k.lower().startswith("delta.constraints.")]
    generated = []
    for f in dt.schema().fields:
        md = f.metadata or {}
        if md.get("delta.invariants"):
            checks.append((f"invariant on {f.name}",
                           json.loads(md["delta.invariants"])["expression"]["expression"]))
        if md.get("delta.generationExpression"):
            generated.append((f.name, md["delta.generationExpression"]))
    return checks, generated


def check_table_rules(cur, source: str, dt: DeltaTable) -> None:
    """Raise :class:`Unsupported` when DuckDB cannot evaluate one of the table's rule expressions
    (Spark SQL) over ``source`` (the target's scan). Bind only (``cur.sql`` does not execute), so it
    runs before anything executes and a refusal leaves an open transaction usable for delta-rs."""
    checks, generated = _table_rules(dt)
    exprs = [x for _, x in checks] + [x for _, x in generated]
    if not exprs:
        return
    try:
        cur.sql(f"SELECT {', '.join(f'({x})' for x in exprs)} FROM {source}")
    except Exception as e:
        raise Unsupported(f"DuckDB cannot evaluate a table rule: {e}") from e


def _counts(cur, table: str, conds: Sequence[str]) -> List[int]:
    """``count(*)`` of the rows of ``table`` matching each of ``conds``, in one pass."""
    sel = ", ".join(f"count(*) FILTER (WHERE {c})" for c in conds)
    return [int(n) for n in cur.execute(f"SELECT {sel} FROM {table}").fetchall()[0]]


def generation_expressions(dt: DeltaTable) -> List[tuple]:
    """``[(column, expression)]`` for the table's generated columns."""
    return _table_rules(dt)[1]


def generated_columns(dt: DeltaTable) -> List[str]:
    return [name for name, _ in generation_expressions(dt)]


def apply_table_rules(cur, table: str, dt: DeltaTable, operation: str) -> None:
    """Make the rows about to be written obey the table's rules, as delta-rs's writer did:
    generated columns are recomputed from their expressions; CHECK constraints and column
    invariants fail the statement on a violating row (a NULL result passes, as in SQL). Run
    :func:`check_table_rules` first."""
    checks, generated = _table_rules(dt)
    if generated:
        types = {r[0]: str(r[1]) for r in cur.execute(f"DESCRIBE {table}").fetchall()}
        cur.execute(f"UPDATE {table} SET " + ", ".join(
            f"{_q(name)} = CAST(({expr}) AS {types[name]})" for name, expr in generated))
    if not checks:
        return
    counts = _counts(cur, table, [f"({expr}) IS FALSE" for _, expr in checks])
    violations = [f"{what} ({expr}) fails on {n} row(s)"
                  for (what, expr), n in zip(checks, counts) if n]
    if violations:
        raise ValueError(f"{operation} refused: " + "; ".join(violations))


def parquet_options(row_group_rows: Optional[int] = None,
                    target_file_size: Optional[int] = None) -> Dict[str, object]:
    """The COPY options of the fixed read layout; ``row_group_rows`` / ``target_file_size`` are the
    per-model overrides (None keeps duckrun's defaults)."""
    rg = int(row_group_rows if row_group_rows is not None else ROW_GROUP_DEFAULT_ROWS)
    return {
        "COMPRESSION": "snappy",
        "ROW_GROUP_SIZE": rg,
        "FILE_SIZE_BYTES": int(target_file_size if target_file_size is not None
                               else DEFAULT_TARGET_FILE_SIZE),
        "DATA_PAGE_SIZE_LIMIT": DATA_PAGE_SIZE_LIMIT,
        # A row group never holds more distinct values than rows; DuckDB preallocates from the limit.
        "DICTIONARY_SIZE_LIMIT": min(DICTIONARY_SIZE_LIMIT, rg),
        "PARQUET_VERSION": "V1",
        "WRITE_BLOOM_FILTER": "false",
    }


# --- stats ---------------------------------------------------------------------------------------
_INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"}
_FLOAT_TYPES = {"FLOAT", "DOUBLE"}


class _Raw(str):
    """A JSON number emitted verbatim (decimals: exact digits, no float round trip)."""


def _stat_value(duck_type: str, s: str):
    """One footer stat string as its Delta JSON value, or None when it can't be spelled exactly."""
    t = duck_type.upper()
    try:
        if t in _INT_TYPES:
            return int(s)
        if t in _FLOAT_TYPES:
            v = float(s)
            return v if math.isfinite(v) else None
        if t.startswith("DECIMAL"):
            d = Decimal(s)
            return _Raw(format(d, "f")) if d.is_finite() else None
        if t == "DATE":
            return _dt.date.fromisoformat(s).isoformat()
        if t == "TIMESTAMP WITH TIME ZONE":
            v = _dt.datetime.fromisoformat(s.replace(" ", "T"))
            if v.tzinfo is None:
                return None
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
            return v.isoformat(timespec="microseconds") + "Z"
        if t == "TIMESTAMP":
            return _dt.datetime.fromisoformat(s.replace(" ", "T")).isoformat(timespec="microseconds")
        if t == "VARCHAR":
            return s
    except (ValueError, InvalidOperation, OverflowError):
        return None
    return None  # BOOLEAN, BLOB, nested, time, interval, … — no min/max stat


def _truncated_max(s: str) -> Optional[str]:
    """An upper bound for every string ``s`` is a prefix of, :data:`STATS_TRUNCATE_LENGTH` chars
    long: the prefix with its last char bumped by one code point (what delta-rs writes). A missing
    max is legal Delta, but deltalake's Arrow filter path prunes such a file (it finds nothing), so
    the bound is kept. None when no char can be bumped."""
    head = s[:STATS_TRUNCATE_LENGTH]
    for i in range(len(head) - 1, -1, -1):
        cp = ord(head[i]) + 1
        if 0xD800 <= cp <= 0xDFFF:
            cp = 0xE000
        if cp <= 0x10FFFF:
            return head[:i] + chr(cp)
    return None


def _json(v) -> str:
    if isinstance(v, _Raw):
        return str(v)
    return json.dumps(v, ensure_ascii=False, allow_nan=False)


def _obj(d: Dict[str, object]) -> str:
    return "{" + ",".join(f"{json.dumps(k, ensure_ascii=False)}:{_json(v)}" for k, v in d.items()) + "}"


def _stat_key(raw_key: str) -> Optional[str]:
    """``'"col"'`` → ``col``; a nested path (``'"s"."a"'``, list elements) → None (top level only)."""
    k = raw_key
    if not (len(k) >= 2 and k[0] == '"' and k[-1] == '"'):
        return None
    inner = k[1:-1]
    if inner.replace('""', "").count('"'):
        return None
    return inner.replace('""', '"')


def _is_nested(duck_type: str) -> bool:
    t = str(duck_type).strip().upper()
    return t.startswith(("STRUCT", "MAP", "UNION")) or t.endswith("]")


def stats_json(num_records: int, column_statistics: Optional[Dict[str, Dict[str, str]]],
               col_types: Dict[str, str], indexed: Optional[Sequence[str]] = None) -> str:
    """Delta ``stats`` JSON for one file from DuckDB's ``RETURN_STATS`` ``column_statistics``. Each
    rule picks "no stat" over a wrong one (a wrong stat silently prunes live rows; a missing one only
    costs pruning): a non-exact max is dropped, a long string min is cut and such a max dropped,
    NaN / infinities / unparseable values are dropped, nested columns get none."""
    mins: Dict[str, object] = {}
    maxs: Dict[str, object] = {}
    nulls: Dict[str, int] = {}
    allowed = None if indexed is None else set(indexed)
    for raw_key, st in (column_statistics or {}).items():
        col = _stat_key(raw_key)
        if col is None or col not in col_types or (allowed is not None and col not in allowed):
            continue
        t = col_types[col]
        if _is_nested(t):
            continue
        if "null_count" in st:
            try:
                nulls[col] = int(st["null_count"])
            except ValueError:
                pass
        if str(st.get("has_nan", "false")).lower() == "true":
            continue
        if "min" in st:
            v = _stat_value(t, st["min"])
            if isinstance(v, str) and t.upper() == "VARCHAR" and len(v) > STATS_TRUNCATE_LENGTH:
                v = v[:STATS_TRUNCATE_LENGTH]
            if v is not None:
                mins[col] = v
        if "max" in st and str(st.get("max_is_exact", "true")).lower() == "true":
            v = _stat_value(t, st["max"])
            if isinstance(v, str) and t.upper() == "VARCHAR" and len(v) > STATS_TRUNCATE_LENGTH:
                v = _truncated_max(v)
            if v is not None:
                maxs[col] = v
    return ("{" + f'"numRecords":{int(num_records)},"minValues":{_obj(mins)},'
            f'"maxValues":{_obj(maxs)},"nullCount":{_obj(nulls)}' + "}")


def indexed_columns(columns: Sequence[str],
                    configuration: Optional[Dict[str, Optional[str]]]) -> Optional[List[str]]:
    """The columns that get stats, per ``delta.dataSkippingStatsColumns`` (an explicit list wins) or
    ``delta.dataSkippingNumIndexedCols`` (first N, default 32; -1 = all). None means all."""
    cfg = configuration or {}
    explicit = cfg.get("delta.dataSkippingStatsColumns")
    if explicit:
        wanted = {c.strip().strip("`").lower() for c in explicit.split(",") if c.strip()}
        return [c for c in columns if c.lower() in wanted]
    try:
        n = int(cfg.get("delta.dataSkippingNumIndexedCols") or DEFAULT_NUM_INDEXED_COLS)
    except ValueError:
        n = DEFAULT_NUM_INDEXED_COLS
    return None if n < 0 else list(columns)[:n]


# --- write ---------------------------------------------------------------------------------------
def _rel_path(filename: str, loc: str) -> str:
    """``filename`` (as COPY reports it) relative to the table root ``loc`` — the Delta add path."""
    f = filename.replace("\\", "/")
    root = loc.replace("\\", "/").rstrip("/") + "/"
    if f.startswith(root):
        return f[len(root):]
    try:
        rel = os.path.relpath(f, loc).replace("\\", "/")
    except ValueError:
        rel = None
    if rel is None or rel == ".." or rel.startswith("../"):
        raise RuntimeError(f"duckrun: COPY wrote {filename!r} outside the table root {loc!r}")
    return rel


def _partition_value(duck_type: str, value):
    """A partition value as Delta serializes it (None for a NULL partition). DuckDB reports a
    TIMESTAMPTZ value with its offset ('2024-01-01 10:30:00+00'), which Delta readers refuse; Delta
    wants the UTC wall clock with microseconds, and the same spelling for a naive TIMESTAMP."""
    if value is None or value in ("NULL", "__HIVE_DEFAULT_PARTITION__"):
        return None
    t = str(duck_type).upper()
    if t not in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP"):
        return str(value)
    try:
        v = _dt.datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return str(value)
    if v.tzinfo is not None:
        v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return v.isoformat(sep=" ", timespec="microseconds")


def _remove_quietly(filename: str) -> None:
    """Delete an orphan file DuckDB wrote (an empty input still produces a 0-row parquet). Best
    effort: an unreferenced file is harmless — vacuum collects it."""
    try:
        if "://" not in filename:
            os.remove(filename)
    except OSError:
        pass


def write_files(cur, table: str, loc: str, *, order_by: Optional[str] = None,
                partition_by: Optional[Sequence[str]] = None,
                row_group_rows: Optional[int] = None, target_file_size: Optional[int] = None,
                configuration: Optional[Dict[str, Optional[str]]] = None,
                mapping: Optional[Dict[str, tuple]] = None,
                not_null: Sequence[str] = (), operation: str = "WRITE",
                data_change: bool = True) -> List[AddAction]:
    """Write DuckDB table ``table`` under ``loc`` with ONE ``COPY`` in the fixed read layout and
    return one ``AddAction`` per non-empty file. ``partition_by`` columns go into the hive
    directories (DuckDB's ``PARTITION_BY``), not the files, as Delta expects. Nothing is committed
    here: until :func:`commit` lands them the files are invisible to readers, and an aborted write
    leaves only unreferenced files that vacuum collects. ``mapping`` (see :func:`column_mapping`)
    writes a column-mapped table's columns under their physical names and field ids, with the stats
    keyed by those names.

    ``order_by`` (the table's sort key) orders the rows; without one, rows carrying their scan
    position (:data:`FILE_COL` / :data:`ROW_COL`) keep it, so a rewrite does not scramble the
    order the table was written in (rows without one, a MERGE's inserts, go last). Those position
    columns are never written."""
    partition_by = list(partition_by or [])
    desc = cur.execute(f"DESCRIBE {table}").fetchall()
    hidden = [r[0] for r in desc if r[0] in _HIDDEN]
    logical = {r[0]: str(r[1]) for r in desc if r[0] not in _HIDDEN}
    if not order_by and len(hidden) == len(_HIDDEN):
        order_by = ", ".join(_q(c) for c in _HIDDEN)
    indexed = indexed_columns([c for c in logical if c not in partition_by], configuration)
    opts = parquet_options(row_group_rows, target_file_size)
    select = f"* EXCLUDE ({', '.join(_q(c) for c in hidden)})" if hidden else "*"
    col_types = logical
    if mapping:
        select = ", ".join(f"{_q(c)} AS {_q(mapping[c][0])}" for c in logical)
        col_types = {mapping[c][0]: t for c, t in logical.items()}
        if indexed is not None:
            indexed = [mapping[c][0] for c in indexed]
        ids = [(phys, fid) for phys, fid in mapping.values() if fid is not None]
        if ids:
            opts["FIELD_IDS"] = "{" + ", ".join(f"{_lit(p)}: {fid}" for p, fid in ids) + "}"
    if partition_by:
        opts["PARTITION_BY"] = "(" + ", ".join(_q(p) for p in partition_by) + ")"
    opt_sql = ", ".join(f"{k} {v}" for k, v in opts.items())
    tag = uuid.uuid4().hex[:8]
    if "://" not in loc:  # local table root: COPY creates the target dir but not its parents
        os.makedirs(loc, exist_ok=True)
    order = f" ORDER BY {order_by}" if order_by else ""
    rows = cur.execute(
        f"COPY (SELECT {select} FROM {table}{order}) TO {_lit(loc.rstrip('/'))} "
        f"(FORMAT parquet, {opt_sql}, APPEND, FILENAME_PATTERN 'part-{tag}-{{uuid}}', RETURN_STATS)"
    ).fetchall()
    if not_null:
        # NOT NULL from the COPY's own statistics, so the data is never read twice: a file with a
        # NULL in such a column fails the write before anything is committed (the files it wrote
        # are unreferenced; vacuum collects them).
        keys = {('"' + (mapping[c][0] if mapping else c).replace('"', '""') + '"'): c
                for c in not_null}
        for filename, count, _size, _footer, colstats, *_ in rows:
            for key, st in (colstats or {}).items():
                if key in keys and int((st or {}).get("null_count") or 0):
                    raise ValueError(f"{operation} would write {st['null_count']} NULL(s) into "
                                     f"NOT NULL column {keys[key]!r}")
    now_ms = int(time.time() * 1000)
    adds: List[AddAction] = []
    for filename, count, size, _footer, colstats, *rest in rows:
        if not count:
            _remove_quietly(filename)
            continue
        pkeys = (rest[0] if rest else None) or {}
        pvals = {p: _partition_value(col_types.get(p, ""), pkeys.get(p)) for p in partition_by}
        adds.append(AddAction(path=_rel_path(filename, loc), size=int(size), partition_values=pvals,
                              modification_time=now_ms, data_change=data_change,
                              stats=stats_json(count, colstats, col_types, indexed)))
    return adds


def removes_for(cur, dt: DeltaTable, filenames: Sequence[str],
                data_change: bool = True) -> List[RemoveAction]:
    """``RemoveAction`` for each live data file named in ``filenames`` (full paths, as
    ``delta_scan(..., filename = true)`` reports them), matched on the whole path under the table
    root — not the file name alone: Spark writes the same file name into every partition folder. A
    compaction passes ``data_change=False``.

    The log stores a path URI-encoded, and ``get_add_actions`` returns it that way; the commit API
    encodes the path it is given. So the remove gets the DECODED path — the file's real relative
    path — or a partition value with a character the encoding touches (a space, ``%``, ``:``, so
    every timestamp partition) is encoded twice, matches no file, and the file stays live."""
    from urllib.parse import unquote

    by_name: Dict[str, list] = {}
    add_actions = dt.get_add_actions(flatten=False)  # noqa: F841 - read below by DuckDB's replacement scan
    for rel, size in cur.sql("SELECT path, size_bytes FROM add_actions").fetchall():
        real = unquote(rel)
        by_name.setdefault(real.rsplit("/", 1)[-1], []).append((real, int(size)))
    now_ms = int(time.time() * 1000)
    out, missing = [], 0
    for f in sorted({f.replace("\\", "/") for f in filenames}):
        hits = [(real, size) for real, size in by_name.get(f.rsplit("/", 1)[-1], [])
                if f == real or f.endswith("/" + real)]
        if len(hits) != 1:
            missing += 1
            continue
        real, size = hits[0]
        out.append(RemoveAction(path=real, data_change=data_change, deletion_timestamp=now_ms,
                                size=size, partition_values=None))
    if missing:
        raise RuntimeError(f"duckrun: {missing} rewritten file(s) are not live in version "
                           f"{dt.version()}; refusing to commit")
    return out


def check_not_null(cur, table: str, dt: DeltaTable, operation: str,
                   columns: Optional[Sequence[str]] = None) -> None:
    """Fail before anything is written when the rewrite would put a NULL in a NOT NULL column —
    the check delta-rs's writer applied. ``columns`` limits it to the columns the statement can
    change (None = all)."""
    only = None if columns is None else {c.lower() for c in columns}
    names = [f.name for f in dt.schema().fields
             if not f.nullable and (only is None or f.name.lower() in only)]
    if not names:
        return
    for name, n in zip(names, _counts(cur, table, [f"{_q(c)} IS NULL" for c in names])):
        if n:
            raise ValueError(f"{operation} would write {n} NULL(s) into NOT NULL column {name!r}")


def commit(dt: DeltaTable, path: str, actions: list, *, read_version: int, operation: str) -> None:
    """Commit ``actions`` as ONE transaction pinned to ``read_version`` (``dt`` must be loaded at
    it) with no rebase: a commit that landed since then fails this one loudly."""
    props = CommitProperties(max_commit_retries=0, custom_metadata={
        "duckrun.readVersion": str(read_version), "duckrun.operation": operation})
    try:
        dt.create_write_transaction(actions, mode="append", schema=dt.schema(),
                                    commit_properties=props)
    except CommitFailedError as e:
        raise CommitFailedError(
            f"{operation.lower()}: table '{path}' changed since version {read_version} "
            f"(a concurrent write committed); {operation.lower()} refused. Re-read and retry."
        ) from e
