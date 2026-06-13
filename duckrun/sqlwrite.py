"""Route SQL write statements in ``conn.sql()`` to Delta instead of DuckDB-native objects.

duckrun registers Delta tables as read-only ``delta_scan`` views, so a bare DuckDB
``CREATE TABLE … AS`` would make an ephemeral DuckDB table and ``INSERT``/``UPDATE``/``DELETE``
would fail against a view. This module classifies a statement and, for the four write kinds,
re-runs it as a *lazy* DuckDB relation streamed into the same ``engine`` write path the dbt
adapter and ``DataFrameWriter.saveAsTable`` already use — exactly as Spark shares one writer
between ``spark.sql`` and ``df.write``. There is no second write path.

The guiding trick: never hand-parse the SELECT body / VALUES. We extract only the *target name*
(and, for INSERT, the optional column list), let DuckDB parse the body, and hand ``write_delta``
a lazy relation so the inserted rows stream through Arrow once (no temp-table double-copy).

Limitations: single statement only, first token must be the write keyword (``WITH … INSERT``,
``INSERT OR REPLACE`` and multi-statement strings pass through and then error on the view); CTAS
takes no partition/options clause (use ``DataFrameWriter`` for that); DELETE/UPDATE are not
concurrency-safe compare-and-swap operations — the dbt adapter stays the path for safe-concurrent
incremental work. UPDATE assignment expressions and DELETE/UPDATE predicates are
delta-rs/datafusion SQL.
"""
import re
from typing import List, Optional, Tuple

from dbt.adapters.duckrun import engine

# A possibly-quoted, possibly-schema-qualified table name token.
_NAME = r'(?:"[^"]+"|[A-Za-z_][\w$]*)(?:\.(?:"[^"]+"|[A-Za-z_][\w$]*))?'

_CTAS_RE = re.compile(
    rf'^CREATE\s+(?P<orreplace>OR\s+REPLACE\s+)?TABLE\s+(?P<ifne>IF\s+NOT\s+EXISTS\s+)?'
    rf'(?P<name>{_NAME})\s*(?:\(\s*(?P<cols>[^()]*?)\s*\)\s*)?AS\s+(?P<body>.+)$',
    re.IGNORECASE | re.DOTALL,
)
_INSERT_RE = re.compile(
    rf'^INSERT\s+INTO\s+(?P<name>{_NAME})\s*'
    rf'(?:\(\s*(?P<cols>(?:"[^"]+"|[\w$]+)(?:\s*,\s*(?:"[^"]+"|[\w$]+))*)\s*\)\s*)?'
    rf'(?P<body>(?:SELECT|WITH|VALUES|FROM|TABLE)\b.+)$',
    re.IGNORECASE | re.DOTALL,
)
_DELETE_RE = re.compile(rf'^DELETE\s+FROM\s+(?P<name>{_NAME})\s*(?P<rest>.*)$',
                        re.IGNORECASE | re.DOTALL)
_UPDATE_RE = re.compile(rf'^UPDATE\s+(?P<name>{_NAME})\s+SET\s+(?P<rest>.+)$',
                        re.IGNORECASE | re.DOTALL)


def _strip_leading(query: str) -> str:
    """Drop leading whitespace and ``--`` / ``/* */`` comments so classification sees the keyword."""
    s = query
    while True:
        t = s.lstrip()
        if t.startswith("--"):
            nl = t.find("\n")
            s = "" if nl == -1 else t[nl + 1:]
        elif t.startswith("/*"):
            end = t.find("*/")
            s = "" if end == -1 else t[end + 2:]
        else:
            return t


def classify(query: str) -> Optional[str]:
    """Return ``"ctas" | "insert" | "delete" | "update"`` for a routable Delta write, else None
    (the statement passes straight through to DuckDB unchanged)."""
    s = _strip_leading(query)
    # CREATE TEMP/TEMPORARY TABLE and CREATE VIEW/SCHEMA are DuckDB-local — never routed.
    if re.match(r'CREATE\s+(OR\s+REPLACE\s+)?(TEMP|TEMPORARY)\b', s, re.IGNORECASE):
        return None
    if _CTAS_RE.match(s):
        return "ctas"
    if re.match(r'INSERT\s+INTO\b', s, re.IGNORECASE) and _INSERT_RE.match(s):
        return "insert"
    if re.match(r'DELETE\s+FROM\b', s, re.IGNORECASE):
        return "delete"
    if _UPDATE_RE.match(s):
        return "update"
    return None


def execute(session, kind: str, query: str):
    """Run a classified write statement against Delta and return an empty DataFrame (Spark parity:
    ``spark.sql`` returns an empty result for write statements)."""
    s = _strip_leading(query)
    {"ctas": _ctas, "insert": _insert, "delete": _delete, "update": _update}[kind](session, s)
    from .session import DataFrame  # local import avoids a session<->sqlwrite import cycle
    return DataFrame(session.con.sql("SELECT 1 WHERE false"), session)


# ---- per-kind handlers ---------------------------------------------------------------------

def _resolve(session, raw_name: str) -> Tuple[str, str, str]:
    """(schema, table, path) for a matched name token (simple, unquoted identifiers)."""
    schema, table = session.resolve(raw_name.replace('"', ""))
    return schema, table, session.table_path(schema, table)


def _finish(session, schema: str, table: str) -> None:
    """Surface the written table as a delta_scan view — the same step saveAsTable runs."""
    from .session import _qid
    session.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(schema)}")
    session._register_view(schema, table)
    session._set_search_path(session._current_database)


def _ctas(session, s: str) -> None:
    m = _CTAS_RE.match(s)
    if not m:
        raise ValueError(f"could not parse CREATE TABLE AS: {s!r}")
    schema, table, path = _resolve(session, m.group("name"))
    so, ct = session.storage_options, session.compaction_threshold

    body = m.group("body").rstrip().rstrip(";")
    if m.group("cols"):  # CREATE TABLE t(a, b) AS … → rename the body's columns positionally
        body = f"SELECT * FROM ({body}) _ctas({m.group('cols')})"
    rel = session.con.sql(body)

    if m.group("orreplace"):
        mode = "overwrite"
    elif m.group("ifne"):
        mode = "ignore"
    elif engine.table_exists(path, so):
        raise ValueError(
            f"table '{schema}.{table}' already exists. Use CREATE OR REPLACE TABLE, "
            f"CREATE TABLE IF NOT EXISTS, or DROP it first."
        )
    else:
        mode = "overwrite"

    # CTAS defines the table from the query, so a replace swaps the schema wholesale (like
    # CREATE OR REPLACE). Harmless on a brand-new table; ignored by write_delta for mode=ignore.
    engine.write_delta(path, rel, mode, overwrite_schema=(mode == "overwrite"),
                       storage_options=so, compaction_threshold=ct)
    _finish(session, schema, table)


def _insert(session, s: str) -> None:
    from .session import _qid, _qlit
    m = _INSERT_RE.match(s)
    if not m:
        raise ValueError(f"could not parse INSERT INTO: {s!r}")
    schema, table, path = _resolve(session, m.group("name"))
    so, ct = session.storage_options, session.compaction_threshold

    if not engine.table_exists(path, so):
        raise ValueError(f"table '{schema}.{table}' does not exist; create it before INSERT.")

    body = m.group("body").rstrip().rstrip(";")
    src = session.con.sql(body)
    scols = src.columns

    # Target schema (column order + types) — metadata only, no data scan.
    tgt = session.con.sql(f"SELECT * FROM delta_scan('{_qlit(path)}') LIMIT 0")
    tcols, ttypes = tgt.columns, [str(t) for t in tgt.types]

    if m.group("cols"):
        targets = [c.strip().strip('"') for c in m.group("cols").split(",")]
        if len(targets) != len(scols):
            raise ValueError(
                f"INSERT column list has {len(targets)} columns but the query produces {len(scols)}."
            )
        src_for = {t.lower(): scols[i] for i, t in enumerate(targets)}
    else:
        if len(scols) != len(tcols):
            raise ValueError(
                f"INSERT produces {len(scols)} columns but '{schema}.{table}' has {len(tcols)}; "
                f"list the target columns explicitly to insert a subset."
            )
        src_for = {tcols[i].lower(): scols[i] for i in range(len(tcols))}

    # Lazy projection: cast/reorder the body's columns into the table's physical schema. Missing
    # columns become NULL (the column must be nullable). Stays a relation → streams to delta-rs.
    exprs: List[str] = []
    for tcol, ttype in zip(tcols, ttypes):
        sc = src_for.get(tcol.lower())
        if sc is None:
            exprs.append(f"CAST(NULL AS {ttype}) AS {_qid(tcol)}")
        else:
            exprs.append(f"CAST(_src.{_qid(sc)} AS {ttype}) AS {_qid(tcol)}")
    projection = session.con.sql(f"SELECT {', '.join(exprs)} FROM ({body}) AS _src")

    engine.write_delta(path, projection, "append", storage_options=so, compaction_threshold=ct)
    _finish(session, schema, table)


def _delete(session, s: str) -> None:
    m = _DELETE_RE.match(s)
    if not m:
        raise ValueError(f"could not parse DELETE FROM: {s!r}")
    schema, table, path = _resolve(session, m.group("name"))
    rest = m.group("rest").strip().rstrip(";").strip()

    predicate = None
    if rest:
        mw = re.match(r'WHERE\s+(?P<pred>.+)$', rest, re.IGNORECASE | re.DOTALL)
        if not mw:
            raise ValueError(f"unsupported DELETE clause: {rest!r} (only WHERE is supported)")
        predicate = mw.group("pred").strip()

    engine.delete_rows(path, predicate, storage_options=session.storage_options,
                       compaction_threshold=session.compaction_threshold)
    _finish(session, schema, table)


def _update(session, s: str) -> None:
    m = _UPDATE_RE.match(s)
    if not m:
        raise ValueError(f"could not parse UPDATE: {s!r}")
    schema, table, path = _resolve(session, m.group("name"))

    rest = m.group("rest").strip().rstrip(";").strip()
    set_str, predicate = _split_top_where(rest)
    updates = {}
    for assign in _split_top(set_str, ","):
        eq = _index_top(assign, "=")
        if eq == -1:
            raise ValueError(f"unsupported UPDATE assignment (no '='): {assign!r}")
        col = assign[:eq].strip().strip('"')
        updates[col] = assign[eq + 1:].strip()
    if not updates:
        raise ValueError("UPDATE has no SET assignments.")

    engine.update_rows(path, updates, predicate, storage_options=session.storage_options,
                       compaction_threshold=session.compaction_threshold)
    _finish(session, schema, table)


# ---- top-level (paren/quote-aware) scanning for UPDATE SET / WHERE -------------------------

def _scan(s: str):
    """Yield (index, char, depth, in_quote) walking ``s`` while tracking paren depth and quotes."""
    depth, quote = 0, None
    for i, c in enumerate(s):
        if quote:
            if c == quote:
                quote = None
            yield i, c, depth, True
            continue
        if c in ("'", '"'):
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        yield i, c, depth, False


def _split_top(s: str, sep: str) -> List[str]:
    """Split ``s`` on top-level (depth 0, unquoted) occurrences of single char ``sep``."""
    out, last = [], 0
    for i, c, depth, q in _scan(s):
        if c == sep and depth == 0 and not q:
            out.append(s[last:i])
            last = i + 1
    out.append(s[last:])
    return out


def _index_top(s: str, ch: str) -> int:
    """Index of the first top-level occurrence of ``ch``, or -1."""
    for i, c, depth, q in _scan(s):
        if c == ch and depth == 0 and not q:
            return i
    return -1


def _split_top_where(s: str) -> Tuple[str, Optional[str]]:
    """Split ``<assignments> [WHERE <predicate>]`` on the first top-level WHERE keyword."""
    low = s.lower()
    for m in re.finditer(r'\bwhere\b', low):
        i = m.start()
        # is this WHERE at top level (paren depth 0, outside quotes)?
        depth, quote, top = 0, None, True
        for j in range(i):
            c = s[j]
            if quote:
                if c == quote:
                    quote = None
            elif c in ("'", '"'):
                quote = c
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
        if depth == 0 and quote is None:
            return s[:i].strip(), s[m.end():].strip()
    return s.strip(), None
