"""Route raw SQL DML against duckrun-managed (Delta-backed) relations to delta_rs.

duckrun intercepts writes at the dbt *materialization* layer (a model/seed/snapshot goes through
the materialization macros -> store_relation -> delta_rs). But a duckrun relation is surfaced as a
read-only ``delta_scan`` view, so *raw* DML sent straight to the connection — ``delete from``,
``update``, ``insert into ... select``, ``alter table ... add column``, ``create table ... as
select`` — lands on a view and fails ("Can only delete from base table"), or would create a native
DuckDB table that bypasses Delta entirely.

This module intercepts those statements (at the cursor, see environment.DuckrunCursorWrapper) and
applies them to the Delta table **via delta_rs only**, then refreshes the ``delta_scan`` view — so
nothing relies on a native, mutable DuckDB table, and every op works on local AND abfss/OneLake
stores (delta_rs carries ``storage_options``). ``create table ... as`` writes a new Delta table;
the mutate forms (delete/update/insert/alter) apply only when a Delta table already exists at the
target (otherwise the statement passes through — e.g. the test's native ``fact``/``seed``).

``drop table`` unregisters the ``delta_scan`` view AND overwrites the table (via delta_rs) to a
one-column ``TOMBSTONE_COLUMN`` marker, which discovery recognizes and hides. It does NOT delete
data: delta_rs has no drop, and removing the Delta files would be a filesystem hack that fails on
object stores. The directory persists until a human purges it; a later ``create table ... as``
overwrites the tombstone with real data and the table is live again.

The seed loader's own SQL (``create table <t> (<col defs>)``, ``insert ... values``, ``COPY``) lands
on a native DuckDB table, not a Delta table: bare ``create table (<col defs>)`` becomes a Delta
table only when a ``default_schema`` is set (the connection API), and the dbt/cursor path passes
None — so the seed's table stays native. ``insert ... values`` *does* match a form here, but the
mutate guard only applies it when a Delta table already exists at the target — the seed's native
table has none, so it falls through untouched. duckrun's own materializations emit ``create ...
view`` (not ``table``), so they pass through too.

Supported / unsupported (what reaches delta_rs):

    create [or replace] table x [if not exists] as <query>   Delta CTAS (query: select/with/(…)); a
                                                              plain create errors if x is live, `or
                                                              replace` overwrites, `if not exists` no-ops
    create [or replace] table x [if not exists] (<col defs>)  empty Delta table (connection API only);
                                                              logs a CREATE TABLE op, same exists rules
    create temp/temporary table …                            native DuckDB (pass through)  ── invariant:
    create view …                                            native DuckDB (pass through)  ── only TEMP
                                                                                            and VIEW are
                                                                                            native; every
                                                                                            other CREATE
                                                                                            TABLE is Delta
    insert into x [(cols)] select …                          Delta append (projected onto target schema)
    insert into x [(cols)] values …                          Delta append (projected onto target schema)
    [with …] insert into x select …                          Delta append (CTE re-attached to the body)
    delete from x [where …]                                  delta_rs delete
    update x set … [where …]                                 delta_rs update
    alter table x add column …                               Delta overwrite (widen schema)
    drop table x                                             tombstone (no data deleted)
    merge into x [a] using s [b] on a.k = b.k when …         delta_rs MERGE (engine.merge_delta_clauses)
                                                             — the FULL delta-rs TableMerger surface:
                                                             WHEN MATCHED [AND p] THEN UPDATE SET * /
                                                             SET col=<expr> / DELETE; WHEN NOT MATCHED
                                                             [AND p] THEN INSERT * / INSERT (cols)
                                                             VALUES (<exprs>); WHEN NOT MATCHED BY
                                                             SOURCE [AND p] THEN UPDATE SET … / DELETE.
                                                             Any number of clauses, applied in order;
                                                             the ON predicate may be any boolean
                                                             (multi-key/range/non-equi). ON/WHEN may
                                                             use your own aliases or the table/relation
                                                             names (normalized to the target/source
                                                             aliases delta_rs uses). Same boundary as
                                                             DeltaTable.merge.
    update … from / delete … using / multi-stmt              NOT handled here — the connection API
                                                             (session.sql) rejects them with a clear
                                                             error; the dbt path never emits them.
"""
import re
from typing import List, Optional, Tuple

from dbt.adapters.events.logging import AdapterLogger

from . import engine

logger = AdapterLogger("Duckrun")

# `drop table` tombstone: a dropped relation is overwritten (via delta_rs) to a table whose ONLY
# column is this marker, so (a) discovery recognizes it as dropped and hides it, and (b) anyone who
# opens the files sees an obviously-not-a-real-table schema rather than a plausible empty table. No
# data is deleted — the directory stays until a human purges it; a later `create table ... as`
# overwrites the marker schema with real data and the table is live again.
TOMBSTONE_COLUMN = "__duckrun_deleted__"


def _columns_are_tombstone(colnames) -> bool:
    return [str(c).lower() for c in colnames] == [TOMBSTONE_COLUMN]


def is_dropped(con, location: str, storage_options=None) -> bool:
    """True if the Delta table at ``location`` is a duckrun drop-tombstone (single marker column).

    Used by discovery (dbt + connection API) to hide dropped tables. Best-effort: anything that
    can't be opened/scanned is treated as 'not a tombstone' (let normal handling deal with it).
    """
    loc_sql = str(location).replace("'", "''")
    try:
        rel = con.execute(f"select * from delta_scan('{loc_sql}') limit 0")
        return _columns_are_tombstone([d[0] for d in rel.description])
    except Exception as exc:  # best-effort: treat unreadable as 'not a tombstone', but leave a trace
        logger.debug(f"duckrun: is_dropped could not scan {location!r}, treating as live: {exc}")
        return False

# --- statement matchers (leading-anchored, DOTALL so multi-line bodies match) ----------------
# `create [or replace] table [if not exists] <rel> as <query>`. The body is ANY query text (a bare
# `select …`, a `with … select …` CTE, or a parenthesised `(select …)`); it's handed to DuckDB
# verbatim so anything DuckDB accepts after `as` works.
_CREATE_AS = re.compile(
    r"\s*create\s+(?P<orrep>or\s+replace\s+)?table\s+(?P<ine>if\s+not\s+exists\s+)?"
    r"(?P<rel>.+?)\s+as\s+(?P<body>.+)",
    re.I | re.S,
)
# `create [or replace] table [if not exists] <rel> (<col defs>)` — no `as`. Connection-API only
# (see _create_coldefs): materializes an EMPTY Delta table so `CREATE TABLE` is always Delta-backed.
_CREATE_COLDEFS = re.compile(
    r"\s*create\s+(?P<orrep>or\s+replace\s+)?table\s+(?P<ine>if\s+not\s+exists\s+)?"
    r"(?P<rel>.+?)\s*\((?P<defs>.+)\)\s*;?\s*",
    re.I | re.S,
)
_INSERT_INTO_RE = re.compile(r"\s*insert\s+into\b", re.I)
_INSERT_SELECT = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)\s*(?:\((?P<cols>[^)]*)\))?\s+(?P<body>select\b.*)",
    re.I | re.S,
)
_INSERT_VALUES = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)\s*(?:\((?P<cols>[^)]*)\))?\s*values\s+(?P<body>\(.+)",
    re.I | re.S,
)
# INSERT is VALUES-or-SELECT, decided by the first TOP-LEVEL keyword (outside quotes/parens/CASE,
# via _find_top_level) — NOT by racing the two regexes above. _INSERT_SELECT's body (`select\b.*`,
# DOTALL) otherwise matches a `select` buried in a string literal in the VALUES payload (e.g.
# Elementary's run-results upload, whose `compiled_code` column value literally contains
# `\n    select * from (…)`), mis-parsing an INSERT…VALUES as INSERT…SELECT — `rel` captures the
# whole statement, _resolve fails, and the write wrongly falls through to the read-only delta_scan
# view ("… is not a table"). The column list is at paren-depth 1 so it's skipped; the real
# VALUES/SELECT keyword is the first one at depth 0.
_VALUES_KW = re.compile(r"\bvalues\b", re.I)
_SELECT_KW = re.compile(r"\bselect\b", re.I)
_DELETE = re.compile(
    r"\s*delete\s+from\s+(?P<rel>.+?)(?:\s+where\s+(?P<where>.+))?\s*;?\s*", re.I | re.S
)
# A predicate that references ANOTHER table via subquery (`… in (select … from other)`). delta_rs's
# delete()/update() predicate engine (datafusion) raises a Rust `not implemented` panic on these
# (delta_datafusion/expr.rs), so duckrun can't hand them to dt.delete(); see _delete for the DuckDB-
# evaluated filtered-overwrite fallback. Plain column-vs-constant/expr predicates don't match.
_PREDICATE_SUBQUERY = re.compile(r"\(\s*select\b", re.I)
_UPDATE = re.compile(
    r"\s*update\s+(?P<rel>.+?)\s+set\s+(?P<set>.+?)(?:\s+where\s+(?P<where>.+?))?\s*;?\s*",
    re.I | re.S,
)
_ALTER_ADD = re.compile(
    r"\s*alter\s+table\s+(?P<rel>.+?)\s+add\s+column\s+(?P<col>\S+)\s+(?P<def>.+?)\s*;?\s*",
    re.I | re.S,
)
_DROP = re.compile(
    r"\s*drop\s+table\s+(?:if\s+exists\s+)?(?P<rel>[^\s;]+)\s*;?\s*", re.I | re.S
)
# `merge into <target> [[as] alias] using <source> ... on ... when ...`. The regex only captures the
# target relation (a dotted/quoted identifier) and hands the remainder (`rest`) to _merge, which uses
# _find_top_level/_find_all_top_level to split USING/ON/WHEN at paren-depth 0 — so those keywords
# nested inside the source subquery or a clause predicate aren't mistaken for the structural ones.
_MERGE = re.compile(
    r"\s*merge\s+into\s+(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\b(?P<rest>.+)",
    re.I | re.S,
)
# Structural MERGE keywords, located at top level by _find_top_level/_find_all_top_level.
_M_USING = re.compile(r"\busing\b", re.I)
_M_ON = re.compile(r"\bon\b", re.I)
_M_WHEN = re.compile(r"\bwhen\b", re.I)
# One WHEN clause, split on its top-level THEN (see _split_when_clause). _M_KIND parses the part
# BEFORE that THEN: kind (most specific first) + optional `AND <pred>`. The THEN boundary is found
# quote/paren-aware (_M_THEN via _find_top_level) so a string literal containing the word `then`
# (e.g. `s.note <> 'x then y'`) doesn't cut the clause in the wrong place.
_M_KIND = re.compile(
    r"\s*when\s+(?P<kind>not\s+matched\s+by\s+source|not\s+matched|matched)\b"
    r"(?:\s+and\s+(?P<pred>.+))?\s*;?\s*",
    re.I | re.S,
)
_M_THEN = re.compile(r"\bthen\b", re.I)
_M_UPDATE_ALL = re.compile(r"\s*update\s+set\s+\*\s*", re.I)
_M_UPDATE_SET = re.compile(r"\s*update\s+set\s+(?P<assign>.+)", re.I | re.S)
_M_INSERT_ALL = re.compile(r"\s*insert\s+\*\s*", re.I)
# `insert (col, …) values (<expr>, …)` — the column list (no parens inside) and the value list,
# which may contain function calls (commas split at top level by _split_top_level_commas).
_M_INSERT_COLS = re.compile(
    r"\s*insert\s*\((?P<cols>[^)]*)\)\s*values\s*\((?P<vals>.+)\)\s*", re.I | re.S
)
_M_DELETE = re.compile(r"\s*delete\s*", re.I)
# `create temp/temporary table …` is DuckDB-local scratch by design and must NEVER be captured —
# checked first in try_handle so it always passes through to native DuckDB (the invariant: only
# CREATE TEMP TABLE is native; every other CREATE TABLE is Delta-backed).
_CREATE_TEMP_RE = re.compile(r"\s*create\s+(?:or\s+replace\s+)?(?:temp|temporary)\b", re.I)
# CTE/whitespace handling: a leading `with …` block followed by a top-level INSERT/UPDATE/DELETE.
# leading `\b` is load-bearing: _find_top_level tries this at every depth-0 index, so without it the
# verb would match inside an identifier (e.g. `update` within `last_update`).
_LEADING_WITH = re.compile(r"\s*with\b", re.I)
_DRIVING_DML = re.compile(r"\b(?:insert\s+into|update|delete\s+from)\b", re.I)
# DuckDB numeric type names (DECIMAL(p,s) matches on the prefix). Used to scope the lossy-narrowing
# guard to numeric→numeric casts only, leaving the intentional timestamp/string alignment untouched.
_NUMERIC_TYPE_RE = re.compile(
    r"^(?:TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT|"
    r"FLOAT|REAL|DOUBLE|DECIMAL)\b", re.I)
# Approximate floating-point target types. Narrowing a numeric into one of these is INTENDED
# approximation (the column was declared float) — duckdb/dbt-duckdb cast decimal/double -> float
# silently too — not silent corruption, so the lossy-narrowing guard skips them. It still fires for
# EXACT targets (e.g. 3.9 -> INTEGER lands 4). See _reject_lossy_numeric_narrowing.
_APPROX_FLOAT_RE = re.compile(r"^(?:FLOAT|REAL|DOUBLE|FLOAT4|FLOAT8)\b", re.I)

# PostgreSQL/DuckDB dollar-quoting: ``$tag$ … $tag$`` (the tag is optional, so ``$$ … $$`` too). The
# body is OPAQUE — no escaping — so a ``;``, a ``'``/``"`` quote, or a ``--``/``/* */`` marker inside
# it is literal text, NOT a structural token. Every quote/paren-aware scanner here must skip a
# dollar-quoted run wholesale; otherwise dbt's persist_docs `COMMENT ON … IS
# $dbt_comment_literal_block$ …text with "quotes"; and ';'… $dbt_comment_literal_block$` (a comment
# body carrying embedded quotes/semicolons) is mis-split into fragments with an unterminated $-quote.
_DOLLAR_OPEN = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")


def _dollar_quote_end(s: str, i: int) -> Optional[int]:
    """If a dollar-quote opens at ``s[i]`` (``$$`` or ``$tag$``), return the index just past its
    matching close; else None (not a dollar-quote, or no close found — leave it to normal scanning)."""
    m = _DOLLAR_OPEN.match(s, i)
    if not m:
        return None
    delim = m.group(0)
    close = s.find(delim, m.end())
    return close + len(delim) if close != -1 else None


def _strip_leading(query: str) -> str:
    """Drop leading whitespace and ``--`` / ``/* */`` comments so the first keyword is visible."""
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


def _strip_comments(sql: str) -> str:
    """Remove ``--`` line and ``/* */`` block comments that are OUTSIDE string/identifier quotes,
    replacing each with a single space so adjacent tokens don't fuse. Quote-aware so a ``--``/``/*``
    inside a literal (``'a -- b'``) is left intact. Applied to the whole statement before structural
    parsing so an inline comment can't inject a false USING/ON/WHEN/THEN boundary (``_strip_leading``
    only peels comments off the *front*)."""
    out, i, n, quote = [], 0, len(sql), None
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
            i += 1
        elif ch in ("'", '"'):
            quote = ch
            out.append(ch)
            i += 1
        elif ch == "$" and _dollar_quote_end(sql, i) is not None:
            de = _dollar_quote_end(sql, i)
            out.append(sql[i:de])  # opaque dollar-quoted body — keep verbatim, scan nothing inside
            i = de
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i)
            i = n if nl == -1 else nl  # stop before the newline so it's preserved
            out.append(" ")
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


# A ``CASE … END`` expression nests its own ``WHEN``/``THEN`` keywords; treat it as a depth level (like
# parens) so a ``CASE WHEN … THEN …`` inside a MERGE clause action/predicate isn't mistaken for the
# structural MERGE ``WHEN``/``THEN``. Detected at an identifier boundary so ``staircase``/``append`` etc.
# don't trip it (the surrounding scan already skips quoted identifiers and string literals).
_CASE_KW = re.compile(r"case\b", re.I)
_END_KW = re.compile(r"end\b", re.I)


def _top_level(s: str, pattern, find_all: bool):
    """Find ``pattern`` matches at paren-depth 0, outside quotes, and outside any ``CASE … END``
    expression. Returns a list of indices when ``find_all`` else the first index (or -1). Lets us
    tell a structural keyword (the ``FROM`` of ``UPDATE … FROM``, the verb after a leading ``WITH``,
    each MERGE ``WHEN``) from the same word nested in a subquery or a ``CASE`` — without a full parser."""
    out: List[int] = []
    depth = case_depth = 0
    quote, i, n = None, 0, len(s)
    while i < n:
        ch = s[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            continue
        if ch == "$":
            de = _dollar_quote_end(s, i)
            if de is not None:
                i = de  # skip an opaque dollar-quoted run (its keywords aren't structural)
                continue
        if ch in "([":
            depth += 1
            i += 1
            continue
        if ch in ")]":
            depth -= 1
            i += 1
            continue
        at_boundary = not (i and (s[i - 1].isalnum() or s[i - 1] == "_"))
        if at_boundary and _CASE_KW.match(s, i):
            case_depth += 1
            i += 4
            continue
        if at_boundary and _END_KW.match(s, i):
            case_depth = max(0, case_depth - 1)
            i += 3
            continue
        if depth == 0 and case_depth == 0 and pattern.match(s, i):
            if not find_all:
                return i
            out.append(i)
        i += 1
    return out if find_all else -1


def _find_top_level(s: str, pattern) -> int:
    """First index where ``pattern`` matches at the top level (see :func:`_top_level`), else -1."""
    return _top_level(s, pattern, find_all=False)


def _find_all_top_level(s: str, pattern) -> List[int]:
    """Every index where ``pattern`` matches at the top level (see :func:`_top_level`). Used to split
    a MERGE body on each structural ``WHEN``."""
    return _top_level(s, pattern, find_all=True)


def _split_leading_with(sql: str) -> Tuple[str, str]:
    """``(with_clause, remainder)`` for ``WITH … <INSERT/UPDATE/DELETE> …``; ``('', sql)`` otherwise.

    So ``WITH c AS (…) INSERT INTO t SELECT … FROM c`` reaches the matchers (which anchor on the
    verb) and the CTE is preserved when the body is evaluated. A leading ``WITH`` that drives a
    plain ``SELECT`` (a read) is left untouched."""
    if not _LEADING_WITH.match(sql):
        return "", sql
    idx = _find_top_level(sql, _DRIVING_DML)
    if idx <= 0:
        return "", sql
    return sql[:idx].rstrip(), sql[idx:]


def _fullmatch(pattern, sql):
    return pattern.fullmatch(sql.strip())


def _insert_kind(sql: str) -> Optional[str]:
    """``'values'`` or ``'select'`` for an INSERT, by whichever keyword appears first at the TOP
    level (outside quotes/parens/CASE — see :func:`_find_top_level`). A keyword inside a string
    literal in the VALUES payload, inside the column list (paren-depth 1), or inside a nested
    subquery/CTE can't flip the classification. ``None`` when neither is found at the top level."""
    vi = _find_top_level(sql, _VALUES_KW)
    si = _find_top_level(sql, _SELECT_KW)
    if vi == -1 and si == -1:
        return None
    if vi != -1 and (si == -1 or vi < si):
        return "values"
    return "select"


def _split_relation(rel: str) -> Tuple[Optional[str], Optional[str]]:
    """`"db"."schema"."tbl"` / `schema.tbl` / `tbl` -> (schema, identifier), quotes stripped."""
    parts = [p.strip().strip('"') for p in rel.strip().split(".")]
    if not parts or not parts[-1]:
        return None, None
    identifier = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    return schema, identifier


def _lead_alias(text: str) -> Optional[str]:
    """The optional ``[AS] <alias>`` on a MERGE target (the text between the table and USING)."""
    s = re.sub(r"(?i)^as\s+", "", text.strip())
    am = re.match(r'"([^"]+)"|(\w+)', s)
    if not am:
        return None
    return am.group(1) if am.group(1) is not None else am.group(2)


def _source_primary_and_alias(operand: str) -> Tuple[Optional[str], Optional[str]]:
    """``(primary_identifier, alias)`` for a MERGE USING operand ``<primary> [AS] <alias> [(cols)]``.

    ``primary_identifier`` is the unquoted last part of a bare/dotted/quoted relation name (so
    ``USING other ON … = other.id`` resolves), or None when the primary is a ``(subquery)``. The
    trailing alias (after an optional ``AS``, before any column-rename list) is returned when present.
    """
    s = operand.strip()
    if not s:
        return None, None
    primary_ident = None
    if s[0] in "([":  # subquery primary — skip to its matching close, tracking quotes/nesting
        depth, i, n, quote = 0, 0, len(s), None
        while i < n:
            ch = s[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch in "([":
                depth += 1
            elif ch in ")]":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        rest = s[i:].strip()
    else:  # relation-name primary — read the (dotted/quoted) name, the remainder is the alias part
        m = re.match(r'(?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+))*', s)
        primary_ident = _split_relation(m.group(0))[1] if m else None
        rest = s[m.end():].strip() if m else ""
    rest = re.sub(r"(?i)^as\s+", "", rest)
    am = re.match(r'"([^"]+)"|(\w+)', rest)
    alias = None
    if am:
        alias = am.group(1) if am.group(1) is not None else am.group(2)
    return primary_ident, alias


_QUALIFIER = re.compile(r"(\w+)\s*\.")


def _rewrite_qualifiers(s: str, mapping) -> str:
    """Replace each ``<alias>.`` qualifier whose (lower-cased) identifier is in ``mapping`` with
    ``<canonical>.``, leaving string literals, quoted identifiers, and unrelated tokens untouched.
    Used to normalize user-chosen MERGE aliases to the canonical ``target``/``source`` names."""
    out, i, n, quote = [], 0, len(s), None
    while i < n:
        ch = s[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            out.append(ch)
            i += 1
            continue
        if not (i and (s[i - 1].isalnum() or s[i - 1] == "_")):  # only at an identifier boundary
            m = _QUALIFIER.match(s, i)
            if m:
                canon = mapping.get(m.group(1).lower())
                if canon:
                    out.append(canon + ".")
                    i = m.end()
                    continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_top_level_commas(s: str) -> List[str]:
    """Split on commas that aren't inside parentheses or quotes (so ``left(email, 3)`` stays whole)."""
    out, depth, start, quote = [], 0, 0, None
    for i, ch in enumerate(s):
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append(s[start:i])
            start = i + 1
    out.append(s[start:])
    return [p.strip() for p in out if p.strip()]


def _qualifiers(s: str) -> List[str]:
    """Every ``<word>.`` qualifier in ``s`` that sits outside string/identifier quotes and at an
    identifier boundary (same quote/boundary rules as :func:`_rewrite_qualifiers`). Used to validate
    that a merge condition references only the canonical ``target``/``source`` relations."""
    out, i, n, quote = [], 0, len(s), None
    while i < n:
        ch = s[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            continue
        if not (i and (s[i - 1].isalnum() or s[i - 1] == "_")):
            mm = _QUALIFIER.match(s, i)
            if mm:
                out.append(mm.group(1))
                i = mm.end()
                continue
        i += 1
    return out


def validate_merge_condition(condition: str) -> None:
    """Raise if the merge ``ON`` condition references a qualifier other than the canonical
    ``target``/``source`` (user aliases are already rewritten to those by the caller). Does NOT
    require a ``target.k = source.k`` key equality — any boolean predicate delta-rs accepts
    (multi-key, range, non-equi) is allowed, matching the full delta-rs MERGE surface. Shared by the
    SQL MERGE handler here and the DataFrame ``DeltaTable.merge`` builder so both enforce ONE
    boundary."""
    for qual in _qualifiers(condition):
        if qual.lower() not in ("target", "source"):
            raise ValueError(
                f"merge condition must use the 'target' and 'source' aliases, got qualifier "
                f"{qual!r}. Write the ON clause with target.<col> / source.<col> (duckrun renames "
                f"the merge relations to target/source)."
            )


def _parse_set_exprs(assign: str) -> dict:
    """``{col: expr}`` for a ``UPDATE SET col = <expr>, …`` clause. The expression is ANY SQL delta-rs
    accepts (a plain ``source.col`` copy or an arbitrary expression like ``source.qty + 1``); it is
    passed to delta-rs verbatim (already alias-rewritten to target/source)."""
    updates: dict = {}
    for a in _split_top_level_commas(assign):
        col, sep, expr = a.partition("=")
        if not sep or not col.strip() or not expr.strip():
            raise ValueError(f"malformed UPDATE SET assignment: {a.strip()!r}")
        updates[col.strip().strip('"')] = expr.strip()
    if not updates:
        raise ValueError("UPDATE SET has no assignments")
    return updates


def _parse_insert_cols(cols_text: str, vals_text: str) -> dict:
    """``{col: value_expr}`` for ``INSERT (col, …) VALUES (<expr>, …)`` — columns and values zipped
    positionally (values split at top level so a function call's commas don't break the pairing)."""
    cols = [c.strip().strip('"') for c in cols_text.split(",") if c.strip()]
    vals = _split_top_level_commas(vals_text)
    if len(cols) != len(vals):
        raise ValueError(
            f"INSERT column/value count mismatch: {len(cols)} column(s), {len(vals)} value(s)"
        )
    if not cols:
        raise ValueError("INSERT has no columns")
    return dict(zip(cols, vals))


def _split_when_clause(clause: str) -> Tuple[str, Optional[str], str]:
    """``(kind, pred, action)`` for one ``WHEN <kind> [AND <pred>] THEN <action>``.

    The clause is split on its first *top-level* ``THEN`` (outside quotes/parens, via
    :func:`_find_top_level`) so a string literal that contains the word ``then`` — e.g.
    ``WHEN MATCHED AND s.note <> 'x then y' THEN …`` — is not mistaken for the clause's THEN.
    """
    s = clause.strip()
    ti = _find_top_level(s, _M_THEN)
    if ti < 0:
        raise ValueError(f"unsupported MERGE clause (no THEN): {s!r}")
    # rstrip a trailing statement terminator so the LAST clause's action (`… INSERT *;`) still
    # matches the action patterns, which the old single-clause regex absorbed via `\s*;?\s*`.
    head, action = s[:ti], s[ti + len("then"):].strip().rstrip(";").strip()
    km = _M_KIND.fullmatch(head)
    if not km:
        raise ValueError(f"unsupported MERGE clause: {s!r}")
    kind = re.sub(r"\s+", " ", km.group("kind").strip().lower())
    pred = km.group("pred")
    return kind, (pred.strip() if pred else None), action


def _build_merge_clause(kind: str, pred: Optional[str], action: str) -> dict:
    """Turn one parsed ``(kind, pred, action)`` WHEN clause into an ``engine.merge_delta_clauses``
    spec dict. Covers the full delta-rs TableMerger surface; only a malformed/typo action raises —
    legality of clause *combinations* is left to delta_rs."""
    if kind == "matched":
        if _M_UPDATE_ALL.fullmatch(action):
            return {"clause": "matched", "action": "update_all", "predicate": pred}
        if _M_DELETE.fullmatch(action):
            return {"clause": "matched", "action": "delete", "predicate": pred}
        sm = _M_UPDATE_SET.fullmatch(action)
        if sm:
            return {"clause": "matched", "action": "update",
                    "updates": _parse_set_exprs(sm.group("assign")), "predicate": pred}
        raise ValueError(
            f"unsupported WHEN MATCHED action (expected UPDATE SET * / UPDATE SET col=… / DELETE): "
            f"{action!r}")
    if kind == "not matched":
        if _M_INSERT_ALL.fullmatch(action):
            return {"clause": "not_matched", "action": "insert_all", "predicate": pred}
        im = _M_INSERT_COLS.fullmatch(action)
        if im:
            return {"clause": "not_matched", "action": "insert",
                    "updates": _parse_insert_cols(im.group("cols"), im.group("vals")),
                    "predicate": pred}
        raise ValueError(
            f"unsupported WHEN NOT MATCHED action (expected INSERT * or INSERT (cols) VALUES (…)): "
            f"{action!r}")
    # "not matched by source": UPDATE SET … or DELETE
    if _M_DELETE.fullmatch(action):
        return {"clause": "not_matched_by_source", "action": "delete", "predicate": pred}
    sm = _M_UPDATE_SET.fullmatch(action)
    if sm:
        return {"clause": "not_matched_by_source", "action": "update",
                "updates": _parse_set_exprs(sm.group("assign")), "predicate": pred}
    raise ValueError(
        f"unsupported WHEN NOT MATCHED BY SOURCE action (expected UPDATE SET … or DELETE): "
        f"{action!r}")


class _DeltaDML:
    """One attempt to handle a statement; ``run()`` returns True if it was applied to Delta."""

    def __init__(self, cursor, root_path: str, storage_options, default_schema=None):
        self.cursor = cursor
        self.root_path = root_path.rstrip("/")
        self.so = storage_options
        self.default_schema = default_schema
        self._with_clause = ""  # a leading `WITH …` preceding an INSERT, prepended to the body

    def _loc(self, schema: str, identifier: str) -> str:
        return f"{self.root_path}/{schema}/{identifier}"

    def _resolve(self, rel: str):
        """(schema, identifier, location) for ``rel``, falling back to default_schema for an
        unqualified name (the connection API relies on a current database). (None, None, None) when
        no schema can be determined."""
        schema, identifier = _split_relation(rel)
        schema = schema or self.default_schema
        if not schema or not identifier:
            return None, None, None
        return schema, identifier, self._loc(schema, identifier)

    def _exists(self, loc: str) -> bool:
        return engine.table_exists(loc, self.so)

    def _refresh_view(self, rel: str, schema: str, loc: str) -> None:
        loc_sql = loc.replace("'", "''")
        self.cursor.execute(f'create schema if not exists "{schema}"')
        self.cursor.execute(
            f"create or replace view {rel} as select * from delta_scan('{loc_sql}')"
        )

    def try_handle(self, sql: str) -> bool:
        # CREATE TEMP/TEMPORARY TABLE is native DuckDB scratch by design — never capture it.
        if _CREATE_TEMP_RE.match(sql):
            return False
        m = _fullmatch(_CREATE_AS, sql)
        if m and "__duckrun" not in m.group("rel"):
            return self._create_as(m)
        m = _fullmatch(_CREATE_COLDEFS, sql)
        if m and "__duckrun" not in m.group("rel"):
            return self._create_coldefs(m)
        if _INSERT_INTO_RE.match(sql):
            # Classify VALUES vs SELECT structurally, THEN apply the matching regex — see _insert_kind.
            kind = _insert_kind(sql)
            if kind == "select":
                m = _fullmatch(_INSERT_SELECT, sql)
                if m:
                    return self._mutate(m, self._insert_select)
            elif kind == "values":
                m = _fullmatch(_INSERT_VALUES, sql)
                if m:
                    return self._mutate(m, self._insert_values)
            return False
        m = _fullmatch(_DELETE, sql)
        if m:
            return self._mutate(m, self._delete)
        m = _fullmatch(_UPDATE, sql)
        if m:
            return self._mutate(m, self._update)
        m = _fullmatch(_ALTER_ADD, sql)
        if m:
            return self._mutate(m, self._alter_add)
        m = _fullmatch(_MERGE, sql)
        if m:
            return self._mutate(m, self._merge)
        m = _fullmatch(_DROP, sql)
        if m:
            return self._drop(m)
        return False

    # -- create table <rel> as <query>: always materialize as a duckrun Delta table ------------
    def _create_as(self, m) -> bool:
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc:
            return False
        live = self._exists(loc) and not is_dropped(self.cursor, loc, self.so)
        # dbt/cursor path (no default_schema): keep the ORIGINAL narrow interception — only a plain
        # `create table … as select …` routes to Delta. A CTE or parenthesised body stays native so
        # dbt keeps owning the relation (dbt-internal CTAS like store_failures' `create table … as
        # (select …)` is a real TABLE dbt later drops/recreates — turning it into a delta_scan VIEW
        # breaks that: "Existing object … is of type View, trying to drop type Table").
        #
        # `create or replace table … as select …` ALSO routes to Delta, but ONLY when the target is
        # already a live duckrun-managed Delta table — i.e. a seed/model duckrun surfaced as a
        # delta_scan view and a post-hook now refills via `… as select … from read_csv('s3://…')`
        # (Tuva's duckdb__load_seed). Without this the `or replace` lands natively and DuckDB rejects
        # replacing the view with a table. Over a NON-duckrun relation (no live Delta at the target),
        # `or replace` stays native so dbt's own `create or replace table` keeps working.
        if self.default_schema is None:
            if not re.match(r"select\b", m.group("body").lstrip(), re.I):
                return False
            if m.group("orrep") and not live:
                return False
        # `if not exists` over a live (non-tombstone) table is a no-op — just (re)surface the view.
        if m.group("ine") and live:
            self._refresh_view(rel, schema, loc)
            return True
        # Connection API: a plain `create table` must NOT silently clobber a live table — that's what
        # `or replace` is for. (The dbt/cursor path keeps overwriting: dbt owns idempotent re-runs.)
        if self.default_schema is not None and live and not m.group("orrep"):
            raise ValueError(
                f"table {schema}.{identifier} already exists — "
                f"use CREATE OR REPLACE TABLE to replace it"
            )
        data = self.cursor.sql(m.group("body"))
        # overwrite_schema so this replaces a prior table (or a drop-tombstone) wholesale — a live
        # table is recreated with the real schema, clearing any tombstone marker.
        engine.write_delta(loc, data, "overwrite", overwrite_schema=True, storage_options=self.so)
        self._refresh_view(rel, schema, loc)
        return True

    # -- create table <rel> (<col defs>): an EMPTY Delta table (connection API only) -----------
    def _create_coldefs(self, m) -> bool:
        # Only the connection API (which carries a current database) makes a bare `CREATE TABLE
        # (col defs)` a Delta table — so `CREATE TABLE` is always Delta-backed there. The dbt/cursor
        # path passes default_schema=None: the seed loader emits this exact form and RELIES on it
        # landing as a native DuckDB table, so we pass through untouched.
        if self.default_schema is None:
            return False
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc:
            return False
        live = self._exists(loc) and not is_dropped(self.cursor, loc, self.so)
        if m.group("ine") and live:                       # IF NOT EXISTS over a live table → no-op
            self._refresh_view(rel, schema, loc)
            return True
        if live and not m.group("orrep"):                 # plain CREATE over a live table → error
            raise ValueError(
                f"table {schema}.{identifier} already exists — "
                f"use CREATE OR REPLACE TABLE to replace it"
            )
        # Let DuckDB parse the column defs (types, constraints, nested parens) by building the table
        # as a TEMP, then take its Arrow schema and create an EMPTY Delta table from it. DeltaTable.create
        # logs a CREATE TABLE operation (not a WRITE/Overwrite). A live table or a drop-tombstone already
        # has files at ``loc``, so it must be replaced (overwrite); otherwise create-if-absent (error).
        tmp = f"__duckrun_empty_{abs(hash((schema, identifier))) & 0xFFFFFFFF}"
        self.cursor.execute(f'create or replace temp table "{tmp}" ({m.group("defs")})')
        try:
            arrow_schema = self.cursor.sql(f'select * from "{tmp}" limit 0').arrow().schema
        finally:
            self.cursor.execute(f'drop table if exists "{tmp}"')
        mode = "overwrite" if self._exists(loc) else "error"
        engine.create_empty_delta(loc, arrow_schema, mode=mode, storage_options=self.so)
        self._refresh_view(rel, schema, loc)
        return True

    # -- forms that only apply when a Delta table already exists at the target ------------------
    def _mutate(self, m, op) -> bool:
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False  # native relation (e.g. the test's `fact`/`seed`) -> let DuckDB handle it
        if self._with_clause and op != self._insert_select:
            return False  # `WITH … UPDATE/DELETE` can't be expressed through a delta_rs predicate
        op(m, rel, schema, loc)
        self._refresh_view(rel, schema, loc)
        return True

    def _delete(self, m, rel, schema, loc) -> None:
        where = m.group("where")
        where = where.strip() if where else None
        if where and _PREDICATE_SUBQUERY.search(where):
            # delta_rs's delete(predicate) can't evaluate a predicate that references ANOTHER table
            # via subquery (`… in (select … from other)`): its datafusion expr engine raises a Rust
            # `not implemented` panic. DuckDB CAN evaluate it (the subquery's table is a live relation
            # on this cursor), so keep the complement and overwrite. `(P) IS NOT TRUE` is the exact
            # set DELETE keeps — rows where the predicate is false OR null (3-valued logic), not just
            # `NOT (P)`. Materialize survivors into a native temp first so the read is fully detached
            # from the Delta table being replaced. Simple predicates still take the fenced path below.
            logger.debug(
                f"duckrun: DELETE predicate on {loc!r} references a subquery; delta_rs can't "
                "evaluate it, falling back to a DuckDB-filtered full overwrite (slower)."
            )
            loc_sql = loc.replace("'", "''")
            tmp = f"__duckrun_del_{abs(hash(loc)) & 0xFFFFFFFF}"
            self.cursor.execute(
                f'create or replace temp table "{tmp}" as '
                f"select * from delta_scan('{loc_sql}') where ({where}) is not true"
            )
            try:
                keep = self.cursor.sql(f'select * from "{tmp}"')
                engine.write_delta(loc, keep, "overwrite", overwrite_schema=True,
                                   storage_options=self.so)
            finally:
                self.cursor.execute(f'drop table if exists "{tmp}"')
            return
        # Route through engine.delete_rows pinned to the version read at statement start — the SAME
        # snapshot-fenced path (read_version → load_as_version, OCC over (vB, HEAD], post-op
        # maintenance) as DeltaTable.forName(...).delete(). conn.sql and the DataFrame handle MUST
        # behave identically; a raw delta-rs delete() at HEAD skipped the pin and the maintenance.
        engine.delete_rows(
            loc, where,
            read_version=engine.table_version(loc, self.so),
            storage_options=self.so,
        )

    def _update(self, m, rel, schema, loc) -> None:
        updates = {}
        for assign in _split_top_level_commas(m.group("set")):
            col, _, expr = assign.partition("=")
            updates[col.strip().strip('"')] = expr.strip()
        where = m.group("where")
        # Same snapshot-fenced path as DeltaTable.forName(...).update() — SQL == DataFrame.
        engine.update_rows(
            loc, updates, where.strip() if where else None,
            read_version=engine.table_version(loc, self.so),
            storage_options=self.so,
        )

    def _insert_select(self, m, rel, schema, loc) -> None:
        body = m.group("body")
        if self._with_clause:  # `WITH … INSERT INTO t SELECT …`: re-attach the CTE to the body
            body = f"{self._with_clause} {body}"
        cols = m.group("cols")
        # Always project onto the target schema — a column list maps by name, no list maps
        # positionally. Routing both through _append_projected gives one place for the intentional
        # type alignment AND the lossy-numeric-narrowing guard (so `insert … select 3.9` is caught too).
        self._append_projected(loc, self._provided(cols) if cols else None, f"({body})")

    def _insert_values(self, m, rel, schema, loc) -> None:
        # `insert into <rel> [(<cols>)] values (...)`: the literals supply every target column when
        # no list is given, in order; otherwise the named columns.
        cols = m.group("cols")
        provided = self._provided(cols) if cols else None
        self._append_projected(loc, provided, f"(values {m.group('body')})")

    @staticmethod
    def _provided(cols: str) -> List[str]:
        return [c.strip().strip('"') for c in cols.split(",")]

    def _append_projected(self, loc, provided, derived: str) -> None:
        """Append a ``derived`` table (a ``(values …)`` tuple list or a ``(select …)`` subquery) to
        the Delta table at ``loc``, projecting its columns onto the FULL target schema: supplied
        columns come from ``derived`` (positional when ``provided`` is None), any unsupplied target
        column is a typed NULL, and every projected column is cast to the target column's type so
        the appended Arrow schema matches the table exactly (what a plain SQL INSERT does, and it
        stops a literal wider than the column from forcing delta_rs to add a new writer feature on
        append)."""
        loc_sql = loc.replace("'", "''")
        template = self.cursor.sql(f"select * from delta_scan('{loc_sql}') limit 0")
        target_cols = list(template.columns)
        target_types = [str(t) for t in template.types]
        by_lower = {c.lower(): c for c in target_cols}

        if provided is None:  # positional → every target column, in order
            provided = target_cols
        else:  # explicit column list → canonicalize to the target's casing
            provided = [by_lower.get(c.lower(), c) for c in provided]
        provided_set = set(provided)

        quoted = ", ".join('"' + c + '"' for c in provided)
        inner = f"{derived} v({quoted})"
        self._reject_lossy_numeric_narrowing(inner, provided, dict(zip(target_cols, target_types)))
        exprs = [
            f'cast(v."{col}" as {typ}) as "{col}"' if col in provided_set
            else f'cast(null as {typ}) as "{col}"'
            for col, typ in zip(target_cols, target_types)
        ]
        data = self.cursor.sql(f"select {', '.join(exprs)} from {inner}")
        engine.write_delta(loc, data, "append", storage_options=self.so)

    def _reject_lossy_numeric_narrowing(self, inner: str, provided, ttype) -> None:
        """Fail loud when a supplied numeric value would be SILENTLY changed by the cast onto its
        target column — e.g. inserting 3.9 into an INTEGER column (which lands 4). The cast in
        :meth:`_append_projected` aligns types ON PURPOSE — timestamp ntz, int widening — and those are
        lossless and intended, so this guard only fires for a numeric→numeric cast where the value does
        NOT survive a round-trip through the target type. Non-numeric casts (timestamps, strings) are
        deliberately left untouched. Raises ``ValueError`` naming the column and an example value.

        Costs one extra evaluation of ``inner`` (trivial for VALUES; a second scan for ``insert …
        select`` — acceptable to turn silent corruption into a loud error)."""
        src = self.cursor.sql(
            "select " + ", ".join(f'v."{c}"' for c in provided) + f" from {inner} limit 0")
        stype = {c: str(t) for c, t in zip(provided, src.types)}
        checks = []  # (col, lossy-predicate) for numeric→numeric casts that could narrow
        for col in provided:
            s, t = stype[col], ttype[col]
            if s == t or not (_NUMERIC_TYPE_RE.match(s) and _NUMERIC_TYPE_RE.match(t)):
                continue
            if _APPROX_FLOAT_RE.match(t):
                continue  # approximate float target: narrowing is intended, not silent corruption
            # round-trip through the target type; try_cast so the probe itself never throws — an
            # out-of-range value becomes NULL → distinct → flagged, same as a fractional loss.
            checks.append(
                (col, f'try_cast(try_cast(v."{col}" as {t}) as {s}) is distinct from v."{col}"'))
        if not checks:
            return
        sel = ", ".join(
            f'count(*) filter (where {pred}) as "n{i}", '
            f'any_value(v."{col}") filter (where {pred}) as "ex{i}"'
            for i, (col, pred) in enumerate(checks))
        row = self.cursor.sql(f"select {sel} from {inner}").fetchone()
        for i, (col, _) in enumerate(checks):
            n, ex = row[2 * i], row[2 * i + 1]
            if n:
                raise ValueError(
                    f"INSERT would silently narrow {n} value(s) for column '{col}' into "
                    f"{ttype[col]} (e.g. {ex!r}). Cast explicitly in the SELECT/VALUES if intended."
                )

    def _alter_add(self, m, rel, schema, loc) -> None:
        col = m.group("col").strip().strip('"')
        # Keep only the column type (drop any trailing DEFAULT / NOT NULL / NULL clause); add it as
        # an all-null column by rewriting the table with overwrite_schema so delta_rs accepts the
        # widened schema. `not null` must be matched before a bare `null`, else the `\s+null\b`
        # alternative eats only the ` null` and leaves `... not` glued onto the type.
        coltype = re.split(
            r"\s+default\b|\s+not\s+null\b|\s+null\b", m.group("def"), flags=re.I
        )[0].strip() or "VARCHAR"
        loc_sql = loc.replace("'", "''")
        data = self.cursor.sql(
            f'select *, cast(null as {coltype}) as "{col}" from delta_scan(\'{loc_sql}\')'
        )
        engine.write_delta(loc, data, "overwrite", overwrite_schema=True, storage_options=self.so)

    # -- merge into <target> using <source> on <cond> when … : full delta_rs MERGE ---------------
    def _merge(self, m, rel, schema, loc) -> None:
        """Dispatch a raw SQL MERGE to ``engine.merge_delta_clauses`` — the full delta-rs TableMerger
        surface, same engine core, snapshot pin, and boundary as the DataFrame ``DeltaTable.merge``
        builder. Any number of ``WHEN MATCHED`` / ``WHEN NOT MATCHED`` / ``WHEN NOT MATCHED BY
        SOURCE`` clauses are parsed in order; the ON condition may be any boolean predicate. The ON
        and WHEN clauses may use the user's own aliases (``MERGE INTO t a USING s b ON a.k = b.k``)
        or the table/relation names; these are normalized to the literal ``target``/``source``
        aliases delta_rs uses, and the ON predicate is then handed to delta_rs verbatim."""
        rest = m.group("rest")
        ui = _find_top_level(rest, _M_USING)
        if ui < 0:
            raise ValueError("MERGE requires a USING <source> clause")
        after_using = rest[ui + len("using"):]
        oi = _find_top_level(after_using, _M_ON)
        if oi < 0:
            raise ValueError("MERGE requires an ON <condition> clause")
        source_part = after_using[:oi]
        cond_clauses = after_using[oi + len("on"):]
        # Normalize whatever aliases the user wrote to the canonical target/source the engine uses.
        # The target may be referenced by its alias (`MERGE INTO t a …`) or its table name; the
        # source by its alias or, for a bare relation, its name. Literal target/source always work
        # (backward compatible). delta_rs renames the relations to target/source itself, so this only
        # has to fix the ON/WHEN qualifiers — source_part is evaluated verbatim and left alone.
        talias = _lead_alias(rest[:ui])
        src_primary, salias = _source_primary_and_alias(source_part)
        mapping = {"target": "target", "source": "source"}
        mapping[(talias or _split_relation(rel)[1] or "target").lower()] = "target"
        sq = salias or src_primary
        if sq:
            mapping[sq.lower()] = "source"
        cond_clauses = _rewrite_qualifiers(cond_clauses, mapping)
        whens = _find_all_top_level(cond_clauses, _M_WHEN)
        if not whens:
            raise ValueError("MERGE requires at least one WHEN clause")
        cond = cond_clauses[:whens[0]].strip()
        if not cond:
            raise ValueError("MERGE requires an ON <condition> clause")
        validate_merge_condition(cond)
        clause_strs = [cond_clauses[a:b] for a, b in zip(whens, whens[1:] + [len(cond_clauses)])]
        # One clause spec per WHEN, in source order — delta_rs evaluates them top-to-bottom and
        # enforces its own legality rules (e.g. multiple unconditional matched clauses), so we don't
        # second-guess combinations here: whatever delta_rs accepts, we accept.
        clauses = [_build_merge_clause(*_split_when_clause(c)) for c in clause_strs]

        # Evaluate the whole USING operand (including any alias) so a bare name, an aliased name, and
        # a subquery with a column-renaming alias (`(values …) t(id, name)`) all work — delta_rs
        # renames the relation to `source` regardless of the SQL alias.
        source = self.cursor.sql(f"select * from {source_part.strip()}")
        engine.merge_delta_clauses(
            loc,
            source,
            cond,
            clauses,
            # Pin the target to the version we read now (single statement) — same as the builder's
            # .merge(), which captures the version at call time.
            read_version=engine.table_version(loc, self.so),
            storage_options=self.so,
        )

    def _drop(self, m) -> bool:
        # `drop table` on a duckrun relation: unregister the delta_scan view AND, via delta_rs,
        # overwrite the table to a one-column tombstone (TOMBSTONE_COLUMN) so a later glob discovery
        # hides it. NO data is deleted — delta_rs has no drop, and removing the Delta files would be
        # a filesystem hack that fails on object stores. The directory persists until a human purges
        # it; a later `create table ... as` overwrites the tombstone with real data. If the relation
        # isn't a duckrun-managed Delta table, fall through and let DuckDB drop the native table.
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False
        tombstone = self.cursor.sql(f"select true as {TOMBSTONE_COLUMN}")
        engine.write_delta(loc, tombstone, "overwrite", overwrite_schema=True, storage_options=self.so)
        self.cursor.execute(f"drop view if exists {rel}")
        return True


def _split_top_level(sql: str) -> List[str]:
    """Split a (comment-stripped) SQL script into its top-level statements — a ``;`` that is outside
    quotes and parens. Returns the non-empty trimmed statements; a single statement yields ``[sql]``."""
    stmts: List[str] = []
    depth, quote, start = 0, None, 0
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            continue
        if ch == "$":
            de = _dollar_quote_end(sql, i)
            if de is not None:
                i = de  # a ';' inside a dollar-quoted body is literal, not a statement boundary
                continue
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == ";" and depth == 0:
            part = sql[start:i].strip()
            if part:
                stmts.append(part)
            start = i + 1
        i += 1
    tail = sql[start:].strip()
    if tail:
        stmts.append(tail)
    return stmts


def _handle_one(cursor, root_path, storage_options, sql: str, default_schema) -> bool:
    """Route ONE statement (already comment-stripped, leading junk removed) to Delta if it's a DML
    form against a duckrun-managed relation. True if applied to Delta; False to pass through."""
    with_clause, body = _split_leading_with(sql)  # peel a leading `WITH …` off an INSERT/etc.
    head = body[:7].lower()                        # cheap pre-filter: only the candidate DML verbs
    if not head.startswith(("delete", "update", "insert", "create", "alter", "drop", "merge")):
        return False
    dml = _DeltaDML(cursor, root_path, storage_options, default_schema)
    dml._with_clause = with_clause
    return dml.try_handle(body)


def handle(cursor, root_path, storage_options, sql: str, default_schema=None) -> bool:
    """Apply ``sql`` to Delta if it's a DML form targeting a duckrun-managed relation, using
    ``cursor`` to evaluate any SELECT body and to (re)create the ``delta_scan`` view.

    Every handled form goes through delta_rs (``engine.write_delta`` / ``DeltaTable.delete`` /
    ``.update``), which carries ``storage_options`` and so works on local AND abfss/OneLake stores.
    ``default_schema`` resolves an unqualified table name (the connection API has a current
    database; the dbt path always renders fully-qualified names so passes None).
    Returns True if handled (the caller must NOT also run it on DuckDB), False to pass through —
    anything unrecognized, or (for the mutate forms) a target that isn't a Delta table.

    A MULTI-statement script (``delete …; insert …`` — e.g. Elementary's delete+insert upsert) is
    split into its top-level statements and each is routed individually: Delta-DML to delta_rs,
    anything else run natively on ``cursor``. dbt-duckdb would hand the whole script to DuckDB, but
    duckrun intercepts per-cursor, so a leading DELETE/INSERT against a Delta relation must not
    swallow the rest of the script as its own predicate/body. (The connection API rejects multi-
    statement upstream in session._unsupported_dml, so the split only fires on the dbt cursor path.)
    """
    if not root_path:
        return False
    sql = _strip_leading(sql)  # so leading comments/whitespace don't hide the verb
    sql = _strip_comments(sql)  # drop interior --/* */ comments so they can't fake a clause boundary
    sql = re.sub(r";\s*$", "", sql)  # drop a trailing ';' terminator. A matcher's body capture is
    # DOTALL (`select\b.*`), so `insert … select … from t;` would carry the ';' into the rewritten
    # `select … from (select … from t;) v(…)` — a syntax error mid-statement. dbt-duckdb runs the raw
    # statement so never hits this; duckrun re-embeds the body, so it must drop the terminator first.
    stmts = _split_top_level(sql)
    if len(stmts) <= 1:
        return _handle_one(cursor, root_path, storage_options, sql, default_schema)
    # Multi-statement script: route each statement; non-Delta ones run on the cursor as DuckDB would.
    for stmt in stmts:
        if not _handle_one(cursor, root_path, storage_options, stmt, default_schema):
            cursor.execute(stmt)
    return True
