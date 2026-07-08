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
from datetime import datetime
from typing import List, Optional, Tuple

import duckdb

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
# Optional table-layout clauses on CREATE TABLE … AS, between the name and AS: DuckDB's own
# `SORTED BY (cols)` / `PARTITIONED BY (cols)` syntax (its parser accepts both), plus `SORTED BY AUTO`
# (a duckrun extension — the session profiles and substitutes explicit columns before this router runs,
# so the router only ever sees the explicit forms). `[^()]*` — a column list has no nested parens.
_SORTED_BY_RE = re.compile(r"\bsorted\s+by\s+(?:(auto)\b|\(([^()]*)\))", re.I)
_PARTITIONED_BY_RE = re.compile(r"\bpartitioned\s+by\s+\(([^()]*)\)", re.I)


def _split_create_layout(rel: str):
    """Peel optional ``SORTED BY (cols)|SORTED BY AUTO`` and ``PARTITIONED BY (cols)`` clauses off a
    CREATE TABLE target. Returns ``(bare_rel, sort, partition)`` — ``sort`` is a column list, the
    string ``'AUTO'``, or None; ``partition`` is a column list or None."""
    sort = partition = None
    ms = _SORTED_BY_RE.search(rel)
    if ms:
        sort = "AUTO" if ms.group(1) else [c.strip().strip('"') for c in ms.group(2).split(",") if c.strip()]
        rel = rel[:ms.start()] + rel[ms.end():]
    mp = _PARTITIONED_BY_RE.search(rel)
    if mp:
        partition = [c.strip().strip('"') for c in mp.group(1).split(",") if c.strip()]
        rel = rel[:mp.start()] + rel[mp.end():]
    return rel.strip(), sort, partition


_INSERT_INTO_RE = re.compile(r"\s*insert\s+into\b", re.I)
# `insert into <rel> [(cols)] [BY NAME] <SELECT | WITH … SELECT>`. rel is non-greedy so the optional
# `BY NAME` (DuckDB's align-source-by-column-name) and a body-leading `WITH …` CTE (`insert into t
# WITH c AS (…) SELECT … FROM c`, where the CTE may even SHADOW the target name) are peeled into their
# own groups instead of being swallowed into rel — otherwise rel captures `colorder BY NAME` /
# `sales WITH sales AS` and _resolve fails. The body starts at the first top-level SELECT-or-WITH.
_INSERT_SELECT = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)"
    r"(?:\s*\((?P<cols>[^)]*)\))?"
    r"(?P<byname>\s+by\s+name)?"
    r"\s+(?P<body>(?:with\b|select\b).*)",
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
# `insert into <t> replace where <pred> <select|values|with …>` — delta_rs replaceWhere: atomically
# overwrite ONLY the rows matching <pred> with the body, as one fenced commit (the Spark/Delta
# `INSERT INTO … REPLACE WHERE`). Capture the target and the whole tail; _insert_replace_where splits
# <pred> from the body at the first TOP-LEVEL query keyword, so a keyword inside a string literal or a
# subquery in the predicate can't be mistaken for the boundary (same scanners as MERGE).
_INSERT_REPLACE = re.compile(
    r"\s*insert\s+into\s+(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\s+"
    r"replace\s+where\b(?P<rest>.+)", re.I | re.S,
)
_REPLACE_BODY_KW = re.compile(r"\b(?:select|values|with)\b", re.I)
# `insert with schema evolution into <t> <select|values>` (Spark/Delta spelling) — append with delta_rs
# schema_mode='merge', so columns the source has and the table lacks are ADDED (existing rows → NULL),
# instead of the plain append's project-to-target (which drops unknown columns).
_INSERT_SCHEMA_EVOLUTION = re.compile(
    r"\s*insert\s+with\s+schema\s+evolution\s+into\s+"
    r"(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\s+(?P<body>(?:select|with|values)\b.+)",
    re.I | re.S,
)
_DELETE = re.compile(
    r"\s*delete\s+from\s+(?P<rel>.+?)(?:\s+where\s+(?P<where>.+))?\s*;?\s*", re.I | re.S
)
# A predicate that references ANOTHER table via subquery (`… in (select … from other)`). delta_rs's
# delete()/update() predicate engine (datafusion) raises a Rust `not implemented` panic on these
# (delta_datafusion/expr.rs), so duckrun can't hand them to dt.delete(); see _delete for the DuckDB-
# evaluated filtered-overwrite fallback. Plain column-vs-constant/expr predicates don't match.
_PREDICATE_SUBQUERY = re.compile(r"\(\s*select\b", re.I)
# Capture only the relation and the whole post-SET body; the SET/WHERE boundary is located
# structurally in _update via _find_top_level (_TOP_WHERE), NOT by regex — a string literal in the
# SET list that contains the word `where` (`set note = 'apply where needed'`) would otherwise
# mis-split the statement. (Same class of bug already fixed for the MERGE THEN boundary.)
_UPDATE = re.compile(
    r"\s*update\s+(?P<rel>.+?)\s+set\s+(?P<body>.+?)\s*;?\s*",
    re.I | re.S,
)
_TOP_WHERE = re.compile(r"\bwhere\b", re.I)
_ALTER_ADD = re.compile(
    r"\s*alter\s+table\s+(?P<rel>.+?)\s+add\s+column\s+(?P<col>\S+)\s+(?P<def>.+?)\s*;?\s*",
    re.I | re.S,
)
# `alter table <t> drop column [if exists] <c>` and `… rename column <old> to <new>`. delta_rs has no
# in-place column drop/rename, so both are done as a fenced overwrite rewrite (DuckDB SELECT * EXCLUDE
# / SELECT * RENAME) under overwrite_schema — same mechanism as ADD COLUMN.
_ALTER_DROP = re.compile(
    r"\s*alter\s+table\s+(?P<rel>.+?)\s+drop\s+column\s+(?:if\s+exists\s+)?(?P<col>[^\s;]+)\s*;?\s*",
    re.I | re.S,
)
_ALTER_RENAME = re.compile(
    r"\s*alter\s+table\s+(?P<rel>.+?)\s+rename\s+column\s+(?P<old>[^\s;]+)\s+to\s+(?P<new>[^\s;]+)\s*;?\s*",
    re.I | re.S,
)
# Trailing column-definition clauses (DEFAULT / NOT NULL / NULL), located structurally in _alter_add
# so a string DEFAULT containing one of these words doesn't truncate the column type. Leading \b is
# load-bearing: _find_top_level probes every depth-0 index, so it must not match mid-identifier.
_ALTER_TAIL = re.compile(r"\b(?:default|not\s+null|null)\b", re.I)
_DROP = re.compile(
    r"\s*drop\s+table\s+(?:if\s+exists\s+)?(?P<rel>[^\s;]+)\s*;?\s*", re.I | re.S
)
# `vacuum [analyze] <table>` — DuckDB's native VACUUM verb, repurposed for Delta maintenance: compact
# small files (dataChange=false) then vacuum files tombstoned past retention. Only a form WITH a table
# operand matches; a bare `vacuum` / `vacuum analyze` (no target) falls through to DuckDB (a no-op
# there), and the `vacuum <table>(col)` stats form doesn't match (trailing paren) so it stays native.
_VACUUM = re.compile(
    r"\s*vacuum\s+(?:analyze\s+)?"
    r"(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\s*;?\s*", re.I | re.S
)
# `restore table <t> to version as of <n>` / `to timestamp as of '<ts>'` — the Spark/Delta RESTORE
# verb, rolled to delta_rs ``DeltaTable.restore`` (a new commit on top of history, itself revertible).
_RESTORE = re.compile(
    r"\s*restore\s+table\s+(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\s+to\s+"
    r"(?:version\s+as\s+of\s+(?P<version>\d+)|timestamp\s+as\s+of\s+'(?P<ts>[^']+)')\s*;?\s*",
    re.I | re.S,
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

# Dollar-quoting primitives (``$tag$ … $tag$``) live in the shared scanner module now, re-exported
# here so this module's scanners and session.py keep importing them from delta_dml. The body is
# OPAQUE — a ``;``, a quote, or a ``--``/``/* */`` marker inside it is literal text, not a structural
# token — so every quote/paren-aware scanner here skips a dollar-quoted run wholesale; otherwise
# dbt's persist_docs `COMMENT ON … IS $dbt_comment_literal_block$ …text with "quotes"; and ';'…
# $dbt_comment_literal_block$` is mis-split into fragments with an unterminated $-quote.
from .sqlscan import _DOLLAR_OPEN, _dollar_quote_end  # noqa: F401  (re-exported)


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


# DML target-relation matcher (verb → relation), used only to detect a 3-part cross-catalog target.
_DML_REL = re.compile(
    r"^(?:insert\s+into|delete\s+from|update|merge\s+into|alter\s+table|"
    r"drop\s+table(?:\s+if\s+exists)?|"
    r"create\s+(?:or\s+replace\s+)?table(?:\s+if\s+not\s+exists)?)\s+(?P<rel>[\w.\"]+)",
    re.IGNORECASE,
)


def dml_target_catalog(query: str) -> Optional[str]:
    """The leading catalog token of a DML statement's target when it's 3-part
    (``catalog.schema.table``), else ``None``. Lets a caller route raw DML to the named catalog's
    root (rather than the current/default one). Shared by the native session and the dbt adapter's
    cursor wrapper so the two use one matcher."""
    _, body = _split_leading_with(_strip_leading(query))  # peel a leading WITH so the verb is visible
    m = _DML_REL.match(_strip_leading(body))
    if not m:
        return None
    parts = _split_dotted(m.group("rel"))  # quote-aware: a dot inside "a.b" isn't a separator
    return parts[0] if len(parts) >= 3 else None


# ── statement route classification — the router's single form-classifier ────────────────────────
_C_CREATE_TABLE = re.compile(r"\s*create\s+(?:or\s+replace\s+)?table\b", re.I)
_C_FROM = re.compile(r"\bfrom\b", re.I)
_C_USING = re.compile(r"\busing\b", re.I)
_C_DELTA_HEAD = re.compile(
    r"\s*(?:insert\s+into|insert\s+with\s+schema\s+evolution|update|delete\s+from|"
    r"merge\s+into|alter\s+table|drop\s+table|restore\s+table)\b", re.I)
_C_VACUUM = re.compile(r"\s*vacuum\s+(?:analyze\s+)?(?:\"|\w)", re.I)  # vacuum <table> → maintenance


def classify(sql: str) -> str:
    """Route ONE statement by form:

      * ``'delta'``       — a DML form applied via delta_rs when the target is a Delta table
                            (CREATE [OR REPLACE] TABLE … , INSERT / UPDATE / DELETE / MERGE /
                            ALTER TABLE / DROP TABLE).
      * ``'reject'``      — a single-statement form delta_rs's predicate engine can't express
                            (``UPDATE … FROM`` / ``DELETE … USING``).
      * ``'passthrough'`` — native DuckDB: reads, ``CREATE TEMP/TEMPORARY`` / ``CREATE VIEW``
                            scratch, ``SHOW`` / ``DESCRIBE`` / ``SET`` / ``PRAGMA``, ….

    Form-level only: it does NOT check whether the target actually exists (the executor falls a
    non-Delta target back to passthrough) and does NOT decide multi-statement policy (the caller
    does). It reuses the same quote / paren / CASE / comment-aware scanners as the executor, so the
    classifier and the code that runs the statement can't drift on the hard parsing cases."""
    body = _strip_comments(_strip_leading(sql))
    low = body.lower()
    # forms delta_rs can't express — a top-level FROM/USING (not one buried in a string or subquery)
    if low.startswith("update") and _find_top_level(body, _C_FROM) != -1:
        return "reject"
    if low.startswith("delete") and _find_top_level(body, _C_USING) != -1:
        return "reject"
    _, inner = _split_leading_with(body)        # peel a leading WITH so the driving verb is visible
    if _CREATE_TEMP_RE.match(inner):            # CREATE TEMP/TEMPORARY … → native scratch
        return "passthrough"
    if _C_CREATE_TABLE.match(inner):            # CREATE [OR REPLACE] TABLE … → Delta-backed
        return "delta"
    if _C_DELTA_HEAD.match(inner):             # insert/update/delete/merge/alter/drop → delta_rs
        return "delta"
    if _C_VACUUM.match(inner):                  # vacuum <table> → delta maintenance (compact + vacuum)
        return "delta"
    return "passthrough"                        # select, create view, show, describe, set, pragma, …


def _blank_string_literals(s: str) -> str:
    """Blank ``'…'`` and ``$tag$…$tag$`` string literals (replace their runs with spaces), leaving
    ``"…"`` quoted IDENTIFIERS intact — so a table name matcher can't be fooled by a literal that
    contains the name, but still sees a quoted-identifier reference to it. Length-preserving."""
    out, quote, i, n = [], None, 0, len(s)
    while i < n:
        ch = s[i]
        if quote:
            out.append(" ")
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch == "'":
            quote = ch
            out.append(" ")
        elif ch == "$":
            de = _dollar_quote_end(s, i)
            if de is not None:
                out.append(" " * (de - i))
                i = de
                continue
            out.append(ch)
        else:
            out.append(ch)
        i += 1
    return "".join(out)


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


def _split_dotted(name: str) -> List[str]:
    """Split a qualified name on the dots that separate its parts, treating a double-quoted span as
    opaque so a dot INSIDE quotes (`"a.b"`, one legal quoted identifier) does NOT split the name.
    Quotes are then stripped per part (matching the historical `.strip('"')`)."""
    parts, start, in_q = [], 0, False
    for i, ch in enumerate(name):
        if ch == '"':
            in_q = not in_q
        elif ch == "." and not in_q:
            parts.append(name[start:i])
            start = i + 1
    parts.append(name[start:])
    return [p.strip().strip('"') for p in parts]


def _reject_unsafe_name(part: str, kind: str) -> None:
    """Reject an identifier that would escape or break the ``<root>/<schema>/<table>`` physical
    layout — a path separator or a ``.``/``..`` traversal component. Necessary because a quoted
    identifier is otherwise opaque to the splitter and can smuggle ``/`` or ``..`` straight into the
    path (``CREATE TABLE "../escape"`` → a write above the lakehouse root)."""
    if "/" in part or "\\" in part or part in (".", ".."):
        raise ValueError(
            f"illegal {kind} name {part!r}: a schema/table name may not contain a path separator "
            f"or be a '.'/'..' path component")


def _split_relation(rel: str) -> Tuple[Optional[str], Optional[str]]:
    """`"db"."schema"."tbl"` / `schema.tbl` / `tbl` / `"a.b"` -> (schema, identifier), quotes stripped.
    Quote-aware: a dot inside a quoted identifier (`"a.b"`) is part of the name, not a separator, so
    `CREATE TABLE "a.b"` is one table named `a.b` rather than schema `a` / table `b`."""
    parts = _split_dotted(rel.strip())
    if not parts or not parts[-1]:
        return None, None
    identifier = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    return schema, identifier


_BARE_IDENT = re.compile(r"[A-Za-z_][\w$]*", re.A)


def _is_clean_relation(rel: str) -> bool:
    """True if ``rel`` is a structurally well-formed 1/2/3-part name: dot-separated parts, each a
    double-quoted span (a dot inside quotes is part of the name) or a bare identifier. Rejects a rel
    that carries STRAY TOKENS after the name — what an unrecognised layout clause leaves behind (e.g.
    ``t SORT BY AUTO``, a mis-spelled ``SORTED BY``, which would otherwise be baked into the name).
    (The whitespace-in-name policy is separate — see :func:`_create_target_error`.)"""
    s = rel.strip()
    n, i = len(s), 0
    while True:
        if i < n and s[i] == '"':                       # quoted part (a dot inside is part of the name)
            j = i + 1
            while j < n:
                if s[j] == '"':
                    if j + 1 < n and s[j + 1] == '"':   # "" — an escaped quote inside the name
                        j += 2
                        continue
                    break
                j += 1
            else:
                return False                            # unterminated quote
            i = j + 1
        else:                                           # bare identifier
            mo = _BARE_IDENT.match(s, i)
            if not mo:
                return False
            i = mo.end()
        while i < n and s[i].isspace():                 # whitespace is allowed only around the dots
            i += 1
        if i >= n:
            return True
        if s[i] != ".":
            return False                                # a stray token after the part → malformed
        i += 1
        while i < n and s[i].isspace():
            i += 1


def _create_target_error(rel: str) -> Optional[str]:
    """Validate a CREATE TABLE target; return an error message, or None if OK. Rejects (a) stray
    tokens (an unrecognised layout clause left in the name) and (b) whitespace in the SCHEMA or TABLE
    part — those become directories on the store and a space in a Delta path trips abfss globbing
    (``%20``). Whitespace in the CATALOG part (a DuckDB attach alias, not a directory) is allowed."""
    if not _is_clean_relation(rel):
        return (f"invalid CREATE TABLE target {rel!r}: a table name can't contain stray tokens. The "
                f"layout clauses are SORTED BY (cols) | SORTED BY AUTO | PARTITIONED BY (cols) — check "
                f"the spelling (it's SORTED BY, not SORT BY).")
    schema, identifier = _split_relation(rel)
    if any(p and any(c.isspace() for c in p) for p in (schema, identifier)):
        return f"invalid CREATE TABLE target {rel!r}: schema and table names can't contain spaces."
    return None


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
        _reject_unsafe_name(schema, "schema")
        _reject_unsafe_name(identifier, "table")
        return schema, identifier, self._loc(schema, identifier)

    def _exists(self, loc: str) -> bool:
        return engine.table_exists(loc, self.so)

    def _refresh_view(self, rel: str, schema: str, loc: str) -> None:
        loc_sql = loc.replace("'", "''")
        # A 3-part rel (catalog.schema.table) targets an ATTACHED catalog: the schema must be created
        # IN that catalog and the view registered there — an unqualified `create schema` would land in
        # the current catalog, then the qualified view create fails on the missing schema (a partial
        # CTAS: data committed, view never registered). A 1/2-part rel stays unqualified in the current
        # catalog, preserving the dbt single-catalog path.
        parts = _split_dotted(rel)  # quote-aware: a dot inside "a.b" isn't a separator
        if len(parts) >= 3:
            self.cursor.execute(f'create schema if not exists "{parts[-3]}"."{schema}"')
        else:
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
        m = _fullmatch(_INSERT_REPLACE, sql)
        if m and "__duckrun" not in m.group("rel"):
            return self._insert_replace_where(m)
        m = _fullmatch(_INSERT_SCHEMA_EVOLUTION, sql)
        if m and "__duckrun" not in m.group("rel"):
            return self._mutate(m, self._insert_schema_evolution)
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
        m = _fullmatch(_ALTER_DROP, sql)
        if m:
            return self._mutate(m, self._alter_drop)
        m = _fullmatch(_ALTER_RENAME, sql)
        if m:
            return self._mutate(m, self._alter_rename)
        m = _fullmatch(_MERGE, sql)
        if m:
            return self._mutate(m, self._merge)
        m = _fullmatch(_DROP, sql)
        if m:
            return self._drop(m)
        m = _fullmatch(_VACUUM, sql)
        if m:
            return self._vacuum(m)
        m = _fullmatch(_RESTORE, sql)
        if m:
            return self._restore(m)
        return False

    # -- create table <rel> as <query>: always materialize as a duckrun Delta table ------------
    def _create_as(self, m) -> bool:
        rel, sort_cols, partition_cols = _split_create_layout(m.group("rel").strip())
        err = _create_target_error(rel)
        if err:
            raise ValueError(err)
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
        body = m.group("body")
        if sort_cols == "AUTO":  # the connection API substitutes explicit columns before we get here
            raise ValueError(
                "CREATE TABLE … SORTED BY AUTO is a connection-API feature (it profiles the query to "
                "pick the sort key); give an explicit SORTED BY (cols) on this path.")
        if sort_cols:  # cluster the write: DuckDB ORDER BY the query, then materialize
            order = ", ".join('"' + c + '"' for c in sort_cols)
            body = f"SELECT * FROM ({body}) ORDER BY {order}"
        data = self.cursor.sql(body)
        # overwrite_schema so this replaces a prior table (or a drop-tombstone) wholesale — a live
        # table is recreated with the real schema, clearing any tombstone marker. Adaptive row-group
        # geometry for a small result is computed inside write_delta (the shared overwrite seam), so
        # this SQL CTAS path and the dbt table path behave identically.
        engine.write_delta(loc, data, "overwrite", overwrite_schema=True,
                           partition_by=partition_cols or None, storage_options=self.so,
                           cur=self.cursor)
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
        err = _create_target_error(rel)
        if err:
            raise ValueError(err)
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
            # Fence the read-modify-write to the version read at statement start (pin the delta_scan to
            # vB, commit via overwrite_if_unchanged) so a writer that commits during this rewrite fails
            # the run loudly instead of being silently clobbered — same CAS as the simple-predicate path.
            vB = engine.table_version(loc, self.so)
            loc_sql = loc.replace("'", "''")
            tmp = f"__duckrun_del_{abs(hash(loc)) & 0xFFFFFFFF}"
            self.cursor.execute(
                f'create or replace temp table "{tmp}" as '
                f"select * from delta_scan('{loc_sql}', version => {vB}) where ({where}) is not true"
            )
            try:
                keep = self.cursor.sql(f'select * from "{tmp}"')
                engine.overwrite_if_unchanged(loc, keep, read_version=vB, overwrite_schema=True,
                                              storage_options=self.so)
            finally:
                self.cursor.execute(f'drop table if exists "{tmp}"')
            return
        # Route through engine.delete_rows pinned to the version read at statement start — the SAME
        # snapshot-fenced path (read_version → load_as_version, OCC over (vB, HEAD], post-op
        # maintenance) every duckrun mutation uses; a raw delta-rs delete() at HEAD would skip the
        # pin and the maintenance.
        engine.delete_rows(
            loc, where,
            read_version=engine.table_version(loc, self.so),
            storage_options=self.so,
            cur=self.cursor,
        )

    def _update(self, m, rel, schema, loc) -> None:
        # Split SET / WHERE structurally (quote/paren/CASE-aware) so a literal containing `where`
        # can't cut the statement in the wrong place.
        body = m.group("body")
        widx = _find_top_level(body, _TOP_WHERE)
        if widx == -1:
            set_clause, where = body, None
        else:
            set_clause = body[:widx]
            where = body[widx + len("where"):].strip().rstrip(";").strip() or None
        updates = {}
        for assign in _split_top_level_commas(set_clause):
            col, _, expr = assign.partition("=")
            updates[col.strip().strip('"')] = expr.strip()
        # Validate SET targets against the real schema BEFORE routing to delta_rs: dt.update() silently
        # accepts an unknown column, writing a no-op commit that changes nothing (a typo'd column name
        # would advance the log while doing nothing). Fail loud with no commit, like a bad SELECT column.
        loc_sql = loc.replace("'", "''")
        target_cols = list(self.cursor.sql(f"select * from delta_scan('{loc_sql}') limit 0").columns)
        by_lower = {c.lower() for c in target_cols}
        unknown = [c for c in updates if c.lower() not in by_lower]
        if unknown:
            raise ValueError(
                f"UPDATE on {loc!r} sets unknown column(s) {unknown}; table columns are {target_cols}"
            )
        if where is None or (where and _PREDICATE_SUBQUERY.search(where)) or \
                any(_PREDICATE_SUBQUERY.search(e) for e in updates.values()):
            # Route to a DuckDB-evaluated fenced overwrite (instead of delta_rs's dt.update) in two cases:
            #  * a subquery in the predicate (`… where id in (select …)`) OR a SET expression (`set g =
            #    (select max(id) from ref)`) — delta_rs's update() hits a datafusion "not implemented"
            #    panic (pyo3 PanicException) on either; DuckDB can evaluate both.
            #  * a predicate-less full-table UPDATE (`update t set i = i + 1`) — delta_rs 1.5.0 silently
            #    updates only SOME rows of a multi-file table when the predicate references no column
            #    (None / a constant both mis-prune to a broken fast path); a column-referencing predicate
            #    is fine. A no-WHERE update rewrites every row anyway, so the full overwrite is equal work.
            # Compute the updated rows there — `CASE WHEN (pred) THEN <expr> ELSE col END` per SET column
            # via SELECT * REPLACE (no predicate → apply <expr> to every row) — and commit a fenced full
            # overwrite, mirroring _delete's fallback. Fenced to vB so a
            # concurrent commit fails loud, not clobbered.
            logger.debug(
                f"duckrun: UPDATE on {loc!r} references a subquery (predicate or SET expr); delta_rs "
                "can't evaluate it, falling back to a DuckDB-evaluated fenced full overwrite (slower)."
            )
            vB = engine.table_version(loc, self.so)
            replaces = ", ".join(
                (f'CASE WHEN ({where}) THEN ({expr}) ELSE "{col}" END AS "{col}"' if where
                 else f'({expr}) AS "{col}"')
                for col, expr in updates.items()
            )
            tmp = f"__duckrun_upd_{abs(hash(loc)) & 0xFFFFFFFF}"
            self.cursor.execute(
                f'create or replace temp table "{tmp}" as '
                f"select * replace ({replaces}) from delta_scan('{loc_sql}', version => {vB})"
            )
            try:
                new = self.cursor.sql(f'select * from "{tmp}"')
                engine.overwrite_if_unchanged(loc, new, read_version=vB, overwrite_schema=True,
                                              storage_options=self.so)
            finally:
                self.cursor.execute(f'drop table if exists "{tmp}"')
            return
        # Same snapshot-fenced path (read_version → OCC) every duckrun mutation uses.
        engine.update_rows(
            loc, updates, where,
            read_version=engine.table_version(loc, self.so),
            storage_options=self.so,
            cur=self.cursor,
        )

    def _insert_select(self, m, rel, schema, loc) -> None:
        body = m.group("body")
        if self._with_clause:  # `WITH … INSERT INTO t SELECT …`: re-attach the CTE to the body
            body = f"{self._with_clause} {body}"
        if m.group("byname"):
            # `INSERT INTO t BY NAME SELECT …`: align the source's OWN columns to the target by name
            # (any target column the source doesn't name becomes NULL). Read the source column names
            # and treat them as the written column list — _project_onto_schema already maps by name.
            provided = list(self.cursor.sql(f"select * from ({body}) limit 0").columns)
        else:
            cols = m.group("cols")
            provided = self._provided(cols) if cols else None
        # Always project onto the target schema — a column list maps by name, no list maps
        # positionally. Routing both through _append_projected gives one place for the intentional
        # type alignment AND the lossy-numeric-narrowing guard (so `insert … select 3.9` is caught too).
        self._append_projected(loc, provided, f"({body})")

    def _insert_values(self, m, rel, schema, loc) -> None:
        # `insert into <rel> [(<cols>)] values (...)`: the literals supply every target column when
        # no list is given, in order; otherwise the named columns.
        cols = m.group("cols")
        provided = self._provided(cols) if cols else None
        body = m.group("body")
        derived = f"(values {body})"
        # Can DuckDB self-type the VALUES columns? Probe on a THROWAWAY connection: the tuples are pure
        # literals (no table refs), and a binder error would otherwise abort self.cursor's transaction.
        try:
            duckdb.connect().sql(f"select * from {derived} limit 0")
        except duckdb.NotImplementedException:
            # A column mixes literal types DuckDB won't self-combine across rows (e.g. 'inf' next to
            # 0.0 → "Cannot combine types VARCHAR and DECIMAL") — exactly what a native INSERT resolves
            # by casting each row to the TARGET column type. Replay the tuples through a target-typed
            # temp so each literal casts per-row (native INSERT semantics), then append that relation.
            # (The clean-typing fast path below keeps the lossy-numeric-narrowing guard intact.)
            self._insert_values_via_typed_temp(loc, provided, body)
            return
        self._append_projected(loc, provided, derived)

    def _insert_values_via_typed_temp(self, loc, provided, body: str) -> None:
        """Append a VALUES list whose columns DuckDB can't self-type by first materializing it into a
        temp table shaped like the target (the written columns, in the target's declared types). DuckDB's
        own INSERT then casts each literal to its destination type per-row — the way a native INSERT does
        — and the typed temp becomes the projected append source (unsupplied columns still NULL-fill)."""
        loc_sql = loc.replace("'", "''")
        template = self.cursor.sql(f"select * from delta_scan('{loc_sql}') limit 0")
        by_lower = {c.lower(): (c, str(t)) for c, t in zip(template.columns, template.types)}
        if provided is None:  # positional → every target column, in order
            cols_types = list(zip(list(template.columns), [str(t) for t in template.types]))
        else:
            cols_types = []
            for c in provided:
                hit = by_lower.get(c.lower())
                if hit is None:
                    raise ValueError(
                        f"INSERT into {loc!r} names unknown column {c!r}; columns are "
                        f"{list(template.columns)}")
                cols_types.append(hit)
        coldefs = ", ".join(f'"{c}" {t}' for c, t in cols_types)
        collist = ", ".join(f'"{c}"' for c, _ in cols_types)
        tmp = f"__duckrun_vals_{abs(hash(loc)) & 0xFFFFFFFF}"
        self.cursor.execute(f'create or replace temp table "{tmp}" ({coldefs})')
        try:
            self.cursor.execute(f'insert into "{tmp}" ({collist}) values {body}')
            self._append_projected(loc, provided, f'"{tmp}"')
        finally:
            self.cursor.execute(f'drop table if exists "{tmp}"')

    @staticmethod
    def _provided(cols: str) -> List[str]:
        return [c.strip().strip('"') for c in cols.split(",")]

    def _project_onto_schema(self, loc, provided, derived: str):
        """Project a ``derived`` table (a ``(values …)`` tuple list or a ``(select …)`` subquery)
        onto the FULL target schema at ``loc`` and return the projected DuckDB relation: supplied
        columns come from ``derived`` (positional when ``provided`` is None), any unsupplied target
        column is a typed NULL, and every projected column is cast to the target column's type so the
        written Arrow schema matches the table exactly (what a plain SQL INSERT does, and it stops a
        literal wider than the column from forcing delta_rs to add a new writer feature on write).

        Shared by the plain append (INSERT … SELECT/VALUES) and the REPLACE WHERE body so both align
        source shape to the table identically — same positional/by-name rule, same lossy-numeric
        guard — instead of REPLACE WHERE matching delta_rs by name and rejecting positional columns
        that INSERT accepts."""
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

        # Arity must match the columns being bound: a SELECT/VALUES source wider than the target (or
        # than the explicit column list) would otherwise be SILENTLY TRUNCATED by the `v(<names>)`
        # alias below (DuckDB drops surplus source columns) — data loss. The VALUES path already errors
        # on too-few names; this makes the SELECT path (and the too-many case) equally loud.
        src_width = len(self.cursor.sql(f"select * from {derived} limit 0").columns)
        if src_width != len(provided):
            raise ValueError(
                f"INSERT source provides {src_width} column(s) but {len(provided)} are being written "
                f"({', '.join(provided)}); the column counts must match — surplus or missing source "
                f"columns are never silently dropped or NULL-filled on this path")

        quoted = ", ".join('"' + c + '"' for c in provided)
        inner = f"{derived} v({quoted})"
        self._reject_lossy_numeric_narrowing(inner, provided, dict(zip(target_cols, target_types)))
        exprs = [
            f'cast(v."{col}" as {typ}) as "{col}"' if col in provided_set
            else f'cast(null as {typ}) as "{col}"'
            for col, typ in zip(target_cols, target_types)
        ]
        return self.cursor.sql(f"select {', '.join(exprs)} from {inner}")

    def _append_projected(self, loc, provided, derived: str) -> None:
        """Append a ``derived`` table to the Delta table at ``loc``, projected onto its full schema
        by :meth:`_project_onto_schema`."""
        data = self._project_onto_schema(loc, provided, derived)
        # Read-then-append to the SAME table (`insert into a select … from a`) is fenced to the
        # version read — the automatic append_if_unchanged — so a concurrent commit fails loud
        # instead of appending stale-derived rows. A plain append of new data (a VALUES list, or a
        # SELECT over other tables) references nothing of the target and stays unfenced.
        read_version = (engine.table_version(loc, self.so)
                        if self._reads_target(derived, loc) else None)
        engine.write_delta(loc, data, "append", storage_options=self.so, cur=self.cursor,
                           read_version=read_version)

    def _reads_target(self, derived: str, loc: str) -> bool:
        """True if the append's source query references the target table (a read-modify-append on one
        relation), so the append must be snapshot-fenced. A VALUES list or a SELECT over other tables
        references nothing of the target and stays a plain, unfenced append.

        Detection is by name: the target is reached through a ``delta_scan`` VIEW, which DuckDB's
        ``get_table_names`` doesn't report, and a path compare is unreliable (8.3 short names, abfss
        URLs). The target's bare name appears in EVERY real reference — bare, ``schema.t`` or
        ``cat.schema.t`` all contain it — so a word-boundary match on the comment/literal-stripped
        source never MISSES a self-reference; it can only over-fence the rare ``… AS <name>`` alias,
        which is the safe direction (an unnecessary fence fails loud, it never silently loses a write)."""
        target = loc.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
        body = _blank_string_literals(_strip_comments(derived))
        if re.search('"' + re.escape(target) + '"', body):            # exact quoted identifier
            return True
        return re.search(r"\b" + re.escape(target) + r"\b", body, re.IGNORECASE) is not None

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
        # widened schema. Locate the DEFAULT/NOT NULL/NULL boundary structurally (quote/paren-aware
        # via _find_top_level), NOT by a quote-blind re.split — a string DEFAULT containing ` not null`
        # (`default 'not null'`) would otherwise truncate the type. Scanning left-to-right, the first
        # top-level tail keyword is hit before its own `null`, so `not null` is handled correctly.
        deftext = m.group("def")
        tidx = _find_top_level(deftext, _ALTER_TAIL)
        coltype = (deftext if tidx == -1 else deftext[:tidx]).strip() or "VARCHAR"
        # Fence this full rewrite to the version read at statement start: a writer that commits
        # during the (potentially long) rewrite must fail the run loudly, not be silently clobbered.
        vB = engine.table_version(loc, self.so)
        loc_sql = loc.replace("'", "''")
        data = self.cursor.sql(
            f'select *, cast(null as {coltype}) as "{col}" from delta_scan(\'{loc_sql}\', version => {vB})'
        )
        engine.overwrite_if_unchanged(loc, data, read_version=vB, overwrite_schema=True,
                                      storage_options=self.so)

    def _alter_drop(self, m, rel, schema, loc) -> None:
        """`alter table <t> drop column <c>`: delta_rs has no in-place column drop, so rewrite the
        table WITHOUT the column (DuckDB ``SELECT * EXCLUDE``) under overwrite_schema, fenced to the
        version read — the same mechanism as ADD COLUMN."""
        col = m.group("col").strip().strip('"')
        vB = engine.table_version(loc, self.so)
        loc_sql = loc.replace("'", "''")
        data = self.cursor.sql(
            f'select * exclude ("{col}") from delta_scan(\'{loc_sql}\', version => {vB})')
        engine.overwrite_if_unchanged(loc, data, read_version=vB, overwrite_schema=True,
                                      storage_options=self.so)

    def _alter_rename(self, m, rel, schema, loc) -> None:
        """`alter table <t> rename column <old> to <new>`: rewrite with the column renamed (DuckDB
        ``SELECT * RENAME``) under overwrite_schema, fenced to the version read."""
        old = m.group("old").strip().strip('"')
        new = m.group("new").strip().strip('"')
        vB = engine.table_version(loc, self.so)
        loc_sql = loc.replace("'", "''")
        data = self.cursor.sql(
            f'select * rename ("{old}" as "{new}") from delta_scan(\'{loc_sql}\', version => {vB})')
        engine.overwrite_if_unchanged(loc, data, read_version=vB, overwrite_schema=True,
                                      storage_options=self.so)

    def _insert_schema_evolution(self, m, rel, schema, loc) -> None:
        """`insert with schema evolution into <t> <select|values>` (Spark/Delta spelling): append with
        delta_rs ``schema_mode='merge'`` — columns the source has that the table lacks are ADDED
        (existing rows get NULL), matched by name, instead of the plain append's project-to-target
        (which silently drops unknown columns). Auto-fenced when the body reads the target."""
        body = m.group("body")
        data = self.cursor.sql(body)
        read_version = engine.table_version(loc, self.so) if self._reads_target(body, loc) else None
        engine.write_delta(loc, data, "append", merge_schema=True, read_version=read_version,
                           storage_options=self.so, cur=self.cursor)

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
        # A WHEN NOT MATCHED BY SOURCE merge (full-sync) anti-joins the WHOLE target, so collecting the
        # source whole to compute target-pruning stats builds a non-spillable hash → OOM on a large
        # source. Stream it instead — the raw-SQL equivalent of the builder's streamed_exec=True.
        by_source = any(c.get("clause") == "not_matched_by_source" for c in clauses)
        engine.merge_delta_clauses(
            loc,
            source,
            cond,
            clauses,
            # Pin the target to the version we read now (single statement) — same as the builder's
            # .merge(), which captures the version at call time.
            read_version=engine.table_version(loc, self.so),
            streamed_exec=by_source,
            storage_options=self.so,
            cur=self.cursor,
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
        if not loc or not self._exists(loc) or is_dropped(self.cursor, loc, self.so):
            # Not a LIVE duckrun table (never existed, native, or already tombstoned) → pass through so
            # DuckDB applies plain DROP / DROP IF EXISTS semantics: a plain `DROP TABLE <gone>` raises
            # (the delta_scan view was already unregistered on the first drop), IF EXISTS is a no-op.
            # Without this, a tombstoned table still has files at loc so a second plain DROP would
            # wrongly succeed (re-tombstone) instead of erroring like SQL requires.
            return False
        # Fence the tombstone write to the version observed at drop time: a drop racing a live writer
        # should fail loud (the writer's commit lands first → CommitFailedError), not silently
        # tombstone over it.
        vB = engine.table_version(loc, self.so)
        tombstone = self.cursor.sql(f"select true as {TOMBSTONE_COLUMN}")
        engine.overwrite_if_unchanged(loc, tombstone, read_version=vB, overwrite_schema=True,
                                      storage_options=self.so)
        self.cursor.execute(f"drop view if exists {rel}")
        return True

    def _vacuum(self, m) -> bool:
        """`vacuum <table>`: Delta maintenance via DuckDB's native VACUUM verb — compact small files
        (delta_rs ``optimize.compact``, committed dataChange=false so it's concurrency-safe) then
        vacuum files tombstoned past the retention window. No snapshot fence is needed: compaction
        doesn't change data and vacuum only removes already-dead files. Returns False (pass through to
        DuckDB) if <table> isn't a duckrun-managed Delta table, so a native DuckDB VACUUM still works."""
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False
        engine.optimize(loc, storage_options=self.so, cur=self.cursor)
        engine.vacuum(loc, storage_options=self.so)
        return True

    def _restore(self, m) -> bool:
        """`restore table <t> to version as of <n>` / `to timestamp as of '<ts>'` (the Spark/Delta
        verb): roll the table back via delta_rs ``DeltaTable.restore`` — a NEW commit on top of
        history, so the restore is itself revertible. Returns False (pass through) if <t> isn't a
        duckrun-managed Delta table."""
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False
        target = (int(m.group("version")) if m.group("version") is not None
                  else datetime.fromisoformat(m.group("ts")))
        engine.restore_to_version(loc, target, storage_options=self.so)
        self._refresh_view(rel, schema, loc)
        return True

    def _insert_replace_where(self, m) -> bool:
        """`insert into <t> replace where <pred> <select|values>`: delta_rs replaceWhere — atomically
        overwrite ONLY the rows matching <pred> with the body's rows, as ONE fenced commit (no torn
        delete-then-append window). Pinned to the version read (CAS, ``max_commit_retries=0``): a
        concurrent commit since then fails it loud. <pred> is a CAST-free expression over the target's
        columns (delta_rs/datafusion). Partition columns are preserved. Returns False (pass through) if
        <t> isn't a live Delta table."""
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False
        rest = m.group("rest")
        # Split predicate | body at the first TOP-LEVEL select/values/with — a keyword inside a string
        # literal, or a subquery in the predicate, sits at depth>0 and can't be the boundary.
        bi = _find_top_level(rest, _REPLACE_BODY_KW)
        if bi < 0:
            raise ValueError("INSERT … REPLACE WHERE requires a SELECT/VALUES body")
        predicate = rest[:bi].strip()
        body = rest[bi:].strip()
        if not predicate:
            raise ValueError("INSERT … REPLACE WHERE requires a <predicate> before the body")
        # Project the body onto the target schema (positional, like INSERT) BEFORE handing it to
        # delta_rs replaceWhere — which otherwise matches by name and rejects positional columns
        # (SELECT 501, 2, 'x') that a plain INSERT accepts. Same projection contract as the append.
        data = self._project_onto_schema(loc, None, f"({body})")
        try:
            pcols = list(engine._delta_table(loc, self.so).metadata().partition_columns or [])
        except Exception:
            pcols = []
        engine.replace_where(
            loc, data, predicate,
            read_version=engine.table_version(loc, self.so),
            partition_by=(pcols or None),
            storage_options=self.so, cur=self.cursor)
        self._refresh_view(rel, schema, loc)
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
    if not head.startswith(("delete", "update", "insert", "create", "alter", "drop", "merge",
                            "vacuum", "restore")):
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
