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
    merge … / update … from / delete … using / multi-stmt   NOT handled here — the connection API
                                                             (session.sql) rejects them with a clear
                                                             error; the dbt path never emits them.
"""
import re
from typing import List, Optional, Tuple

from . import engine

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
    except Exception:
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
_INSERT_SELECT = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)\s*(?:\((?P<cols>[^)]*)\))?\s+(?P<body>select\b.*)",
    re.I | re.S,
)
_INSERT_VALUES = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)\s*(?:\((?P<cols>[^)]*)\))?\s*values\s+(?P<body>\(.+)",
    re.I | re.S,
)
_DELETE = re.compile(
    r"\s*delete\s+from\s+(?P<rel>.+?)(?:\s+where\s+(?P<where>.+))?\s*;?\s*", re.I | re.S
)
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
# `create temp/temporary table …` is DuckDB-local scratch by design and must NEVER be captured —
# checked first in try_handle so it always passes through to native DuckDB (the invariant: only
# CREATE TEMP TABLE is native; every other CREATE TABLE is Delta-backed).
_CREATE_TEMP_RE = re.compile(r"\s*create\s+(?:or\s+replace\s+)?(?:temp|temporary)\b", re.I)
# CTE/whitespace handling: a leading `with …` block followed by a top-level INSERT/UPDATE/DELETE.
# leading `\b` is load-bearing: _find_top_level tries this at every depth-0 index, so without it the
# verb would match inside an identifier (e.g. `update` within `last_update`).
_LEADING_WITH = re.compile(r"\s*with\b", re.I)
_DRIVING_DML = re.compile(r"\b(?:insert\s+into|update|delete\s+from)\b", re.I)


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


def _find_top_level(s: str, pattern) -> int:
    """Index of the first ``pattern`` match at paren-depth 0 and outside quotes, else -1.

    Lets us tell a top-level clause (the ``FROM`` of ``UPDATE … FROM``, the verb after a leading
    ``WITH``) from the same keyword nested in a subquery, without a full SQL parser."""
    depth, quote, i, n = 0, None, 0, len(s)
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
        elif depth == 0 and pattern.match(s, i):
            return i
        i += 1
    return -1


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


def _split_relation(rel: str) -> Tuple[Optional[str], Optional[str]]:
    """`"db"."schema"."tbl"` / `schema.tbl` / `tbl` -> (schema, identifier), quotes stripped."""
    parts = [p.strip().strip('"') for p in rel.strip().split(".")]
    if not parts or not parts[-1]:
        return None, None
    identifier = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    return schema, identifier


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
        m = _fullmatch(_INSERT_SELECT, sql)
        if m:
            return self._mutate(m, self._insert_select)
        m = _fullmatch(_INSERT_VALUES, sql)
        if m:
            return self._mutate(m, self._insert_values)
        m = _fullmatch(_DELETE, sql)
        if m:
            return self._mutate(m, self._delete)
        m = _fullmatch(_UPDATE, sql)
        if m:
            return self._mutate(m, self._update)
        m = _fullmatch(_ALTER_ADD, sql)
        if m:
            return self._mutate(m, self._alter_add)
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
        # dbt/cursor path (no default_schema): keep the ORIGINAL narrow interception — only a plain
        # `create table … as select …` routes to Delta. The wider forms (`or replace`, a CTE or a
        # parenthesised body) are a connection-API affordance; on the dbt path they must stay native
        # so dbt keeps owning the relation. dbt-internal CTAS like store_failures' `create table … as
        # (select …)` is a real TABLE dbt later drops/recreates — turning it into a delta_scan VIEW
        # breaks that ("Existing object … is of type View, trying to drop type Table").
        if self.default_schema is None and (
            m.group("orrep") or not re.match(r"select\b", m.group("body").lstrip(), re.I)
        ):
            return False
        live = self._exists(loc) and not is_dropped(self.cursor, loc, self.so)
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
        engine._delta_table(loc, self.so).delete(predicate=where.strip() if where else None)

    def _update(self, m, rel, schema, loc) -> None:
        updates = {}
        for assign in _split_top_level_commas(m.group("set")):
            col, _, expr = assign.partition("=")
            updates[col.strip().strip('"')] = expr.strip()
        where = m.group("where")
        engine._delta_table(loc, self.so).update(
            updates=updates, predicate=where.strip() if where else None
        )

    def _insert_select(self, m, rel, schema, loc) -> None:
        body = m.group("body")
        if self._with_clause:  # `WITH … INSERT INTO t SELECT …`: re-attach the CTE to the body
            body = f"{self._with_clause} {body}"
        cols = m.group("cols")
        if cols:  # `insert into t (a, b) select …` → project the query onto the named columns
            self._append_projected(loc, self._provided(cols), f"({body})")
        else:  # column count/order already matches the target → append as-is
            engine.write_delta(loc, self.cursor.sql(body), "append", storage_options=self.so)

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
        exprs = [
            f'cast(v."{col}" as {typ}) as "{col}"' if col in provided_set
            else f'cast(null as {typ}) as "{col}"'
            for col, typ in zip(target_cols, target_types)
        ]
        data = self.cursor.sql(f"select {', '.join(exprs)} from {inner}")
        engine.write_delta(loc, data, "append", storage_options=self.so)

    def _alter_add(self, m, rel, schema, loc) -> None:
        col = m.group("col").strip().strip('"')
        # Keep only the column type (drop any DEFAULT/NULL clause); add it as an all-null column by
        # rewriting the table with overwrite_schema so delta_rs accepts the widened schema.
        coltype = re.split(r"\s+default\b|\s+null\b", m.group("def"), flags=re.I)[0].strip() or "VARCHAR"
        loc_sql = loc.replace("'", "''")
        data = self.cursor.sql(
            f'select *, cast(null as {coltype}) as "{col}" from delta_scan(\'{loc_sql}\')'
        )
        engine.write_delta(loc, data, "overwrite", overwrite_schema=True, storage_options=self.so)

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


def handle(cursor, root_path, storage_options, sql: str, default_schema=None) -> bool:
    """Apply ``sql`` to Delta if it's a DML form targeting a duckrun-managed relation, using
    ``cursor`` to evaluate any SELECT body and to (re)create the ``delta_scan`` view.

    Every handled form goes through delta_rs (``engine.write_delta`` / ``DeltaTable.delete`` /
    ``.update``), which carries ``storage_options`` and so works on local AND abfss/OneLake stores.
    ``default_schema`` resolves an unqualified table name (the connection API has a current
    database; the dbt path always renders fully-qualified names so passes None).
    Returns True if handled (the caller must NOT also run it on DuckDB), False to pass through —
    anything unrecognized, or (for the mutate forms) a target that isn't a Delta table.
    """
    if not root_path:
        return False
    sql = _strip_leading(sql)  # so leading comments/whitespace don't hide the verb
    with_clause, body = _split_leading_with(sql)  # peel a leading `WITH …` off an INSERT/etc.
    # Cheap pre-filter: only the candidate DML verbs.
    head = body[:7].lower()
    if not head.startswith(("delete", "update", "insert", "create", "alter", "drop")):
        return False
    dml = _DeltaDML(cursor, root_path, storage_options, default_schema)
    dml._with_clause = with_clause
    return dml.try_handle(body)
