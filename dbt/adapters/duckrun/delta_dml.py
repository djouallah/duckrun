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

The seed loader's own SQL (``create table <t> (<col defs>)``, ``insert ... values``, ``COPY``) does
not match these forms and is never intercepted; duckrun's own materializations emit ``create ...
view`` (not ``table``), so they pass through untouched too.
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
_CREATE_AS = re.compile(
    r"\s*create\s+table\s+(?:if\s+not\s+exists\s+)?(?P<rel>.+?)\s+as\s+(?P<body>select\b.*)",
    re.I | re.S,
)
_INSERT_SELECT = re.compile(
    r"\s*insert\s+into\s+(?P<rel>.+?)\s+(?P<body>select\b.*)", re.I | re.S
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
        m = _fullmatch(_CREATE_AS, sql)
        if m and "__duckrun" not in m.group("rel"):
            return self._create_as(m)
        m = _fullmatch(_INSERT_SELECT, sql)
        if m:
            return self._mutate(m, self._insert_select)
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

    # -- create table <rel> as <select>: always materialize as a duckrun Delta table -----------
    def _create_as(self, m) -> bool:
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc:
            return False
        data = self.cursor.sql(m.group("body"))
        # overwrite_schema so this replaces a prior table (or a drop-tombstone) wholesale — a live
        # table is recreated with the real schema, clearing any tombstone marker.
        engine.write_delta(loc, data, "overwrite", overwrite_schema=True, storage_options=self.so)
        self._refresh_view(rel, schema, loc)
        return True

    # -- forms that only apply when a Delta table already exists at the target ------------------
    def _mutate(self, m, op) -> bool:
        rel = m.group("rel").strip()
        schema, identifier, loc = self._resolve(rel)
        if not loc or not self._exists(loc):
            return False  # native relation (e.g. the test's `fact`/`seed`) -> let DuckDB handle it
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
        data = self.cursor.sql(m.group("body"))
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
    # Cheap pre-filter: only the candidate DML verbs.
    head = sql.lstrip()[:7].lower()
    if not head.startswith(("delete", "update", "insert", "create", "alter", "drop")):
        return False
    return _DeltaDML(cursor, root_path, storage_options, default_schema).try_handle(sql)
