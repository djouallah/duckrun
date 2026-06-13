"""A Delta-Lake-shaped ``DeltaTable.merge(...)`` upsert builder.

Mirrors the Spark/Delta ``DeltaTable`` merge API and runs on the adapter's
:func:`engine.merge_delta`, so it inherits the cgroup-aware spill caps and post-merge
compaction/vacuum. This is the upsert path — ``saveAsTable`` deliberately does not merge.

Supported clauses map onto delta-rs's ``when_matched_update_all`` / ``when_matched_update`` /
``when_not_matched_insert_all``. Shapes delta-rs can't express (arbitrary value maps, aliases
other than ``target``/``source``, update-only merges) raise a clear error rather than being
silently dropped — the same posture the adapter takes for unsupported merge features.
"""
import re
from typing import Dict, List, Optional

from dbt.adapters.duckrun import engine

_EQ = re.compile(r"^\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*$", re.IGNORECASE)


def _parse_condition(condition: str):
    """Split a merge condition into (unique_key, extra_predicates).

    Each ``AND``-separated term of the form ``target.X = source.X`` (same column, either order)
    contributes ``X`` as a merge key; any other term is passed through as an extra predicate
    (delta-rs ANDs it into the merge condition). Raises if no key equality is found or an alias
    other than ``target``/``source`` is used.
    """
    keys: List[str] = []
    predicates: List[str] = []
    for term in re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE):
        term = term.strip()
        if not term:
            continue
        m = _EQ.match(term)
        if m:
            a_alias, a_col, b_alias, b_col = (g.lower() for g in m.groups())
            aliases = {a_alias, b_alias}
            if aliases == {"target", "source"} and a_col == b_col:
                keys.append(m.group(2))  # original-case column
                continue
            if not aliases <= {"target", "source"}:
                raise ValueError(
                    f"merge condition must use 'target'/'source' aliases, got: {term!r}"
                )
        # A non-key term (range predicate, target.a = source.b, etc.) → pass through.
        predicates.append(term)
    if not keys:
        raise ValueError(
            "merge condition has no 'target.<col> = source.<col>' key equality; "
            f"got: {condition!r}"
        )
    return keys, predicates


class DeltaMergeBuilder:
    def __init__(self, table: "DeltaTable", source, condition: str):
        self._table = table
        self._source = source  # DataFrame
        self._keys, self._predicates = _parse_condition(condition)
        self._matched = None       # None | ("all", cond) | ("cols", [cols], cond)
        self._not_matched = None   # None | ("all", cond)

    def whenMatchedUpdateAll(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        self._matched = ("all", None, condition)
        return self

    def whenMatchedUpdate(self, set: Dict[str, str], condition: Optional[str] = None
                          ) -> "DeltaMergeBuilder":
        cols = []
        for col, expr in set.items():
            norm = str(expr).strip().lower()
            if norm not in (f"source.{col.lower()}", f'"source"."{col.lower()}"'):
                raise ValueError(
                    f"whenMatchedUpdate only supports plain column copies (set={{'{col}': "
                    f"'source.{col}'}}); arbitrary expression {expr!r} is not supported."
                )
            cols.append(col)
        self._matched = ("cols", cols, condition)
        return self

    def whenNotMatchedInsertAll(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        self._not_matched = ("all", condition)
        return self

    def execute(self) -> None:
        if self._matched is None and self._not_matched is None:
            raise ValueError("merge has no clauses; add whenMatchedUpdate*/whenNotMatchedInsertAll.")
        if self._not_matched is None:
            raise ValueError(
                "update-only merge (no whenNotMatchedInsertAll) is not supported in v1; "
                "add .whenNotMatchedInsertAll() for an upsert, or omit the matched clause for "
                "insert-only."
            )

        insert_only = self._matched is None
        update_columns = None
        update_condition = None
        if not insert_only:
            kind, cols, update_condition = self._matched
            if kind == "cols":
                update_columns = cols
        insert_condition = self._not_matched[1]

        engine.merge_delta(
            self._table.path,
            self._source.relation,
            self._keys if len(self._keys) > 1 else self._keys[0],
            insert_only=insert_only,
            update_columns=update_columns,
            predicates=self._predicates or None,
            update_condition=update_condition,
            insert_condition=insert_condition,
            storage_options=self._table.storage_options,
            compaction_threshold=self._table.compaction_threshold,
        )
        self._table._refresh_view()


class DeltaTable:
    """A handle to a Delta table for merging. Build with :meth:`forName` or :meth:`forPath`."""

    def __init__(self, session, path: str, schema: Optional[str] = None,
                 table: Optional[str] = None):
        self._session = session
        self.path = path
        self.storage_options = session.storage_options
        self.compaction_threshold = session.compaction_threshold
        self._schema = schema
        self._table = table

    @classmethod
    def forName(cls, session, name: str) -> "DeltaTable":
        schema, table = session.resolve(name)
        return cls(session, session.table_path(schema, table), schema, table)

    @classmethod
    def forPath(cls, session, path: str) -> "DeltaTable":
        return cls(session, path)

    def merge(self, source, condition: str) -> DeltaMergeBuilder:
        return DeltaMergeBuilder(self, source, condition)

    def _refresh_view(self):
        # Only a forName() table maps to a registered view; forPath() has no name to refresh.
        if self._schema is not None and self._table is not None:
            self._session._register_view(self._schema, self._table)
