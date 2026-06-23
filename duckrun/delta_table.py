"""A Delta-Lake-shaped ``DeltaTable.merge(...)`` upsert builder.

Mirrors the Delta ``DeltaTable`` merge API and runs on the adapter's
:func:`engine.merge_delta_clauses`, so it inherits the cgroup-aware spill caps and post-merge
compaction/vacuum. This is the upsert path — ``saveAsTable`` deliberately does not merge.

Exposes the FULL delta-rs ``TableMerger`` surface: ``whenMatchedUpdateAll`` / ``whenMatchedUpdate``
(arbitrary expressions) / ``whenMatchedDelete`` / ``whenNotMatchedInsertAll`` /
``whenNotMatchedInsert`` / ``whenNotMatchedBySourceUpdate`` / ``whenNotMatchedBySourceDelete``,
any number of clauses applied in order. The merge ``condition`` must reference the literal
``target``/``source`` aliases (validated up front); the only shape delta-rs can't express —
``MERGE … RETURNING`` — is simply not part of this API.
"""
import re
from typing import Dict, List, Optional

from dbt.adapters.duckrun import delta_dml, engine

# delta-spark addresses the source parquet as ``"parquet.`<path>`"``; we accept that and a bare path.
_PARQUET_IDENT = re.compile(r"^\s*parquet\s*\.\s*`(?P<path>.+)`\s*$", re.IGNORECASE | re.DOTALL)


def _parse_parquet_identifier(identifier: str) -> str:
    """Pull the directory out of a delta-spark ``"parquet.`<path>`"`` identifier; a bare path
    (no ``parquet.`…``` wrapper) is returned as-is."""
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValueError("convertToDelta identifier must be a non-empty path string.")
    m = _PARQUET_IDENT.match(identifier)
    return m.group("path").strip() if m else identifier.strip()

# The merge condition must reference the literal ``target``/``source`` aliases; that alias check is
# shared with the raw-SQL MERGE handler (delta_dml.validate_merge_condition), so the DataFrame
# builder and conn.sql("MERGE …") accept exactly the same conditions. Both feed one ordered list of
# clauses to engine.merge_delta_clauses — the full delta-rs TableMerger surface.


class DeltaMergeBuilder:
    def __init__(self, table: "DeltaTable", source, condition: str,
                 read_version: Optional[int] = None):
        self._table = table
        self._source = source  # DataFrame
        delta_dml.validate_merge_condition(condition)
        self._condition = condition  # handed to delta_rs verbatim as the merge predicate
        self._clauses: List[dict] = []  # ordered engine.merge_delta_clauses specs
        # Pin the target to this version so OCC validates (vB, HEAD]: pass the same vB you pinned
        # the source read to (forName(...).version() → delta_scan('…', version => vB)) and source +
        # target are ONE snapshot — exactly a single-snapshot MERGE. None merges against HEAD.
        self._read_version = read_version

    def whenMatchedUpdateAll(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        self._clauses.append({"clause": "matched", "action": "update_all", "predicate": condition})
        return self

    def whenMatchedUpdate(self, condition: Optional[str] = None,
                          set: Optional[Dict[str, str]] = None) -> "DeltaMergeBuilder":
        if not set:
            raise ValueError("whenMatchedUpdate requires a non-empty 'set' mapping; "
                             "use whenMatchedUpdateAll() to copy every column.")
        self._clauses.append({"clause": "matched", "action": "update",
                              "updates": {c: str(e) for c, e in set.items()},
                              "predicate": condition})
        return self

    def whenMatchedDelete(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        """``WHEN MATCHED [AND <condition>] THEN DELETE`` — delete matched target rows (e.g. a
        CDC tombstone flag on the source)."""
        self._clauses.append({"clause": "matched", "action": "delete", "predicate": condition})
        return self

    def whenNotMatchedInsertAll(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        self._clauses.append({"clause": "not_matched", "action": "insert_all", "predicate": condition})
        return self

    def whenNotMatchedInsert(self, condition: Optional[str] = None,
                             values: Optional[Dict[str, str]] = None) -> "DeltaMergeBuilder":
        """``WHEN NOT MATCHED [AND <condition>] THEN INSERT (cols) VALUES (<exprs>)`` — ``values`` maps
        each target column to an expression (e.g. ``{"id": "source.id", "name": "upper(source.name)"}``)."""
        if not values:
            raise ValueError("whenNotMatchedInsert requires a non-empty 'values' mapping; "
                             "use whenNotMatchedInsertAll() to insert every column.")
        self._clauses.append({"clause": "not_matched", "action": "insert",
                              "updates": {c: str(e) for c, e in values.items()},
                              "predicate": condition})
        return self

    def whenNotMatchedBySourceUpdate(self, condition: Optional[str] = None,
                                     set: Optional[Dict[str, str]] = None) -> "DeltaMergeBuilder":
        """``WHEN NOT MATCHED BY SOURCE [AND <condition>] THEN UPDATE SET …`` — update target rows the
        source doesn't carry (e.g. mark them inactive)."""
        if not set:
            raise ValueError("whenNotMatchedBySourceUpdate requires a non-empty 'set' mapping.")
        self._clauses.append({"clause": "not_matched_by_source", "action": "update",
                              "updates": {c: str(e) for c, e in set.items()},
                              "predicate": condition})
        return self

    def whenNotMatchedBySourceDelete(self, condition: Optional[str] = None) -> "DeltaMergeBuilder":
        """The "WHEN NOT MATCHED BY SOURCE THEN DELETE" form: delete target rows the source doesn't
        carry. No ``condition`` = delete every unmatched target row (full sync); a predicate string
        scopes the deletion."""
        self._clauses.append({"clause": "not_matched_by_source", "action": "delete",
                              "predicate": condition})
        return self

    def execute(self) -> None:
        self._table._session._require_writable("merge", self._table._catalog)
        if not self._clauses:
            raise ValueError("merge has no clauses; add whenMatched*/whenNotMatched*/"
                             "whenNotMatchedBySource* before execute().")
        # The ordered clause list is the full delta-rs TableMerger surface; delta_rs enforces its own
        # legality rules on combinations, so we don't pre-reject shapes here.
        engine.merge_delta_clauses(
            self._table.path,
            self._source.relation,
            self._condition,
            self._clauses,
            read_version=self._read_version,
            storage_options=self._table.storage_options,
            compaction_threshold=self._table.compaction_threshold,
        )
        self._table._resnapshot()
        self._table._refresh_view()


class DeltaTable:
    """A handle to a Delta table for merging. Build with :meth:`forName` or :meth:`forPath`."""

    def __init__(self, session, path: str, schema: Optional[str] = None,
                 table: Optional[str] = None, catalog: Optional[str] = None,
                 storage_options=None):
        self._session = session
        self.path = path
        # forName resolves the target catalog's storage_options; forPath / forName-in-current-catalog
        # fall back to the current catalog's (the session.storage_options property).
        self.storage_options = storage_options if storage_options is not None else session.storage_options
        self.compaction_threshold = session.compaction_threshold
        self._schema = schema
        self._table = table
        self._catalog = catalog
        # Snapshot isolation: capture the table version ONCE when the handle is taken. Every
        # read-modify-write through this handle (merge/delete/update) is pinned to THIS version
        # (load_as_version) and validated under delta-rs native OCC over (V, HEAD], so a CONFLICTING
        # write that landed after the handle was taken fails the mutation loudly — "the version I
        # read == the version I commit against". A handle to a not-yet-existing table has no version
        # (None); a mutation on it then raises the engine's clear "requires read_version" error.
        self._read_version = (
            engine.table_version(self.path, self.storage_options)
            if engine.table_exists(self.path, self.storage_options) else None
        )

    @classmethod
    def forName(cls, session, name: str) -> "DeltaTable":
        catalog, schema, table = session._resolve(name)
        return cls(session, session._table_path(schema, table, catalog), schema, table,
                   catalog, session._catalog_storage_options(catalog))

    @classmethod
    def forPath(cls, session, path: str) -> "DeltaTable":
        return cls(session, path)

    @classmethod
    def convertToDelta(cls, session, identifier: str, partitionSchema=None) -> "DeltaTable":
        """Convert an existing parquet directory to Delta IN PLACE and return a handle to it
        (delta-spark ``DeltaTable.convertToDelta``). The conversion is **zero-copy** — a
        ``_delta_log`` is written over the parquet, the data files are not rewritten.

        ``identifier`` is the delta-spark form ``"parquet.`<path>`"`` (a bare ``<path>`` is also
        accepted). ``partitionSchema`` is a pyarrow ``Schema`` of the Hive-partition columns for a
        partitioned dir, or ``None`` when unpartitioned. The path is storage-neutral (local / s3 /
        gs / az / OneLake) — reads use the session's already-minted credentials.

        The result is a by-path table: read it back with ``conn.read.format('delta').load(path)``, or
        if it sits under a catalog root, ``conn.refresh()`` to surface it as a discoverable view.
        """
        path = _parse_parquet_identifier(identifier).replace("\\", "/").rstrip("/")
        session._require_writable("convert parquet to Delta")
        engine.convert_to_delta(path, session.storage_options, partition_by=partitionSchema)
        return cls.forPath(session, path)

    def merge(self, source, condition: str) -> DeltaMergeBuilder:
        """Begin a DataFrame-style merge of ``source`` into this table on ``condition``.

        The merge is snapshot-pinned to the version captured when this handle was taken
        (``forName``/``forPath``): the commit validates OCC against it, so a concurrent writer that
        landed since then fails the commit loudly instead of silently interleaving (single-snapshot
        MERGE). Nothing for the caller to pass."""
        return DeltaMergeBuilder(self, source, condition, read_version=self._read_version)

    def version(self) -> int:
        """Current Delta version of the table (``DeltaTable`` history head)."""
        return engine.table_version(self.path, self.storage_options)

    def _resnapshot(self) -> None:
        """Advance the handle's pinned version to current HEAD after a successful mutation through
        it, so a SECOND mutation on the same handle fences to the post-mutation version instead of
        the stale construction-time one — only a FOREIGN write between operations makes it fail."""
        self._read_version = engine.table_version(self.path, self.storage_options)

    def history(self, limit: Optional[int] = None) -> List[Dict]:
        """Delta commit history (delta_rs ``DeltaTable.history``) — newest first; each entry is a
        dict with ``version``, ``timestamp``, ``operation``, … Use it to find a version to time-travel
        to: ``conn.read.format("delta").option("versionAsOf", N).load(path)``. ``limit`` caps the
        number of commits read."""
        return engine.table_history(self.path, self.storage_options, limit)

    def delete(self, predicate: Optional[str] = None) -> None:
        """Delete rows matching ``predicate`` (a delta_rs/datafusion SQL expression), or every row
        when ``predicate`` is None. ``DeltaTable.delete``."""
        self._session._require_writable("delete", self._catalog)
        engine.delete_rows(self.path, predicate, read_version=self._read_version,
                           storage_options=self.storage_options,
                           compaction_threshold=self.compaction_threshold)
        self._resnapshot()
        self._refresh_view()

    def update(self, condition: Optional[str] = None, set: Optional[Dict[str, str]] = None) -> None:
        """Set ``{column: expression}`` for rows matching ``condition`` (delta_rs/datafusion SQL), or
        every row when ``condition`` is None. Mirrors delta-spark ``DeltaTable.update``."""
        if not set:
            raise ValueError("update() requires a non-empty 'set' mapping of {column: expression}.")
        self._session._require_writable("update", self._catalog)
        engine.update_rows(self.path, set, condition, read_version=self._read_version,
                           storage_options=self.storage_options,
                           compaction_threshold=self.compaction_threshold)
        self._resnapshot()
        self._refresh_view()

    def vacuum(self, retention_hours: Optional[int] = None, dry_run: bool = False,
               enforce_retention_duration: bool = True) -> List[str]:
        """Delete data files no longer referenced and older than the retention window, returning the
        paths removed (delta-spark ``DeltaTable.vacuum``). Unlike delta_rs's API the default here
        actually deletes (``dry_run=False``), matching Spark; pass ``dry_run=True`` to only list.
        ``retention_hours`` below the configured window needs ``enforce_retention_duration=False``."""
        self._session._require_writable("vacuum", self._catalog)
        return engine.vacuum(self.path, retention_hours=retention_hours, dry_run=dry_run,
                             enforce_retention_duration=enforce_retention_duration,
                             storage_options=self.storage_options)

    def optimize(self, zorder_by: Optional[List[str]] = None,
                 target_size: Optional[int] = None) -> Dict:
        """Compact small files (delta-spark ``DeltaTable.optimize``), returning the operation
        metrics. Pass ``zorder_by`` to Z-order on those columns instead of a plain compaction."""
        self._session._require_writable("optimize", self._catalog)
        metrics = engine.optimize(self.path, zorder_by=zorder_by, target_size=target_size,
                                  storage_options=self.storage_options)
        self._refresh_view()
        return metrics

    def restoreToVersion(self, version: int) -> None:
        """Restore the table to an earlier Delta ``version`` (delta-spark ``DeltaTable.restoreToVersion``).
        It commits a new version on top of history, so the restore is itself revertible."""
        self._session._require_writable("restoreToVersion", self._catalog)
        engine.restore_to_version(self.path, version, storage_options=self.storage_options)
        self._refresh_view()

    def _refresh_view(self):
        # Only a forName() table maps to a registered view; forPath() has no name to refresh.
        if self._schema is not None and self._table is not None:
            catalog = self._catalog or self._session._current_catalog
            self._session._register_view(catalog, self._schema, self._table)
