"""
dbt-duckdb plugin that materializes a model relation as a Delta Lake table.

This is the one piece dbt-duckdb lacks: a Delta *write*. The plugin stashes the DuckDB
connection (``configure_connection``), and on ``store()`` hands the model relation
straight to delta_rs. DuckDB relations expose the Arrow C-stream interface, which
deltalake 1.x consumes directly, so there is no pyarrow dependency.
"""
from typing import Any, Optional

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

from . import engine


class Plugin(BasePlugin):
    """Registered automatically by the duckrun adapter (alias ``duckrun``)."""

    def initialize(self, config: dict) -> None:
        config = config or {}
        self._storage_options: Optional[dict] = config.get("storage_options")
        self._compaction_threshold: int = int(config.get("compaction_threshold", 100))
        self._conn = None

    def configure_connection(self, conn) -> None:
        # Stash the live DuckDB connection so store()/load() can use it later.
        self._conn = conn
        # Ensure delta_scan() is available for the model relation views.
        try:
            conn.execute("INSTALL delta; LOAD delta;")
        except Exception:
            pass

    def _cursor(self):
        if self._conn is None:
            raise RuntimeError(
                "duckrun delta plugin has no DuckDB connection; "
                "configure_connection was not called."
            )
        try:
            return self._conn.cursor()
        except Exception:
            return self._conn

    # ------------------------------------------------------------------ write
    def store(self, target_config: TargetConfig) -> None:
        path = target_config.location.path
        cfg = target_config.config or {}

        partition_by = cfg.get("partition_by")
        merge_schema = bool(cfg.get("merge_schema", False))
        unique_key = cfg.get("unique_key")
        incremental = bool(cfg.get("incremental", False))
        full_refresh = bool(cfg.get("full_refresh", False))
        storage_options = cfg.get("storage_options", self._storage_options)

        # Keep `cur` referenced for the whole write so the relation's Arrow stream
        # stays valid while deltalake consumes it.
        cur = self._cursor()
        name = self._relation_name(target_config.relation)
        data = cur.sql(f"SELECT * FROM {name}")

        exists = engine.table_exists(path, storage_options)

        # Table-like (non-incremental) models always overwrite. Incremental models
        # overwrite on first run / full-refresh, then merge (on unique_key) or append.
        if not incremental or full_refresh or not exists:
            engine.write_delta(
                path, data, "overwrite",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        elif unique_key:
            engine.merge_delta(
                path, data, unique_key,
                storage_options=storage_options,
            )
        else:
            engine.write_delta(
                path, data, "append",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )

    @staticmethod
    def _relation_name(relation: Any) -> str:
        return relation.render() if hasattr(relation, "render") else str(relation)

    # ------------------------------------------------------------------- read
    def load(self, source_config: SourceConfig):
        path = source_config.get("delta_table_path") or source_config.get("location")
        if not path:
            raise ValueError(
                "Delta source requires 'delta_table_path' (or 'location') in meta."
            )
        # Read via DuckDB's delta_scan so no pyarrow import is required.
        return self._cursor().sql(f"SELECT * FROM delta_scan('{path}')")

    def default_materialization(self) -> str:
        return "view"
