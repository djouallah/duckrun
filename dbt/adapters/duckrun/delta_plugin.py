"""
dbt-duckdb plugin that materializes a model relation as a Delta Lake table.

This is the one piece dbt-duckdb lacks: a Delta *write*. The plugin stashes the DuckDB
connection (``configure_connection``), and on ``store()`` reads the model relation as an
Arrow batch and hands it to the delta_rs engine. It also implements ``load()`` so Delta
tables can be used as dbt sources.
"""
from typing import Any, Optional

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

from deltalake import DeltaTable

from . import engine


class Plugin(BasePlugin):
    """Registered in profiles as ``module: dbt.adapters.duckrun.delta_plugin``.

    The duckrun adapter auto-registers it, so users normally don't configure it
    explicitly.
    """

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

        batch = self._read_relation(target_config.relation)

        exists = engine.table_exists(path, storage_options)

        # Table-like (non-incremental) models always overwrite. Incremental models
        # overwrite on first run / full-refresh, then merge (on unique_key) or append.
        if not incremental or full_refresh or not exists:
            engine.write_delta(
                path, batch, "overwrite",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        elif unique_key:
            engine.merge_delta(
                path, batch, unique_key,
                storage_options=storage_options,
            )
        else:
            engine.write_delta(
                path, batch, "append",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )

    def _read_relation(self, relation: Any):
        if self._conn is None:
            raise RuntimeError(
                "duckrun delta plugin has no DuckDB connection; "
                "configure_connection was not called."
            )
        name = relation.render() if hasattr(relation, "render") else str(relation)
        # Use a fresh cursor on the same connection so we don't disturb dbt's cursor.
        try:
            cur = self._conn.cursor()
        except Exception:
            cur = self._conn
        cur.execute(f"SELECT * FROM {name}")
        # RecordBatchReader is consumed once by the deltalake writer.
        return cur.fetch_record_batch()

    # ------------------------------------------------------------------- read
    def load(self, source_config: SourceConfig):
        path = source_config.get("delta_table_path") or source_config.get("location")
        if not path:
            raise ValueError(
                "Delta source requires 'delta_table_path' (or 'location') in meta."
            )
        storage_options = source_config.get("storage_options", self._storage_options)
        dt = (
            DeltaTable(path, storage_options=storage_options)
            if storage_options
            else DeltaTable(path)
        )
        return dt.to_pyarrow_dataset()

    def default_materialization(self) -> str:
        return "view"
