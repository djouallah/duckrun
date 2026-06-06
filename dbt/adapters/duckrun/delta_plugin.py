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

try:  # raise on_schema_change='fail' as a dbt compilation error (matches dbt semantics)
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


class Plugin(BasePlugin):
    """Registered automatically by the duckrun adapter (alias ``duckrun``)."""

    def initialize(self, config: dict) -> None:
        config = config or {}
        self._storage_options: Optional[dict] = config.get("storage_options")
        self._compaction_threshold: int = int(config.get("compaction_threshold", 100))
        self._conn = None
        self._cursor_handle = None

    def configure_connection(self, conn) -> None:
        # Stash the live DuckDB connection so store()/load() can use it later.
        self._conn = conn
        # Ensure delta_scan() is available for the model relation views.
        try:
            conn.execute("INSTALL delta; LOAD delta;")
        except Exception:
            pass
        # If a bearer token was supplied in storage_options (e.g. OneLake/ADLS),
        # create a matching DuckDB Azure secret so delta_scan() can read the tables.
        # In a notebook where the secret is already provided this is skipped.
        so = self._storage_options or {}
        token = so.get("bearer_token") or so.get("token") or so.get("access_token")
        if token:
            try:
                conn.execute("INSTALL azure; LOAD azure;")
                conn.execute(
                    "CREATE OR REPLACE SECRET duckrun_onelake "
                    f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}')"
                )
            except Exception:
                pass

    def configure_cursor(self, cursor) -> None:
        # dbt creates a fresh child cursor per model connection (see dbt-duckdb's
        # initialize_cursor) and runs that model's pre-hooks / staged-model DDL on it.
        # DuckDB session state (e.g. SET VARIABLE used by getvariable()/read_csv()) is
        # cursor-local, so store()/load() MUST read on this same cursor — not a new child
        # of the shared connection — or the variables/relations won't be visible.
        self._cursor_handle = cursor

    def _cursor(self):
        # Prefer the live per-model cursor (shares the session where pre-hook variables and
        # the staged relation were created); fall back to the shared connection.
        if self._cursor_handle is not None:
            return self._cursor_handle
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
        strategy = cfg.get("incremental_strategy")
        # Per-model override wins; fall back to the credential-level storage_options.
        # (Use `or` because the macro always sets the key, often to None.)
        storage_options = cfg.get("storage_options") or self._storage_options

        # Keep `cur` referenced for the whole write so the relation's Arrow stream
        # stays valid while deltalake consumes it.
        cur = self._cursor()
        name = self._relation_name(target_config.relation)
        data = cur.sql(f"SELECT * FROM {name}")

        exists = engine.table_exists(path, storage_options)

        # Table-like (non-incremental) models always overwrite. Incremental models
        # overwrite on first run / full-refresh, then apply the incremental strategy.
        if not incremental or full_refresh or not exists:
            engine.write_delta(
                path, data, "overwrite",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
            return

        # Resolve the incremental strategy: default to merge when a unique_key is
        # given, else a plain append. delta_rs has no separate delete+insert; with a
        # unique_key it is equivalent to an upsert, so treat it as merge.
        strategy = strategy or ("merge" if unique_key else "append")
        if strategy in ("delete+insert", "delete_insert"):
            strategy = "merge"

        if strategy in ("merge", "insert"):
            if not unique_key:
                raise ValueError(
                    f"incremental_strategy='{strategy}' requires a unique_key."
                )
            # on_schema_change: detect added/removed columns vs the existing table and
            # decide whether to let delta_rs evolve the schema (or fail). Default 'ignore'.
            on_schema_change = (cfg.get("on_schema_change") or "ignore").lower()
            evolve_schema = self._resolve_schema_change(
                on_schema_change, path, data, storage_options
            )
            engine.merge_delta(
                path, data, unique_key,
                insert_only=(strategy == "insert"),
                update_columns=cfg.get("merge_update_columns"),
                exclude_columns=cfg.get("merge_exclude_columns"),
                predicates=self._merge_predicates(cfg),
                merge_schema=evolve_schema,
                storage_options=storage_options,
            )
        elif strategy == "append":
            engine.write_delta(
                path, data, "append",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        else:
            raise ValueError(
                f"Unknown incremental_strategy '{strategy}'. "
                "Use 'merge', 'insert', or 'append'."
            )

    @staticmethod
    def _relation_name(relation: Any) -> str:
        return relation.render() if hasattr(relation, "render") else str(relation)

    @staticmethod
    def _merge_predicates(cfg: dict):
        """dbt ``incremental_predicates`` (or ``predicates``), with dbt's standard merge
        aliases rewritten to the ones delta_rs uses here."""
        preds = cfg.get("incremental_predicates") or cfg.get("predicates")
        if not preds:
            return None
        if isinstance(preds, str):
            preds = [preds]
        out = []
        for p in preds:
            p = str(p).replace("DBT_INTERNAL_DEST", "target").replace("DBT_INTERNAL_SOURCE", "source")
            out.append(p)
        return out

    @staticmethod
    def _resolve_schema_change(on_schema_change, path, data, storage_options) -> bool:
        """Handle dbt ``on_schema_change`` for the merge path.

        Returns whether delta_rs should evolve the table schema (``merge_schema``).
        - ignore (default): no evolution.
        - append_new_columns / sync_all_columns: evolve so new columns are added.
        - fail: raise if the incoming columns differ from the table's.
        """
        if on_schema_change in ("ignore", "", None):
            return False
        existing = [c.lower() for c in engine.delta_columns(path, storage_options)]
        incoming = [c.lower() for c in data.columns]
        added = [c for c in incoming if c not in existing]
        removed = [c for c in existing if c not in incoming]
        if on_schema_change == "fail" and (added or removed):
            raise CompilationError(
                "The source and target schemas on this incremental model are out of sync: "
                f"added={added or '[]'}, removed={removed or '[]'} "
                "(on_schema_change='fail')."
            )
        # append_new_columns / sync_all_columns: let delta_rs union in the new columns.
        # (delta_rs can add but not drop columns, so sync_all_columns is add-only here.)
        return bool(added) or on_schema_change == "sync_all_columns"

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
