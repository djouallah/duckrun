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
from . import secret

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
        # DuckDB's memory_limit as it stood when the connection was configured (profile value,
        # or DuckDB's own default). Restored on the overwrite/append path so DuckDB manages its
        # own memory; the merge path tightens it to DuckDB's split share instead.
        self._baseline_memory_limit: Optional[str] = None
        # Microbatch: remember which (invocation, path) pairs we've written this run, so a
        # multi-batch --full-refresh truncates the table on its *first* batch only and appends
        # the rest. Keyed by dbt's per-run invocation_id, so two runs in one process (the test
        # harness / a notebook) don't see each other's batches.
        self._microbatch_seen: set = set()

    def configure_connection(self, conn) -> None:
        # Stash the live DuckDB connection so store()/load() can use it later.
        self._conn = conn
        # Ensure delta_scan() is available for the model relation views.
        try:
            conn.execute("INSTALL delta; LOAD delta;")
        except Exception:
            pass
        # Always-on write-path tuning (preserve_insertion_order=false + a temp_directory to spill
        # to). The DuckDB memory_limit is NOT touched here: a plain overwrite/append lets DuckDB
        # manage its own memory; only the merge path tightens it (set_merge_memory_limit), pairing
        # with the merge's max_spill_size. Capture the baseline limit (profile value or DuckDB's
        # default) so the write path can restore it after a merge tightened the shared connection.
        try:
            engine.configure_duckdb_session(conn)
            self._baseline_memory_limit = engine.read_memory_limit(conn)
        except Exception:
            pass
        # If a bearer token was supplied in storage_options (e.g. OneLake/ADLS), mint a
        # matching DuckDB Azure secret so delta_scan() can read the tables. Same helper the
        # adapter uses before discovery, so the two paths can't drift. In a notebook where
        # the secret is already provided there's no token, so this is a no-op.
        try:
            secret.ensure_azure_secret(conn, self._storage_options)
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
        # Start every model with DuckDB's memory managing itself (the write-path default). Undoes
        # any tightening a previous merge left on the shared connection; the merge branch below
        # re-tightens. So the 0.3/0.6 split applies to merge ONLY — overwrite/append do nothing.
        engine.restore_memory_limit(cur, self._baseline_memory_limit)
        name = self._relation_name(target_config.relation)
        data = cur.sql(f"SELECT * FROM {name}")

        exists = engine.table_exists(path, storage_options)

        # Microbatch is delete+insert per event_time window, not a key-based upsert, so it
        # bypasses the generic overwrite/merge dispatch below (which would clobber every batch
        # under --full-refresh, since dbt marks each microbatch batch full_refresh in that case).
        if incremental and strategy == "microbatch":
            self._store_microbatch(
                path, cur, name, cfg, storage_options, exists, full_refresh
            )
            return

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
            # Merge is the only path where DuckDB and the delta_rs pool peak together: tighten
            # DuckDB to its share so the two fit. (write/append above left it at the baseline.)
            engine.set_merge_memory_limit(cur)
            # streamed_exec: default False so delta_rs collects the source and uses its stats to
            # prune the target (right for small incremental deltas into a large table). A model
            # whose source is itself huge can set merge_streamed_exec=true to stream it instead.
            sx = cfg.get("merge_streamed_exec")
            engine.merge_delta(
                path, data, unique_key,
                insert_only=(strategy == "insert"),
                update_columns=cfg.get("merge_update_columns"),
                exclude_columns=cfg.get("merge_exclude_columns"),
                predicates=self._merge_predicates(cfg),
                merge_schema=evolve_schema,
                max_spill_size=cfg.get("merge_max_spill_size"),
                streamed_exec=(False if sx is None else bool(sx)),
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        elif strategy == "append":
            engine.write_delta(
                path, data, "append",
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        elif strategy == "safeappend":
            # Optimistic append: commit only if the table version has not moved since the model
            # *started* (read_version, captured before it read {{ this }}), else fail so dbt errors
            # and the orchestrator re-runs. Pinning to the start version — not HEAD at write time —
            # is what closes the read→write gap: a writer that commits any time during the build
            # makes this fail instead of appending a duplicate. No dedup — that's the SQL's job.
            # Compare-and-swap via delta_rs max_commit_retries=0 (see engine).
            engine.append_if_unchanged(
                path, data,
                read_version=cfg.get("read_version"),
                partition_by=partition_by,
                merge_schema=merge_schema,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        else:
            raise ValueError(
                f"Unknown incremental_strategy '{strategy}'. "
                "Use 'merge', 'insert', 'append', or 'safeappend'."
            )

    def _store_microbatch(
        self, path, cur, name, cfg, storage_options, exists, full_refresh
    ) -> None:
        """dbt ``incremental_strategy='microbatch'``: for the current batch window
        ``[event_time_start, event_time_end)``, delete the rows already in that window and
        insert the batch's rows — an idempotent delete+insert keyed on the event-time range.

        dbt drives this by re-running the model once per batch with bounds it computes from
        ``event_time`` / ``batch_size`` / ``begin`` / ``lookback`` and passes down via the
        materialization macro (``batch_start`` / ``batch_end`` / ``invocation_id``).
        """
        # microbatch is range-based, not key-based; unique_key would be silently misleading.
        if cfg.get("unique_key"):
            raise CompilationError(
                "incremental_strategy='microbatch' does not support 'unique_key'. "
                "Microbatch deletes+inserts each batch by its 'event_time' window, not by key. "
                "Remove 'unique_key' or use incremental_strategy='merge'."
            )
        event_time = cfg.get("event_time")
        start = cfg.get("batch_start")
        end = cfg.get("batch_end")
        if not event_time:
            raise CompilationError(
                "microbatch incremental strategy requires an 'event_time' model config."
            )
        if not (start and end):
            raise CompilationError(
                "microbatch incremental strategy requires batch bounds "
                "('event_time_start'/'event_time_end') in the run context."
            )

        # Re-filter the staged rows to this batch's window (dbt also filters the model's
        # inputs, but this keeps the delete and the insert covering exactly the same range).
        window = cur.sql(
            f"SELECT * FROM {name} WHERE "
            f"CAST({event_time} AS TIMESTAMP) >= CAST('{start}' AS TIMESTAMP) "
            f"AND CAST({event_time} AS TIMESTAMP) < CAST('{end}' AS TIMESTAMP)"
        )

        # First batch of a --full-refresh run truncates; later batches (and every batch of a
        # normal run) append into the window. A brand-new table is just created.
        seen_key = (cfg.get("invocation_id"), path)
        first_batch = seen_key not in self._microbatch_seen
        self._microbatch_seen.add(seen_key)

        if not exists or (full_refresh and first_batch):
            engine.write_delta(
                path, window, "overwrite",
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
            )
        else:
            engine.delete_insert_window(
                path, window,
                column=event_time, start=start, end=end,
                storage_options=storage_options,
                compaction_threshold=self._compaction_threshold,
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
