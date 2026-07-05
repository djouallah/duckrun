"""
dbt-duckdb plugin that materializes a model relation as a Delta Lake table.

This is the one piece dbt-duckdb lacks: a Delta *write*. The plugin stashes the DuckDB
connection (``configure_connection``), and on ``store()`` hands the model relation
straight to delta_rs. DuckDB relations expose the Arrow C-stream interface, which
deltalake 1.x consumes directly, so there is no pyarrow dependency.
"""
import os
import re
from typing import Any, Optional

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

from . import engine
from . import secret
from . import sqlscan

try:  # raise on_schema_change='fail' as a dbt compilation error (matches dbt semantics)
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


class Plugin(BasePlugin):
    """Registered automatically by the duckrun adapter (alias ``duckrun``)."""

    def initialize(self, config: dict) -> None:
        config = config or {}
        self._storage_options: Optional[dict] = config.get("storage_options")
        # Per-catalog write config (issue #7): {alias: {root_path, storage_options}} + the default
        # catalog's name, so store() can pick the write token by the relation's database. Empty for
        # a single-catalog project — store() then always uses the default storage_options.
        self._catalogs: dict = config.get("catalogs") or {}
        self._default_database = config.get("default_database")
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
        except Exception:  # best-effort: delta may already be loaded / autoloaded; reads still work
            pass
        # Always-on write-path tuning (preserve_insertion_order=false + a temp_directory to spill
        # to). The DuckDB memory_limit is NOT touched here — store() sets it per model: the write
        # path clamps to _WRITE_MEM_FRACTION of the effective limit (set_write_memory_limit), the
        # merge path to its 0.3 share (set_merge_memory_limit). Capture the baseline limit (profile
        # value or DuckDB's default) so store() can clamp DOWN from it rather than guess a floor.
        try:
            engine.configure_duckdb_session(conn)
            self._baseline_memory_limit = engine.read_memory_limit(conn)
        except Exception:  # best-effort tuning: a failure just leaves DuckDB's defaults in place
            pass
        # If a bearer token was supplied in storage_options (e.g. OneLake/ADLS), mint a
        # matching DuckDB Azure secret so delta_scan() can read the tables. Same helper the
        # adapter uses before discovery, so the two paths can't drift. In a notebook where
        # the secret is already provided there's no token, so this is a no-op.
        try:
            secret.ensure_azure_secret(conn, self._storage_options)
        except Exception:  # best-effort: no token (local/notebook) -> no secret needed, a no-op
            pass

    def configure_cursor(self, cursor) -> None:
        # dbt creates a fresh child cursor per model connection (see dbt-duckdb's
        # initialize_cursor) and runs that model's pre-hooks / staged-model DDL on it.
        # DuckDB session state (e.g. SET VARIABLE used by getvariable()/read_csv()) is
        # cursor-local, so store()/load() MUST read on this same cursor — not a new child
        # of the shared connection — or the variables/relations won't be visible.
        self._cursor_handle = cursor
        # OneLake token refresh. A run longer than the bearer token's ~1h life would 401 mid-build
        # (the token is captured once at connection-open). dbt calls this once per model, so it's the
        # natural place to re-mint just before this model's reads (delta_scan of {{ this }}) and its
        # store() write. No-op unless the token is a JWT near expiry AND a live source can refresh it
        # (Fabric / azure-identity), so short jobs and the local path are untouched. Refresh the
        # default AND every catalog: a stale aliased token would 401 only on that Lakehouse.
        self._refresh_token(cursor, self._storage_options, self._default_database, is_default=True,
                            setter=lambda so: setattr(self, "_storage_options", so))
        for alias, cat in self._catalogs.items():
            cat = cat or {}
            self._refresh_token(cursor, cat.get("storage_options"), alias, is_default=False,
                                setter=lambda so, c=cat: c.__setitem__("storage_options", so))

    def _catalog_storage_options(self, database):
        """The write token for the catalog a relation lands in (its ``database``), falling back to
        the default catalog's ``storage_options``. Identity for a single-catalog project."""
        if database is not None:
            db = str(database).strip('"')
            if db in self._catalogs:
                return (self._catalogs[db] or {}).get("storage_options")
        return self._storage_options

    def _refresh_token(self, cursor, so, catalog, is_default, setter) -> None:
        if not secret.bearer_token(so):
            return
        fresh = secret.refreshed(so)
        if fresh is so:  # token still valid (the common path)
            return
        setter(fresh)  # token was actually re-acquired — keep the live copy in sync
        try:
            if is_default:
                secret.ensure_azure_secret(cursor, fresh)
            else:
                root = (self._catalogs.get(catalog) or {}).get("root_path")
                secret.mint_scoped_secret(cursor, secret.scoped_secret_name(catalog), root, fresh)
            if os.environ.get("DUCKRUN_AUTH_DEBUG"):
                print(f"[duckrun-auth] configure_cursor: re-minted DuckDB secret for catalog {catalog!r}", flush=True)
        except Exception as e:  # best-effort: a transient refresh failure keeps the old secret
            if os.environ.get("DUCKRUN_AUTH_DEBUG"):
                print(f"[duckrun-auth] configure_cursor: re-mint failed for {catalog!r}: {e!r}", flush=True)

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
        except Exception:  # best-effort: if a child cursor can't be made, use the shared connection
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
        # Per-model override wins; fall back to the write token of the catalog this relation lands
        # in (its `database`), then the default catalog's token. (Use `or` because the macro always
        # sets the key, often to None.) For a single-catalog project this is exactly the default
        # storage_options, unchanged.
        storage_options = cfg.get("storage_options") or self._catalog_storage_options(
            getattr(target_config.relation, "database", None)
        )

        # Keep `cur` referenced for the whole write so the relation's Arrow stream
        # stays valid while deltalake consumes it.
        cur = self._cursor()
        # Start every model bounded to the write-path share (_WRITE_MEM_FRACTION of the effective
        # limit), clamping DuckDB's host-physical-RAM default that OOM-kills us on containers. Also
        # undoes any tightening a previous merge left on the shared connection; the merge branch
        # below re-tightens to its 0.3 share. So the write clamp applies to overwrite/append/
        # safeappend/microbatch, and the 0.3/0.6 split applies to merge ONLY.
        engine.set_write_memory_limit(cur, self._baseline_memory_limit)
        name = self._relation_name(target_config.relation)
        # sort_by makes the write order EXPLICIT. A trailing ORDER BY inside the model SQL is not
        # honored here — the staged relation is read through a wrapper SELECT *, and with
        # preserve_insertion_order=false DuckDB may reorder any result lacking a top-level ORDER BY.
        # A top-level ORDER BY on this read IS honored, so long RLE runs / dictionary locality (the
        # point of the Parquet tuning) are deterministic regardless of the global flag.
        sort_by = cfg.get("sort_by")
        if sort_by:
            cols = sort_by if isinstance(sort_by, (list, tuple)) else [sort_by]
            order = ", ".join('"' + str(c).strip().strip('"').replace('"', '""') + '"' for c in cols)
            data = cur.sql(f"SELECT * FROM {name} ORDER BY {order}")
        else:
            data = cur.sql(f"SELECT * FROM {name}")

        exists = engine.table_exists(path, storage_options)

        # Contradiction guard (closes a silent data-loss window). dbt resolved this model as
        # incremental because run-start disk discovery saw the table, so the model SQL already
        # filtered to only-new rows. If the table now can't be opened at store time *and* that's
        # not a deliberate full-refresh, something is wrong — most likely a transient storage
        # error that table_exists() now (correctly) does NOT swallow, or the table was deleted
        # mid-run. Overwriting here would replace the whole table with just the increment. Refuse.
        dbt_believes_exists = bool(cfg.get("dbt_believes_exists", False))
        if incremental and not full_refresh and not exists and dbt_believes_exists:
            raise RuntimeError(
                "dbt resolved this model as incremental (target existed at discovery) but the "
                "Delta table is not found at store time. Refusing to overwrite — rerun, or pass "
                "--full-refresh if the table was deliberately deleted."
            )

        # Contract NOT NULL enforcement (config(contract={enforced:true}) with a not_null column
        # constraint). duckrun writes via delta_rs, not SQL DDL, so dbt-core's column-constraint
        # DDL never runs. Guard the staged rows BEFORE any write: a null in a not-null column
        # raises, and because nothing has been written yet the prior Delta version is untouched
        # (the rollback the constraint tests assert). Message carries "NOT NULL constraint failed"
        # to match dbt's standard contract-error phrasing.
        not_null_columns = cfg.get("not_null_columns") or []

        # #14: for a keyed merge, optionally evaluate the model SQL ONCE into a DuckDB temp table so
        # the not-null guard, the merge cardinality guard, and delta_rs's source collection all see
        # IDENTICAL rows — otherwise a nondeterministic model (now(), a moved external source) lets the
        # guards vouch for rows that aren't the ones merged, and the SQL is re-run 2-3x. Opt in via
        # merge_materialize_source, or automatically when a not-null contract is active. Streaming
        # stays the default (memory behavior unchanged) for merges without a guard. The temp is bounded
        # by the write memory clamp + spill dir like any other DuckDB result.
        src_tmp = None
        _resolved_strategy = strategy or ("merge" if unique_key else "append")
        _merge_path = (incremental and not full_refresh and exists
                       and _resolved_strategy in ("merge", "insert"))
        if _merge_path and (cfg.get("merge_materialize_source") or not_null_columns):
            src_tmp = f'"__duckrun_msrc_{abs(hash(path)) & 0xFFFFFFFF}"'
            cur.execute(f"CREATE OR REPLACE TEMP TABLE {src_tmp} AS SELECT * FROM {name}")
            name = src_tmp                       # guard + view name both read the one materialization
            data = cur.sql(f"SELECT * FROM {src_tmp}")

        if not_null_columns:
            self._assert_not_null(cur, name, not_null_columns)

        # Microbatch is delete+insert per event_time window, not a key-based upsert, so it
        # bypasses the generic overwrite/merge dispatch below (which would clobber every batch
        # under --full-refresh, since dbt marks each microbatch batch full_refresh in that case).
        if incremental and strategy == "microbatch":
            self._store_microbatch(
                path, cur, name, cfg, storage_options, exists, full_refresh,
                read_version=cfg.get("read_version"),
            )
            return

        # Table-like (non-incremental) models always overwrite. Incremental models
        # overwrite on first run / full-refresh, then apply the incremental strategy.
        if not incremental or full_refresh or not exists:
            # This branch is a CREATE OR REPLACE: a table model, a --full-refresh, or a first run.
            # When we are REPLACING an existing table (exists), allow delta_rs to replace the schema
            # wholesale (schema_mode="overwrite") — the model SQL defines the new schema, exactly as
            # `CREATE OR REPLACE TABLE` does on every other warehouse. Without it, delta_rs's strict
            # overwrite keeps the OLD schema/protocol and so can't change a column's type or write a
            # column needing a new writer feature the old table lacks (e.g. retyping to ::timestamp /
            # timestampNtz). This is scoped to the full-rebuild replace ONLY — NOT append, safeappend,
            # merge, or microbatch, which must keep their strict, schema-stable writes. A fresh create
            # (not exists) doesn't need it. A user's explicit merge_schema still wins.
            overwrite_schema = exists and not merge_schema
            with engine.mem_profile("overwrite", con=cur):
                engine.write_delta(
                    path, data, "overwrite",
                    partition_by=partition_by,
                    merge_schema=merge_schema,
                    overwrite_schema=overwrite_schema,
                    storage_options=storage_options,
                    cur=cur,
                )
            return

        # Resolve the incremental strategy: default to merge when a unique_key is given, else append.
        strategy = strategy or ("merge" if unique_key else "append")

        # delete+insert: delete the target rows whose unique_key appears in the incoming batch, then
        # insert EVERY incoming row (duplicates preserved) — computed in DuckDB and written as a
        # fenced full-table overwrite (overwrite_if_unchanged, CAS to vB). NOT an alias for merge:
        # merge UPDATEs matched rows and REJECTS a duplicate-key source (duckrun fails loud, like
        # Spark/Snowflake/BigQuery), whereas delete+insert replaces whole rows and tolerates duplicate
        # keys — matching dbt-duckdb's delete+insert exactly.
        if strategy in ("delete+insert", "delete_insert"):
            if not unique_key:
                raise ValueError("incremental_strategy='delete+insert' requires a unique_key.")
            self._store_delete_insert(
                path, cur, name, unique_key, storage_options,
                read_version=cfg.get("read_version"), partition_by=partition_by,
                incremental_predicates=cfg.get("incremental_predicates"),
            )
            return

        if strategy in ("merge", "insert"):
            # Validate the merge config shape FIRST — before any Delta access, memory tuning, or
            # write — so an invalid config fails fast and cleanly (no partial/late delta_rs
            # "Schema error" after the log has moved). Messages mirror dbt-duckdb's
            # validate_merge_config so the behavior is portable.
            self._validate_merge_config(cfg)
            if not unique_key:
                raise ValueError(
                    f"incremental_strategy='{strategy}' requires a unique_key."
                )
            # NOTE: a duplicate-key source is rejected downstream in engine.merge_delta_clauses (the
            # shared chokepoint for the dbt merge/insert strategies AND the DataFrame/SQL merge API),
            # so the cardinality rule is enforced identically across every merge path.
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
            # merge_clauses / merge_update_set_expressions need delta_rs's full ordered clause list
            # (matched-delete, multiple matched clauses, custom SET expressions) — divert to the
            # clause-core. Everything else stays on the byte-identical flat-kwarg merge_delta path.
            clause_specs = self._custom_merge_clauses(cfg, data.columns, unique_key)
            with engine.mem_profile("merge", con=cur):
                if clause_specs is not None:
                    engine.merge_delta_clauses(
                        path, data,
                        self._merge_on_predicate(unique_key, cfg, data.columns),
                        clause_specs,
                        merge_schema=evolve_schema,
                        max_spill_size=cfg.get("merge_max_spill_size"),
                        streamed_exec=(False if sx is None else bool(sx)),
                        read_version=cfg.get("read_version"),
                        storage_options=storage_options,
                        cur=cur,
                    )
                else:
                    engine.merge_delta(
                        path, data, unique_key,
                        insert_only=(strategy == "insert"),
                        update_columns=cfg.get("merge_update_columns"),
                        exclude_columns=cfg.get("merge_exclude_columns"),
                        predicates=self._merge_predicates(cfg, data.columns),
                        update_condition=self._rewrite_merge_aliases(cfg.get("merge_update_condition")),
                        insert_condition=self._rewrite_merge_aliases(cfg.get("merge_insert_condition")),
                        merge_schema=evolve_schema,
                        max_spill_size=cfg.get("merge_max_spill_size"),
                        streamed_exec=(False if sx is None else bool(sx)),
                        # Pin the merge target to the version the model read (vB, captured before it
                        # read {{ this }}), so OCC validates (vB, HEAD] — read and commit are one snapshot.
                        read_version=cfg.get("read_version"),
                        storage_options=storage_options,
                        cur=cur,
                    )
            if src_tmp is not None:
                cur.execute(f"DROP TABLE IF EXISTS {src_tmp}")  # #14: release the materialized source
        elif strategy == "append":
            with engine.mem_profile("append", con=cur):
                engine.write_delta(
                    path, data, "append",
                    partition_by=partition_by,
                    merge_schema=merge_schema,
                    storage_options=storage_options,
                    cur=cur,
                )
        elif strategy in ("append_if_unchanged", "safeappend"):
            # Optimistic append (``safeappend`` is the deprecated alias): commit only if the table
            # version has not moved since the model *started* (read_version, captured before it read
            # {{ this }}), else fail so dbt errors and the orchestrator re-runs. Pinning to the start
            # version — not HEAD at write time — is what closes the read→write gap: a writer that
            # commits any time during the build makes this fail instead of appending a duplicate. No
            # dedup — that's the SQL's job. Compare-and-swap via delta_rs max_commit_retries=0 (engine).
            with engine.mem_profile("append_if_unchanged", con=cur):
                engine.append_if_unchanged(
                    path, data,
                    read_version=cfg.get("read_version"),
                    partition_by=partition_by,
                    merge_schema=merge_schema,
                    storage_options=storage_options,
                    cur=cur,
                )
        else:
            raise ValueError(
                f"Unknown incremental_strategy '{strategy}'. "
                "Use 'merge', 'insert', 'delete+insert', 'append', or 'append_if_unchanged'."
            )

    def _store_microbatch(
        self, path, cur, name, cfg, storage_options, exists, full_refresh,
        read_version=None,
    ) -> None:
        """dbt ``incremental_strategy='microbatch'``: for the current batch window
        ``[event_time_start, event_time_end)``, atomically replace the rows already in that window
        with the batch's rows (``replaceWhere`` — a single Delta commit), keyed on the
        event-time range. ``read_version`` (the model's ``vB``) pins/fences that commit.

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
        invocation = cfg.get("invocation_id")
        # Only the current run's batches matter; drop bookkeeping from earlier invocations so this
        # set can't grow unbounded in a long-lived process (a notebook / runner doing many runs).
        self._microbatch_seen = {k for k in self._microbatch_seen if k[0] == invocation}
        seen_key = (invocation, path)
        first_batch = seen_key not in self._microbatch_seen
        self._microbatch_seen.add(seen_key)

        partition_by = cfg.get("partition_by")
        if not exists or (full_refresh and first_batch):
            engine.write_delta(
                path, window, "overwrite",
                partition_by=partition_by,
                storage_options=storage_options,
                cur=cur,
            )
        else:
            engine.replace_window(
                path, window,
                column=event_time, start=start, end=end,
                read_version=read_version,
                partition_by=partition_by,
                storage_options=storage_options,
                cur=cur,
            )

    def _store_delete_insert(
        self, path, cur, name, unique_key, storage_options,
        read_version=None, partition_by=None, incremental_predicates=None,
    ) -> None:
        """dbt ``incremental_strategy='delete+insert'``: delete the target rows whose ``unique_key``
        is present in the incoming batch (optionally further restricted by ``incremental_predicates``),
        then insert EVERY incoming row (duplicates preserved). Same result as dbt-duckdb's
        delete+insert — including tolerating duplicate keys in the batch (which merge rejects), and
        ``incremental_predicates`` that spare a matched key so its old row STAYS while its new row is
        also inserted (a deliberate duplicate, per dbt's contract).

        The delete condition is evaluated in DuckDB, not delta_rs: keep the target rows the delete
        would NOT remove (``(delete_cond) IS NOT TRUE`` — DELETE removes only TRUE matches, so FALSE
        and NULL rows stay, matching SQL DELETE incl. NULL keys), UNION the whole batch, and overwrite.
        delta_rs's own ``delete(predicate)`` is avoided on purpose: it runs the predicate through the
        Delta kernel for file-skipping, which rejects an Int32-column vs Int64-literal comparison (the
        type bare integer literals get) — DuckDB just coerces. Staging through a TEMP table detaches
        the read from the table before the overwrite replaces it.

        The delete is an in-DuckDB anti-join (``EXISTS (SELECT 1 FROM batch s WHERE s.k = t.k)``), not
        a Python-materialized ``IN (lit, …)`` set — so a multi-million-row batch does not round-trip
        its whole key set into a giant SQL string. Plain ``=`` (not ``IS NOT DISTINCT FROM``) so a NULL
        target key never matches, exactly like SQL ``IN`` / dbt-duckdb.

        This path is only reached when the table already EXISTS (see store's dispatch), so it is always
        fenced: pin the read and commit to ``read_version`` (or, if a concurrent writer created the
        table mid-run so ``read_version`` is None, to the version captured now)."""
        keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
        keys = [str(k).strip().strip('"') for k in keys]
        # Empty batch → nothing to delete or insert (an incremental no-op). Probe with LIMIT 1 rather
        # than pulling DISTINCT keys into Python.
        if cur.sql(f"SELECT 1 FROM {name} LIMIT 1").fetchone() is None:
            return
        loc_sql = path.replace("'", "''")
        # Always fenced: reached only when the table exists. read_version is None only if a writer
        # created it during this run — capture the current version and pin to that.
        vB = read_version if read_version is not None else engine.table_version(path, storage_options)

        target_cols = list(cur.sql(f"SELECT * FROM delta_scan('{loc_sql}', version => {vB}) LIMIT 0").columns)
        batch_cols = list(cur.sql(f"SELECT * FROM {name} LIMIT 0").columns)
        # Loud failure on a column mismatch instead of letting an explicit projection produce a DuckDB
        # binder error (or a positional overwrite silently shift values) — mirror on_schema_change='fail'.
        tset, bset = {c.lower() for c in target_cols}, {c.lower() for c in batch_cols}
        if tset != bset:
            added = sorted(c for c in batch_cols if c.lower() not in tset)
            removed = sorted(c for c in target_cols if c.lower() not in bset)
            raise CompilationError(
                "delete+insert: the model's columns do not match the target table. "
                + (f"Added: {added}. " if added else "")
                + (f"Missing: {removed}. " if removed else "")
                + "Reconcile the model SQL with the table (or use on_schema_change / --full-refresh)."
            )

        key_join = " AND ".join(f's."{k}" = t."{k}"' for k in keys)
        delete_cond = f"EXISTS (SELECT 1 FROM {name} s WHERE {key_join})"
        preds = self._delete_insert_predicates(incremental_predicates)
        if preds:
            # Qualify bare target columns to the target alias `t` (quote-aware, not regex — see #4).
            preds = [sqlscan.qualify_identifiers(p, target_cols, prefix="t") for p in preds]
            delete_cond = "(" + delete_cond + ") AND " + " AND ".join(f"({p})" for p in preds)
        # Project the batch onto the target column list (by name, target order) so the UNION aligns by
        # column regardless of the model SELECT's column order — a reorder can't shift values.
        tcols_t = ", ".join(f't."{c}"' for c in target_cols)
        tcols = ", ".join(f'"{c}"' for c in target_cols)
        tmp = f"__duckrun_di_{abs(hash(path)) & 0xFFFFFFFF}"
        cur.execute(
            f'CREATE OR REPLACE TEMP TABLE "{tmp}" AS '
            f"SELECT {tcols_t} FROM delta_scan('{loc_sql}', version => {vB}) t "
            f"WHERE ({delete_cond}) IS NOT TRUE "
            f"UNION ALL SELECT {tcols} FROM {name}"
        )
        try:
            data = cur.sql(f'SELECT * FROM "{tmp}"')
            # Fence the overwrite to vB (the version the kept rows were read at): CAS via
            # overwrite_if_unchanged so a writer that committed since vB fails the run loudly instead
            # of being silently clobbered. Same snapshot for the read and the commit, exactly like merge.
            engine.overwrite_if_unchanged(
                path, data,
                read_version=vB,
                partition_by=partition_by,
                storage_options=storage_options,
            )
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{tmp}"')

    @staticmethod
    def _delete_insert_predicates(incremental_predicates) -> list:
        """Normalize ``incremental_predicates`` (a list of SQL strings, or one string) to target-side
        predicates for the delta_rs DELETE — dropping dbt's ``DBT_INTERNAL_DEST`` alias, since the
        delete runs against the target table directly.

        The alias strip is quote-aware (via ``sqlscan``) so a ``DBT_INTERNAL_DEST.`` appearing inside
        a string literal is not removed."""
        if not incremental_predicates:
            return []
        preds = (incremental_predicates if isinstance(incremental_predicates, (list, tuple))
                 else [incremental_predicates])
        out = []
        for p in preds:
            p = sqlscan.strip_qualifier(str(p).strip(), "DBT_INTERNAL_DEST")
            if p:
                out.append(p)
        return out

    @staticmethod
    def _relation_name(relation: Any) -> str:
        return relation.render() if hasattr(relation, "render") else str(relation)

    @staticmethod
    def _assert_not_null(cur, name: str, columns) -> None:
        """Raise if any of ``columns`` contains a NULL in the staged relation ``name``.

        A pre-write DuckDB guard query — the honest, engine-agnostic way to enforce a contract
        NOT NULL constraint when the materialization is a delta_rs write rather than SQL DDL.
        Runs before the Delta write, so a violation leaves the existing table (and its version)
        untouched. The double-quoted identifiers handle column names that need quoting.

        One pass: a single ``count(*) FILTER (WHERE col IS NULL)`` per column over one evaluation of
        the staged view, rather than N full model evaluations. Raise for the first non-zero column,
        keeping the same message.
        """
        cols = list(columns)
        if not cols:
            return
        quoted = ['"' + str(col).replace('"', '""') + '"' for col in cols]
        selects = ", ".join(f"count(*) FILTER (WHERE {q} IS NULL)" for q in quoted)
        counts = cur.sql(f"SELECT {selects} FROM {name}").fetchone()
        for col, cnt in zip(cols, counts):
            if cnt:
                raise CompilationError(
                    f"NOT NULL constraint failed: column '{col}' in this contracted model "
                    f"produced {cnt} null value(s). Fix the model SQL or relax the contract."
                )

    @staticmethod
    def _validate_merge_config(cfg: dict) -> None:
        """Fail fast on an invalid merge config, BEFORE any Delta access or write.

        Ported from dbt-duckdb's ``validate_merge_config`` macro (and its helpers) so the error
        messages match exactly — duckrun otherwise passes raw config to delta_rs, which dies
        late with a generic "Schema error" *after* it has started touching the table. The shape
        checks (string / list / dict, merge_clauses structure, basic-vs-clauses conflict) are
        engine-agnostic, so validating them here is honest even though delta_rs doesn't act on
        every key. All problems are collected and raised together.
        """
        def is_string(v):
            return isinstance(v, str)

        def is_sequence(v):
            return isinstance(v, (list, tuple))

        def is_mapping(v):
            return isinstance(v, dict)

        errors = []
        # field name -> expected shape; order matters for the conflict message.
        base_fields = {
            "merge_update_condition": "string",
            "merge_insert_condition": "string",
            "merge_on_using_columns": "sequence",
            "merge_update_columns": "sequence",
            "merge_update_set_expressions": "mapping",
            "merge_exclude_columns": "sequence",
            "merge_returning_columns": "sequence",
        }

        for name, ftype in base_fields.items():
            val = cfg.get(name)
            if val is None:
                continue
            if ftype == "string":
                if not is_string(val):
                    errors.append(f"{name} must be a string, found: {val}")
            elif ftype == "sequence":
                if not is_sequence(val):
                    errors.append(f"{name} must be a list")
                else:
                    for item in val:
                        if not is_string(item):
                            errors.append(f"{name} must contain only string values, found: {item}")
            elif ftype == "mapping":
                if not is_mapping(val):
                    errors.append(f"{name} must be a dictionary, found: {val}")

        merge_clauses = cfg.get("merge_clauses")
        if merge_clauses is not None:
            if not is_mapping(merge_clauses):
                errors.append(f"merge_clauses must be a dictionary, found: {merge_clauses}")
            else:
                clause_keys = ("when_matched", "when_not_matched", "when_not_matched_by_source")
                if not any(k in merge_clauses for k in clause_keys):
                    errors.append(
                        "merge_clauses must contain at least one of "
                        "'when_matched', 'when_not_matched', or 'when_not_matched_by_source' keys"
                    )
                for ct in clause_keys:
                    if ct not in merge_clauses:
                        continue
                    clause = merge_clauses.get(ct)
                    if not is_sequence(clause):
                        errors.append(f"merge_clauses.{ct} must be a list")
                    elif len(clause) == 0:
                        errors.append(f"merge_clauses.{ct} must contain at least one element")
                    else:
                        for c in clause:
                            if not is_mapping(c):
                                errors.append(
                                    f"merge_clauses.{ct} elements must be dictionaries, found: {c}"
                                )
                # Basic merge configs are ignored when merge_clauses is set — flag the conflict.
                conflicting = []
                for name, ftype in base_fields.items():
                    if name in ("merge_on_using_columns", "merge_returning_columns"):
                        continue
                    val = cfg.get(name)
                    if val is None:
                        continue
                    if ftype == "sequence":
                        if is_sequence(val) and len(val) > 0:
                            conflicting.append(name)
                        elif not is_sequence(val):
                            conflicting.append(name)
                    elif ftype == "mapping":
                        if is_mapping(val) and len(val.keys()) > 0:
                            conflicting.append(name)
                    else:
                        conflicting.append(name)
                if conflicting:
                    errors.append(
                        "When merge_clauses is specified, the following basic merge "
                        "configurations will be ignored and should be removed: "
                        + ", ".join(conflicting)
                        + ". Define your merge behavior within merge_clauses instead."
                    )

        if errors:
            raise CompilationError("MERGE configuration errors:\n" + "\n".join(errors))

        # Shape is valid — now REJECT (don't silently ignore) any present-and-valid merge config
        # whose *semantics* delta_rs can't express. Accepting these and then quietly running a plain
        # upsert is the same silent-divergence class as the WS1 data-loss bug: the run is green but
        # the result ignores what the user asked for. merge_update_condition / merge_insert_condition
        # ARE honored (delta_rs per-clause predicates — see merge_delta), so they are NOT rejected;
        # merge_update_columns / merge_exclude_columns / incremental_predicates are honored too.
        # merge_clauses and merge_update_set_expressions ARE honored now (translated to delta_rs's
        # full TableMerger clause list — see _custom_merge_clauses), so they are not rejected.
        # (merge_returning_columns is a caller-side return value duckrun never surfaces, so ignoring
        # it changes no table state — left unflagged.)
        unsupported = [k for k in ("merge_on_using_columns",) if cfg.get(k)]
        if unsupported:
            raise CompilationError(
                "duckrun cannot honor these merge configs (delta_rs has no equivalent), and "
                "refuses to run them as a plain upsert because that would silently ignore what you "
                "asked for: " + ", ".join(unsupported) + ". Supported merge controls: unique_key, "
                "merge_update_columns, merge_exclude_columns, merge_update_condition, "
                "merge_insert_condition, merge_update_set_expressions, merge_clauses, "
                "incremental_predicates. Remove the unsupported keys or express the logic with the "
                "supported ones."
            )

    @staticmethod
    def _rewrite_merge_aliases(expr):
        """Rewrite dbt's standard merge aliases (DBT_INTERNAL_DEST/SOURCE) to the target/source
        aliases delta_rs uses here. Returns None unchanged so an absent condition stays absent."""
        if not expr:
            return None
        return str(expr).replace("DBT_INTERNAL_DEST", "target").replace("DBT_INTERNAL_SOURCE", "source")

    @staticmethod
    def _qualify_predicate(expr, columns):
        """Prefix bare references to known target columns with ``target.``.

        duckrun folds ``incremental_predicates`` into the merge condition
        (``target.k = source.k AND <predicate>``). A bare column there (e.g. ``id != 2``) exists
        on BOTH the source and target, so delta_rs rejects it as an ambiguous reference. dbt's
        ``incremental_predicates`` constrain the existing/target rows (the delete+insert delete, the
        merge ON), so we qualify bare column tokens to ``target.``. Only exact column-name tokens
        that aren't already qualified (preceded by ``.``) or quoted/literal are rewritten — literals
        and functions (e.g. ``current_date``, which is not a column) are left untouched.

        Quote-aware (via ``sqlscan``) rather than a regex over the raw text: a regex would rewrite a
        column name that appears *inside* a string literal (``'archived status'`` -> corrupted)."""
        return sqlscan.qualify_identifiers(expr, columns, prefix="target")

    @classmethod
    def _merge_predicates(cls, cfg: dict, columns=None):
        """dbt ``incremental_predicates`` (or ``predicates``), with dbt's standard merge
        aliases rewritten to the ones delta_rs uses here and bare column refs qualified to
        ``target.`` (see ``_qualify_predicate``)."""
        preds = cfg.get("incremental_predicates") or cfg.get("predicates")
        if not preds:
            return None
        if isinstance(preds, str):
            preds = [preds]
        return [cls._qualify_predicate(cls._rewrite_merge_aliases(p), columns) for p in preds]

    @classmethod
    def _merge_on_predicate(cls, unique_key, cfg: dict, columns=None) -> str:
        """The full MERGE ``ON`` predicate ``target.k = source.k [AND …]`` — the same string
        ``merge_delta`` builds internally from ``unique_key`` + ``incremental_predicates``. Used by
        the clause-core path (merge_clauses / merge_update_set_expressions), which takes the predicate
        directly rather than a unique_key."""
        keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
        # Quote the join keys (mixed case / reserved word / spaces produce invalid datafusion SQL
        # otherwise); strip any quotes the user already added, then double-quote and escape. Mirrors
        # engine.merge_delta so both merge paths emit the same ON predicate.
        conditions = [
            f'target.{q} = source.{q}'
            for q in ('"' + str(k).strip().strip('"').replace('"', '""') + '"' for k in keys)
        ]
        preds = cls._merge_predicates(cfg, columns)
        if preds:
            conditions.extend(p for p in preds if p)
        return " AND ".join(conditions)

    @classmethod
    def _custom_merge_clauses(cls, cfg: dict, columns, unique_key):
        """Return an ordered ``engine.merge_delta_clauses`` spec list when the config uses
        ``merge_clauses`` or ``merge_update_set_expressions`` (delta_rs's full TableMerger surface),
        else None so the caller stays on the standard flat-kwarg ``merge_delta`` path. The two knobs
        are mutually exclusive (``_validate_merge_config`` rejects ``merge_clauses`` mixed with the
        basic configs)."""
        if cfg.get("merge_clauses"):
            return cls._specs_from_merge_clauses(cfg.get("merge_clauses"), columns, unique_key)
        if cfg.get("merge_update_set_expressions"):
            return cls._specs_from_set_expressions(cfg, columns, unique_key)
        return None

    @staticmethod
    def _key_set(unique_key):
        keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
        return {str(k).lower() for k in keys}

    @classmethod
    def _explicit_cols(cls, spec, allcols, keys) -> list:
        """Columns named by an explicit-mode ``merge_clauses`` update/insert: ``{'include': [...]}``,
        ``{'exclude': [...]}``, or (absent) every non-key column."""
        if isinstance(spec, dict):
            if spec.get("include"):
                return [str(c) for c in spec["include"]]
            if spec.get("exclude"):
                ex = {str(e).lower() for e in spec["exclude"]}
                return [c for c in allcols if c.lower() not in ex]
        return [c for c in allcols if c.lower() not in keys]

    @classmethod
    def _specs_from_set_expressions(cls, cfg: dict, columns, unique_key) -> list:
        """``merge_update_set_expressions``: a matched UPDATE that copies every (non-key) column from
        source, with the named columns overridden by custom SQL expressions, plus the standard
        not-matched INSERT *. Mirrors dbt-duckdb semantics."""
        keys = cls._key_set(unique_key)
        allcols = [str(c) for c in columns]
        update_cols = cfg.get("merge_update_columns")
        exclude_cols = cfg.get("merge_exclude_columns")
        if update_cols:
            base = [str(c) for c in update_cols]
        elif exclude_cols:
            ex = {str(e).lower() for e in exclude_cols}
            base = [c for c in allcols if c.lower() not in ex and c.lower() not in keys]
        else:
            base = [c for c in allcols if c.lower() not in keys]
        updates = {c: f"source.{c}" for c in base}
        for col, expr in cfg["merge_update_set_expressions"].items():
            updates[col] = cls._rewrite_merge_aliases(expr)
        return [
            {"clause": "matched", "action": "update", "updates": updates,
             "predicate": cls._rewrite_merge_aliases(cfg.get("merge_update_condition"))},
            {"clause": "not_matched", "action": "insert_all",
             "predicate": cls._rewrite_merge_aliases(cfg.get("merge_insert_condition"))},
        ]

    @classmethod
    def _specs_from_merge_clauses(cls, merge_clauses: dict, columns, unique_key) -> list:
        """Translate a dbt-duckdb ``merge_clauses`` dict into delta_rs clause specs (applied in
        order). ``mode: by_name`` → UPDATE/INSERT all columns; ``mode: explicit`` → the columns named
        by ``update``/``insert`` include/exclude; ``action: delete`` → delete. ``when_matched`` →
        update/delete, ``when_not_matched`` → insert, ``when_not_matched_by_source`` → update/delete
        (rows the source doesn't carry — full-sync semantics)."""
        keys = cls._key_set(unique_key)
        allcols = [str(c) for c in columns]
        specs = []
        for c in merge_clauses.get("when_matched", []) or []:
            action = (c.get("action") or "").lower()
            cond = cls._rewrite_merge_aliases(c.get("condition"))
            if action == "update":
                if (c.get("mode") or "by_name").lower() == "by_name":
                    specs.append({"clause": "matched", "action": "update_all", "predicate": cond})
                else:
                    cols = cls._explicit_cols(c.get("update"), allcols, keys)
                    specs.append({"clause": "matched", "action": "update",
                                  "updates": {col: f"source.{col}" for col in cols},
                                  "predicate": cond})
            elif action == "delete":
                specs.append({"clause": "matched", "action": "delete", "predicate": cond})
            else:
                raise CompilationError(
                    f"unsupported merge_clauses.when_matched action: {action!r} "
                    f"(expected 'update' or 'delete')")
        for c in merge_clauses.get("when_not_matched", []) or []:
            action = (c.get("action") or "").lower()
            cond = cls._rewrite_merge_aliases(c.get("condition"))
            if action == "insert":
                if (c.get("mode") or "by_name").lower() == "by_name":
                    specs.append({"clause": "not_matched", "action": "insert_all", "predicate": cond})
                else:
                    cols = cls._explicit_cols(c.get("insert") or c.get("update"), allcols, keys)
                    specs.append({"clause": "not_matched", "action": "insert",
                                  "updates": {col: f"source.{col}" for col in cols},
                                  "predicate": cond})
            else:
                raise CompilationError(
                    f"unsupported merge_clauses.when_not_matched action: {action!r} "
                    f"(expected 'insert')")
        for c in merge_clauses.get("when_not_matched_by_source", []) or []:
            action = (c.get("action") or "").lower()
            cond = cls._rewrite_merge_aliases(c.get("condition"))
            if action == "update":
                cols = cls._explicit_cols(c.get("update"), allcols, keys)
                # by-source rows have no source row, so columns can't be copied from source — only an
                # explicit expression map makes sense. Support the common case (set literals/exprs)
                # via merge_clauses' update.set, else require it.
                set_map = c.get("set") or (c.get("update") or {}).get("set")
                if set_map:
                    specs.append({"clause": "not_matched_by_source", "action": "update",
                                  "updates": {k: cls._rewrite_merge_aliases(v) for k, v in set_map.items()},
                                  "predicate": cond})
                else:
                    raise CompilationError(
                        "merge_clauses.when_not_matched_by_source update requires a 'set' map "
                        "(by-source rows have no source columns to copy)")
            elif action == "delete":
                specs.append({"clause": "not_matched_by_source", "action": "delete", "predicate": cond})
            else:
                raise CompilationError(
                    f"unsupported merge_clauses.when_not_matched_by_source action: {action!r} "
                    f"(expected 'update' or 'delete')")
        return specs

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
    @staticmethod
    def source_scan_sql(source_config: SourceConfig) -> str:
        """SQL that scans a ``meta.plugin: duckrun`` source.

        The source can be a Delta table, a CSV, a Parquet, or a JSON file. ``delta_table_path``
        forces Delta (back-compat); otherwise the path is ``location``/``path`` and the format is
        ``meta.format`` or inferred from the extension (a bare directory is a Delta table).
        A source declares *where/what* (location + format) only; CSV parsing is left to
        ``read_csv_auto``'s detection — anything that needs hand-tuned parse options belongs in
        a model's ``read_csv(...)``, not the source. DuckrunEnvironment.load_source wraps this in
        ``CREATE OR REPLACE VIEW`` — a connection-independent catalog view, so no pyarrow and no
        copying the source into a table.
        """
        delta_path = source_config.get("delta_table_path")
        path = delta_path or source_config.get("location") or source_config.get("path")
        if not path:
            raise ValueError(
                "duckrun source requires 'delta_table_path', 'location', or 'path' in meta."
            )

        fmt = (source_config.get("format") or "").strip().lower()
        if delta_path:
            fmt = "delta"
        if not fmt:
            lower = str(path).lower()
            if lower.endswith(".csv") or lower.endswith(".csv.gz"):
                fmt = "csv"
            elif lower.endswith(".parquet") or lower.endswith(".pq"):
                fmt = "parquet"
            elif lower.endswith(".json") or lower.endswith(".ndjson") or lower.endswith(".json.gz"):
                fmt = "json"
            else:
                fmt = "delta"

        # Escape single quotes so a path can't break out of the string literal.
        path_sql = str(path).replace("'", "''")
        if fmt == "delta":
            return f"SELECT * FROM delta_scan('{path_sql}')"
        if fmt == "parquet":
            return f"SELECT * FROM read_parquet('{path_sql}')"
        if fmt == "csv":
            # read_csv_auto detects header/types; a source carries no parse options by design.
            return f"SELECT * FROM read_csv_auto('{path_sql}')"
        if fmt == "json":
            # read_json_auto detects records/columns; like CSV, a source carries no parse options.
            # Raise maximum_object_size from the 16 MB default — web JSON exports (e.g. a GeoJSON
            # FeatureCollection) routinely exceed it, and a source has no other place to set it.
            return f"SELECT * FROM read_json_auto('{path_sql}', maximum_object_size=2147483647)"
        raise ValueError(
            f"Unsupported duckrun source format {fmt!r}; expected 'csv', 'parquet', 'json', or 'delta'."
        )

    def load(self, source_config: SourceConfig):
        # Kept for dbt-duckdb's stock load_source path; DuckrunEnvironment registers duckrun
        # sources as catalog views via source_scan_sql instead of this relation.
        return self._cursor().sql(self.source_scan_sql(source_config))

    def default_materialization(self) -> str:
        return "view"
