"""
dbt-duckdb plugin that materializes a model relation as a Delta Lake table.

This is the one piece dbt-duckdb lacks: a Delta *write*. The plugin stashes the DuckDB
connection (``configure_connection``), and on ``store()`` hands the model relation
straight to delta_rs. DuckDB relations expose the Arrow C-stream interface, which
deltalake 1.x consumes directly, so there is no pyarrow dependency.
"""
import os
import re
import threading
from typing import Any, Optional

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

from . import delta_dml
from . import engine
from . import secret
from . import sortkey
from . import sqlscan

try:  # raise on_schema_change='fail' as a dbt compilation error (matches dbt semantics)
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


class Plugin(BasePlugin):
    """Registered automatically by the duckrun adapter (alias ``duckrun``)."""

    # Class-level fallbacks, replaced per instance in initialize(). They exist so a Plugin built
    # without going through initialize() (``Plugin.__new__``) still has a cursor slot and a lock.
    _local = threading.local()
    _lock = threading.Lock()

    @property
    def _cursor_handle(self):
        """The DuckDB cursor this thread is working on, or None.

        PER THREAD, not per plugin: dbt instantiates the plugin ONCE per environment but gives every
        worker thread its own cursor, and store() hands that cursor's relation to delta_rs as a live
        Arrow stream. A single shared slot meant the last configure_cursor won, so at threads>1 two
        models would stream from one DuckDB connection — which DuckDB rejects outright ("Attempting
        to execute an unsuccessful or closed pending query result"). Keeping it thread-local is what
        keeps each model's stream on the cursor that staged it."""
        return getattr(self._local, "cursor", None)

    @_cursor_handle.setter
    def _cursor_handle(self, cursor) -> None:
        self._local.cursor = cursor

    def initialize(self, config: dict) -> None:
        config = config or {}
        self._storage_options: Optional[dict] = config.get("storage_options")
        # Per-catalog write config (issue #7): {alias: {root_path, storage_options}} + the default
        # catalog's name, so store() can pick the write token by the relation's database. Empty for
        # a single-catalog project — store() then always uses the default storage_options.
        self._catalogs: dict = config.get("catalogs") or {}
        self._default_database = config.get("default_database")
        self._conn = None
        self._local = threading.local()  # backs _cursor_handle; see the property above
        # Guards the shared mutable state below against concurrent models: _microbatch_seen's
        # check-then-act (two threads must not both decide they're the first batch) and the token
        # refresh's read-modify-write of _storage_options / _catalogs.
        self._lock = threading.Lock()
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
        # Always-on session tuning (preserve_insertion_order=false, a temp_directory to spill to,
        # and the one memory_limit pin — 85% of the effective limit, tighten-only, all thread
        # counts). No per-model memory tuning happens after this; delta_rs merges are bounded by
        # their own spill caps behind the merge gate.
        try:
            engine.configure_duckdb_session(conn)
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
        # of the shared connection — or the variables/relations won't be visible. dbt resolves each
        # thread's handle on that thread, so this lands in that thread's slot (see _cursor_handle)
        # and store() on the same thread reads it back.
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
        if fresh is so:  # token still valid (the common path — stays lock-free)
            return
        # Only the genuine re-acquisition takes the lock: the setter rewrites plugin-wide state and
        # re-mints an instance-global DuckDB secret, neither of which tolerates two threads at once.
        with self._lock:
            setter(fresh)  # token was actually re-acquired — keep the live copy in sync
            self._mint_refreshed_secret(cursor, fresh, catalog, is_default)

    def _mint_refreshed_secret(self, cursor, fresh, catalog, is_default) -> None:
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
        # Prefer THIS THREAD's live per-model cursor (shares the session where pre-hook variables
        # and the staged relation were created); fall back to the shared connection.
        cursor = self._cursor_handle
        if cursor is not None:
            return cursor
        if engine.RUN_THREADS > 1:
            # The fallback below would hand this model a cursor that can't see its staged relation
            # or its pre-hook variables, and — worse under concurrency — a second thread streaming
            # off the shared connection. Fail loud instead: silence here is a corrupt/failed write.
            raise RuntimeError(
                "duckrun: no DuckDB cursor was configured on this thread, but the run is "
                f"multi-threaded (threads={engine.RUN_THREADS}). Re-run with `threads: 1` and "
                "report this — the write path cannot safely share a cursor across threads."
            )
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
        # Per-model write geometry (both optional; None = the fixed defaults). Explicit ceilings
        # like sort_by/partition_by: duckrun-only configs upstream parses and ignores. Validated
        # loudly HERE, before any Delta access, so a typo fails the model cleanly.
        row_group_rows, target_file_size = self._geometry_config(cfg)
        # issue #42: keep-NTZ escape hatch (+timestamp_ntz: true). None = unset → the engine
        # falls back to DUCKRUN_TIMESTAMP_NTZ; validated loudly here like the geometry configs.
        timestamp_ntz = self._timestamp_ntz_config(cfg)
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
        # A profile that omits bearer_token is fine: for an abfss:// target, self-acquire a OneLake
        # token (Fabric notebook / OIDC / azure-identity) so the delta-rs write authenticates — the
        # same fallback the read/discovery path uses. No-op for local/az:// or when a token is present.
        storage_options = secret.with_onelake_token(path, storage_options)

        # Keep `cur` referenced for the whole write so the relation's Arrow stream
        # stays valid while deltalake consumes it.
        cur = self._cursor()
        name = self._relation_name(target_config.relation)
        dt0 = engine.open_if_exists(path, storage_options)
        exists = dt0 is not None

        # A duckrun drop-tombstone (the notebook API's `drop table` overwrites the table to a
        # one-column marker; nothing is deleted) IS a real Delta table, so `exists` alone would
        # send an incremental model down the merge branch — merging into the marker's schema and
        # dying on "no field named target.<key>". Discovery already HIDES tombstones from dbt's
        # relation cache; mirror that here at store time: the next build of a dropped table is
        # CREATE-after-DROP, a full overwrite. `exists` stays True so the overwrite branch takes
        # schema_mode="overwrite" and replaces the marker schema with the model's real one.
        if exists and delta_dml.is_dropped_dt(dt0):
            incremental = False

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

        _resolved_strategy = strategy or ("merge" if unique_key else "append")
        not_null_columns = cfg.get("not_null_columns") or []
        _merge_path = (incremental and not full_refresh and exists
                       and _resolved_strategy in ("merge", "insert"))
        # Does resolving sort_by mean PROFILING? Only 'auto' does, and only where the write sorts.
        _profile_sort = (
            isinstance(cfg.get("sort_by"), str) and cfg["sort_by"].strip().lower() == "auto"
            and not self._sort_by_is_inert(
                cfg, incremental, full_refresh, exists, _resolved_strategy))

        # MERGE-path materialization (#14): the not-null guard, the merge cardinality guard and
        # delta_rs's source collection must all see IDENTICAL rows, or a nondeterministic model
        # (now(), a moved external source) lets the guards vouch for rows that aren't the ones
        # written. Merge-only: the profiled overwrite path stages its own substrate below.
        src_tmp = None
        psub = None
        model_name = name           # the RELATION's name — survives the temp-table rebinds below so
        #                             logs and the profile label keep naming the model, not a
        #                             staging table (the benchmark history records the label, and
        #                             0.4.54 briefly recorded the temp name).
        if _merge_path and (cfg.get("merge_materialize_source") or not_null_columns):
            src_tmp = '"' + engine.tmp_name("msrc", path) + '"'
            cur.execute(f"CREATE OR REPLACE TEMP TABLE {src_tmp} AS SELECT * FROM {name}")
            name = src_tmp          # guards and write all read the one materialization

        # sort_by='auto' profiling SUBSTRATE (sortkey v6). The staged relation is a VIEW over the
        # model SQL, so every scan re-runs it — and the profiler scans its source several times.
        # Copying ALL rows first (0.4.54's msrc) was the defect at scale: materializing 591.7M rows
        # cost ~5 min and every profile pass re-read a 38 GB spill (~49 min of a 72-min build). Now:
        #   - exact COUNT(*) off the view first (footer metadata for scan models — near-free);
        #   - at/below sortkey.SUBSTRATE_CAP: a FULL local copy — it doubles as the staging table
        #     (guards + write read it; one model evaluation), byte-identical to the old behavior;
        #   - above the cap: a deterministic hash(row) % K subset (~cap rows) for the profile ONLY,
        #     and the write reads the VIEW directly (re-evaluating a bare scan is far cheaper than
        #     copying the world; a nondeterministic model at worst gets a marginally stale key).
        profile_src, full_rows = name, None
        if _profile_sort:
            full_rows = int(cur.sql(f"SELECT count(*) FROM {name}").fetchone()[0] or 0)
            k = sortkey.substrate_modulus(full_rows)
            psub = '"' + engine.tmp_name("psub", path) + '"'
            if k == 1:
                cur.execute(f"CREATE OR REPLACE TEMP TABLE {psub} AS SELECT * FROM {name}")
                name = psub         # the full copy IS the staging table
                full_rows = None    # profile covers every row — exact semantics, v5-identical
            else:
                cur.execute(f"CREATE OR REPLACE TEMP TABLE {psub} AS "
                            f"SELECT * FROM {name} _r WHERE hash(_r) % {k} = 0")
            profile_src = psub

        # sort_by makes the write order EXPLICIT. A trailing ORDER BY inside the model SQL is not
        # honored here — the staged relation is read through a wrapper SELECT *, and with
        # preserve_insertion_order=false DuckDB may reorder any result lacking a top-level ORDER BY.
        # A top-level ORDER BY on this read IS honored, so long RLE runs / dictionary locality (the
        # point of the Parquet tuning) are deterministic regardless of the global flag.
        # sort_by='auto' resolves here to concrete columns (or None); rewrite cfg so every
        # downstream cfg.get('sort_by') reader (the merge branch) sees the resolved value too.
        #
        # Resolved HERE, after `exists` and the tombstone adjustment, rather than at the top of
        # store(): `_sort_by_is_inert` needs the branch this write will actually take, because
        # resolving 'auto' means PROFILING the staged relation and three branches then throw the
        # answer away (see that method). A project-wide `+sort_by: auto` used to pay that profile on
        # every incremental run of every merge model.
        sort_by, auto_geom = self._resolve_sort_by(
            cur, profile_src, cfg.get("sort_by"), partition_by, profile=_profile_sort,
            display_name=model_name, full_rows=full_rows)
        cfg["sort_by"] = sort_by
        # Wide-DECIMAL narrowing, the dbt spelling of session._narrow_wide_decimals: a
        # DECIMAL(p>18) maps to a 16-byte FLBA that arrow-rs never dictionary-encodes, so a
        # SORTED BY AUTO write narrows it back to INT64 territory when the exact column max fits.
        # Overwrite branch ONLY — an append/merge increment must keep the existing table's schema —
        # and only on the profiled path, whose geometry already prices the narrowed width
        # (auto_sort_cols narrow_decimals=True in _resolve_sort_by).
        select_body = f"SELECT * FROM {name}"
        if _profile_sort and (not incremental or full_refresh or not exists):
            select_body = self._narrow_wide_decimals_select(cur, name)
        if sort_by:
            cols = sort_by if isinstance(sort_by, (list, tuple)) else [sort_by]
            order = ", ".join(engine.quote_ident(c) for c in cols)
            data = cur.sql(f"{select_body} ORDER BY {order}")
        else:
            data = cur.sql(select_body)

        # Contract NOT NULL enforcement (config(contract={enforced:true}) with a not_null column
        # constraint). duckrun writes via delta_rs, not SQL DDL, so dbt-core's column-constraint
        # DDL never runs. Guard the staged rows BEFORE any write: a null in a not-null column
        # raises, and because nothing has been written yet the prior Delta version is untouched
        # (the rollback the constraint tests assert). Message carries "NOT NULL constraint failed"
        # to match dbt's standard contract-error phrasing.
        if not_null_columns:
            self._assert_not_null(cur, name, not_null_columns)

        # try/finally: src_tmp may now be created for the auto-sort profile on branches that
        # return early (overwrite, append, microbatch, delete+insert), so the one release point
        # has to cover every exit. The two eager drops inside stay — they free a full copy of the
        # model result before post-write maintenance runs, and DROP ... IF EXISTS is idempotent.
        try:
                # Microbatch is delete+insert per event_time window, not a key-based upsert, so it
                # bypasses the generic overwrite/merge dispatch below (which would clobber every batch
                # under --full-refresh, since dbt marks each microbatch batch full_refresh in that case).
                if incremental and strategy == "microbatch":
                    self._store_microbatch(
                        path, cur, name, cfg, storage_options, exists, full_refresh,
                        read_version=cfg.get("read_version"),
                        row_group_rows=row_group_rows, target_file_size=target_file_size,
                        timestamp_ntz=timestamp_ntz, existing_dt=dt0,
                    )
                    return

                # Table-like (non-incremental) models always overwrite. Incremental models
                # overwrite on first run / full-refresh, then apply the incremental strategy.
                if not incremental or full_refresh or not exists:
                    # The AUTO profile's geometry lands ONLY on this branch: it measures the whole
                    # result, so it describes a full rewrite and nothing else — an append or an
                    # incremental increment knows nothing about the table it joins. An explicit
                    # per-model geometry wins verbatim, and declaring EITHER knob backs this off
                    # entirely rather than half-honoring a declared layout.
                    _rg, _tfs = row_group_rows, target_file_size
                    if auto_geom and _rg is None and _tfs is None:
                        _rg, _tfs = auto_geom["row_group_rows"], auto_geom["target_file_size"]
                    self._store_overwrite(path, cur, data, partition_by, merge_schema, exists,
                                          storage_options,
                                          row_group_rows=_rg,
                                          target_file_size=_tfs,
                                          timestamp_ntz=timestamp_ntz, existing_dt=dt0)
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
                        row_group_rows=row_group_rows, target_file_size=target_file_size,
                        timestamp_ntz=timestamp_ntz, existing_dt=dt0,
                    )
                    return

                # insert (insert-only) is the one incremental shape that never removes a row, so it is a
                # PURE APPEND — computed as a DuckDB anti-join and committed with `add` actions only, no
                # target file rewritten. delta_rs's merge produces the same table at a cost that scales with
                # the target's partition span instead of the batch. The advanced clause surface
                # (merge_clauses / merge_update_set_expressions) takes the _store_merge branch below instead,
                # but lands at the SAME engine seam — so a clause list that IS insert-only (dbt-duckdb's
                # `merge_clauses: {when_matched: [{action: do_nothing}]}`) gets the anti-join too. The
                # documented way back to delta_rs, for either spelling, is `merge_streamed_exec: true`.
                if (strategy == "insert"
                        and self._custom_merge_clauses(cfg, data.columns, unique_key) is None):
                    self._validate_merge_config(cfg)
                    if not unique_key:
                        raise ValueError("incremental_strategy='insert' requires a unique_key.")
                    evolve_schema, _ = self._resolve_schema_change(
                        (cfg.get("on_schema_change") or "ignore").lower(), path, data, storage_options
                    )
                    # No per-model memory tuning: the session pin covers every path, and the anti-join
                    # runs at DuckDB's full pinned limit. The merge overrides are forwarded so the
                    # `merge_streamed_exec: true` escape hatch (and the spill caps, should the engine
                    # fall through to a real delta_rs merge) work on this spelling too.
                    with engine.mem_profile("insert", con=cur):
                        self._store_insert(
                            path, cur, name, data, unique_key, storage_options,
                            read_version=cfg.get("read_version"),
                            partition_by=partition_by,
                            merge_schema=evolve_schema,
                            incremental_predicates=cfg.get("incremental_predicates"),
                            insert_condition=cfg.get("merge_insert_condition"),
                            sort_by=sort_by,
                            max_spill_size=cfg.get("merge_max_spill_size"),
                            max_temp_directory_size=cfg.get("merge_max_temp_directory_size"),
                            streamed_exec=bool(cfg.get("merge_streamed_exec")),
                            row_group_rows=row_group_rows, target_file_size=target_file_size,
                            timestamp_ntz=timestamp_ntz, existing_dt=dt0,
                        )
                    if src_tmp is not None:
                        cur.execute(f"DROP TABLE IF EXISTS {src_tmp}")
                    return

                if strategy in ("merge", "insert"):
                    self._store_merge(path, cur, data, cfg, unique_key, strategy, storage_options,
                                      src_tmp, row_group_rows=row_group_rows,
                                      target_file_size=target_file_size,
                                      timestamp_ntz=timestamp_ntz, existing_dt=dt0)
                elif strategy == "append":
                    self._store_append(path, cur, data, cfg, partition_by, merge_schema, storage_options,
                                       row_group_rows=row_group_rows, target_file_size=target_file_size,
                                       timestamp_ntz=timestamp_ntz, existing_dt=dt0)
                else:
                    raise ValueError(
                        f"Unknown incremental_strategy '{strategy}'. "
                        "Use 'merge', 'insert', 'delete+insert', 'append', or 'microbatch'."
                    )
        finally:
            if src_tmp is not None:
                cur.execute(f"DROP TABLE IF EXISTS {src_tmp}")
            if psub is not None:
                cur.execute(f"DROP TABLE IF EXISTS {psub}")

    @staticmethod
    def _geometry_config(cfg):
        """The per-model write-geometry overrides: ``max_row_group_size`` (rows — deltalake's own
        ``WriterProperties`` spelling) and ``target_file_size_mb`` (megabytes; converted to bytes
        HERE — everything below the plugin speaks bytes). Returns ``(row_group_rows, target_bytes)``,
        each ``None`` when unset (the fixed 8M ceiling / 128 MB roll stay in charge).
        Explicit values are CEILINGS the engine honors verbatim.
        Ints or digit-strings accepted (YAML quoting); anything else fails the model loudly."""
        def _pos_int(key):
            val = cfg.get(key)
            if val is None:
                return None
            if isinstance(val, str) and val.strip().isdigit():
                val = int(val.strip())
            if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                raise ValueError(f"{key} must be a positive integer, got {val!r}")
            return val
        rg = _pos_int("max_row_group_size")
        mb = _pos_int("target_file_size_mb")
        return rg, (mb * 1024 * 1024 if mb is not None else None)

    @staticmethod
    def _timestamp_ntz_config(cfg):
        """The per-model ``timestamp_ntz`` escape hatch (issue #42): True keeps naive TIMESTAMP
        columns as Delta ``timestamp_ntz`` instead of the default UTC-adjust coercion. Returns
        None when unset (the engine then consults ``DUCKRUN_TIMESTAMP_NTZ``). Bools or the YAML
        string spellings accepted; anything else fails the model loudly, before any Delta access."""
        val = cfg.get("timestamp_ntz")
        if val is None:
            return None
        if isinstance(val, bool):
            return val
        if isinstance(val, str) and val.strip().lower() in ("true", "false"):
            return val.strip().lower() == "true"
        raise ValueError(f"timestamp_ntz must be true or false, got {val!r}")

    @staticmethod
    def _sort_by_is_inert(cfg, incremental, full_refresh, exists, resolved_strategy) -> bool:
        """True when the branch this write will take never reads ``sort_by``, so resolving
        ``'auto'`` would profile the staged relation and throw the answer away.

        ``sort_by`` is not a delta_rs option — it is a DuckDB ``ORDER BY`` baked into the ``data``
        relation in :meth:`store`. Three branches never read ``data``: ``_store_microbatch`` and
        ``_store_delete_insert`` take the staged relation NAME instead, and the plain
        ``engine.merge_delta`` branch writes into the target's existing layout (documented in
        ``docs/dbt-adapter.md`` and in :meth:`_store_merge`'s docstring). A ``+sort_by: auto`` set
        project-wide therefore used to pay a full profile on every incremental run of every merge
        model for a value that was discarded on the next line.

        Deliberately CONSERVATIVE — it returns True only for branches proven inert, so anything
        unrecognised keeps today's behavior (profile). In particular a custom clause list
        (``merge_clauses`` / ``merge_update_set_expressions``) may route to the insert-only
        anti-join + append inside ``engine.merge_delta_clauses``, which DOES honor ``sort_by``, so
        those keep profiling. The key presence test mirrors ``_custom_merge_clauses``'s own dispatch
        without calling the spec builders — they can raise, and validation must stay where it is."""
        # Microbatch first: its dispatch is `incremental and strategy == 'microbatch'` alone — it
        # deliberately bypasses the overwrite/merge routing below (dbt marks every batch
        # full_refresh under --full-refresh, which would otherwise clobber each one).
        if incremental and resolved_strategy == "microbatch":
            return True
        if not (incremental and not full_refresh and exists):
            return False                                    # overwrite branch — honors sort_by
        if resolved_strategy in ("delete+insert", "delete_insert"):
            return True
        if resolved_strategy == "merge":
            return not (cfg.get("merge_clauses") or cfg.get("merge_update_set_expressions"))
        return False                                        # append / insert / unknown — honors it

    @staticmethod
    def _narrow_wide_decimals_select(cur, name):
        """``SELECT * [REPLACE (…)] FROM name`` that narrows wide-DECIMAL columns so they regain
        dictionary encoding, or the plain passthrough when there is nothing to narrow.

        The dbt port of :meth:`duckrun.session.DuckSession._narrow_wide_decimals` — same rule
        (``DECIMAL(p>18, s)`` → ``DECIMAL(18, s)`` via :func:`sortkey.decimal_narrow_target`), same
        exact ``max(abs(c))`` guard (one aggregate scan over just the wide columns, so the
        unconditional cast can never overflow at write time), same ``DUCKRUN_NARROW_DECIMALS``
        kill switch. Advisories go to the adapter log instead of stdout. Best-effort: any failure
        returns the passthrough — narrowing is an optimization, never a reason to fail a model."""
        default = f"SELECT * FROM {name}"
        if os.environ.get("DUCKRUN_NARROW_DECIMALS", "1") == "0":
            return default
        try:
            desc = cur.sql(f"DESCRIBE SELECT * FROM {name}").fetchall()
            wide = []
            for row in desc:
                col, typ = row[0], str(row[1])
                dm = sortkey._DECIMAL_RE.fullmatch(typ.strip())
                if dm and int(dm.group(1)) > sortkey._DECIMAL_NARROW_PRECISION:
                    wide.append((col, typ))
            if not wide:
                return default
            aggs = ", ".join(f"max(abs({engine.quote_ident(c)}))" for c, _ in wide)
            maxes = cur.sql(f"SELECT {aggs} FROM {name}").fetchone()
            repl = []
            for (col, typ), mv in zip(wide, maxes):
                target = sortkey.decimal_narrow_target(typ, mv)
                if target:
                    repl.append(f"CAST({engine.quote_ident(col)} AS {target}) "
                                f"AS {engine.quote_ident(col)}")
                    engine.logger.info(
                        f"duckrun: {col} {typ} -> {target} "
                        f"(max {mv if mv is not None else 'NULL'}; FLBA has no dictionary in arrow-rs)")
                else:
                    engine.logger.info(
                        f"duckrun: {col} {typ} kept: max too large to narrow - no dictionary encoding")
            if not repl:
                return default
            return f"SELECT * REPLACE ({', '.join(repl)}) FROM {name}"
        except Exception as exc:
            engine.logger.debug(f"duckrun: decimal narrowing skipped: {exc}")
            return default

    def _resolve_sort_by(self, cur, name, sort_by, partition_by, *, profile=True,
                         display_name=None, full_rows=None):
        """Resolve ``sort_by='auto'`` (case-insensitive scalar) into concrete columns by profiling
        the staged relation via :func:`engine.auto_sort_cols` — the dbt spelling of the connection
        API's ``CREATE TABLE … SORTED BY AUTO``, backed by the same sampler. No payoff resolves to
        ``None`` (unsorted write, exactly as connect() drops the clause). ``'auto'`` inside a list
        is rejected — which also means a column literally named ``auto`` can't be addressed here.
        Every other config value passes through untouched.

        ``display_name`` is what logs and the profile label call the relation; ``name`` is what
        gets QUERIED. They diverge when the caller staged the model into a ``__duckrun_msrc_*``
        temp table — the log line and the benchmark history keying off it must keep saying
        ``mart.fct_trips``, not the staging table's hash.

        ``profile=False`` (see :meth:`_sort_by_is_inert`) resolves ``'auto'`` to ``None`` WITHOUT
        profiling, for a write branch that would discard the key anyway. Validation is deliberately
        NOT gated on it: ``['auto']`` still raises on every path, so a typo can't go quiet just
        because the model happens to be a merge.

        Returns ``(sort_by, geom)``. ``geom`` is the one-row-group-per-file write geometry the same
        profile paid for (see :func:`engine.auto_sort_cols`), or ``None`` on every path that did not
        profile — an explicit column list, a non-auto value, or an inert branch — which is what keeps
        the geometry tied to a real profile instead of leaking onto writes that never had one."""
        if isinstance(sort_by, (list, tuple)):
            if any(isinstance(c, str) and c.strip().lower() == "auto" for c in sort_by):
                raise ValueError(
                    "sort_by: 'auto' must be the scalar value (sort_by='auto'), not a list "
                    "element. Use an explicit column list otherwise."
                )
            return sort_by, None
        if not (isinstance(sort_by, str) and sort_by.strip().lower() == "auto"):
            return sort_by, None
        disp = display_name or name
        if not profile:
            engine.logger.debug(
                f"duckrun: sort_by=auto skipped for {disp} — this write path does not sort")
            return None, None
        pcols = (list(partition_by) if isinstance(partition_by, (list, tuple))
                 else [partition_by] if partition_by else [])
        key, lines, geom = engine.auto_sort_cols(cur, name, partition_cols=pcols,
                                                 label=("model", disp), narrow_decimals=True,
                                                 full_rows=full_rows)
        for line in lines:  # full advisory (model version, per-column verdicts) at debug
            engine.logger.debug(line)
        engine.logger.info(
            f"duckrun: sort_by=auto for {disp} -> "
            + (", ".join(key) if key else "no sort (nothing pays off)"))
        return key or None, geom

    def _store_overwrite(self, path, cur, data, partition_by, merge_schema, exists,
                         storage_options, row_group_rows=None, target_file_size=None,
                         timestamp_ntz=None, existing_dt=None) -> None:
        """The CREATE OR REPLACE branch: a table model, a --full-refresh, or a first run.
        When we are REPLACING an existing table (exists), allow delta_rs to replace the schema
        wholesale (schema_mode="overwrite") — the model SQL defines the new schema, exactly as
        `CREATE OR REPLACE TABLE` does on every other warehouse. Without it, delta_rs's strict
        overwrite keeps the OLD schema/protocol and so can't change a column's type or write a
        column needing a new writer feature the old table lacks (e.g. retyping to ::timestamp /
        timestampNtz). This is scoped to the full-rebuild replace ONLY — NOT append, merge, or
        microbatch, which must keep their strict, schema-stable writes. A fresh create
        (not exists) doesn't need it. A user's explicit merge_schema still wins."""
        overwrite_schema = exists and not merge_schema
        with engine.mem_profile("overwrite", con=cur):
            engine.write_delta(
                path, data, "overwrite",
                partition_by=partition_by,
                merge_schema=merge_schema,
                overwrite_schema=overwrite_schema,
                storage_options=storage_options,
                cur=cur,
                row_group_rows=row_group_rows,
                target_file_size=target_file_size,
                timestamp_ntz=timestamp_ntz,
                existing_dt=existing_dt,
            )

    def _store_merge(self, path, cur, data, cfg, unique_key, strategy, storage_options,
                     src_tmp, row_group_rows=None, target_file_size=None,
                     timestamp_ntz=None, existing_dt=None) -> None:
        """The merge / insert strategies: validate config, resolve on_schema_change, and dispatch
        to the clause-core (merge_clauses / merge_update_set_expressions) or the flat-kwarg
        merge_delta path.

        ``partition_by`` / ``sort_by`` are forwarded even though a merge writes into whatever
        partitioning the table already has: an insert-only clause list is ROUTED to the anti-join +
        plain append inside ``engine.merge_delta_clauses``, and that append needs them for the exact
        partition ``IN`` probe filter (``engine.probe_filters``) and the write order. They are inert on
        the delta_rs merge branch, which never reads them."""
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
        evolve_schema, existing_cols = self._resolve_schema_change(
            on_schema_change, path, data, storage_options
        )
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
                    existing_columns=existing_cols,
                    max_spill_size=cfg.get("merge_max_spill_size"),
                    max_temp_directory_size=cfg.get("merge_max_temp_directory_size"),
                    streamed_exec=(False if sx is None else bool(sx)),
                    read_version=cfg.get("read_version"),
                    partition_by=cfg.get("partition_by"),
                    sort_by=cfg.get("sort_by"),
                    storage_options=storage_options,
                    cur=cur,
                    row_group_rows=row_group_rows,
                    target_file_size=target_file_size,
                    timestamp_ntz=timestamp_ntz,
                    existing_dt=existing_dt,
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
                    existing_columns=existing_cols,
                    max_spill_size=cfg.get("merge_max_spill_size"),
                    max_temp_directory_size=cfg.get("merge_max_temp_directory_size"),
                    streamed_exec=(False if sx is None else bool(sx)),
                    # Pin the merge target to the version the model read (vB, captured before it
                    # read {{ this }}), so OCC validates (vB, HEAD] — read and commit are one snapshot.
                    read_version=cfg.get("read_version"),
                    storage_options=storage_options,
                    cur=cur,
                    row_group_rows=row_group_rows,
                    target_file_size=target_file_size,
                    timestamp_ntz=timestamp_ntz,
                    existing_dt=existing_dt,
                )
        if src_tmp is not None:
            cur.execute(f"DROP TABLE IF EXISTS {src_tmp}")  # #14: release the materialized source

    def _store_append(self, path, cur, data, cfg, partition_by, merge_schema,
                      storage_options, row_group_rows=None, target_file_size=None,
                      timestamp_ntz=None, existing_dt=None) -> None:
        """The append strategy. A read-modify-append on the SAME table — the model read {{ this }}
        (e.g. an incremental append whose watermark is `max(ts) from {{ this }}`) — is fenced to the
        version the model read (vB, captured before it read {{ this }}): a concurrent commit any time
        during the build makes it fail loudly (CommitFailedError) instead of appending a duplicate.
        This is the automatic append_if_unchanged behavior — no strategy to pick. A plain append of
        NEW data (no {{ this }} read) is unfenced (last-writer-wins / additive). CAS via delta_rs
        max_commit_retries=0 (engine). No dedup — that's the SQL's job."""
        rv = cfg.get("read_version") if cfg.get("reads_self") else None
        with engine.mem_profile("append", con=cur):
            if rv is not None:
                engine.append_if_unchanged(
                    path, data,
                    read_version=rv,
                    partition_by=partition_by,
                    merge_schema=merge_schema,
                    storage_options=storage_options,
                    cur=cur,
                    row_group_rows=row_group_rows,
                    target_file_size=target_file_size,
                    timestamp_ntz=timestamp_ntz,
                    existing_dt=existing_dt,
                )
            else:
                engine.write_delta(
                    path, data, "append",
                    partition_by=partition_by,
                    merge_schema=merge_schema,
                    storage_options=storage_options,
                    cur=cur,
                    row_group_rows=row_group_rows,
                    target_file_size=target_file_size,
                    timestamp_ntz=timestamp_ntz,
                    existing_dt=existing_dt,
                )

    def _store_microbatch(
        self, path, cur, name, cfg, storage_options, exists, full_refresh,
        read_version=None, row_group_rows=None, target_file_size=None,
        timestamp_ntz=None, existing_dt=None,
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
        _etime = engine.quote_ident(event_time)
        window = cur.sql(
            f"SELECT * FROM {name} WHERE "
            f"CAST({_etime} AS TIMESTAMP) >= CAST('{start}' AS TIMESTAMP) "
            f"AND CAST({_etime} AS TIMESTAMP) < CAST('{end}' AS TIMESTAMP)"
        )

        # First batch of a --full-refresh run truncates; later batches (and every batch of a
        # normal run) append into the window. A brand-new table is just created.
        invocation = cfg.get("invocation_id")
        # Locked: dbt fans a microbatch model's batches across the SAME pool it sizes from `threads`,
        # so at threads>1 two batches hit this concurrently. Unguarded, both would read an empty set,
        # both would call themselves the first batch, and both would overwrite — silently dropping a
        # batch on --full-refresh. Prune, test and add must be one critical section.
        with self._lock:
            # Only the current run's batches matter; drop bookkeeping from earlier invocations so
            # this set can't grow unbounded in a long-lived process (a notebook / runner doing many
            # runs).
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
                row_group_rows=row_group_rows,
                target_file_size=target_file_size,
                timestamp_ntz=timestamp_ntz,
                existing_dt=existing_dt,
            )
        else:
            engine.replace_window(
                path, window,
                column=event_time, start=start, end=end,
                read_version=read_version,
                partition_by=partition_by,
                storage_options=storage_options,
                cur=cur,
                row_group_rows=row_group_rows,
                target_file_size=target_file_size,
                timestamp_ntz=timestamp_ntz,
                existing_dt=existing_dt,
            )

    def _store_delete_insert(
        self, path, cur, name, unique_key, storage_options,
        read_version=None, partition_by=None, incremental_predicates=None,
        row_group_rows=None, target_file_size=None,
        timestamp_ntz=None, existing_dt=None,
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

        tgt_rel = cur.sql(f"SELECT * FROM delta_scan('{loc_sql}', version => {vB}) LIMIT 0")
        batch_rel = cur.sql(f"SELECT * FROM {name} LIMIT 0")
        target_cols = list(tgt_rel.columns)
        batch_cols = list(batch_rel.columns)
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
        # issue #42 strategy parity: a batch column that is naive while the target column is tz-aware
        # gets the explicit UTC read here — the bare UNION would otherwise implicit-cast it through
        # the session TimeZone, making delete+insert disagree with merge on the same model. NTZ
        # targets are untouched (both sides stay naive; the engine seam then target-aware-skips).
        tgt_types = {c.lower(): str(t).upper() for c, t in zip(target_cols, tgt_rel.types)}
        batch_naive = (set() if engine.resolve_timestamp_ntz(timestamp_ntz) else
                       {c.lower() for c, t in zip(batch_cols, batch_rel.types)
                        if str(t).upper() in engine._NAIVE_TS_TYPES})

        def _bcol(c):
            if (c.lower() in batch_naive
                    and tgt_types.get(c.lower(), "").startswith("TIMESTAMP WITH TIME ZONE")):
                return f'timezone(\'UTC\', CAST("{c}" AS TIMESTAMP)) AS "{c}"'
            return f'"{c}"'

        tcols_t = ", ".join(f't."{c}"' for c in target_cols)
        tcols = ", ".join(_bcol(c) for c in target_cols)
        tmp = engine.tmp_name("di", path)
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
                cur=cur,
                row_group_rows=row_group_rows,
                target_file_size=target_file_size,
                timestamp_ntz=timestamp_ntz,
                existing_dt=existing_dt,
            )
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{tmp}"')

    def _store_insert(
        self, path, cur, name, data, unique_key, storage_options,
        read_version=None, partition_by=None, merge_schema=False,
        incremental_predicates=None, insert_condition=None, sort_by=None,
        max_spill_size=None, max_temp_directory_size=None, streamed_exec=False,
        row_group_rows=None, target_file_size=None,
        timestamp_ntz=None, existing_dt=None,
    ) -> None:
        """dbt ``incremental_strategy='insert'``: append only the batch rows whose ``unique_key`` is
        not already in the target — an idempotent dedupe-append.

        This method is dbt POLICY only: resolve the model's config into a merge, enforce the dbt-shaped
        guards (column reconciliation against ``on_schema_change``, the insert-condition restriction),
        and hand off. The MECHANISM — the DuckDB anti-join committed as a plain append — lives in
        ``engine.insert_delta``, reached through ``engine.merge_delta``'s insert-only clause list, which
        ``engine.merge_delta_clauses`` diverts. That indirection is the point: a raw
        ``MERGE INTO … WHEN NOT MATCHED THEN INSERT *`` funnels through the same seam, so the same
        operation cannot execute one way from dbt and another way from SQL.

        Insert-only is the one incremental shape that never removes a row, so no target file is
        rewritten and the Delta commit carries ``add`` actions only. See ``engine.insert_delta`` for the
        equivalence argument (NULL keys included), the always-on snapshot fence, and why a batch that
        adds nothing writes no commit at all."""
        keys = unique_key if isinstance(unique_key, (list, tuple)) else [unique_key]
        keys = [str(k).strip().strip('"') for k in keys]
        # Empty batch → nothing to insert (an incremental no-op). Probe with LIMIT 1 rather than
        # counting, exactly as the delete+insert path does.
        if cur.sql(f"SELECT 1 FROM {name} LIMIT 1").fetchone() is None:
            return

        loc_sql = path.replace("'", "''")
        # Reached only when the table exists (see store's dispatch). read_version is None only if a
        # writer created it during this run — capture the current version and pin to that.
        vB = read_version if read_version is not None else engine.table_version(path, storage_options)

        target_cols = list(cur.sql(f"SELECT * FROM delta_scan('{loc_sql}', version => {vB}) LIMIT 0").columns)
        batch_cols = list(cur.sql(f"SELECT * FROM {name} LIMIT 0").columns)
        tset = {c.lower() for c in target_cols}
        bset = {c.lower() for c in batch_cols}
        missing = [c for c in target_cols if c.lower() not in bset]
        added = [c for c in batch_cols if c.lower() not in tset]
        # Loud failure on a column mismatch rather than a positional append silently shifting values
        # — mirrors on_schema_change='fail' and the delete+insert guard. `added` is allowed only when
        # on_schema_change resolved to an evolving mode (merge_schema). dbt-shaped error, so it stays
        # here rather than in the engine (which raises a plain ValueError as a backstop).
        if missing or (added and not merge_schema):
            raise CompilationError(
                "insert: the model's columns do not match the target table. "
                + (f"Added: {sorted(added)}. " if added else "")
                + (f"Missing: {sorted(missing)}. " if missing else "")
                + "Reconcile the model SQL with the table (or use on_schema_change / --full-refresh)."
            )

        cond = self._rewrite_merge_aliases(insert_condition)
        if cond and sqlscan.has_qualifier(cond, "target"):
            raise CompilationError(
                "merge_insert_condition references the target on an insert-only strategy, but an "
                "unmatched row has no target to read. Reference only the source."
            )

        engine.merge_delta(
            path, data, unique_key,
            insert_only=True,
            predicates=self._merge_predicates(
                {"incremental_predicates": incremental_predicates}, target_cols),
            insert_condition=cond,
            merge_schema=merge_schema,
            existing_columns=target_cols,
            read_version=vB,
            partition_by=partition_by,
            sort_by=sort_by,
            storage_options=storage_options,
            cur=cur,
            max_spill_size=max_spill_size,
            max_temp_directory_size=max_temp_directory_size,
            streamed_exec=streamed_exec,
            row_group_rows=row_group_rows,
            target_file_size=target_file_size,
            timestamp_ntz=timestamp_ntz,
            existing_dt=existing_dt,
        )

    @staticmethod
    def _probe_filters(cur, name, partition_by, join_keys) -> list:
        """The constant probe filters for the insert-only anti-join — ``engine.probe_filters``.

        Kept as a thin delegate so the plugin and the raw-SQL router derive IDENTICAL filters from one
        implementation; see ``engine.probe_filters`` for the result-neutrality argument."""
        return engine.probe_filters(cur, name, partition_by, join_keys)

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
                    # Wording pinned by the vendored conformance test (dbt-duckdb 1.11.0 asserts
                    # the "'when_matched' or 'when_not_matched'" substring verbatim); the duckrun
                    # superset key is mentioned after it.
                    errors.append(
                        "merge_clauses must contain at least one of "
                        "'when_matched' or 'when_not_matched' keys "
                        "('when_not_matched_by_source' is also accepted)"
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
        """The full MERGE ``ON`` predicate ``target.k = source.k [AND …]`` — literally the same
        builder ``merge_delta`` uses (``engine.merge_on_predicate``), so the clause-core path
        (merge_clauses / merge_update_set_expressions) and the flat-kwarg path cannot drift on
        quoting or shape."""
        return engine.merge_on_predicate(unique_key, cls._merge_predicates(cfg, columns))

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
    def _clause_condition(cls, cond):
        """One clause's ``condition``, normalized to a single delta_rs predicate.

        dbt-duckdb accepts a string OR a list of strings, AND-ing the list (``merge.sql``: ``AND
        ({{ condition | join(') AND (') }})``). duckrun took the string only, so a list reached
        ``_rewrite_merge_aliases`` and stringified into garbage SQL — accept both spellings."""
        if isinstance(cond, (list, tuple)):
            parts = [str(p) for p in cond if p]
            if not parts:
                return None
            cond = parts[0] if len(parts) == 1 else " AND ".join(f"({p})" for p in parts)
        return cls._rewrite_merge_aliases(cond)

    @staticmethod
    def _clause_mode(c: dict) -> str:
        """A clause's ``mode``, defaulting to dbt-duckdb's ``by_name``.

        Upstream's ``by_name`` / ``by_position`` / ``star`` all mean *every column*, which delta_rs
        expresses as ``update_all`` / ``insert_all``; only ``explicit`` names columns. (by-name vs
        by-position is a surface spelling, not a behavior duckrun can differ on — the source is
        projected onto the target's columns by name either way.)"""
        return (c.get("mode") or "by_name").lower()

    @classmethod
    def _explicit_updates(cls, spec, allcols, keys) -> dict:
        """The ``{col: expr}`` map for an explicit-mode matched UPDATE: the columns named by
        ``include``/``exclude`` copied from source, with ``set_expressions`` overriding — the same
        shape ``merge_update_set_expressions`` produces (dbt-duckdb ``merge.sql`` explicit mode)."""
        updates = {col: f"source.{col}" for col in cls._explicit_cols(spec, allcols, keys)}
        for col, expr in ((spec if isinstance(spec, dict) else {}).get("set_expressions") or {}).items():
            updates[str(col)] = cls._rewrite_merge_aliases(expr)
        return updates

    @classmethod
    def _insert_updates(cls, spec, allcols, keys) -> dict:
        """The ``{col: expr}`` map for an explicit-mode not-matched INSERT.

        dbt-duckdb spells this ``insert: {'columns': [...], 'values': [...]}`` (rendered as
        ``INSERT (cols) VALUES (vals)``), so the pairs come straight from those two lists. Without
        ``columns`` it falls back to duckrun's ``include``/``exclude`` spelling, copying each named
        column from source."""
        spec = spec if isinstance(spec, dict) else {}
        cols = spec.get("columns")
        if cols:
            vals = spec.get("values") or []
            if len(vals) != len(cols):
                raise CompilationError(
                    f"merge_clauses insert lists {len(cols)} column(s) but {len(vals)} value(s); "
                    "they must pair up one-to-one."
                )
            return {str(c).strip(): cls._rewrite_merge_aliases(v) for c, v in zip(cols, vals)}
        return {col: f"source.{col}" for col in cls._explicit_cols(spec, allcols, keys)}

    @classmethod
    def _by_source_spec(cls, c: dict, cond, allcols, keys) -> dict:
        """One ``WHEN NOT MATCHED BY SOURCE`` clause spec — reached from duckrun's
        ``when_not_matched_by_source`` group and from dbt-duckdb's portable ``{'by': 'source'}``
        entry inside ``when_not_matched``."""
        action = (c.get("action") or "").lower()
        if action == "do_nothing":
            return {"clause": "not_matched_by_source", "action": "do_nothing", "predicate": cond}
        if action == "delete":
            return {"clause": "not_matched_by_source", "action": "delete", "predicate": cond}
        if action == "update":
            # by-source rows have no source row, so columns can't be copied from source — only an
            # explicit expression map makes sense. Accept `set` (duckrun) or upstream's
            # `set_expressions`, else require one.
            set_map = (c.get("set") or c.get("set_expressions")
                       or (c.get("update") or {}).get("set")
                       or (c.get("update") or {}).get("set_expressions"))
            if not set_map:
                raise CompilationError(
                    "merge_clauses.when_not_matched_by_source update requires a 'set' map "
                    "(by-source rows have no source columns to copy)")
            return {"clause": "not_matched_by_source", "action": "update",
                    "updates": {k: cls._rewrite_merge_aliases(v) for k, v in set_map.items()},
                    "predicate": cond}
        raise cls._unsupported_action("when_not_matched_by_source", action,
                                      "'update', 'delete' or 'do_nothing'")

    @staticmethod
    def _unsupported_action(group: str, action: str, expected: str) -> CompilationError:
        """The rejection for an action duckrun cannot run — never silently ignored.

        ``error`` is a real dbt-duckdb action (it makes DuckDB raise when such a row is found); delta_rs
        has no ERROR clause, so it is refused at compile time rather than dropped."""
        extra = ""
        if action == "error":
            extra = (" dbt-duckdb's 'error' action has no delta_rs equivalent (delta_rs cannot raise "
                     "from a merge clause), so duckrun refuses it instead of ignoring it.")
        return CompilationError(
            f"unsupported merge_clauses.{group} action: {action!r} (expected {expected}).{extra}")

    # dbt-duckdb's IMPLICIT clause defaults (``merge.sql`` + ``merge_defaults.sql``): a merge_clauses
    # dict that omits a key still gets that key's default clause — ``when_matched`` -> UPDATE BY NAME,
    # ``when_not_matched`` -> INSERT BY NAME. duckrun mirrors them so a config means the same thing on
    # both adapters; without the not-matched default, `{'when_matched': [{'action': 'do_nothing'}]}`
    # (dbt-duckdb's insert-only spelling, issue #20) would fold to ZERO clauses instead of an insert.
    # Upstream's defaults carry merge_update_condition / merge_insert_condition, which
    # _validate_merge_config already REJECTS alongside merge_clauses, so the predicate is always None.
    _CLAUSE_DEFAULTS = {
        "when_matched": ({"action": "update", "mode": "by_name"},),
        "when_not_matched": ({"action": "insert", "mode": "by_name"},),
    }

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
        """Translate a dbt-duckdb ``merge_clauses`` dict into delta_rs clause specs (applied in order).

        Mirrors dbt-duckdb's ``merge.sql`` spelling for spelling, so one config expresses the same
        merge on both adapters (a project targeting both should not need a per-target branch):
        ``when_matched`` → update / delete / do_nothing, ``when_not_matched`` → insert / do_nothing
        (plus ``by: source``, upstream's portable form of the by-source clause),
        ``when_not_matched_by_source`` → update / delete / do_nothing (rows the source doesn't carry —
        full-sync semantics). ``mode`` by_name / by_position / star → all columns; ``explicit`` → the
        columns named by ``update``/``insert``.

        ``do_nothing`` has no delta_rs action; it becomes a marker ``engine.resolve_do_nothing`` folds
        away at the merge seam (first-match-wins guards on later same-kind clauses), exactly like a raw
        SQL ``THEN DO NOTHING``. Combined with the implicit defaults below, dbt-duckdb's insert-only
        spelling ``{'when_matched': [{'action': 'do_nothing'}]}`` resolves to a single unconditional
        ``WHEN NOT MATCHED THEN INSERT *`` — the shape ``engine.merge_delta_clauses`` routes to the
        cheap DuckDB anti-join + plain append (issue #20)."""
        keys = cls._key_set(unique_key)
        allcols = [str(c) for c in columns]
        # `when_not_matched_by_source` is duckrun's own extension — dbt-duckdb's merge_clauses has no
        # such key, so there is no upstream default to match and a full-sync clause list stays fully
        # EXPLICIT (silently adding an implicit upsert to a CDC config would be a nasty surprise). The
        # portable way to get the defaults AND a by-source clause is upstream's `{'by': 'source'}` entry
        # inside when_not_matched.
        implicit = "when_not_matched_by_source" not in merge_clauses

        def group(name):
            if name in merge_clauses:
                return merge_clauses.get(name) or []
            return cls._CLAUSE_DEFAULTS.get(name, ()) if implicit else ()

        specs = []
        for c in group("when_matched"):
            action = (c.get("action") or "").lower()
            cond = cls._clause_condition(c.get("condition"))
            if action == "do_nothing":
                specs.append({"clause": "matched", "action": "do_nothing", "predicate": cond})
            elif action == "update":
                if cls._clause_mode(c) != "explicit":
                    specs.append({"clause": "matched", "action": "update_all", "predicate": cond})
                else:
                    specs.append({"clause": "matched", "action": "update",
                                  "updates": cls._explicit_updates(c.get("update"), allcols, keys),
                                  "predicate": cond})
            elif action == "delete":
                specs.append({"clause": "matched", "action": "delete", "predicate": cond})
            else:
                raise cls._unsupported_action("when_matched", action,
                                              "'update', 'delete' or 'do_nothing'")
        for c in group("when_not_matched"):
            action = (c.get("action") or "").lower()
            cond = cls._clause_condition(c.get("condition"))
            # dbt-duckdb writes the by-source clause as an entry here (`WHEN NOT MATCHED BY SOURCE`);
            # `by: target` (or no `by`) is the plain not-matched clause.
            if str(c.get("by") or "").lower() == "source":
                specs.append(cls._by_source_spec(c, cond, allcols, keys))
            elif action == "do_nothing":
                specs.append({"clause": "not_matched", "action": "do_nothing", "predicate": cond})
            elif action == "insert":
                if cls._clause_mode(c) != "explicit":
                    specs.append({"clause": "not_matched", "action": "insert_all", "predicate": cond})
                else:
                    specs.append({"clause": "not_matched", "action": "insert",
                                  "updates": cls._insert_updates(
                                      c.get("insert") or c.get("update"), allcols, keys),
                                  "predicate": cond})
            else:
                raise cls._unsupported_action("when_not_matched", action,
                                              "'insert' or 'do_nothing'")
        for c in merge_clauses.get("when_not_matched_by_source", []) or []:
            specs.append(cls._by_source_spec(
                c, cls._clause_condition(c.get("condition")), allcols, keys))
        return specs

    @staticmethod
    def _resolve_schema_change(on_schema_change, path, data, storage_options):
        """Handle dbt ``on_schema_change`` for the merge path.

        Returns ``(evolve, existing_columns)``: whether delta_rs should evolve the table schema
        (``merge_schema``), plus the target's column list read while deciding — threaded into the
        merge (``existing_columns=``) so the evolve step doesn't re-open the same immutable
        snapshot's log for the identical answer. ``existing_columns`` is None on the ignore path
        (nothing was read).
        - ignore (default): no evolution.
        - append_new_columns / sync_all_columns: evolve so new columns are added.
        - fail: raise if the incoming columns differ from the table's.
        """
        if on_schema_change in ("ignore", "", None):
            return False, None
        columns = engine.delta_columns(path, storage_options)
        existing = [c.lower() for c in columns]
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
        return bool(added) or on_schema_change == "sync_all_columns", columns

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

        # A source path may be spelled with the OneLake `<workspace>/<item>` shorthand too (typically
        # `{{ env_var('WAREHOUSE_PATH') }}/<schema>/<table>` where the env holds the short form) —
        # expand it to the abfss:// URL the scan functions understand. Same expander as root_path.
        from .remote import expand_onelake_shorthand
        path = expand_onelake_shorthand(str(path))

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
