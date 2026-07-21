"""
The duckrun adapter: a thin subclass of dbt-duckdb.

State lives entirely in Delta Lake. Writes always go through delta_rs (see engine.py /
delta_plugin.py); DuckDB is only ever used to *read* (via ``delta_scan``) and to run model
logic. Each existing Delta table is surfaced to dbt as a plain ``delta_scan`` view named to
match ``database.schema.identifier`` so ``{{ this }}``, ``ref()`` and ``is_incremental()``
resolve, with no attach or re-attach when a table is created.
"""
from dbt.adapters.base.meta import available
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.impl import DuckDBAdapter

from dbt.adapters.duckrun import delta_dml
from dbt.adapters.duckrun import remote
from dbt.adapters.duckrun import secret
from dbt.adapters.duckrun.credentials import DuckrunCredentials
from dbt.adapters.duckrun.environment import DuckrunEnvironment

logger = AdapterLogger("Duckrun")

try:  # dbt 1.8+
    from dbt.adapters.contracts.relation import RelationType
except ImportError:  # pragma: no cover - older layouts
    from dbt.contracts.relation import RelationType


class DuckrunConnectionManager(DuckDBConnectionManager):
    TYPE = "duckrun"

    @classmethod
    def open(cls, connection):
        # Fail loud if the kernel still has Fabric's stale duckdb/deltalake loaded (installed an
        # upgrade but skipped notebookutils.session.restartPython()). Lazy import: same wheel.
        from duckrun._runtime import check_runtime_versions
        check_runtime_versions()
        # duckrun runs single-threaded, so it uses ONE DuckDB connection for the whole run
        # (DuckrunEnvironment) instead of dbt-duckdb's per-handle cursors — see environment.py.
        # Pre-seed the base class's singleton _ENV with it for the local case; remote/MotherDuck
        # fall through to dbt-duckdb's stock environment selection in the base open().
        base = DuckDBConnectionManager
        if connection.state != ConnectionState.OPEN:
            creds = cls.get_credentials(connection.credentials)
            is_local = not getattr(creds, "remote", None) and not getattr(
                creds, "is_motherduck", False
            )
            if is_local:
                with base._LOCK:
                    if not base._ENV or base._ENV.creds != creds:
                        base._ENV = DuckrunEnvironment(creds)
        # dbt-duckdb stores its singleton Environment on whichever class `open` is invoked on.
        # adapter.store_relation() looks it up via the *base* class (DuckDBConnectionManager.env()),
        # so delegate to the base to keep _ENV there, then mirror it onto this subclass.
        handle = base.open(connection)
        DuckrunConnectionManager._ENV = base._ENV
        return handle


class DuckrunAdapter(DuckDBAdapter):
    ConnectionManager = DuckrunConnectionManager
    Credentials = DuckrunCredentials

    def __init__(self, config, mp_context):
        super().__init__(config, mp_context)
        # duckrun's Delta write path is single-threaded: parallel models would collide on the
        # shared DuckDB connection (Arrow stream held open across the delta_rs write). dbt sizes
        # its run thread pool from config.threads (dbt.task.runnable), and the adapter shares
        # that RuntimeConfig object — so pin it to 1 here, overriding whatever the profile says.
        # `threads` is deliberately undocumented for duckrun; users never need to set it.
        try:
            config.threads = 1
        except Exception:  # pragma: no cover - frozen config fallback
            try:
                object.__setattr__(config, "threads", 1)
            except Exception:  # pragma: no cover
                pass
        # Verify the pin actually took. Leaving threads > 1 is not a degraded run — it silently
        # corrupts data (parallel models collide on the shared connection's open Arrow stream), so
        # fail loud rather than let it through. A failed run is strictly better than a corrupt table.
        if getattr(config, "threads", None) != 1:
            raise RuntimeError(
                "duckrun could not pin the run to a single thread (config.threads is "
                f"{getattr(config, 'threads', None)!r}). The Delta write path is not thread-safe: "
                "parallel models would collide on the shared DuckDB connection and can corrupt "
                "tables. Set `threads: 1` in your profile and re-run."
            )

    @available
    def delta_table_exists(self, location) -> bool:
        """True if a Delta table already exists at ``location``."""
        from . import engine
        so = self.config.credentials.storage_options_for_location(location)
        return engine.table_exists(location, so)

    @available
    def delta_version(self, location):
        """Current Delta version at ``location``, or None if the table does not exist. Captured
        at the *start* of a model build (before it reads ``{{ this }}``) so a ``append_if_unchanged`` can
        pin to it: if any writer commits during the run, the append fails instead of landing a
        duplicate."""
        from . import engine
        from deltalake.exceptions import TableNotFoundError
        so = self.config.credentials.storage_options_for_location(location)
        try:
            return engine._delta_table(location, so).version()
        except TableNotFoundError:
            # Genuinely-missing table -> None (first run overwrites; append_if_unchanged has no pin yet).
            # A real fault (transient storage error, bad token) must RE-RAISE: swallowing it here
            # would silently degrade append_if_unchanged's start-of-build pin to HEAD-at-write, reopening
            # the read->write race the pin exists to close.
            return None

    @available
    def delta_state(self, location) -> dict:
        """``{'exists': bool, 'version': int|None}`` from a SINGLE DeltaTable open, so a model's
        materialization asks the log once instead of twice (delta_table_exists + delta_version were
        two separate opens = two remote log replays per model). Same semantics as calling both:
        a genuinely-missing table -> ``{exists: False, version: None}``; a real fault (transient
        storage error, bad token) RE-RAISES rather than masquerading as absent (see delta_version)."""
        from . import engine
        from deltalake.exceptions import TableNotFoundError
        so = self.config.credentials.storage_options_for_location(location)
        try:
            return {"exists": True, "version": engine._delta_table(location, so).version()}
        except TableNotFoundError:
            return {"exists": False, "version": None}

    @available
    def persist_delta_docs(self, location, relation_docs, column_docs) -> None:
        """Persist a model's relation/column descriptions into the Delta table's metadata so they
        survive into a later ``dbt docs generate`` (a fresh process that rebuilds the views from
        disk). Called from the materialization after the write. Best-effort."""
        from . import engine
        so = self.config.credentials.storage_options_for_location(location)
        try:
            engine.persist_docs_to_delta(location, relation_docs, dict(column_docs or {}), so)
        except Exception as exc:  # best-effort: docs persistence must not fail the build
            logger.debug(f"duckrun: could not persist Delta docs at {location!r}: {exc}")

    def _apply_delta_comments(self, cursor, relation, location, dt=None):
        """Re-apply persisted Delta docs (relation + column descriptions) as COMMENT ON statements
        on a freshly (re-)registered delta_scan view, so catalog queries (``dbt docs generate``)
        return non-null comments even in a process that never ran the model. Best-effort.
        ``dt`` reuses discovery's already-opened handle instead of a second log open."""
        from . import engine
        if dt is not None:
            relation_docs, column_docs = engine.docs_from_dt(dt)
        else:
            so = self.config.credentials.storage_options_for_location(location)
            relation_docs, column_docs = engine.read_delta_docs(location, so)
        if not relation_docs and not column_docs:
            return
        # The physical object is a view, so COMMENT ON VIEW (DuckDB reports it under the view's
        # catalog entry, which get_catalog reads).
        for stmt in engine.comment_on_sql(relation.render(), "view", relation_docs, column_docs):
            try:
                cursor.execute(stmt)
            except Exception as exc:  # best-effort: a single failed comment must not abort registration
                logger.debug(f"duckrun: could not apply comment for {location!r}: {exc}")

    # ------------------------------------------------------------------ discovery
    def _cursor(self):
        return self.connections.get_thread_connection().handle.cursor()

    def _mint_secret_guarded(self, cursor, storage_options):
        """``secret.ensure_azure_secret``, skipped when this run's connection already holds this
        exact token's secret. Discovery mints per schema and secrets are database-global, so
        every repeat with an unchanged token was pure waste (INSTALL/LOAD + CREATE SECRET per
        schema per command). Guarded on the live thread handle via a weakref — a rebuilt
        connection or a rotated token still re-mints; a non-weakreffable handle just mints
        every time (the old behavior)."""
        import weakref
        token = secret.bearer_token(storage_options)
        try:
            handle = self.connections.get_thread_connection().handle
        except Exception:  # no thread connection (bare-adapter harness) → just mint, unguarded
            handle = None
        prev = getattr(self, "_minted_secret", None)
        if token and handle is not None and prev and prev[0]() is handle and prev[1] == token:
            return
        if secret.ensure_azure_secret(cursor, storage_options) and token and handle is not None:
            try:
                self._minted_secret = (weakref.ref(handle), token)
            except TypeError:  # pragma: no cover - handle type without weakref support
                pass

    def _ensure_remote_secret(self, cursor):
        """Mint the DuckDB Azure secret from a ``bearer_token`` in ``storage_options`` on
        this connection, before discovery globs the store.

        For an abfss:// OneLake target the bearer token is the only credential and dbt-duckdb
        never creates a secret for it (unlike a profile ``secrets:`` block, which it mints at
        connection-open). Read-only commands materialize nothing, so without this the very
        first thing that touches the store is the discovery glob below — which throws on
        ``abfss://`` with no secret, making every table look absent ("schema does not exist").
        A no-op when no token is configured (local/az://).
        """
        so = getattr(self.config.credentials, "storage_options", None)
        try:
            self._mint_secret_guarded(cursor, so)
        except Exception as exc:  # pragma: no cover - logged, then discovery proceeds
            logger.debug(f"duckrun: could not create Azure secret before Delta discovery: {exc}")

    def _discover_delta_relations(self, schema_relation):
        """Discover Delta tables physically present under ``root_path/<schema>`` and return
        them as relations so they land in dbt's relation cache.

        This is what makes the adapter stateless across processes: dbt rebuilds its relation
        cache at run start by calling list_relations_without_caching for every manifest
        schema, even on a fresh in-memory DuckDB. Returning a table-typed relation per Delta
        table makes ``is_incremental()`` true on later runs.

        This only returns the relations (for the cache); list_relations_without_caching then
        calls _register_delta_view on each so the physical ``delta_scan`` view exists too —
        which is what lets read-only commands (``dbt test``/``show``/``docs``), that run no
        model and so never hit a materialization, still query the table. For ``run``/``build``
        the materialization re-creates the view anyway (pre-register {{ this }} + step-4 view).
        """
        # Resolve the root/token of the catalog this (database, schema) belongs to: the default
        # catalog for the target database, an attached catalog for a `+database: <alias>` model.
        # Single-catalog projects resolve to the top-level root_path/storage_options, unchanged.
        creds = self.config.credentials
        root_path, storage_options = creds.root_for(schema_relation.database)
        if not root_path:
            return []

        schema = schema_relation.schema
        database = schema_relation.database

        # OneLake (abfss://) can't be listed with DuckDB glob (duckdb/duckdb-azure#174), so
        # enumerate table directories with the OneLake DFS REST API; local / az:// stores use
        # DuckDB glob, which works there.
        if remote.is_abfss(root_path):
            # Inside a Fabric notebook the profile carries no token (the notebook has its own); grab
            # one from notebookutils so the REST list AND the delta_scan views work — otherwise a
            # read-only command finds zero tables ("schema does not exist"). No-op when a token is
            # already present or none can be acquired. Persist it onto the default catalog's creds so
            # the write/refresh paths (configure_cursor) reuse it instead of re-fetching per schema.
            fresh = secret.with_onelake_token(root_path, storage_options)
            if secret.bearer_token(fresh) and not secret.bearer_token(storage_options):
                storage_options = fresh
                if str(database).strip('"') not in (getattr(creds, "catalogs", None) or {}):
                    creds.storage_options = storage_options
            # Mint the DuckDB Azure secret from that token so the delta_scan views we're about to
            # register are actually queryable by read-only commands.
            try:
                self._mint_secret_guarded(self._cursor(), storage_options)
            except Exception as exc:  # pragma: no cover - logged, then discovery proceeds
                logger.debug(f"duckrun: could not mint Azure secret before abfss discovery: {exc}")
            names = self._discover_via_rest(root_path, schema, storage_options)
        else:
            names = self._discover_via_glob(root_path, schema)

        return [
            self.Relation.create(
                database=database, schema=schema, identifier=name, type=RelationType.Table
            )
            for name in names
        ]

    def _discover_via_rest(self, root_path, schema, so=None):
        """Table names under ``<root_path>/<schema>`` on a OneLake/ADLS store, via REST. ``so`` is
        the catalog's storage_options (the token for THIS root)."""
        try:
            return remote.list_delta_tables(root_path, str(schema).strip('"'), so)
        except remote.OneLakeAccessError:
            # A genuine access failure (wrong tenant / item not in workspace) must fail loud, NOT
            # masquerade as "no Delta tables" — otherwise a dbt test/docs run goes silently green
            # against a store it can't even see.
            raise
        except Exception as exc:
            # A transient/other listing failure stays best-effort: log (debug) and fall back to the
            # in-memory catalog, so a momentary blip doesn't abort the run (writes fail loud anyway).
            logger.debug(f"duckrun: OneLake table listing failed under {root_path}/{schema}: {exc}")
            return []

    def _discover_via_glob(self, root_path, schema):
        """Table names under ``<root_path>/<schema>`` on a local / az:// store, via DuckDB glob."""
        cursor = self._cursor()
        # az:// needs its Azure secret on the connection before the glob can authenticate.
        self._ensure_remote_secret(cursor)
        return remote.list_delta_tables_via_glob(cursor, root_path, schema)

    def _open_delta_tables(self, targets):
        """Concurrent Delta-log opens for discovery — see :func:`engine.open_delta_tables`
        (shared with the connection API's catalog refresh)."""
        from . import engine
        return engine.open_delta_tables(targets)

    def _register_delta_view(self, relation, dt=None):
        """Create the ``delta_scan`` view for a discovered Delta relation on the live
        connection so read-only commands (``dbt test``, ``dbt show``, ``dbt docs``) that
        never materialize anything can still query the model.

        ``dbt run``/``build`` create this same view in the materialization (step 4); doing it
        here too is harmless — the materialization's ``create or replace`` just supersedes it.
        But for a command that runs no models, this is the only place the physical view gets
        created from the Delta tables discovered on disk. ``dt`` is discovery's already-opened
        handle, reused for the docs read. The caller has already created the schema (once, not
        per relation).
        """
        root_path, _ = self.config.credentials.root_for(relation.database)
        if not root_path:
            return
        location = (
            root_path.rstrip("/")
            + "/" + str(relation.schema).strip('"')
            + "/" + str(relation.identifier).strip('"')
        )
        try:
            cursor = self._cursor()
            loc_sql = location.replace("'", "''")  # escape quotes in the delta_scan path literal
            cursor.execute(
                f"create or replace view {relation.render()} as "
                f"select * from delta_scan('{loc_sql}')"
            )
            # Re-apply persisted docs as COMMENT ON so a docs/test/show process (which rebuilt
            # this view from disk and so lost any in-run comment) still reports them.
            self._apply_delta_comments(cursor, relation, location, dt=dt)
        except Exception as exc:
            # A table mid-write or an unreadable log shouldn't abort cache population;
            # the materialization re-creates the view on the next run regardless. Log at
            # debug so a systematic failure (e.g. a missing secret) is still diagnosable.
            logger.debug(f"duckrun: could not register delta_scan view for {location!r}: {exc}")

    def list_relations_without_caching(self, schema_relation):
        try:
            in_memory = list(super().list_relations_without_caching(schema_relation))
        except Exception:  # best-effort: a missing/empty in-memory schema still allows disk discovery
            in_memory = []

        discovered = self._discover_delta_relations(schema_relation)
        if not discovered:
            return in_memory

        # Hide drop-tombstones: a `drop table` overwrites the table to a one-column marker (no data
        # deleted). Such a table must not surface as a relation. Check before registering. Every
        # discovered relation shares this schema_relation's catalog, so resolve its root/token once.
        # ONE delta-rs open per relation serves BOTH the tombstone check and the persisted-docs read
        # that view registration re-applies — this path runs for every manifest schema on every dbt
        # command, and the opens run concurrently (_open_delta_tables) because serialized log
        # replays over OneLake were a ~30s startup tax on `dbt show` (issue #16).
        root_path, so = self.config.credentials.root_for(schema_relation.database)
        root_path = root_path or ""
        cur = self._cursor()
        locs = [(root_path.rstrip("/") + "/" + str(rel.schema).strip('"')
                 + "/" + str(rel.identifier).strip('"')) for rel in discovered]
        dts = self._open_delta_tables([(loc, so) for loc in locs])
        live = []
        for rel, loc, dt in zip(discovered, locs, dts):
            if dt is not None:
                if delta_dml.is_dropped_dt(dt):
                    continue
            # delta-rs couldn't open it (e.g. an az:// store whose credential lives only in the
            # DuckDB `secrets:` block) — fall back to the original DuckDB-side probe so a
            # tombstone there is still hidden, exactly as before.
            elif delta_dml.is_dropped(cur, loc, so):
                continue
            live.append((rel, dt))
        discovered = [rel for rel, _ in live]
        if not discovered:
            return in_memory

        # Physically register each discovered Delta table as a delta_scan view so read-only
        # commands (dbt test/show/docs) can query models without a prior in-process run. The
        # schema is created once here, not per relation — every relation shares it, and the
        # repeated probe+create pairs were pure log noise (issue #16).
        try:
            self.create_schema(schema_relation)
        except Exception as exc:  # best-effort, same contract as view registration below
            logger.debug(f"duckrun: could not create schema for {schema_relation}: {exc}")
        for rel, dt in live:
            self._register_delta_view(rel, dt=dt)

        # A Delta table on disk is the source of truth, so disk discovery WINS over the
        # in-memory catalog. This matters when several dbt runs share one process (dbt's test
        # harness, a notebook, a long-lived runner): run 1 leaves a `delta_scan` *view* (type
        # view) in the in-memory DuckDB, and if that stale view shadowed the disk table here,
        # the relation would be reported as a view and is_incremental() would be false on run
        # 2 — making the model clobber instead of merge. Reporting the discovered table (type
        # table) instead makes a shared process behave exactly like a fresh one. Non-Delta
        # relations (native `view`/`seed`) aren't discovered, so they pass through untouched.
        discovered_names = {str(r.identifier).strip('"').lower() for r in discovered}
        merged = [
            r for r in in_memory
            if str(r.identifier).strip('"').lower() not in discovered_names
        ]
        merged.extend(discovered)
        return merged

    # --- dbt docs: table stats from the Delta log -------------------------------------------------
    # The stock catalog query (duckrun__get_catalog) emits only column metadata, so dbt-docs shows an
    # empty Stats panel (issue #3). dbt assembles the panel from columns named
    # stats:<key>:{label,value,description,include}; we enrich the catalog agate table with those,
    # sourced from each relation's Delta log (engine.delta_stats — no data scan). Done in Python here
    # rather than in SQL because byte size / last-modified live in the Delta log, not DuckDB metadata.
    _STATS_SPEC = (
        ("num_rows", "Row Count", "Number of rows in the table"),
        ("bytes", "Approximate Size", "Approximate size of the table on disk (bytes)"),
        ("last_modified", "Last Modified", "Time of the most recent Delta commit (UTC)"),
    )

    def get_catalog(self, *args, **kwargs):
        table, exceptions = super().get_catalog(*args, **kwargs)
        return self._with_delta_stats(table), exceptions

    def get_catalog_by_relations(self, *args, **kwargs):
        table, exceptions = super().get_catalog_by_relations(*args, **kwargs)
        return self._with_delta_stats(table), exceptions

    def _with_delta_stats(self, table):
        """Return ``table`` with stats:* columns appended, sourced per-relation from the Delta log.

        A relation with no Delta table at ``root_path/schema/name`` (a native ``view``, a
        drop-tombstone) gets ``include=False`` stats, so dbt leaves it statless. Best-effort: if
        anything goes wrong the original table is returned unchanged — docs must never break.
        """
        from datetime import datetime, timezone
        from dbt_common.clients.agate_helper import table_from_data_flat
        from . import engine

        creds = self.config.credentials
        if len(table.rows) == 0 or not (creds.root_path or creds.catalogs):
            return table
        cur = self._cursor()

        cache = {}
        cols = list(table.column_names)

        # Pre-open every distinct relation's Delta log concurrently (same serial-log-replay tax
        # as discovery, issue #16); the row loop below stays serial — it does DuckDB cursor work.
        # Each row carries its catalog (table_database); resolve that catalog's root/token so
        # stats for a `+database: <alias>` model come from the right Lakehouse.
        targets = {}
        for r in table.rows:
            d = dict(zip(cols, r))
            key = (d.get("table_database"), d.get("table_schema"), d.get("table_name"))
            if key in targets:
                continue
            root_path, so = creds.root_for(key[0])
            if root_path:
                targets[key] = (root_path.rstrip("/") + "/" + str(key[1]).strip('"')
                                + "/" + str(key[2]).strip('"'), so)
        opened = dict(zip(targets, self._open_delta_tables(list(targets.values()))))

        def stats_for(database, schema, name):
            key = (database, schema, name)
            if key not in cache:
                if key not in targets:  # no root for this catalog
                    cache[key] = None
                    return None
                loc, so = targets[key]
                # One open serves the tombstone check and the stats read (was two per relation).
                dt = opened.get(key)
                if dt is not None:
                    cache[key] = (None if delta_dml.is_dropped_dt(dt)
                                  else engine.delta_stats(cur, loc, so, dt=dt))
                else:
                    # delta-rs couldn't open it — original DuckDB-side path, exactly as before.
                    cache[key] = (None if delta_dml.is_dropped(cur, loc, so)
                                  else engine.delta_stats(cur, loc, so))
            return cache[key]

        stat_cols = [f"stats:{k}:{p}" for k, _, _ in self._STATS_SPEC
                     for p in ("label", "value", "description", "include")]
        rows = []
        for r in table.rows:
            d = dict(zip(cols, r))
            st = stats_for(d.get("table_database"), d.get("table_schema"), d.get("table_name"))
            for k, label, desc in self._STATS_SPEC:
                present = st is not None and st.get(k) is not None
                if k == "last_modified" and present:
                    val = datetime.fromtimestamp(st[k] / 1000, tz=timezone.utc).isoformat()
                else:
                    val = st.get(k) if present else None
                d[f"stats:{k}:label"] = label
                d[f"stats:{k}:value"] = val
                d[f"stats:{k}:description"] = desc
                d[f"stats:{k}:include"] = bool(present)
            rows.append(d)
        return table_from_data_flat(rows, cols + stat_cols)
