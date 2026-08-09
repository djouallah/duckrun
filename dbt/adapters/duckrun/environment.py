"""duckrun's DuckDB environment.

dbt-duckdb resolves a plugin source by calling ``Plugin.load()``, registering the returned
``DuckDBPyRelation`` on a cursor, and (for view materialization) caching it in ``_REGISTERED_DF``
to re-register on every later cursor. But a ``DuckDBPyRelation`` is bound to the connection that
created it, and dbt-duckdb hands each ``handle()`` a fresh ``self.conn.cursor()`` (a *separate*
DuckDB connection) — so the re-registration throws "... created by another Connection".

duckrun already surfaces every model's Delta table as a plain SQL ``delta_scan`` *view* in the
catalog, which is connection-independent and therefore visible to every cursor and to a fresh
process. We do the same for plugin sources here: instead of registering a Python relation, create
``CREATE OR REPLACE VIEW <source> AS <scan sql>``. No pyarrow, no copying the source into a table,
and no dependence on dbt-duckdb's per-cursor relation re-registration.
"""
import os
import re
import threading

import duckdb

from dbt.adapters.duckdb.environments.local import (
    DuckDBConnectionWrapper,
    DuckDBCursorWrapper,
    LocalEnvironment,
)
from dbt.adapters.events.logging import AdapterLogger

from . import delta_dml
from . import secret

logger = AdapterLogger("Duckrun")

# Serializes OneLake token refreshes across threads. Module-level rather than per-wrapper because
# the state it protects — the shared credentials object and the instance-global DuckDB secrets —
# is shared by every cursor, and each dbt thread gets its own wrapper.
_CREDS_REFRESH_LOCK = threading.Lock()

# DuckDB's catalog error for a relation that isn't bound. Two shapes (duckdb 1.5):
#   Catalog Error: Table with name my_model does not exist!                       (schema exists)
#   Catalog Error: Table with name "main_x2.other" does not exist because schema "main_x2" does not exist.
# The captured name is bare in the first and schema-qualified in the second. If DuckDB ever
# rewords this, the lazy bind simply never fires and the original error surfaces unchanged —
# degraded back to pre-fix behavior, pinned by tests/adapter/test_run_operation_columns.py.
_MISSING_REL = re.compile(r'(?:Table|View) with name "?([^"!\n]+?)"? does not exist')

# A qualified identifier in SQL text: `schema.table` or `catalog.schema.table`, each part plain or
# double-quoted. Used only after a catalog error, to recover the schema a bare missing-table name
# was addressed under.
_IDENT = r'(?:"[^"]+"|[A-Za-z_][\w$]*)'
_QUAL_REF = re.compile(rf"({_IDENT})\.({_IDENT})(?:\.({_IDENT}))?")


class DuckrunReadOnlyError(RuntimeError):
    """A write was attempted on a read-only duckrun cursor (``duckrun.dbt_project``)."""


# Statements that write OUTSIDE the DuckDB catalog — files, or another attached database — from a
# session holding live store credentials. `classify` calls these passthrough (correctly: they are
# native DuckDB, no delta_rs route), so the read-only cursor must catch them itself: unlike a
# catalog write there is no delta_scan view downstream to refuse them.
_COPY_HEAD = re.compile(r"\s*copy\b", re.I)
_COPY_TO_KW = re.compile(r"\bto\b", re.I)   # leading \b: `manifesto` must not read as ... TO
_EXPORT_HEAD = re.compile(r"\s*export\s+database\b", re.I)


class _DeltaBindCursor(DuckDBCursorWrapper):
    """The cursor machinery both of duckrun's cursor kinds need: the OneLake token refresh every
    statement depends on, and the lazy bind that resolves a duckrun model to its Delta location
    after a catalog error.

    Deliberately knows NOTHING about writing. :class:`DuckrunCursorWrapper` (the run path) adds the
    delta_rs DML interception on top; :class:`DuckrunDebugCursor` (the read-only debug session) does
    not — and that absence IS its read-only guarantee: ``delta_dml.handle`` is nowhere in its MRO,
    so a write has no route to delta_rs at all. It lands on the read-only ``delta_scan`` view
    instead, which DuckDB rejects. Structural, not a flag someone can flip.
    """

    def __init__(self, cursor, credentials):
        super().__init__(cursor)
        self._duckrun_creds = credentials

    def _run_with_lazy_binds(self, sql, run):
        """``run()``, retried after binding whatever Delta relations a catalog error names.

        ``run`` is a callable rather than a fixed ``super().execute(...)`` because the two cursor
        kinds need the SAME bind-and-retry around DIFFERENT operations: the run path executes,
        while the debug path calls ``cursor.sql()`` for a relation. See
        :meth:`DuckrunDebugCursor.sql` for why the distinction matters."""
        try:
            return run()
        except duckdb.CatalogException as exc:
            return self._retry_after_lazy_binds(sql, exc, run)

    # --- lazy bind on a catalog error (issue #24, part B) -----------------------------------------
    # A duckrun model is a `delta_scan` view that only exists once dbt has populated its relation
    # cache, and `dbt run-operation` (or any command under --no-populate-cache) never populates it —
    # so raw SQL against a model that is sitting on disk died with
    #   Catalog Error: Table with name my_model does not exist!
    # The eager fix (bind every manifest schema up front) would re-introduce exactly the discovery
    # startup cost #16 removed, on every operation. So bind lazily instead: only when a statement
    # has ALREADY failed with a catalog error, resolve the missing relation to its Delta location,
    # bind that one view, and retry. Every working path pays nothing — no error, none of this runs —
    # and a genuinely missing table re-raises the original error unchanged.

    def _retry_after_lazy_binds(self, sql, exc, run):
        """Bind the Delta relation(s) a catalog error names, then retry ``run`` — repeatedly, since
        a join of two unbound models errors on one table at a time. Terminates because every pass
        must bind at least one NEW relation (a query names finitely many); re-raises the latest
        error when it can't, so a post-bind failure (e.g. a missing column on the now-visible
        model) surfaces as itself rather than as the stale catalog error."""
        attempted = set()
        for _ in range(8):  # belt over the must-bind-new rule; no sane statement joins more
            fresh = [c for c in self._missing_delta_candidates(exc, sql) if c not in attempted]
            attempted.update(fresh)
            if not any([self._lazy_bind_delta_view(*c) for c in fresh]):  # bind ALL before deciding
                raise exc
            try:
                return run()
            except duckdb.CatalogException as next_exc:
                exc = next_exc
        raise exc

    def _missing_delta_candidates(self, exc, sql):
        """``[(catalog, schema, table), …]`` the error could be talking about, unquoted, in SQL
        order. DuckDB names the missing table bare when its schema exists, so the schema (and
        catalog) are recovered from the statement's own qualified references to that name; a name
        the statement never qualifies is addressed under the profile's default schema. False
        positives (an alias.column that happens to match) are harmless — they just fail the
        on-disk existence check in the bind."""
        m = _MISSING_REL.search(str(exc))
        if not m:
            return []
        parts = [p.strip('"') for p in m.group(1).split(".")]
        if len(parts) == 3:
            return [tuple(parts)]
        if len(parts) == 2:  # "schema.table" — the schema-does-not-exist shape
            return [(None, parts[0], parts[1])]
        missing = parts[0].lower()
        candidates = []
        for ref in _QUAL_REF.finditer(sql):
            cat, schema, table = ref.group(1), ref.group(2), ref.group(3)
            if table is None:
                cat, schema, table = None, cat, schema
            c = (cat and cat.strip('"'), schema.strip('"'), table.strip('"'))
            if c[2].lower() == missing and c not in candidates:
                candidates.append(c)
        if not candidates:
            default_schema = getattr(self._duckrun_creds, "schema", None) or "main"
            candidates = [(None, default_schema, parts[0])]
        return candidates

    def _lazy_bind_delta_view(self, catalog, schema, table) -> bool:
        """Register the ``delta_scan`` view for one ``(catalog, schema, table)``. True only when a
        view was actually created. Same contract as adapter introspection's on-demand bind, via
        the shared :func:`delta_dml.live_delta_target`: a table that isn't on disk and a
        drop-tombstone must not surface. Every failure is swallowed at debug — the caller re-raises
        the original catalog error, which must not be masked by a bind gone wrong."""
        try:
            # root_for falls back to the default catalog for an undeclared name — which is what a
            # dbt-rendered `"memory"."main"."events"` needs — and self-acquires the OneLake token.
            root_path, so = self._duckrun_creds.root_for(catalog)
            if not root_path:
                return False
            location = root_path.rstrip("/") + "/" + schema + "/" + table
            should_bind, _ = delta_dml.live_delta_target(self._cursor, location, so)
            if not should_bind:
                return False
            prefix = f'"{catalog}".' if catalog else ""
            # The view needs its schema; a custom-schema model has none in a fresh in-memory DuckDB.
            self._cursor.execute(f'create schema if not exists {prefix}"{schema}"')
            loc_sql = location.replace("'", "''")
            self._cursor.execute(
                f'create or replace view {prefix}"{schema}"."{table}" as '
                f"select * from delta_scan('{loc_sql}')"
            )
            return True
        except Exception as e:
            logger.debug(f"duckrun: lazy bind for {catalog}.{schema}.{table} failed: {e}")
            return False

    def _refresh_onelake_token(self, creds) -> None:
        # Refresh the default catalog and every attached catalog: a stale aliased token would 401 only
        # on that Lakehouse, mid-build. Each catalog carries its own token, so each is refreshed and
        # its (scoped) secret re-minted independently. No-op unless a token is genuinely near expiry.
        default_db = getattr(creds, "database", None)
        self._refresh_one(creds.storage_options, default_db, is_default=True,
                          setter=lambda so: setattr(creds, "storage_options", so))
        for alias, cfg in (getattr(creds, "catalogs", None) or {}).items():
            cfg = cfg or {}
            self._refresh_one(cfg.get("storage_options"), alias, is_default=False,
                              setter=lambda so, c=cfg: c.__setitem__("storage_options", so))

    def _refresh_one(self, so, catalog, is_default, setter) -> None:
        root = None if is_default else (self._duckrun_creds.catalogs.get(catalog) or {}).get("root_path")
        if not secret.bearer_token(so):
            return  # no token to refresh (local / az:// / notebook) — never touches the lock
        # The credentials object and its DuckDB secrets are shared by every thread, and this runs on
        # EVERY statement, so a refresh must not interleave: two threads could otherwise both
        # re-acquire and both issue CREATE OR REPLACE SECRET against the one DuckDB instance.
        # Contention is negligible — on the common path the lock is held just long enough to read a
        # cached JWT expiry, and the real re-acquisition happens at most once per token lifetime.
        with _CREDS_REFRESH_LOCK:
            fresh = secret.refresh_catalog_secret(
                self._cursor, catalog, so, is_default=is_default, root=root
            )
            if fresh is not so:
                setter(fresh)  # keep the live copy DML/discovery read from in sync


class DuckrunCursorWrapper(_DeltaBindCursor):
    """Cursor wrapper that routes raw DML against duckrun-managed (Delta-backed) relations to
    delta_rs instead of running it on the read-only ``delta_scan`` view.

    Every SQL statement — whether issued by dbt's connection manager or by the adapter-test
    harness (which goes straight to ``conn.handle.cursor().execute``) — funnels through here, so
    this is the single production interception point. Non-matching statements, parameterized
    statements (the seed loader's ``insert ... values (?)``), and DML against native relations all
    fall through to DuckDB unchanged. See delta_dml.handle.
    """

    def execute(self, sql, bindings=None):
        creds = self._duckrun_creds
        # OneLake token freshness — the universal guard. configure_cursor re-mints per model, but a
        # long build's later phases (dbt's tests / on-run-end reads) run on a reused cursor that it
        # never revisits, so the once-minted DuckDB secret + storage_options go stale and every
        # delta_scan 401s past the token's ~1h life. EVERY statement funnels through here, so this is
        # the one place that covers them all. Cheap: refreshed() only parses the JWT expiry and returns
        # the same object unless the token is genuinely near expiry — it hits the network at most once
        # per token lifetime, not per statement.
        self._refresh_onelake_token(creds)
        if bindings is None:
            # Route raw DML to the catalog its target names (a 3-part `catalog.schema.table`), else
            # the default catalog. `root_for` falls back to the default when there are no catalogs,
            # so single-catalog behavior is unchanged.
            target_cat = delta_dml.dml_target_catalog(sql)
            root_path, storage_options = creds.root_for(target_cat)
            # Self-acquire a OneLake token for an abfss:// target whose profile omits bearer_token, so
            # raw-DML / snapshot delta-rs writes authenticate (mirrors the plugin + read paths).
            storage_options = secret.with_onelake_token(root_path, storage_options)
            if delta_dml.handle(self._cursor, root_path, storage_options, sql):
                return self._cursor  # applied to Delta; nothing to run on DuckDB
        return self._run_with_lazy_binds(
            sql, lambda: super(DuckrunCursorWrapper, self).execute(sql, bindings))


class DuckrunDebugCursor(_DeltaBindCursor):
    """Read-only cursor for the dbt debug session (``duckrun.dbt_project``), issue #29.

    Sibling of :class:`DuckrunCursorWrapper`, not a subclass, and that is the whole point: it
    inherits the token refresh and the lazy bind, and it inherits NO route to delta_rs. A write
    can't be re-enabled by a flag here because there is nothing to re-enable — it falls through to
    a ``delta_scan`` view, which DuckDB refuses to write to. A debug session built from the real
    profile sits one typo away from a production write, so that has to hold by construction.
    """

    def sql(self, query):
        """A ``DuckDBPyRelation`` for ``query``, lazy-binding any duckrun model it names.

        Defined explicitly in order to SHADOW ``DuckDBCursorWrapper.__getattr__``, which forwards
        every attribute except ``execute`` straight through to the raw DuckDB cursor — so a plain
        ``cursor.sql(...)`` goes around the lazy bind entirely and dies on the first model dbt's
        relation cache never bound, which in a fresh debug session is every model.

        Routing through ``execute`` instead would bind the views but materialize the entire result
        to do it — and the lazy filter pushdown that makes returning a relation worthwhile would be
        paid for twice. Retrying ``sql()`` costs nothing by comparison: DuckDB only binds here, it
        reads no data until the relation is consumed."""
        self._reject_write(query)
        self._refresh_onelake_token(self._duckrun_creds)
        return self._run_with_lazy_binds(query, lambda: self._cursor.sql(query))

    def execute(self, sql, bindings=None):
        """The DBAPI path, under the same read-only contract — ``get_columns_in_relation`` and any
        other adapter introspection a debug session wants to reuse go through ``execute``."""
        self._reject_write(sql)
        self._refresh_onelake_token(self._duckrun_creds)
        return self._run_with_lazy_binds(
            sql, lambda: super(DuckrunDebugCursor, self).execute(sql, bindings))

    def _reject_write(self, sql) -> None:
        """Raise for a statement whose FORM writes.

        For a Delta write this is the error MESSAGE, not the safety — the safety is that this class
        has no delta_rs route at all. Without it a write still fails, but as DuckDB's bare "cannot
        ... a view", which never explains that the view is a view because duckrun made it one.
        Classified by the very ``delta_dml.classify`` the write path routes on, so "what counts as
        a write" can't drift between the two. ``CREATE TEMP TABLE`` / ``CREATE VIEW`` classify as
        passthrough and stay allowed: scratch objects are how you actually debug.

        For ``COPY ... TO`` / ``COPY FROM DATABASE ... TO`` / ``EXPORT DATABASE`` the check IS the
        safety: they classify passthrough (native DuckDB, no delta_rs), but they write files —
        anywhere the session's live store credentials reach, including the lakehouse — and there is
        no read-only view downstream to refuse them. ``COPY <table> FROM ...`` (a load into a
        scratch table) keeps working; a top-level ``TO`` is what marks the writing direction.

        Single-statement by construction (classify takes one statement). A multi-statement script
        whose first statement reads slips past the message — and then fails anyway, on the view, in
        DuckDB. Degraded message, never degraded safety."""
        first = " ".join(str(sql).split())[:120]
        if delta_dml.classify(sql) != "passthrough":
            raise DuckrunReadOnlyError(
                f"read-only debug session: this statement writes.\n    {first}\n"
                "duckrun models are read-only delta_scan views here, so nothing in this session "
                "can reach delta_rs. To write, use `dbt run` or "
                "duckrun.connect(..., read_only=False)."
            )
        mask = delta_dml._blank_string_literals(
            delta_dml._strip_comments(str(sql), keep_length=True))
        if _EXPORT_HEAD.match(mask) or (
                _COPY_HEAD.match(mask)
                and delta_dml._find_top_level(mask, _COPY_TO_KW) >= 0):
            raise DuckrunReadOnlyError(
                f"read-only debug session: this statement writes files.\n    {first}\n"
                "COPY ... TO / EXPORT DATABASE write with this session's store credentials, so a "
                "read-only session refuses them. To export, materialize through "
                "duckrun.connect(..., read_only=False), or fetch the relation and write it "
                "client-side."
            )


class DuckrunEnvironment(LocalEnvironment):
    def handle(self):
        # Swap dbt-duckdb's cursor wrapper for ours so raw DML on Delta relations is intercepted
        # on every cursor (connection-manager AND test-harness paths) — see DuckrunCursorWrapper.
        return self._handle_with(DuckrunCursorWrapper)

    def debug_handle(self):
        """A handle for the read-only dbt debug session (``duckrun.dbt_project``, issue #29).

        The SAME connection a real run uses — same secrets, same catalog ATTACHes, same lazy bind —
        differing in exactly one thing: its cursor is a :class:`DuckrunDebugCursor`, which has no
        route to delta_rs. That is what "the run path minus the write" means concretely. The debug
        session never reconstructs a connection of its own, so it cannot drift from the adapter."""
        return self._handle_with(DuckrunDebugCursor)

    def _handle_with(self, cursor_cls):
        h = super().handle()  # initializes self.conn (+ mints the default catalog's secret)
        self._ensure_default_secret()
        self._attach_catalogs()
        if isinstance(h, DuckDBConnectionWrapper):
            h._cursor = cursor_cls(h._cursor._cursor, self.creds)
        return h

    def _ensure_default_secret(self) -> None:
        """Mint the default catalog's DuckDB Azure secret from a SELF-ACQUIRED OneLake token.

        dbt-duckdb's connection open (via the delta plugin's configure_connection) mints the secret
        from the profile's ``storage_options`` VERBATIM — which is empty under pure-OIDC self-acquire,
        so no read secret is minted and every in-model OneLake read (``delta_scan`` of ``{{ this }}``,
        ``read_parquet`` of ``Files/``, a python model on the raw connection) authenticates
        anonymously → ``Unauthorized`` (delta_scan falls back to Azure IMDS). ``root_for()`` resolves
        the default root through ``_with_token``, which self-acquires for an abfss:// root the same way
        the write path does — so reads and writes can't drift. No-op for local/az:// roots or when a
        token is already present; a genuine abfss root with no acquirable token raises (fail loud —
        better than a later opaque ``Unauthorized``).

        Guarded on (connection, token) identity like ``_attach_catalogs``: ``handle()`` runs per
        statement and secrets are database-global, so re-minting an unchanged token is pure waste;
        a rebuilt connection or a rotated token still re-mints. The guard is a check-then-act over
        state shared by every thread, and it mints on the SHARED connection, so it runs under the
        environment's lock — at ``threads > 1`` dbt opens each worker's handle concurrently and they
        would otherwise all mint at once."""
        with self.lock:
            if self.conn is None:
                return
            _, so = self.creds.root_for()   # self-acquires for an abfss:// default root
            token = secret.bearer_token(so)
            if token and getattr(self, "_secret_conn", None) is self.conn \
                    and getattr(self, "_secret_token", None) == token:
                return
            if secret.ensure_azure_secret(self.conn, so):
                self._secret_conn, self._secret_token = self.conn, token

    def _attach_catalogs(self) -> None:
        """ATTACH each declared (non-default) catalog as an in-memory DuckDB catalog and mint its
        path-scoped Azure secret, so `alias.schema.table` relations resolve and each Lakehouse's
        reads authenticate with its own token. Runs once, on the shared connection, before the first
        model/discovery — cross-catalog `ref()` needs the alias to exist up front. No-op when no
        catalogs are declared (single-catalog behavior is unchanged).

        Under the environment's lock: at ``threads > 1`` every worker opens its handle concurrently,
        and the once-only guard plus the ATTACH/CREATE SECRET DDL all target the SHARED connection."""
        catalogs = getattr(self.creds, "catalogs", None)
        if not catalogs:
            return
        with self.lock:
            # Guard on the connection identity, not a plain flag: if the env's in-memory connection
            # is ever rebuilt, the ATTACHes are gone with it, so we must re-attach on the new one.
            if self.conn is None or getattr(self, "_attached_conn", None) is self.conn:
                return
            default_db = getattr(self.creds, "database", None)
            for alias, cfg in catalogs.items():
                if alias == default_db:
                    continue  # the default catalog is the base connection itself
                cfg = cfg or {}
                try:
                    # A quoted identifier so an alias with odd characters can't break the ATTACH.
                    self.conn.execute(f'ATTACH IF NOT EXISTS \':memory:\' AS "{alias}"')
                    # Self-acquire for a token-less abfss:// catalog root (same as the default +
                    # write paths) so a pure-OIDC multi-catalog project can READ each Lakehouse,
                    # not just write.
                    secret.mint_scoped_secret(
                        self.conn, secret.scoped_secret_name(alias), cfg.get("root_path"),
                        secret.with_onelake_token(cfg.get("root_path"), cfg.get("storage_options")),
                    )
                except Exception as e:  # best-effort: a bad attach shouldn't sink a usable run
                    if os.environ.get("DUCKRUN_AUTH_DEBUG"):
                        print(f"[duckrun] could not attach catalog {alias!r}: {e!r}", flush=True)
            self._attached_conn = self.conn

    def load_source(self, plugin_name: str, source_config):
        plugin = self._plugins.get(plugin_name)
        # Only special-case the duckrun plugin (it knows how to turn a source into scan SQL).
        # Anything else falls back to dbt-duckdb's stock relation-registration path.
        scan_sql = getattr(plugin, "source_scan_sql", None)
        if scan_sql is None:
            return super().load_source(plugin_name, source_config)

        # Create the catalog view on a RAW child cursor of the shared DuckDB database. We must NOT
        # go through self.handle() here: handle() runs initialize_cursor -> plugin.configure_cursor,
        # which overwrites the delta plugin's live per-model cursor with this throwaway one — and we
        # then close it. So a source resolved inside a model's own run() (e.g.
        # `run_query(... {{ source(...) }} ...)` in the model body) would leave the plugin's store()
        # writing on a closed cursor: "Connection already closed". A raw cursor has no such side
        # effect, and CREATE OR REPLACE VIEW is lazy (the scan — and its httpfs/json/spatial
        # extensions — runs later on whichever initialized per-node cursor reads the view).
        # The whole body is under the lock, not just the lazy connect. dbt compiles nodes across its
        # thread pool and resolves a plugin source once per referencing node, so at threads>1 two
        # nodes on the same source race to CREATE OR REPLACE the same catalog entry from separate
        # DuckDB transactions — which DuckDB rejects outright:
        #   TransactionContext Error: Catalog write-write conflict on create with "...View\0<source>"
        # Unlike the delta_scan views the adapter registers during discovery, this path has no
        # best-effort fallback (the source MUST exist for the node to compile), so a conflict kills
        # the run. Serializing costs nothing: a project has a handful of sources, each registered
        # once, and CREATE OR REPLACE VIEW is lazy — the scan itself runs later, off the lock, on
        # whichever per-node cursor reads the view.
        with self.lock:
            if self.conn is None:
                self.conn = self.initialize_db(self.creds, self._plugins)
            cursor = self.conn.cursor()
            try:
                if source_config.schema:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {source_config.schema}")
                cursor.execute(
                    f"CREATE OR REPLACE VIEW {source_config.table_name()} AS "
                    f"{scan_sql(source_config)}"
                )
            finally:
                cursor.close()
