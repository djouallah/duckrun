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
import threading

from dbt.adapters.duckdb.environments.local import (
    DuckDBConnectionWrapper,
    DuckDBCursorWrapper,
    LocalEnvironment,
)

from . import delta_dml
from . import secret

# Serializes OneLake token refreshes across threads. Module-level rather than per-wrapper because
# the state it protects — the shared credentials object and the instance-global DuckDB secrets —
# is shared by every cursor, and each dbt thread gets its own wrapper.
_CREDS_REFRESH_LOCK = threading.Lock()


class DuckrunCursorWrapper(DuckDBCursorWrapper):
    """Cursor wrapper that routes raw DML against duckrun-managed (Delta-backed) relations to
    delta_rs instead of running it on the read-only ``delta_scan`` view.

    Every SQL statement — whether issued by dbt's connection manager or by the adapter-test
    harness (which goes straight to ``conn.handle.cursor().execute``) — funnels through here, so
    this is the single production interception point. Non-matching statements, parameterized
    statements (the seed loader's ``insert ... values (?)``), and DML against native relations all
    fall through to DuckDB unchanged. See delta_dml.handle.
    """

    def __init__(self, cursor, credentials):
        super().__init__(cursor)
        self._duckrun_creds = credentials

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
        return super().execute(sql, bindings)

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


class DuckrunEnvironment(LocalEnvironment):
    def handle(self):
        # Swap dbt-duckdb's cursor wrapper for ours so raw DML on Delta relations is intercepted
        # on every cursor (connection-manager AND test-harness paths) — see DuckrunCursorWrapper.
        h = super().handle()  # initializes self.conn (+ mints the default catalog's secret)
        self._ensure_default_secret()
        self._attach_catalogs()
        if isinstance(h, DuckDBConnectionWrapper):
            h._cursor = DuckrunCursorWrapper(h._cursor._cursor, self.creds)
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
