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

from dbt.adapters.duckdb.environments.local import (
    DuckDBConnectionWrapper,
    DuckDBCursorWrapper,
    LocalEnvironment,
)

from . import delta_dml
from . import secret


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
        if not secret.bearer_token(so):
            return
        fresh = secret.refreshed(so)
        if fresh is so:
            return  # token still valid (the common path) — nothing to do
        setter(fresh)  # keep the live copy DML/discovery read from in sync
        try:
            if is_default:
                secret.ensure_azure_secret(self._cursor, fresh)
            else:
                root = (self._duckrun_creds.catalogs.get(catalog) or {}).get("root_path")
                secret.mint_scoped_secret(
                    self._cursor, secret.scoped_secret_name(catalog), root, fresh
                )
            if os.environ.get("DUCKRUN_AUTH_DEBUG"):
                print(f"[duckrun-auth] execute: re-minted DuckDB secret for catalog {catalog!r}", flush=True)
        except Exception as e:  # best-effort: a transient refresh failure keeps the old secret
            if os.environ.get("DUCKRUN_AUTH_DEBUG"):
                print(f"[duckrun-auth] execute: re-mint failed for {catalog!r}: {e!r}", flush=True)


class DuckrunEnvironment(LocalEnvironment):
    def handle(self):
        # Swap dbt-duckdb's cursor wrapper for ours so raw DML on Delta relations is intercepted
        # on every cursor (connection-manager AND test-harness paths) — see DuckrunCursorWrapper.
        h = super().handle()  # initializes self.conn (+ mints the default catalog's secret)
        self._attach_catalogs()
        if isinstance(h, DuckDBConnectionWrapper):
            h._cursor = DuckrunCursorWrapper(h._cursor._cursor, self.creds)
        return h

    def _attach_catalogs(self) -> None:
        """ATTACH each declared (non-default) catalog as an in-memory DuckDB catalog and mint its
        path-scoped Azure secret, so `alias.schema.table` relations resolve and each Lakehouse's
        reads authenticate with its own token. Runs once, on the shared connection, before the first
        model/discovery — cross-catalog `ref()` needs the alias to exist up front. No-op when no
        catalogs are declared (single-catalog behavior is unchanged)."""
        catalogs = getattr(self.creds, "catalogs", None)
        # Guard on the connection identity, not a plain flag: if the env's in-memory connection is
        # ever rebuilt, the ATTACHes are gone with it, so we must re-attach on the new one.
        if not catalogs or self.conn is None or getattr(self, "_attached_conn", None) is self.conn:
            return
        default_db = getattr(self.creds, "database", None)
        for alias, cfg in catalogs.items():
            if alias == default_db:
                continue  # the default catalog is the base connection itself
            cfg = cfg or {}
            try:
                # A quoted identifier so an alias with odd characters can't break the ATTACH.
                self.conn.execute(f'ATTACH IF NOT EXISTS \':memory:\' AS "{alias}"')
                secret.mint_scoped_secret(
                    self.conn, secret.scoped_secret_name(alias),
                    cfg.get("root_path"), cfg.get("storage_options"),
                )
            except Exception as e:  # best-effort: a bad attach shouldn't sink an otherwise-usable run
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
