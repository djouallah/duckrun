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
from dbt.adapters.duckdb.environments.local import (
    DuckDBConnectionWrapper,
    DuckDBCursorWrapper,
    LocalEnvironment,
)

from . import delta_dml


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
        if bindings is None:
            creds = self._duckrun_creds
            if delta_dml.handle(
                self._cursor,
                getattr(creds, "root_path", None),
                getattr(creds, "storage_options", None),
                sql,
            ):
                return self._cursor  # applied to Delta; nothing to run on DuckDB
        return super().execute(sql, bindings)


class DuckrunEnvironment(LocalEnvironment):
    def handle(self):
        # Swap dbt-duckdb's cursor wrapper for ours so raw DML on Delta relations is intercepted
        # on every cursor (connection-manager AND test-harness paths) — see DuckrunCursorWrapper.
        h = super().handle()
        if isinstance(h, DuckDBConnectionWrapper):
            h._cursor = DuckrunCursorWrapper(h._cursor._cursor, self.creds)
        return h

    def load_source(self, plugin_name: str, source_config):
        plugin = self._plugins.get(plugin_name)
        # Only special-case the duckrun plugin (it knows how to turn a source into scan SQL).
        # Anything else falls back to dbt-duckdb's stock relation-registration path.
        scan_sql = getattr(plugin, "source_scan_sql", None)
        if scan_sql is None:
            return super().load_source(plugin_name, source_config)

        handle = self.handle()
        cursor = handle.cursor()
        try:
            if source_config.schema:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {source_config.schema}")
            # A catalog view is shared across all cursors of this DuckDB database, so it resolves
            # for whichever per-node cursor reads the source (and is rebuilt in a fresh process).
            cursor.execute(
                f"CREATE OR REPLACE VIEW {source_config.table_name()} AS "
                f"{scan_sql(source_config)}"
            )
        finally:
            cursor.close()
            handle.close()
