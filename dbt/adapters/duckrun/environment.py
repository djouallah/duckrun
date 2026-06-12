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
from dbt.adapters.duckdb.environments.local import LocalEnvironment


class DuckrunEnvironment(LocalEnvironment):
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
