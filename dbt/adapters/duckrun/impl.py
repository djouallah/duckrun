"""
The duckrun adapter: a thin subclass of dbt-duckdb.

State lives entirely in Delta Lake. Writes always go through delta_rs (see engine.py /
delta_plugin.py); DuckDB is only ever used to *read* (via ``delta_scan``) and to run model
logic. Each existing Delta table is surfaced to dbt as a plain ``delta_scan`` view named to
match ``database.schema.identifier`` so ``{{ this }}``, ``ref()`` and ``is_incremental()``
resolve — with no ``delta_classic`` attach and no re-attach when a table is created.
"""
from dbt.adapters.base.meta import available
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.impl import DuckDBAdapter

from dbt.adapters.duckrun.credentials import DuckrunCredentials

try:  # dbt 1.8+
    from dbt.adapters.contracts.relation import RelationType
except ImportError:  # pragma: no cover - older layouts
    from dbt.contracts.relation import RelationType


class DuckrunConnectionManager(DuckDBConnectionManager):
    TYPE = "duckrun"

    @classmethod
    def open(cls, connection):
        # dbt-duckdb stores its singleton Environment on whichever class `open` is
        # invoked on. adapter.store_relation() looks it up via the *base* class
        # (DuckDBConnectionManager.env()), so delegate to the base to keep _ENV there,
        # then mirror it onto this subclass for any instance-level lookups.
        handle = DuckDBConnectionManager.open(connection)
        DuckrunConnectionManager._ENV = DuckDBConnectionManager._ENV
        return handle


class DuckrunAdapter(DuckDBAdapter):
    ConnectionManager = DuckrunConnectionManager
    Credentials = DuckrunCredentials

    @available
    def delta_table_exists(self, location) -> bool:
        """True if a Delta table already exists at ``location``."""
        from . import engine
        so = getattr(self.config.credentials, "storage_options", None)
        return engine.table_exists(location, so)

    # ------------------------------------------------------------------ discovery
    def _cursor(self):
        return self.connections.get_thread_connection().handle.cursor()

    def _discover_delta_relations(self, schema_relation):
        """Discover Delta tables physically present under ``root_path/<schema>`` and surface
        each as a ``delta_scan`` view named ``<db>.<schema>.<table>``.

        This is what makes the adapter stateless across processes: dbt rebuilds its relation
        cache at run start by calling list_relations_without_caching for every manifest
        schema, even on a fresh in-memory DuckDB. Here we (a) recreate a view over each
        existing Delta table so ``{{ this }}`` / ``ref()`` are queryable, and (b) return the
        tables as relations so they land in the cache and ``is_incremental()`` is true.
        """
        root_path = getattr(self.config.credentials, "root_path", None)
        if not root_path:
            return []

        schema = schema_relation.schema
        database = schema_relation.database
        base = root_path.rstrip("/") + "/" + str(schema).strip('"')

        cursor = self._cursor()
        # `*` matches one path segment (the table dir); every committed Delta table has at
        # least one commit json (00..0.json is unreliable after cleanup_metadata()).
        pattern = (base + "/*/_delta_log/*.json").replace("'", "''")
        try:
            rows = cursor.execute(
                f"SELECT DISTINCT file FROM glob('{pattern}')"
            ).fetchall()
        except Exception:
            return []

        marker = "/_delta_log/"
        seen = {}
        for (file_path,) in rows:
            idx = file_path.find(marker)
            if idx == -1:
                continue
            location = file_path[:idx]
            name = location.rsplit("/", 1)[-1]
            if name and name not in seen:
                seen[name] = location

        relations = []
        for name, location in seen.items():
            rel = self.Relation.create(
                database=database, schema=schema, identifier=name, type=RelationType.Table
            )
            location_sql = location.replace("'", "''")
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {rel.without_identifier().render()}")
                cursor.execute(
                    f"CREATE OR REPLACE VIEW {rel.render()} AS "
                    f"SELECT * FROM delta_scan('{location_sql}')"
                )
            except Exception:
                # A table whose view can't be built (e.g. transient read error) is simply
                # not advertised; dbt will treat it as not-existing.
                continue
            relations.append(rel)
        return relations

    def list_relations_without_caching(self, schema_relation):
        try:
            in_memory = list(super().list_relations_without_caching(schema_relation))
        except Exception:
            in_memory = []

        discovered = self._discover_delta_relations(schema_relation)
        if not discovered:
            return in_memory

        existing = {str(r.identifier).strip('"').lower() for r in in_memory}
        merged = list(in_memory)
        for rel in discovered:
            if str(rel.identifier).strip('"').lower() not in existing:
                merged.append(rel)
        return merged
