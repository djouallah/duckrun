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
        """Discover Delta tables physically present under ``root_path/<schema>`` and return
        them as relations so they land in dbt's relation cache.

        This is what makes the adapter stateless across processes: dbt rebuilds its relation
        cache at run start by calling list_relations_without_caching for every manifest
        schema, even on a fresh in-memory DuckDB. Returning a table-typed relation per Delta
        table makes ``is_incremental()`` true on later runs. We deliberately do NOT create the
        ``delta_scan`` views here: discovery runs during the before_run cache-population phase,
        and views created on that connection don't survive to the model-run phase. The view is
        (re)created in the materialization instead (pre-register {{ this }} + step-4 view).
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
        names = []
        for (file_path,) in rows:
            # glob returns OS-native separators (backslashes on Windows); normalize so the
            # marker match and table-name split work regardless of platform / store.
            fp = file_path.replace("\\", "/")
            idx = fp.find(marker)
            if idx == -1:
                continue
            name = fp[:idx].rsplit("/", 1)[-1]
            if name and name not in names:
                names.append(name)

        return [
            self.Relation.create(
                database=database, schema=schema, identifier=name, type=RelationType.Table
            )
            for name in names
        ]

    def list_relations_without_caching(self, schema_relation):
        try:
            in_memory = list(super().list_relations_without_caching(schema_relation))
        except Exception:
            in_memory = []

        discovered = self._discover_delta_relations(schema_relation)
        if not discovered:
            return in_memory

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
