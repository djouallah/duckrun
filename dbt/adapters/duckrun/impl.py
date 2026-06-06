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
            except Exception:
                pass

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
        table makes ``is_incremental()`` true on later runs.

        This only returns the relations (for the cache); list_relations_without_caching then
        calls _register_delta_view on each so the physical ``delta_scan`` view exists too —
        which is what lets read-only commands (``dbt test``/``show``/``docs``), that run no
        model and so never hit a materialization, still query the table. For ``run``/``build``
        the materialization re-creates the view anyway (pre-register {{ this }} + step-4 view).
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

    def _register_delta_view(self, relation):
        """Create the ``delta_scan`` view for a discovered Delta relation on the live
        connection so read-only commands (``dbt test``, ``dbt show``, ``dbt docs``) that
        never materialize anything can still query the model.

        ``dbt run``/``build`` create this same view in the materialization (step 4); doing it
        here too is harmless — the materialization's ``create or replace`` just supersedes it.
        But for a command that runs no models, this is the only place the physical view gets
        created from the Delta tables discovered on disk.
        """
        root_path = getattr(self.config.credentials, "root_path", None)
        if not root_path:
            return
        location = (
            root_path.rstrip("/")
            + "/" + str(relation.schema).strip('"')
            + "/" + str(relation.identifier).strip('"')
        )
        try:
            self.create_schema(relation)
            cursor = self._cursor()
            cursor.execute(
                f"create or replace view {relation.render()} as "
                f"select * from delta_scan('{location}')"
            )
        except Exception:
            # A table mid-write or an unreadable log shouldn't abort cache population;
            # the materialization re-creates the view on the next run regardless.
            pass

    def list_relations_without_caching(self, schema_relation):
        try:
            in_memory = list(super().list_relations_without_caching(schema_relation))
        except Exception:
            in_memory = []

        discovered = self._discover_delta_relations(schema_relation)
        if not discovered:
            return in_memory

        # Physically register each discovered Delta table as a delta_scan view so read-only
        # commands (dbt test/show/docs) can query models without a prior in-process run.
        for rel in discovered:
            self._register_delta_view(rel)

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
