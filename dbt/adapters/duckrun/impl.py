"""
The duckrun adapter: a thin subclass of dbt-duckdb.

Everything (connections, catalog, seeds, sources, type mapping) is inherited from
dbt-duckdb. We only rename the connection type to ``duckrun`` so the adapter and its
credentials agree, and point at our credentials class. The new ``delta`` materialization
is added via the include macros (see dbt/include/duckrun); dbt-duckdb's own
materializations come along through ``AdapterPlugin(dependencies=['duckdb'])``.
"""
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.duckdb.impl import DuckDBAdapter

from dbt.adapters.duckrun.credentials import DuckrunCredentials


class DuckrunConnectionManager(DuckDBConnectionManager):
    TYPE = "duckrun"


class DuckrunAdapter(DuckDBAdapter):
    ConnectionManager = DuckrunConnectionManager
    Credentials = DuckrunCredentials
