"""duckrun: a dbt adapter that runs SQL in DuckDB and materializes to Delta Lake.

Built on top of dbt-duckdb. ``dependencies=['duckdb']`` makes dbt load all of
dbt-duckdb's macros alongside ours, so the only thing we add is the ``delta``
materialization plus the Delta-write plugin.
"""
from dbt.adapters.base import AdapterPlugin

from dbt.adapters.duckrun.credentials import DuckrunCredentials
from dbt.adapters.duckrun.impl import DuckrunAdapter
from dbt.adapters.duckrun.__version__ import version as __version__
from dbt.include import duckrun

Plugin = AdapterPlugin(
    adapter=DuckrunAdapter,
    credentials=DuckrunCredentials,
    include_path=duckrun.PACKAGE_PATH,
    dependencies=["duckdb"],
)

__all__ = ["DuckrunAdapter", "DuckrunCredentials", "Plugin", "__version__"]
