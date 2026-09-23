"""duckrun: a dbt adapter that runs SQL in DuckDB and materializes to Delta Lake.

Built on top of dbt-duckdb. ``dependencies=['duckdb']`` makes dbt load all of
dbt-duckdb's macros alongside ours, so the only thing we add is the ``delta``
materialization plus the Delta-write plugin.

The adapter classes load lazily (PEP 562): ``duckrun.connect()`` imports the dbt-free helpers
in this package (engine, delta_dml, secret, …), and a plain ``pip install duckrun`` without the
``[dbt]`` extra has no dbt-core to import. dbt itself reads ``Plugin`` off this module, which
triggers the load as before.
"""
from dbt.adapters.duckrun.__version__ import version as __version__

__all__ = ["DuckrunAdapter", "DuckrunCredentials", "Plugin", "__version__"]


def __getattr__(name):
    if name not in ("DuckrunAdapter", "DuckrunCredentials", "Plugin"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from dbt.adapters.base import AdapterPlugin

    from dbt.adapters.duckrun.credentials import DuckrunCredentials
    from dbt.adapters.duckrun.impl import DuckrunAdapter
    from dbt.include import duckrun

    g = globals()
    g["DuckrunAdapter"] = DuckrunAdapter
    g["DuckrunCredentials"] = DuckrunCredentials
    g["Plugin"] = AdapterPlugin(
        adapter=DuckrunAdapter,
        credentials=DuckrunCredentials,
        include_path=duckrun.PACKAGE_PATH,
        dependencies=["duckdb"],
    )
    return g[name]
