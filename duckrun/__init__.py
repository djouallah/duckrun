"""duckrun — a storage-neutral, DataFrame-style connection over a Delta lakehouse.

    import duckrun
    conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
    conn.sql("SHOW TABLES").show()
    # connect() is read-only by default; pass read_only=False to enable writes:
    w = duckrun.connect("abfss://…/Tables/dbo", read_only=False)
    w.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")

Works the same against a local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``.
This is the interactive/notebook API; the dbt adapter lives under ``dbt.adapters.duckrun``.
"""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

from .session import connect, DuckSession, DataFrame
from .delta_table import DeltaTable

try:
    # Single source of truth: the installed distribution's version (built from pyproject.toml).
    # Avoids a hand-maintained string drifting from the real package version — it had: this said
    # "0.3.20" while the published wheel was 0.3.21.
    __version__ = _pkg_version("duckrun")
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0+unknown"

__all__ = ["connect", "DuckSession", "DataFrame", "DeltaTable", "__version__"]
