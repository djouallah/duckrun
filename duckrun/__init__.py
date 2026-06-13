"""duckrun — a storage-neutral, Spark-shaped connection over a Delta lakehouse.

    import duckrun
    conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
    conn.sql("SHOW TABLES").show()
    conn.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")

Works the same against a local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``.
This is the interactive/notebook API; the dbt adapter lives under ``dbt.adapters.duckrun``.
"""
from .session import connect, DuckSession, DataFrame
from .delta_table import DeltaTable

__version__ = "0.3.17"

__all__ = ["connect", "DuckSession", "DataFrame", "DeltaTable", "__version__"]
