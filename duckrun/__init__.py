"""duckrun — a storage-neutral, SQL-first connection over a Delta lakehouse.

    import duckrun
    conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
    conn = duckrun.connect("ws/lh.Lakehouse/dbo")   # same thing — OneLake shorthand
    conn.sql("SHOW TABLES").show()
    # connect() is read-only by default; pass read_only=False to enable writes:
    w = duckrun.connect("abfss://…/Tables/dbo", read_only=False)
    w.sql("CREATE OR REPLACE TABLE orders_copy AS SELECT * FROM orders")

A Fabric lakehouse can also be opened through its Iceberg REST catalog, which DuckDB drives
natively (listing, schemas, reads and writes) — duckrun only supplies the token and the ATTACH:

    conn = duckrun.connect("ws/sales.Lakehouse", format="iceberg", read_only=False)
    conn.sql("CREATE TABLE dbo.t AS SELECT 1")

`conn.sql()` returns DuckDB's native relation; writes are ordinary SQL DML routed to delta-rs.
Works the same against a local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``.
This is the interactive/notebook API; the dbt adapter lives under ``dbt.adapters.duckrun``.
"""
import sys
from importlib.metadata import PackageNotFoundError, version as _pkg_version

# A progress print must never abort real work: on a stock Windows console (cp1252), a
# non-encodable character in a table/path name raises UnicodeEncodeError out of print()
# (issue #15 — a deploy died mid-provisioning over a progress line). Degrade to `?` instead.
for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        try:
            _s.reconfigure(errors="replace")
        except Exception:
            pass
del _s

from .session import connect, DuckSession
from .iceberg import IcebergSession
from .fabric_remote import RemoteRunner, RemoteResult
from .workspace import workspace, Workspace, ScriptResult
# Debug a dbt model and get a DuckDB relation back. Safe to import eagerly: dbt_debug imports dbt
# only inside its functions. `dbt.adapters` is already pulled in by session.py, but dbt's CLI layer
# (dbtRunner) is a further ~0.9s of import that only a debug session needs — and importing it here
# would also run dbt.adapters.duckrun.impl -> duckrun._runtime while `duckrun` is still initializing.
from .dbt_debug import dbt_project, DbtProject

try:
    # Single source of truth: the installed distribution's version (built from pyproject.toml).
    # Avoids a hand-maintained string drifting from the real package version — it had: this said
    # "0.3.20" while the published wheel was 0.3.21.
    __version__ = _pkg_version("duckrun")
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0+unknown"

__all__ = ["connect", "DuckSession", "IcebergSession", "RemoteRunner", "RemoteResult",
           "workspace", "Workspace", "ScriptResult", "dbt_project", "DbtProject", "__version__"]
