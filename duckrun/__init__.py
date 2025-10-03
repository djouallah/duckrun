"""Duckrun - Lakehouse task runner powered by DuckDB"""

from duckrun.core import Duckrun

__version__ = "0.1.0"

# Expose connect at module level for: import duckrun as dr
connect = Duckrun.connect

__all__ = ["Duckrun", "connect"]