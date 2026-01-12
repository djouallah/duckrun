"""Duckrun - Lakehouse task runner powered by DuckDB"""

from duckrun.core import Duckrun
from duckrun.notebook import import_notebook_from_web, import_notebook, deploy_notebook

__version__ = "0.2.26"

# Expose unified connect method at module level
connect = Duckrun.connect

__all__ = ["Duckrun", "connect", "import_notebook_from_web", "import_notebook", "deploy_notebook"]