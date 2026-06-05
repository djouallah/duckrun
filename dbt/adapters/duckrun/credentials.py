"""
Credentials for the duckrun adapter.

A thin subclass of dbt-duckdb's credentials that (a) reports ``type='duckrun'`` and
(b) auto-registers the Delta-write plugin so users don't need a ``plugins:`` block.
"""
from dataclasses import dataclass
from typing import Dict, Optional

from dbt.adapters.duckdb.credentials import DuckDBCredentials, PluginConfig

# Module path of our Delta-write plugin (see delta_plugin.Plugin).
_PLUGIN_MODULE = "dbt.adapters.duckrun.delta_plugin"
# Name the materialization uses when calling adapter.store_relation(...).
PLUGIN_ALIAS = "duckrun"


@dataclass
class DuckrunCredentials(DuckDBCredentials):
    # Default Delta location used when a model doesn't set config(location=...).
    # Example: "./warehouse" or "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables".
    root_path: Optional[str] = None
    # Passed through to deltalake for remote stores. In a notebook where the DuckDB
    # secret is already provided this can be left empty.
    storage_options: Optional[Dict[str, str]] = None

    @property
    def type(self) -> str:
        return "duckrun"

    @property
    def delta_attach(self) -> Optional[str]:
        """
        Alias of a read-only ``delta_classic`` attachment that backs the dbt
        ``database`` (i.e. ``{{ this }}`` resolves to it). When set, the delta
        materialization runs in "attach state mode": it reads existing tables from
        this read-only catalog and never tries to CREATE inside it — it stages in the
        writable in-memory ``memory`` catalog and writes the table via delta_rs.

        Returns ``None`` for the plain delta_scan/local mode.
        """
        for att in (self.attach or []):
            # `attach` items are Attachment dataclasses at runtime, but tolerate dicts.
            if isinstance(att, dict):
                alias = att.get("alias")
                options = att.get("options") or {}
                atype = (att.get("type") or options.get("type") or "")
                read_only = bool(att.get("read_only") or options.get("read_only"))
            else:
                options = getattr(att, "options", None) or {}
                alias = getattr(att, "alias", None)
                atype = (getattr(att, "type", None) or options.get("type") or "")
                read_only = bool(getattr(att, "read_only", False) or options.get("read_only"))
            if str(atype).lower() == "delta_classic" and read_only and alias == self.database:
                return alias
        return None

    def _connection_keys(self):
        # Expose root_path + delta_attach on the Jinja `target` so the delta
        # materialization can resolve a default location and pick the write/read mode
        # (dbt only surfaces listed keys).
        return tuple(super()._connection_keys()) + ("root_path", "delta_attach")

    def __post_init__(self):
        # Ensure the Delta-write plugin is registered exactly once.
        plugins = list(self.plugins or [])
        already = any(
            getattr(p, "alias", None) == PLUGIN_ALIAS or getattr(p, "module", None) == _PLUGIN_MODULE
            for p in plugins
        )
        if not already:
            plugin_config = {"storage_options": self.storage_options} if self.storage_options else None
            plugins.append(PluginConfig(module=_PLUGIN_MODULE, alias=PLUGIN_ALIAS, config=plugin_config))
        self.plugins = plugins
        parent_post_init = getattr(super(), "__post_init__", None)
        if parent_post_init is not None:
            parent_post_init()
