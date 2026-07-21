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
    # Additional named write roots ("catalogs"), so one dbt project can span multiple Fabric
    # Lakehouses. Shape: {alias: {root_path: ..., storage_options: {...}}}. A model targets one
    # with the standard `+database: <alias>` config; the default catalog is `database`
    # (`root_path`/`storage_options` above). Absent -> single-catalog, byte-identical to before.
    catalogs: Optional[Dict[str, Dict]] = None
    # Public alias -> root_path map (NO tokens), surfaced on the Jinja `target` so the
    # materialization can resolve a model's write root by its database. Populated in
    # __post_init__; never set it in the profile.
    catalog_locations: Optional[Dict[str, str]] = None

    @property
    def type(self) -> str:
        return "duckrun"

    def _connection_keys(self):
        # Expose root_path (+ the alias->root map) on the Jinja `target` so the delta
        # materialization can resolve a write location (dbt only surfaces listed keys). The raw
        # `catalogs` field carries tokens, so it is deliberately NOT surfaced here — only the
        # token-free `catalog_locations`.
        return tuple(super()._connection_keys()) + ("root_path", "catalog_locations")

    def catalog_roots(self) -> Dict[str, tuple]:
        """`{database: (root_path, storage_options)}` for every catalog, the default included.

        The default entry keys on `self.database` (the target's default catalog). This is the dbt
        analogue of the native session's catalog registry, and the single source of truth for
        `root_for`."""
        roots = {self.database: (self.root_path, self.storage_options)}
        for alias, cfg in (self.catalogs or {}).items():
            cfg = cfg or {}
            roots[alias] = (cfg.get("root_path"), cfg.get("storage_options"))
        return roots

    @staticmethod
    def _with_token(root, storage_options):
        """Self-acquire a OneLake bearer token for an abfss:// root that has none (GitHub OIDC /
        azure-identity / Fabric notebook) so a profile can omit it. No-op for local/az:// roots or
        when a token is already present. Every catalog resolves through here, so per-catalog writes,
        reads, discovery and stats all get a token even in a multi-catalog project. Cheap: auth caches
        the token per scope, so the repeated resolver calls don't re-mint."""
        from . import secret
        return secret.with_onelake_token(root, storage_options)

    def storage_options_for_location(self, location) -> Optional[Dict[str, str]]:
        """The ``storage_options`` of whichever catalog root is a prefix of ``location``, else the
        default. Used by the ``@available`` helpers that receive a bare path (delta_version /
        delta_table_exists / persist docs) with no database to key on. Longest-prefix wins so a
        nested catalog root resolves before a shorter one. Identity for a single-catalog project."""
        if location and self.catalogs:
            loc = str(location).replace("\\", "/")
            best_root, best_so = None, self.storage_options
            for root, so in self.catalog_roots().values():
                if not root:
                    continue
                r = str(root).replace("\\", "/").rstrip("/")
                if loc == r or loc.startswith(r + "/"):
                    if best_root is None or len(r) > len(best_root):
                        best_root, best_so = r, so
            return self._with_token(best_root or self.root_path, best_so)
        return self._with_token(self.root_path, self.storage_options)

    def root_for(self, database=None) -> tuple:
        """`(root_path, storage_options)` for `database`, falling back to the default catalog when
        `database` is None or not a declared catalog. The one resolver every write/read/discovery
        path routes through, so a project with no `catalogs:` behaves exactly as before."""
        roots = self.catalog_roots()
        if database is not None:
            db = str(database).strip('"')
            if db in roots:
                root, so = roots[db]
                return (root, self._with_token(root, so))
        return (self.root_path, self._with_token(self.root_path, self.storage_options))

    def __post_init__(self):
        # Normalize a trailing slash off every root once, here, so every consumer sees one spelling.
        # The write-path macros concatenate ``root ~ '/' ~ schema ~ '/' ~ id`` while discovery / DML /
        # stats all rstrip('/'), so a profile root_path ending in '/' produced a distinct ``root//…``
        # key on an object store — a location discovery never lists. Strip it at the source instead.
        # Each root also accepts the OneLake `<workspace>/<item>` shorthand ("ws/lh.Lakehouse",
        # "<ws-guid>/<lh-guid>") — expanded here, at the one place a root enters the adapter, so
        # every consumer downstream still sees a plain abfss:// URL. Same expander duckrun.connect
        # uses, so a profile and a notebook read a path identically.
        from .remote import expand_onelake_shorthand
        if self.root_path:
            self.root_path = expand_onelake_shorthand(self.root_path).rstrip("/")
        for cfg in (self.catalogs or {}).values():
            if cfg and cfg.get("root_path"):
                cfg["root_path"] = expand_onelake_shorthand(cfg["root_path"]).rstrip("/")
        # Token-free alias -> root_path map for the Jinja target (see catalog_locations).
        self.catalog_locations = {
            alias: (cfg or {}).get("root_path") for alias, cfg in (self.catalogs or {}).items()
        } or None
        # Ensure the Delta-write plugin is registered exactly once.
        plugins = list(self.plugins or [])
        already = any(
            getattr(p, "alias", None) == PLUGIN_ALIAS or getattr(p, "module", None) == _PLUGIN_MODULE
            for p in plugins
        )
        if not already:
            plugin_config = {}
            if self.storage_options:
                plugin_config["storage_options"] = self.storage_options
            if self.catalogs:
                # store() picks the write token by relation.database; it needs the per-catalog
                # storage_options and the default catalog's name to tell them apart.
                plugin_config["catalogs"] = self.catalogs
                plugin_config["default_database"] = self.database
            plugins.append(
                PluginConfig(module=_PLUGIN_MODULE, alias=PLUGIN_ALIAS, config=plugin_config or None)
            )
        self.plugins = plugins
        parent_post_init = getattr(super(), "__post_init__", None)
        if parent_post_init is not None:
            parent_post_init()
