"""
Credentials for the duckrun adapter.

A thin subclass of dbt-duckdb's credentials that (a) reports ``type='duckrun'``,
(b) auto-registers the Delta-write plugin so users don't need a ``plugins:`` block, and
(c) expands ``format: iceberg`` into the ``attach:`` / ``secrets:`` blocks dbt-duckdb already
knows how to execute.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.duckdb.credentials import DuckDBCredentials, PluginConfig

# Module path of our Delta-write plugin (see delta_plugin.Plugin).
_PLUGIN_MODULE = "dbt.adapters.duckrun.delta_plugin"
# Name the materialization uses when calling adapter.store_relation(...).
PLUGIN_ALIAS = "duckrun"
# Name of the DuckDB Azure secret the synthesized iceberg profile mints (same as the Delta path's,
# so a mixed project has one storage credential, not two).
_ICEBERG_SECRET = "duckrun_onelake"


@dataclass
class DuckrunCredentials(DuckDBCredentials):
    # Table format of the target: "delta" (default — DuckDB reads/writes Delta via delta-rs) or
    # "iceberg" (Fabric's Iceberg REST catalog, which DuckDB drives natively; see
    # __pre_deserialize__, which turns it into a stock dbt-duckdb `attach:` entry).
    format: Optional[str] = None
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
    # Names of the natively-attached catalogs (see native_catalogs), surfaced on the Jinja `target`
    # so a materialization can tell "DuckDB owns this catalog" from "duckrun writes Delta here".
    # Populated in __post_init__; never set it in the profile.
    native_catalog_names: Optional[list] = None

    @property
    def type(self) -> str:
        return "duckrun"

    def _connection_keys(self):
        # Expose root_path (+ the alias->root map) on the Jinja `target` so the delta
        # materialization can resolve a write location (dbt only surfaces listed keys). The raw
        # `catalogs` field carries tokens, so it is deliberately NOT surfaced here — only the
        # token-free `catalog_locations`.
        return tuple(super()._connection_keys()) + ("root_path", "catalog_locations",
                                                    "native_catalog_names")

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

    def native_catalogs(self) -> set:
        """Aliases of catalogs DuckDB owns natively — every `attach:` entry, whether hand-written or
        synthesized from `format: iceberg`. Their tables are real DuckDB relations, not Delta
        directories, so duckrun must keep its hands off them: no Delta root, no DML interception,
        no delta_scan views."""
        return {a.alias for a in (self.attach or []) if getattr(a, "alias", None)}

    def root_for(self, database=None) -> tuple:
        """`(root_path, storage_options)` for `database`, falling back to the default catalog when
        `database` is None or not a declared catalog. The one resolver every write/read/discovery
        path routes through, so a project with no `catalogs:` behaves exactly as before.

        A natively-attached catalog (`native_catalogs`) resolves to `(None, None)` instead of that
        fallback — otherwise an Iceberg model's `CREATE TABLE AS` would be routed to the DEFAULT
        Delta root and land in the wrong lakehouse. With no root, `delta_dml.handle` passes the
        statement through to DuckDB and discovery skips the catalog, which is exactly right."""
        roots = self.catalog_roots()
        if database is not None:
            db = str(database).strip('"')
            if db in self.native_catalogs():
                return (None, None)
            if db in roots:
                root, so = roots[db]
                return (root, self._with_token(root, so))
        return (self.root_path, self._with_token(self.root_path, self.storage_options))

    @classmethod
    def __pre_deserialize__(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Expand ``format: iceberg`` into the stock dbt-duckdb profile blocks that drive it.

        A Fabric lakehouse's Iceberg REST catalog is entirely DuckDB's: dbt-duckdb's ``attach:``
        already renders ``ATTACH … (TYPE ICEBERG, …)`` and executes it at connection open, and its
        ``secrets:`` mints the Azure credential DuckDB writes the parquet files with. All the user
        should have to say is *which lakehouse*, so this turns::

            format: iceberg
            root_path: ws/sales.Lakehouse

        into those two blocks — endpoint, ATTACH flags, alias/``database`` and a self-acquired
        OneLake token included (no ``ONELAKE_TOKEN`` env var). Done HERE rather than in
        ``__post_init__`` because dbt-duckdb validates ``database`` against the declared attach
        aliases in ``__pre_deserialize__``: an entry added later would be invisible to that check.

        ``root_path`` is consumed (not expanded to ``abfss://``): the REST catalog's warehouse
        identifier is the ``<workspace>/<item>`` form, and leaving no Delta root behind is what
        makes ``root_for`` return ``(None, None)`` so nothing is routed to delta-rs.
        """
        fmt = str(data.get("format") or "delta").strip().lower()
        if fmt not in ("delta", "iceberg"):
            from dbt_common.exceptions import DbtRuntimeError
            raise DbtRuntimeError(
                f"duckrun: unknown format '{data.get('format')}' in profile; "
                f"use format: delta (default) or format: iceberg."
            )
        if fmt == "iceberg":
            data = cls._expand_iceberg_profile(dict(data))
        return super().__pre_deserialize__(data)

    @classmethod
    def _expand_iceberg_profile(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        # duckrun.iceberg owns the endpoint + ATTACH option set, so the notebook session and this
        # profile can't drift. Imported lazily: the adapter stays importable on its own.
        from duckrun import iceberg
        from . import secret

        # Idempotent: dbt deserializes a profile more than once, and the expansion consumes
        # root_path — a second pass must recognize its own output rather than re-expand it.
        if any(isinstance(a, dict) and str(a.get("type", "")).lower() == "iceberg"
               for a in (data.get("attach") or [])):
            return data
        warehouse = data.pop("root_path", None)
        if not warehouse:
            from dbt_common.exceptions import DbtRuntimeError
            raise DbtRuntimeError(
                "duckrun: format: iceberg needs the lakehouse in 'root_path', e.g. "
                "root_path: ws/sales.Lakehouse (or '<workspace-guid>/<item-guid>')."
            )
        workspace, item, schema = iceberg.parse_iceberg_target(warehouse)
        alias = data.get("database") or iceberg.catalog_name_for(item)
        # Self-acquire the OneLake token (Fabric notebook -> GitHub OIDC -> env -> azure-identity),
        # the same chain the Delta path uses. It authenticates the catalog API; the secret below
        # authenticates the storage the data files are written to.
        so = secret.with_onelake_token(iceberg.abfss_root_for(workspace, item), None)
        token = secret.bearer_token(so) or ""

        options = {"endpoint": iceberg.ICEBERG_ENDPOINT, "token": token, "default_schema": schema}
        options.update(iceberg.ATTACH_OPTIONS)
        data["attach"] = list(data.get("attach") or []) + [
            {"path": f"{workspace}/{item}", "alias": alias, "type": "iceberg", "options": options}
        ]
        data.setdefault("database", alias)

        secrets = list(data.get("secrets") or [])
        if token and not any(isinstance(s, dict) and s.get("name") == _ICEBERG_SECRET
                             for s in secrets):
            secrets.append({"type": "azure", "name": _ICEBERG_SECRET,
                            "provider": "access_token", "access_token": token})
        data["secrets"] = secrets

        settings = dict(data.get("settings") or {})
        settings.setdefault("preserve_insertion_order", False)
        # NOTE the azure transport is deliberately NOT pinned via `settings:` — dbt-duckdb applies
        # settings per CURSOR, and a cursor-scoped SET never reaches the azure extension's
        # connection threads. The delta plugin's configure_connection pins it with SET GLOBAL on
        # the parent connection instead (before the ATTACHes run).
        data["settings"] = settings
        return data

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
        self.native_catalog_names = sorted(self.native_catalogs()) or None
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
