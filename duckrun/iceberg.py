"""A SQL-only session over a Fabric Lakehouse's **Iceberg REST catalog**.

    conn = duckrun.connect("ws/lh.Lakehouse", format="iceberg")
    conn.sql("SHOW TABLES").show()

Against the Iceberg endpoint DuckDB is the whole engine: it lists the catalog, resolves schemas and
tables, reads, and writes (``CREATE SCHEMA`` / ``CREATE TABLE AS`` / ``INSERT`` / ``DROP``). So this
module is deliberately tiny — none of the Delta machinery (table discovery, ``delta_scan`` views,
delta-rs DML routing, memory clamps, sort-key rewrites) applies, and ``sql()`` inspects nothing:
it hands the statement to DuckDB verbatim. ``read_only`` is DuckDB's own ``ATTACH … (READ_ONLY)``
flag, enforced by DuckDB, not by duckrun reading your SQL.

What duckrun contributes is the boilerplate the endpoint needs: a OneLake token (Fabric notebook →
GitHub OIDC → env → azure-identity, the same chain the Delta path uses), the storage secret DuckDB
writes its parquet files with, the endpoint, and the ATTACH options.
"""
import re
from typing import Dict, Optional

import duckdb

from dbt.adapters.duckrun import engine, remote, secret

# Fabric's Iceberg REST catalog endpoint (private preview). The catalog *API*; data files still go
# to OneLake over abfss, which is why the azure storage secret is minted alongside the ATTACH.
ICEBERG_ENDPOINT = "https://onelake.table.fabric.microsoft.com/iceberg"

# The ATTACH options Fabric needs, verified against the live endpoint on DuckDB 1.5.4.
# ACCESS_DELEGATION_MODE 'none': the catalog vends no credentials, so DuckDB authenticates storage
# with our own secret. The two staging flags let CREATE TABLE commit straight through the catalog.
# The booleans are spelled as STRINGS deliberately: dbt-duckdb's Attachment renderer, which turns
# this same dict into the profile's ATTACH, DROPS an option whose value is Python ``False`` — a
# quoted 'false' survives and DuckDB casts it.
ATTACH_OPTIONS = {
    "access_delegation_mode": "none",
    "stage_create_tables": "false",
    "skip_create_table_metadata_updates": "true",
}

_DEFAULT_SCHEMA = "dbo"


def _qid(name: str) -> str:
    """Quote a SQL identifier (catalog/schema name)."""
    return '"' + str(name).replace('"', '""') + '"'


def _qlit(text: str) -> str:
    """Escape a SQL string literal body."""
    return str(text).replace("'", "''")


def parse_iceberg_target(path: str):
    """Normalize ``path`` into ``(workspace, item, schema)`` for the Iceberg REST catalog.

    The catalog's warehouse identifier is ``<workspace>/<item>`` — friendly (``ws/sales.Lakehouse``)
    or GUIDs (``<ws-guid>/<item-guid>``) — NOT an abfss URL, so this deliberately does the opposite
    of :func:`remote.expand_onelake_shorthand`: an ``abfss://`` path is reversed back to the two
    segments. ``ws/lh.Lakehouse/dbo`` and ``ws/lh.Lakehouse/Tables/dbo`` both name schema ``dbo``;
    an omitted schema defaults to ``dbo`` (Fabric's own default).
    """
    p = str(path or "").replace("\\", "/").strip("/")
    if remote.is_abfss(p):
        workspace, _host, rest = remote._parse_abfss(p)
        parts = [s for s in rest.split("/") if s]
        item, tail = (parts[0] if parts else ""), parts[1:]
    else:
        parts = [s for s in p.split("/") if s]
        if len(parts) < 2:
            raise ValueError(
                f"iceberg: '{path}' is not a lakehouse — pass '<workspace>/<item>.Lakehouse' "
                f"(or '<workspace-guid>/<item-guid>'), optionally with a schema: "
                f"'ws/sales.Lakehouse/dbo'."
            )
        workspace, item, tail = parts[0], parts[1], parts[2:]
    if tail and tail[0].lower() in ("tables", "files"):
        tail = tail[1:]
    return workspace, item, (tail[0] if tail else _DEFAULT_SCHEMA)


def abfss_root_for(workspace: str, item: str) -> str:
    """The lakehouse's abfss root. The Iceberg catalog is addressed as ``<workspace>/<item>``, but
    token acquisition (``secret.with_onelake_token``) keys off an ``abfss://`` path — so the two
    surfaces get their credential from the same seam as the Delta path."""
    return f"abfss://{workspace}@{remote.ONELAKE_HOST}/{item}/Tables"


def catalog_name_for(item: str) -> str:
    """A SQL-safe catalog name derived from the lakehouse item (``sales.Lakehouse`` → ``sales``),
    or ``"data"`` when nothing readable can be derived (a GUID item) — same fallback, and the same
    non-reserved word, as the Delta session's :func:`session._derive_catalog_name`."""
    seg = str(item or "").strip()
    for suffix in remote._ITEM_SUFFIXES:
        if seg.lower().endswith(suffix):
            seg = seg[: -len(suffix)]
            break
    if not seg or remote._GUID.match(seg):
        return "data"
    return re.sub(r"[^0-9A-Za-z_]", "_", seg).strip("_") or "data"


def attach_iceberg_catalog(con, name: str, path: str, schema: Optional[str] = None,
                           storage_options: Optional[Dict[str, str]] = None,
                           read_only: bool = True):
    """ATTACH ``path``'s Iceberg REST catalog on ``con`` as catalog ``name``.

    Returns ``(storage_options, schema)`` — the options carrying the acquired token (so the caller
    can re-mint on refresh) and the resolved default schema. The one seam every surface goes
    through, so the notebook session, an attached catalog, and the dbt path can't drift.
    """
    workspace, item, parsed_schema = parse_iceberg_target(path)
    schema = schema or parsed_schema
    so = secret.with_onelake_token(abfss_root_for(workspace, item),
                                   dict(storage_options) if storage_options else None)
    # The ATTACH TOKEN authenticates the catalog API only; DuckDB writes/reads the parquet data
    # files over abfss and needs its own storage credential. Same token, both places.
    secret.ensure_azure_secret(con, so)
    token = secret.bearer_token(so) or ""
    con.execute("INSTALL iceberg; LOAD iceberg;")
    opts = [
        "TYPE ICEBERG",
        f"ENDPOINT '{_qlit(ICEBERG_ENDPOINT)}'",
        f"TOKEN '{_qlit(token)}'",
        *(f"{k.upper()} '{_qlit(v)}'" for k, v in ATTACH_OPTIONS.items()),
        f"DEFAULT_SCHEMA '{_qlit(schema)}'",
    ]
    if read_only:
        opts.append("READ_ONLY")
    con.execute(f"ATTACH OR REPLACE '{_qlit(workspace + '/' + item)}' AS {_qid(name)} "
                f"({', '.join(opts)})")
    return so, schema


class IcebergSession:
    """A session bound to one or more Iceberg REST catalogs. ``sql()`` is DuckDB, unfiltered."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]] = None,
                 schema: Optional[str] = None, read_only: bool = True,
                 name: Optional[str] = None):
        self.read_only = read_only
        self.con = duckdb.connect()
        engine.configure_duckdb_session(self.con)
        # name -> (path, storage_options, schema, read_only); the token lives in storage_options so
        # a re-attach after rotation can be minted from the same entry.
        self._catalogs: Dict[str, tuple] = {}
        try:
            catalog = name or catalog_name_for(parse_iceberg_target(path)[1])
            _so, resolved = self._attach(catalog, path, storage_options, schema, read_only)
            self.con.execute(f"USE {_qid(catalog)}.{_qid(resolved)}")
        except Exception:
            self.con.close()  # don't leak the connection when the attach/token step fails
            raise

    def _attach(self, name: str, path: str, storage_options, schema, read_only: bool):
        so, resolved = attach_iceberg_catalog(self.con, name, path, schema, storage_options,
                                              read_only)
        self._catalogs[name] = (path, so, resolved, read_only)
        return so, resolved

    def attach(self, path: str, name: Optional[str] = None,
               storage_options: Optional[Dict[str, str]] = None,
               schema: Optional[str] = None,
               read_only: Optional[bool] = None) -> "IcebergSession":
        """Attach a second+ lakehouse's Iceberg catalog, so ``catalog.schema.table`` resolves across
        lakehouses. ``read_only`` defaults to the session's mode. Returns ``self`` so it chains."""
        catalog = name or catalog_name_for(parse_iceberg_target(path)[1])
        if catalog in self._catalogs:
            raise ValueError(f"catalog name '{catalog}' is already attached; choose another name.")
        self._attach(catalog, path, storage_options,
                     schema, self.read_only if read_only is None else bool(read_only))
        return self

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Run ``query`` and return DuckDB's native ``DuckDBPyRelation``.

        Nothing is parsed or rewritten. Reads, DDL and writes are all DuckDB's own against the
        Iceberg catalog; what a catalog permits (including read-only, which is DuckDB's ATTACH
        flag) is decided by DuckDB and Fabric, not by duckrun."""
        return self.con.sql(query)

    def register(self, name: str, obj) -> "IcebergSession":
        """Register a Python object (DataFrame, Arrow table, …) as a DuckDB view named ``name`` so
        SQL can join against it. Mirrors ``DuckSession.register``."""
        self.con.register(name, obj)
        return self

    def close(self):
        """Close the underlying DuckDB connection; the session is unusable afterwards."""
        self.con.close()

    def __enter__(self) -> "IcebergSession":
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    @property
    def _connection(self):
        """The underlying DuckDB connection (internal escape hatch)."""
        return self.con
