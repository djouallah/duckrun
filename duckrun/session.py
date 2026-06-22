"""A DataFrame-style, storage-neutral connection over a Delta lakehouse.

``duckrun.connect(path)`` opens a DuckDB connection, discovers the Delta tables under the store,
registers each as a ``delta_scan`` view, and hands back a :class:`DuckSession` whose surface
offers ``.sql()``, ``.table()``, ``.read``, ``.catalog``, and a
``DataFrame`` with a ``.write…saveAsTable()``.

It is storage-neutral — local path, ``s3://``, ``gs://``, ``az://``, OneLake ``abfss://`` — because
every storage concern (token → secret, table discovery, the Delta write path) is delegated to the
``dbt.adapters.duckrun`` modules that already handle all of them. This module is glue.
"""
import os
import re
from collections import namedtuple
from typing import Dict, List, Optional

import duckdb

from dbt.adapters.duckrun import delta_dml, engine, remote, secret
from . import auth
from ._runtime import check_runtime_versions


# Statements that would WRITE to a table — rejected by the read-only conn.sql() with a pointer to
# the DataFrame write API. INSERT/UPDATE/DELETE/MERGE against a read-only delta_scan view error in
# DuckDB anyway; CREATE [OR REPLACE] TABLE … is the dangerous one — it silently makes an ephemeral
# DuckDB-local table that never reaches Delta — so it must be caught BEFORE executing. CREATE
# TEMP/TEMPORARY TABLE and CREATE VIEW are DuckDB-local scratch by design and pass through.
_WRITE_KEYWORD_RE = re.compile(r"^(insert|update|delete|merge)\b", re.IGNORECASE)
_CREATE_TABLE_RE = re.compile(r"^create\s+(or\s+replace\s+)?table\b", re.IGNORECASE)
_DML_TARGET_RE = re.compile(
    r"^(?:insert\s+into|delete\s+from|update)\s+(?P<rel>\"?[\w.]+\"?)", re.IGNORECASE)
_CREATE_TEMP_RE = re.compile(r"^create\s+(or\s+replace\s+)?(temp|temporary)\b", re.IGNORECASE)

# DML forms that genuinely can't be expressed through delta_rs (delta_dml.handle never applies them):
# rejected up front with a form-specific pointer rather than letting DuckDB raise a cryptic error on
# the read-only delta_scan view (or, for UPDATE … FROM, silently mangling the SET clause).
# leading `\b`: _find_top_level probes every depth-0 index (see delta_dml._find_top_level).
_TOP_FROM = re.compile(r"\bfrom\b", re.IGNORECASE)
_TOP_USING = re.compile(r"\busing\b", re.IGNORECASE)
_strip_leading = delta_dml._strip_leading  # shared comment/whitespace stripper

_MERGE_MSG = (
    "conn.sql() can't run a SQL MERGE via delta_rs. Use the DataFrame write API: "
    "df.write.saveAsTable(...) to create/append, df.write.option('replaceWhere', …) to overwrite "
    "a slice, or DeltaTable.forName(conn, name).merge(...)/.delete()/.update()."
)
_UPDATE_FROM_MSG = (
    "conn.sql() can't run UPDATE … FROM via delta_rs. Rewrite the SET values as correlated "
    "subqueries, or use DeltaTable.forName(conn, name).update(...)/.merge(...)."
)
_DELETE_USING_MSG = (
    "conn.sql() can't run DELETE … USING via delta_rs. Rewrite the predicate as a correlated "
    "subquery (DELETE … WHERE … IN (SELECT …)), or use "
    "DeltaTable.forName(conn, name).delete(...)/.merge(...)."
)
_MULTI_MSG = (
    "conn.sql() runs one statement at a time — split the batch into separate conn.sql() calls."
)
_READ_ONLY_MSG = (
    "catalog '{catalog}' is read-only — cannot {op}. duckrun opens read-only by default; enable "
    "Delta writes (saveAsTable / insertInto / save / merge / insert / update / delete / replaceWhere) "
    "with connect(read_only=False) for the primary, or conn.attach(path, name='{catalog}', "
    "read_only=False) for an attached catalog."
)


def _unsupported_dml(query: str) -> Optional[str]:
    """An error message if ``query`` is a DML form duckrun can't route to delta_rs, else None."""
    s = _strip_leading(query)
    low = s.lower()
    if low.startswith("merge"):
        return _MERGE_MSG
    if low.startswith("update") and delta_dml._find_top_level(s, _TOP_FROM) != -1:
        return _UPDATE_FROM_MSG
    if low.startswith("delete") and delta_dml._find_top_level(s, _TOP_USING) != -1:
        return _DELETE_USING_MSG
    if re.match(r"(insert|update|delete|merge|create|alter|drop)\b", low) and _is_multi_statement(s):
        return _MULTI_MSG
    return None


def _is_multi_statement(s: str) -> bool:
    """True if ``s`` holds more than one statement (a top-level ``;`` with anything after it)."""
    depth, quote = 0, None
    for i, ch in enumerate(s):
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == ";" and depth == 0 and s[i + 1:].strip():
            return True
    return False


def _is_delta_write(query: str) -> bool:
    """True if ``query`` is a statement that would write a Delta table (and so must go through the
    DataFrame write API instead). CREATE TEMP/TEMPORARY TABLE and CREATE VIEW are NOT writes."""
    s = _strip_leading(query)
    if _WRITE_KEYWORD_RE.match(s):
        return True
    return bool(_CREATE_TABLE_RE.match(s)) and not _CREATE_TEMP_RE.match(s)


# ---- createDataFrame helpers --------------------------------------------------------------
# PySpark DDL type spellings → DuckDB types. Anything not listed (INTEGER, DECIMAL(10,2), …) is
# already a DuckDB type and passes through untouched.
_SPARK_TO_DUCKDB_TYPE = {
    "int": "INTEGER", "integer": "INTEGER",
    "long": "BIGINT", "bigint": "BIGINT",
    "short": "SMALLINT", "smallint": "SMALLINT",
    "byte": "TINYINT", "tinyint": "TINYINT",
    "string": "VARCHAR", "str": "VARCHAR",
    "double": "DOUBLE", "float": "FLOAT", "real": "FLOAT",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "date": "DATE", "timestamp": "TIMESTAMP",
    "binary": "BLOB",
}


def _split_top_level_commas(s: str) -> List[str]:
    """Split ``s`` on commas that are not inside parentheses, so ``DECIMAL(10,2)`` stays intact."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(s[start:i])
            start = i + 1
    parts.append(s[start:])
    return [p.strip() for p in parts if p.strip()]


def _map_type(spark_type: str) -> str:
    """Map a (possibly parameterised) Spark/DuckDB type spelling to a DuckDB type."""
    base = spark_type.split("(", 1)[0].strip().lower()
    mapped = _SPARK_TO_DUCKDB_TYPE.get(base)
    if mapped is None:
        return spark_type.strip()  # already a DuckDB type (INTEGER, DECIMAL(10,2), …)
    paren = spark_type[spark_type.find("("):] if "(" in spark_type else ""
    return mapped + paren


def _parse_ddl_schema(ddl: str):
    """``"id int, name: string"`` → ``(["id", "name"], ["INTEGER", "VARCHAR"])``."""
    names, types = [], []
    for field in _split_top_level_commas(ddl):
        name, _, typ = field.replace(":", " ", 1).strip().partition(" ")
        if not name or not typ.strip():
            raise ValueError(f"bad schema field {field!r}; expected 'name type'")
        names.append(name.strip().strip('"'))
        types.append(_map_type(typ.strip()))
    return names, types


def _parse_schema(schema):
    """Normalise the ``schema`` arg to ``(names | None, duckdb_types | None)``."""
    if schema is None:
        return None, None
    if isinstance(schema, str):
        return _parse_ddl_schema(schema)
    if isinstance(schema, (list, tuple)) and all(isinstance(c, str) for c in schema):
        return list(schema), None
    raise TypeError(
        "schema must be None, a list of column names, or a DDL string like 'id int, name string'")


def _as_pandas(data):
    """Return ``data`` if it is a pandas DataFrame (pandas optional), else None."""
    try:
        import pandas
    except ImportError:
        return None
    return data if isinstance(data, pandas.DataFrame) else None


def _is_arrow(data) -> bool:
    """True if ``data`` is a pyarrow Table / RecordBatchReader (pyarrow optional)."""
    try:
        import pyarrow
    except ImportError:
        return False
    return isinstance(data, (pyarrow.Table, pyarrow.RecordBatchReader))


def _project_rename(rel, names: List[str]):
    cur = rel.columns
    if len(names) != len(cur):
        raise ValueError(f"schema lists {len(names)} columns but data has {len(cur)}")
    return rel.project(", ".join(f'"{c}" AS "{n}"' for c, n in zip(cur, names)))


def _project_cast(rel, types: List[str]):
    cur = rel.columns
    if len(types) != len(cur):
        raise ValueError(f"schema lists {len(types)} types but data has {len(cur)} columns")
    return rel.project(", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t in zip(cur, types)))


def _delta_write_message(query: str) -> str:
    """The error for a raw-SQL write conn.sql() can't route to delta_rs. For an INSERT/UPDATE/DELETE
    whose target isn't a discovered Delta table — the common cause being a typo or a table written
    out-of-band before refresh() — name the table and give form-appropriate guidance, instead of the
    generic 'use the DataFrame write API' redirect (which misdirects: for UPDATE/DELETE the problem is the
    missing table, not the API)."""
    s = _strip_leading(query)
    m = _DML_TARGET_RE.match(s)
    if m:
        rel = m.group("rel").strip('"')
        verb = s.split(None, 1)[0].lower()
        if verb in ("update", "delete"):
            return (
                f"conn.sql(): no Delta table '{rel}' to {verb}. conn.sql() DML only targets a "
                f"discovered Delta table — check the name, or call conn.refresh() if it was just "
                f"written out-of-band."
            )
        return (  # insert into a table that doesn't exist yet
            f"conn.sql(): no Delta table '{rel}' to insert into. Create it first with "
            f"df.write.saveAsTable('{rel}'), then insert."
        )
    return (  # a CREATE … AS that didn't resolve, or any other unrouted Delta write
        "conn.sql() can't write a Delta table from raw SQL here. "
        "Use the DataFrame write API: df.write.saveAsTable(...) to create/append, "
        "df.write.option('replaceWhere', …) to overwrite a slice, or "
        "DeltaTable.forName(conn, name).merge(...)/.delete()/.update()."
    )


def _qid(name: str) -> str:
    """Quote a SQL identifier (schema/table/view name)."""
    return '"' + str(name).replace('"', '""') + '"'


def _qlit(text: str) -> str:
    """Escape a SQL string literal body (the path inside ``delta_scan('...')``)."""
    return str(text).replace("'", "''")


def _strip_query_context(msg: str) -> str:
    """DuckDB appends the offending statement to errors as ``\\nLINE N: <sql>\\n   ^``. When that
    statement is one duckrun generated internally (the ``delta_scan`` view), echoing it back is
    noise that makes the failure look like it's about the caller's input. Keep the real error
    text; drop the generated-SQL context."""
    idx = msg.find("\nLINE ")
    return msg[:idx].rstrip() if idx != -1 else msg


_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _onelake_guid_hint(root_path: str) -> Optional[str]:
    """Workaround note for the OneLake ``delta_scan`` bug, shown only when a friendly-name
    ``abfss://`` path is involved. OneLake's delta_scan can fail to enumerate a valid table's
    ``_delta_log`` when the path uses friendly workspace/lakehouse names (duckdb-delta#307); the
    GUID form reads fine. Returns ``None`` for non-abfss paths or paths already using GUIDs (no
    point nagging those)."""
    if not remote.is_abfss(root_path):
        return None
    workspace, _host, path = remote._parse_abfss(root_path)
    lakehouse = path.split("/", 1)[0] if path else ""
    if lakehouse.lower().endswith(".lakehouse"):
        lakehouse = lakehouse[: -len(".Lakehouse")]
    if _GUID.match(workspace) and _GUID.match(lakehouse):
        return None
    return (
        "OneLake's delta_scan can fail to read a valid table's _delta_log when the abfss path uses "
        "friendly names — a known upstream issue (duckdb-delta#307). Until it's fixed, use the "
        "workspace and lakehouse GUIDs, e.g. "
        "abfss://<workspace-guid>@onelake.dfs.fabric.microsoft.com/<lakehouse-guid>/Tables"
    )


def _split_root_schema(path: str, schema: Optional[str]):
    """Normalize ``path`` into ``(root_path, schema)``.

    ``root_path`` is the directory that *contains* schema folders (so a table lives at
    ``root_path/<schema>/<table>``). When ``schema`` is passed explicitly it wins and ``path`` is
    taken as the root verbatim. Otherwise, for OneLake (``abfss://``) we honor the ``…/Tables`` and
    ``…/Tables/<schema>`` convention — the segment after ``Tables`` (if any) becomes the schema and
    the root is truncated to ``…/Tables``. For every other store an omitted schema means
    "discover all schema folders under ``path``".
    """
    p = path.rstrip("/")
    if schema is not None:
        return p, schema
    if remote.is_abfss(p):
        low = p.lower()
        marker = "/tables"
        idx = low.rfind(marker)
        if idx != -1:
            root = p[: idx + len(marker)]  # up to and including the original-case "Tables"
            after = p[idx + len(marker):].strip("/")
            if after:
                return root, after.split("/")[0]
            return root, None
    return p, None


# One attached lakehouse root = one DuckDB catalog. The primary (from connect) is the first entry;
# attach() adds more. `schema_filter` mirrors connect's `schema=` (restrict discovery to one schema);
# `read_only` is per-catalog so a read-only reference store (e.g. a Fabric Warehouse) can sit next to
# a writable lakehouse in one session.
_CatEntry = namedtuple("_CatEntry", ["name", "root_path", "storage_options", "schema_filter", "read_only"])


def _derive_catalog_name(root_path: str) -> Optional[str]:
    """A catalog identifier from a lakehouse root's last readable segment — strips a trailing
    ``…/Tables`` and a ``.Lakehouse`` suffix, sanitizes to SQL-identifier chars. Returns ``None`` for
    a GUID-only or empty segment (the caller must require an explicit ``name=``)."""
    p = root_path.replace("\\", "/").rstrip("/")
    idx = p.lower().rfind("/tables")
    if idx != -1:
        p = p[:idx]
    seg = p.rstrip("/").rsplit("/", 1)[-1]
    if "@" in seg:  # abfss container@host with no further path — take the container
        seg = seg.split("@", 1)[0]
    if seg.lower().endswith(".lakehouse"):
        seg = seg[: -len(".lakehouse")]
    seg = seg.strip()
    if not seg or _GUID.match(seg):
        return None
    sanitized = re.sub(r"[^0-9A-Za-z_]", "_", seg).strip("_")
    return sanitized or None


def _secret_name(catalog_name: str) -> str:
    """A stable DuckDB secret name for an attached catalog's scoped Azure token."""
    return "duckrun_cat_" + re.sub(r"[^0-9A-Za-z_]", "_", catalog_name)


def _mint_scoped_secret(con, secret_name: str, root: str, storage_options) -> bool:
    """Mint a path-**scoped** DuckDB Azure secret for an attached OneLake catalog, so two different
    bearer tokens (one per workspace) coexist in one connection — DuckDB picks the longest-matching
    scope, so each catalog's reads use its own token. No-op (False) when there's no token."""
    token = secret.bearer_token(storage_options)
    if not token:
        return False
    con.execute("INSTALL azure; LOAD azure;")
    transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if transport:
        con.execute(f"SET GLOBAL azure_transport_option_type = '{transport.replace(chr(39), chr(39) * 2)}'")
    token_sql = token.replace("'", "''")
    scope_sql = str(root).replace("'", "''")
    con.execute(
        f"CREATE OR REPLACE SECRET {secret_name} "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token_sql}', SCOPE '{scope_sql}')"
    )
    return True


# DML target-relation matcher (verb → relation), used only to detect a 3-part cross-catalog target.
_DML_REL = re.compile(
    r"^(?:insert\s+into|delete\s+from|update|alter\s+table|drop\s+table(?:\s+if\s+exists)?|"
    r"create\s+(?:or\s+replace\s+)?table(?:\s+if\s+not\s+exists)?)\s+(?P<rel>[\w.\"]+)",
    re.IGNORECASE,
)


def _dml_target_catalog(query: str) -> Optional[str]:
    """The leading catalog token of a DML statement's target when it's 3-part
    (``catalog.schema.table``), else ``None``. Lets ``sql()`` route raw DML to the named catalog's
    root (rather than the current one)."""
    s = _strip_leading(query)
    _, body = delta_dml._split_leading_with(s)  # peel a leading WITH so the verb is visible
    m = _DML_REL.match(_strip_leading(body))
    if not m:
        return None
    parts = [p.strip().strip('"') for p in m.group("rel").split(".")]
    return parts[0] if len(parts) >= 3 else None


class DuckSession:
    """A session handle bound to one or more Delta lakehouse roots, each surfaced as a catalog."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]],
                 schema: Optional[str], compaction_threshold: int, read_only: bool = True,
                 name: Optional[str] = None):
        self.compaction_threshold = compaction_threshold
        self.read_only = read_only

        self.con = duckdb.connect()
        engine.configure_duckdb_session(self.con)

        # The catalog registry: name -> _CatEntry. The primary (from connect) is the first entry;
        # attach() adds more. root_path / storage_options stay readable as properties that return the
        # *current* catalog's values, so existing single-catalog callers keep working unchanged.
        self._catalogs: Dict[str, _CatEntry] = {}
        self._current_catalog: Optional[str] = None
        self._current_database: Optional[str] = None
        self.catalog = Catalog(self)

        root, schema_filter = _split_root_schema(path, schema)
        # Catalog is first-class, so single- and multi-catalog sessions share one code path
        # (catalog.schema.table works either way). The primary's name: an explicit name= wins, else
        # derive it from the URL (OneLake lakehouse name / local folder), else fall back to "data"
        # (a non-reserved word — usable bare in SQL, unlike "default"/"main").
        catalog_name = name or _derive_catalog_name(root) or "data"
        self._attach_catalog(catalog_name, root, storage_options, schema_filter, primary=True, quiet=False)

    # ---- catalog registry ------------------------------------------------------------------

    @property
    def root_path(self) -> str:
        """The current catalog's lakehouse root (back-compat for single-catalog callers)."""
        return self._catalogs[self._current_catalog].root_path

    @property
    def storage_options(self) -> Optional[Dict[str, str]]:
        """The current catalog's storage_options (back-compat for single-catalog callers)."""
        return self._catalogs[self._current_catalog].storage_options

    def _attach_catalog(self, name: str, root: str, storage_options, schema_filter,
                        primary: bool = False, quiet: bool = True, read_only=None):
        """Register a lakehouse root as a named DuckDB catalog, mint its read secret, discover its
        tables. The primary keeps the original unscoped OneLake secret; attached catalogs get a
        path-scoped secret so two different OneLake tokens can coexist in one connection. ``read_only``
        defaults to the session default (the primary's mode) when None."""
        root = root.replace("\\", "/").rstrip("/")
        ro = self.read_only if read_only is None else bool(read_only)
        so = dict(storage_options) if storage_options else None
        # OneLake with no caller-supplied token: acquire one (Fabric / env / azure-identity) so both
        # the DuckDB read secret and delta-rs writes can authenticate.
        if remote.is_abfss(root) and not secret.bearer_token(so):
            so = dict(so or {})
            so["bearer_token"] = auth.get_onelake_token()
        # The Azure secret must be minted before any delta_scan/glob. No-op without a bearer token.
        try:
            if primary:
                secret.ensure_azure_secret(self.con, so)
            else:
                _mint_scoped_secret(self.con, _secret_name(name), root, so)
        except Exception as exc:  # surfaced, not swallowed as "no tables" later
            print(f"⚠️  could not mint OneLake secret for catalog '{name}': {exc}")

        self.con.execute(f"ATTACH ':memory:' AS {_qid(name)}")
        self._catalogs[name] = _CatEntry(name, root, so, schema_filter, ro)
        if primary:
            self._current_catalog = name
        self._refresh_catalog(name, quiet=quiet)

    def attach(self, path: str, name: Optional[str] = None,
               storage_options: Optional[Dict[str, str]] = None,
               schema: Optional[str] = None, read_only: Optional[bool] = None) -> "DuckSession":
        """Attach a second+ lakehouse as a named catalog, so ``catalog.schema.table`` resolves across
        lakehouses.

        ``name`` is derived from a friendly path when omitted, but is **mandatory** for a GUID-only
        OneLake path (raises, since there is no readable name to derive). The mapping is bijective:
        re-attaching a URL (under any name), or reusing a name, raises. ``schema`` restricts discovery
        to a single schema, exactly as in :func:`connect`. ``read_only`` fences writes to *this* catalog
        independently of the session (default: inherit the session's mode) — so a read-only reference
        store (e.g. a Fabric Warehouse) can sit next to a writable lakehouse. Returns ``self`` so it
        chains.
        """
        root, schema_filter = _split_root_schema(path, schema)
        root = root.replace("\\", "/").rstrip("/")
        if name is None:
            name = _derive_catalog_name(root)
            if name is None:
                raise ValueError(
                    f"could not derive a catalog name from '{path}' (a GUID-only OneLake path has no "
                    f"readable name); pass name= explicitly, e.g. conn.attach(path, name='sales')."
                )
        if name in self._catalogs:
            raise ValueError(f"catalog name '{name}' is already attached; choose another name.")
        for entry in self._catalogs.values():
            if entry.root_path == root:
                raise ValueError(f"that lakehouse is already attached as catalog '{entry.name}'.")
        self._attach_catalog(name, root, storage_options, schema_filter, primary=False, quiet=False,
                             read_only=read_only)
        return self

    # ---- discovery -------------------------------------------------------------------------

    def _list_tables(self, root: str, schema: str, so) -> List[str]:
        if remote.is_abfss(root):
            return remote.list_delta_tables(root, str(schema), so)
        return remote.list_delta_tables_via_glob(self.con, root, str(schema))

    def _list_schemas(self, root: str, so) -> List[str]:
        if remote.is_abfss(root):
            # Immediate sub-directories under the root (…/Tables) are the schema folders.
            return remote.list_delta_tables(root, "", so)
        # Local / s3 / gcs / az: glob two levels deep and collect the schema segment.
        pattern = _qlit(root.rstrip("/") + "/*/*/_delta_log/*.json")
        try:
            rows = self.con.execute(f"SELECT DISTINCT file FROM glob('{pattern}')").fetchall()
        except Exception:
            return []
        marker = "/_delta_log/"
        schemas: List[str] = []
        for (fp,) in rows:
            fp = fp.replace("\\", "/")
            idx = fp.find(marker)
            if idx == -1:
                continue
            # …/<schema>/<table>/_delta_log/…  → take <schema>
            parts = fp[:idx].rsplit("/", 2)
            if len(parts) >= 2 and parts[-2] and parts[-2] not in schemas:
                schemas.append(parts[-2])
        return schemas

    def refresh(self, quiet: bool = False, catalog: Optional[str] = None):
        """Re-discover Delta tables and (re)register their ``delta_scan`` views.

        Refreshes every attached catalog by default; pass ``catalog=`` to refresh just one. Call
        after writing tables out-of-band (or from another process) to surface them. ``quiet=True``
        skips the per-catalog banner (used by the catalog existence checks, which refresh on every
        call).
        """
        names = [catalog] if catalog is not None else list(self._catalogs)
        for name in names:
            self._refresh_catalog(name, quiet=quiet)
        return self

    def _refresh_catalog(self, name: str, quiet: bool = True):
        entry = self._catalogs[name]
        root, so = entry.root_path, entry.storage_options
        if entry.schema_filter is not None:
            mapping = {entry.schema_filter: self._list_tables(root, entry.schema_filter, so)}
        else:
            mapping = {s: self._list_tables(root, s, so) for s in self._list_schemas(root, so)}

        registered = []
        for schema, tables in mapping.items():
            if not tables:
                continue
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(name)}.{_qid(schema)}")
            for table in tables:
                # Hide drop-tombstones (a `drop table` overwrites the table to a one-column marker;
                # no data is deleted, the files persist, but the table must not surface).
                if delta_dml.is_dropped(self.con, f"{root}/{schema}/{table}", so):
                    continue
                self._register_view(name, schema, table)
                registered.append(f"{schema}.{table}")

        schemas = list(mapping.keys())
        if name == self._current_catalog and self._current_database is None:
            self._current_database = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
        if name == self._current_catalog:
            self._use(self._current_catalog, self._current_database)

        if not quiet:
            lh = root.rstrip("/").rsplit("/", 1)[-1]
            print(f"🔌 Connected to {lh} (catalog '{name}') — discovered {len(registered)} table(s)"
                  + (": " + ", ".join(registered) if registered else ""))
        return registered

    def _register_view(self, catalog: str, schema: str, table: str):
        entry = self._catalogs[catalog]
        path = f"{entry.root_path}/{schema}/{table}"
        try:
            self.con.execute(
                f"CREATE OR REPLACE VIEW {_qid(catalog)}.{_qid(schema)}.{_qid(table)} AS "
                f"SELECT * FROM delta_scan('{_qlit(path)}')"
            )
        except Exception as exc:
            # delta_scan failed reading the table. Keep the real engine error (it's the signal —
            # e.g. the OneLake "No files in log segment" delta-kernel bug), but drop DuckDB's echo
            # of the CREATE VIEW statement *we* generated, and say which table/path it was. Suppress
            # the chained original (`from None`) so the noisy SQL echo doesn't reappear in tracebacks.
            hint = _onelake_guid_hint(entry.root_path)
            raise RuntimeError(
                f"duckrun: could not read Delta table {catalog}.{schema}.{table} at '{path}':\n"
                f"{_strip_query_context(str(exc))}"
                + (f"\n\n{hint}" if hint else "")
            ) from None

    def _use(self, catalog: str, schema: Optional[str]):
        """Switch DuckDB's current catalog (and schema, when present) so unqualified and 2-part
        names resolve in the current catalog. A missing/empty schema shouldn't abort connect."""
        try:
            if schema:
                self.con.execute(f"USE {_qid(catalog)}.{_qid(schema)}")
            else:
                self.con.execute(f"USE {_qid(catalog)}")
        except Exception:  # an empty/absent schema shouldn't abort connect
            pass

    def _table_path(self, schema: str, table: str, catalog: Optional[str] = None) -> str:
        cat = catalog if catalog is not None else self._current_catalog
        return f"{self._catalogs[cat].root_path}/{schema}/{table}"

    def _catalog_storage_options(self, catalog: str):
        return self._catalogs[catalog].storage_options

    def _resolve(self, name: str):
        """Split a possibly-qualified name into ``(catalog, schema, table)``.

        3 parts → ``catalog.schema.table`` (the catalog must be attached); 2 parts → ``schema.table``
        in the current catalog; 1 part → table in the current catalog + database."""
        parts = [p.strip().strip('"') for p in name.split(".")]
        if len(parts) >= 3:
            catalog, schema, table = parts[-3], parts[-2], parts[-1]
            if catalog not in self._catalogs:
                raise ValueError(
                    f"unknown catalog '{catalog}'; attached catalogs: {list(self._catalogs)}. "
                    f"Attach it with conn.attach(path, name='{catalog}')."
                )
            return catalog, schema, table
        if len(parts) == 2:
            return self._current_catalog, parts[0], parts[1]
        return self._current_catalog, self._current_database, parts[0]

    def _require_writable(self, op: str, catalog: Optional[str] = None):
        """Raise unless the target catalog (default: the current one) is writable. Guards every
        Delta-write entry point (the DataFrame write API, the DeltaTable mutators, and raw write-DML
        in sql()). ``read_only`` is per-catalog, so a read-only attached store fails loud here even
        when the session/primary is writable."""
        cat = catalog if catalog is not None else self._current_catalog
        if self._catalogs[cat].read_only:
            raise PermissionError(_READ_ONLY_MSG.format(op=op, catalog=cat))

    # ---- DataFrame-style surface --------------------------------------------------------------

    def sql(self, query: str) -> "DataFrame":
        """Run a query and return a :class:`DataFrame`.

        Reads pass straight through to DuckDB over the ``delta_scan`` views (time-travel works for
        free — ``conn.sql("from delta_scan('path', version => 0)")``).

        Delta **DML** is applied to the Delta table via delta_rs (works local AND on OneLake):
        ``create table … as select`` (overwrite), ``insert into … select``/``insert into … values``
        (append), ``delete``/``update`` (delta_rs delete/update), ``alter table … add column``, and
        ``drop table`` (tombstone — marks the table dropped without deleting data; a human purges
        the files). After a DML statement the catalog is refreshed.

        ``merge`` isn't expressible via delta_rs DML here — use the DataFrame write API instead:
        ``df.write.saveAsTable(...)`` or
        ``DeltaTable.forName(conn, name).merge(...)/.delete()/.update()``.
        ``CREATE TEMP/VIEW`` and other DuckDB-local scratch DDL pass through to DuckDB.
        """
        # Raw DML routes through delta_rs against ONE root. A 3-part target names which catalog that
        # is (``_dml_target_catalog`` → its name); an unqualified/2-part target is the current catalog.
        # Reads (SELECT) are unaffected — DuckDB resolves every attached catalog natively.
        target_cat = _dml_target_catalog(query)
        if target_cat is not None and target_cat not in self._catalogs:
            raise ValueError(
                f"unknown catalog '{target_cat}'; attached catalogs: {list(self._catalogs)}. "
                f"Attach it with conn.attach(path, name='{target_cat}')."
            )
        write_cat = target_cat if target_cat is not None else self._current_catalog
        # The read-only gate is the *target* catalog's — a read-only attached store fails loud even
        # when the current catalog is writable. _WRITE_KEYWORD_RE covers insert/update/delete/merge.
        if self._catalogs[write_cat].read_only and _is_delta_write(query):
            raise PermissionError(_READ_ONLY_MSG.format(op="run write DML", catalog=write_cat))
        unsupported = _unsupported_dml(query)
        if unsupported:
            raise ValueError(unsupported)
        entry = self._catalogs[write_cat]
        if delta_dml.handle(self.con, entry.root_path, entry.storage_options, query,
                            default_schema=self._current_database):
            self.refresh(quiet=True, catalog=write_cat)
            return DataFrame(self.con.sql("SELECT 'ok' AS status"), self)
        if _is_delta_write(query):
            raise ValueError(_delta_write_message(query))
        return DataFrame(self.con.sql(query), self)

    def table(self, name: str) -> "DataFrame":
        catalog, schema, table = self._resolve(name)
        return DataFrame(
            self.con.sql(f"SELECT * FROM {_qid(catalog)}.{_qid(schema)}.{_qid(table)}"), self)

    def createDataFrame(self, data, schema=None, samplingRatio=None,
                        verifySchema: bool = True) -> "DataFrame":
        """Create a :class:`DataFrame` from in-memory data — the ``createDataFrame`` API.

        ``data`` is a list of tuples/lists (a list of scalars is treated as a single column), a
        pandas ``DataFrame``, or a pyarrow ``Table`` / ``RecordBatchReader``. ``schema`` is
        ``None`` (names inferred — ``_1, _2, …`` for tuples, the frame's own names otherwise), a
        list of column names, or a DDL string (``"id int, name string"``; ``:`` between name and
        type is also accepted). The data is materialised as a relation on duckrun's own DuckDB
        connection — write it to Delta with ``df.write.saveAsTable(...)``.

        ``samplingRatio`` is accepted for signature parity and ignored; ``verifySchema`` is
        best-effort — DuckDB casts/validates when the relation is built.
        """
        names, types = _parse_schema(schema)

        pdf = _as_pandas(data)
        if pdf is not None:
            rel = self.con.from_df(pdf)
        elif _is_arrow(data):
            rel = self.con.from_arrow(data)
        else:
            rows = list(data)
            if rows and not isinstance(rows[0], (tuple, list)):
                rows = [(v,) for v in rows]  # list of scalars → a single column
            if not rows:
                if not types:
                    raise ValueError(
                        "cannot infer schema from an empty dataset — pass a DDL schema, e.g. "
                        "createDataFrame([], 'id int, name string')")
                cols = ", ".join(f'CAST(NULL AS {t}) AS "{n}"' for n, t in zip(names, types))
                return DataFrame(self.con.sql(f"SELECT {cols} WHERE 1=0"), self)
            ncols = len(rows[0])
            if any(len(r) != ncols for r in rows):
                raise ValueError("all rows must have the same number of columns")
            placeholders = ", ".join("(" + ", ".join(["?"] * ncols) + ")" for _ in rows)
            collist = ", ".join(f'"_{i + 1}"' for i in range(ncols))
            flat = [v for r in rows for v in r]
            rel = self.con.sql(
                f"SELECT * FROM (VALUES {placeholders}) AS t({collist})", params=flat)

        if names is not None:
            rel = _project_rename(rel, names)
        if types is not None:
            rel = _project_cast(rel, types)
        return DataFrame(rel, self)

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    def stop(self):
        """Close the underlying DuckDB connection (Spark's ``SparkSession.stop()``). The session is
        unusable afterwards — registered views and the minted secret go with the connection."""
        self.con.close()

    @property
    def _connection(self):
        """The underlying DuckDB connection (internal escape hatch)."""
        return self.con


class DataFrame:
    """Wraps a DuckDB relation; exposes a DataFrame-style ``.write`` plus a few DataFrame aliases.

    Anything not defined here falls through to the underlying DuckDB relation, so ``.df()``,
    ``.arrow()``, ``.fetchall()``, ``.fetchnumpy()`` etc. all keep working.
    """

    def __init__(self, relation, session: DuckSession):
        self.relation = relation
        self.session = session

    @property
    def write(self) -> "DataFrameWriter":
        return DataFrameWriter(self)

    # DataFrame aliases over the DuckDB relation.
    def show(self, *a, **k):
        return self.relation.show(*a, **k)

    def toPandas(self):
        return self.relation.df()

    def toArrow(self):
        """Spark's ``DataFrame.toArrow()`` by name — but **streaming, not materialized**. Spark
        returns a fully-collected ``pyarrow.Table``; duckrun returns a lazy
        ``pyarrow.RecordBatchReader`` (DuckDB's ``to_arrow_reader()``), so large results are pulled
        one batch at a time instead of loaded whole into memory."""
        return self.relation.to_arrow_reader()

    def collect(self):
        return self.relation.fetchall()

    def count(self) -> int:
        return self.relation.aggregate("count(*)").fetchone()[0]

    def createOrReplaceTempView(self, name: str) -> "DataFrame":
        """Register this DataFrame as a session-scoped view named ``name``, so it can be queried by
        name via ``conn.sql("select * from name")`` (the ``createOrReplaceTempView`` API).

        This is the path-read counterpart to ``saveAsTable``: ``conn.read.format("delta").load(path)`` returns a
        DataFrame but registers nothing, so this is how a by-path read becomes queryable by name. The
        view is **native DuckDB and ephemeral** — it is not a Delta table, is not written to storage,
        and does not appear in ``conn.catalog``; use ``saveAsTable`` to persist as Delta. Returns
        ``self`` so it chains."""
        self.relation.create_view(name, replace=True)
        return self

    def __getattr__(self, name):
        # Only reached for attributes not found on DataFrame itself.
        return getattr(self.relation, name)


class DataFrameReader:
    """``DataFrameReader``: read a path/table into a :class:`DataFrame` without it having to
    be a pre-registered view. Storage-neutral via the session's already-minted secret."""

    def __init__(self, session: DuckSession):
        self.session = session
        self._format = "delta"
        self._options: Dict[str, str] = {}

    def format(self, fmt: str) -> "DataFrameReader":
        self._format = fmt.lower()
        return self

    def option(self, key: str, value) -> "DataFrameReader":
        self._options[key] = value
        return self

    def load(self, path: str) -> DataFrame:
        fmt = self._format
        if fmt == "delta":
            # Time travel mirrors spark.read.option("versionAsOf", N): pin the read to that Delta
            # version via duckdb-delta's `version =>` parameter. timestampAsOf has no delta_scan
            # equivalent in this build, so reject it rather than silently ignoring it.
            if "timestampAsOf" in self._options:
                raise ValueError(
                    "read.option('timestampAsOf', …) is not supported — duckdb-delta time-travels "
                    "by version only. Use option('versionAsOf', N)."
                )
            version = self._options.get("versionAsOf")
            if version is not None:
                try:
                    version = int(version)
                except (TypeError, ValueError):
                    raise ValueError(f"versionAsOf must be an integer Delta version, got {version!r}.")
                scan = f"delta_scan('{_qlit(path)}', version => {version})"
            else:
                scan = f"delta_scan('{_qlit(path)}')"
        elif fmt == "parquet":
            scan = f"read_parquet('{_qlit(path)}')"
        elif fmt == "csv":
            opts = "".join(f", {k}={_csv_opt(v)}" for k, v in self._options.items())
            scan = f"read_csv_auto('{_qlit(path)}'{opts})"
        else:
            raise ValueError(f"Unsupported read format '{fmt}'. Use 'delta', 'parquet', or 'csv'.")
        return DataFrame(self.session.con.sql(f"SELECT * FROM {scan}"), self.session)

    def parquet(self, path: str) -> DataFrame:
        return self.format("parquet").load(path)

    def csv(self, path: str) -> DataFrame:
        return self.format("csv").load(path)

    def table(self, name: str) -> DataFrame:
        return self.session.table(name)


def _csv_opt(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + _qlit(str(value)) + "'"


class DataFrameWriter:
    """``DataFrameWriter`` over delta-rs (the adapter's :func:`engine.write_delta`).

    Beyond the standard modes it adds ``"safeappend"`` — the same optimistic, fail-loud append as the
    dbt adapter's incremental strategy: it commits only if the table version has not moved since
    the call (compare-and-swap), else raises ``CommitFailedError``. Non-standard, but identical
    behaviour to ``safeappend`` in dbt."""

    _MODES = {"overwrite", "append", "safeappend", "ignore", "error", "errorifexists"}

    def __init__(self, df: DataFrame):
        self._df = df
        self._format = "delta"
        self._mode = "error"  # the default SaveMode
        self._merge_schema = False
        self._overwrite_schema = False
        self._partition_by: Optional[List[str]] = None
        self._replace_where: Optional[str] = None

    def format(self, fmt: str) -> "DataFrameWriter":
        if fmt.lower() != "delta":
            raise ValueError(f"Only 'delta' is supported, got '{fmt}'.")
        self._format = "delta"
        return self

    def mode(self, mode: str) -> "DataFrameWriter":
        m = mode.lower()
        if m not in self._MODES:
            raise ValueError(f"mode must be one of {sorted(self._MODES)}, got '{mode}'.")
        self._mode = m
        return self

    def option(self, key: str, value) -> "DataFrameWriter":
        if key == "replaceWhere":
            # Atomically replace the rows matching this predicate with the written data, in a single
            # Delta commit (the delta-spark replaceWhere / INSERT OVERWRITE form). Requires
            # mode('overwrite'); snapshot-fenced at write time so a concurrent writer fails loudly.
            self._replace_where = str(value)
            return self
        truthy = str(value).lower() in ("true", "1")
        if key == "mergeSchema":
            self._merge_schema = truthy
        elif key == "overwriteSchema":
            self._overwrite_schema = truthy
        else:
            raise ValueError(
                "Unsupported write option '{}'. Supported: 'mergeSchema', 'overwriteSchema', "
                "'replaceWhere'.".format(key)
            )
        return self

    def partitionBy(self, *cols) -> "DataFrameWriter":
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = tuple(cols[0])
        self._partition_by = list(cols)
        return self

    def _write(self, path: str, descr: str, so=None, catalog=None) -> None:
        """Apply the configured mode to the Delta table at ``path`` (storage-neutral). ``descr``
        names the target in the mode='error' message. ``so`` / ``catalog`` are the target catalog's
        storage_options / name (default to the current catalog's). Shared by saveAsTable and save."""
        session = self._df.session
        session._require_writable("write a Delta table", catalog)
        if so is None:
            so = session.storage_options

        if self._replace_where is not None:
            # df.write.option("replaceWhere", pred).mode("overwrite").save()/saveAsTable() — a single
            # Delta commit that swaps only the matching partition/rows. Snapshot-fenced: pin the
            # version now and CAS the commit, so a concurrent writer fails loudly.
            if self._mode != "overwrite":
                raise ValueError(
                    "option('replaceWhere', …) requires mode('overwrite'), got mode('%s')." % self._mode
                )
            engine.replace_where(
                path,
                self._df.relation,
                self._replace_where,
                read_version=engine.table_version(path, so),
                partition_by=self._partition_by,
                storage_options=so,
                compaction_threshold=session.compaction_threshold,
            )
            return

        mode = self._mode
        if mode in ("error", "errorifexists"):
            if engine.table_exists(path, so):
                raise ValueError(
                    f"{descr} already exists (mode='error'). "
                    f"Use mode('overwrite'), mode('append'), mode('safeappend'), or mode('ignore')."
                )
            mode = "overwrite"

        if mode == "safeappend":
            # Optimistic append, identical to the dbt safeappend strategy: pin to the version now
            # and CAS the commit, so a writer that lands between this read and the commit fails the
            # append (fail loud) instead of duplicating. On a missing table there is nothing to
            # fence against, so create it via a plain append (matches dbt's first-run create).
            if engine.table_exists(path, so):
                engine.append_if_unchanged(
                    path,
                    self._df.relation,
                    read_version=engine.table_version(path, so),
                    partition_by=self._partition_by,
                    merge_schema=self._merge_schema,
                    storage_options=so,
                    compaction_threshold=session.compaction_threshold,
                )
            else:
                engine.write_delta(
                    path,
                    self._df.relation,
                    mode="append",
                    partition_by=self._partition_by,
                    merge_schema=self._merge_schema,
                    storage_options=so,
                    compaction_threshold=session.compaction_threshold,
                )
        else:
            engine.write_delta(
                path,
                self._df.relation,
                mode=mode,
                partition_by=self._partition_by,
                merge_schema=self._merge_schema,
                overwrite_schema=self._overwrite_schema,
                storage_options=so,
                compaction_threshold=session.compaction_threshold,
            )

    def save(self, path: str) -> str:
        """``df.write.save(path)`` — write to a Delta table by PATH, not catalog name.

        Storage-neutral (local / s3:// / gs:// / az:// / abfss://). Unlike :meth:`saveAsTable`,
        the result is addressed only by ``path`` — there is no schema.table name to register a
        view for — so it is read back with ``conn.read.format("delta").load(path)`` / ``delta_scan('<path>')``,
        not as an unqualified table. Returns ``path``."""
        self._write(path, f"delta table at '{path}'")
        return path

    def saveAsTable(self, name: str) -> str:
        session = self._df.session
        catalog, schema, table = session._resolve(name)
        path = session._table_path(schema, table, catalog)
        self._write(path, f"table '{catalog}.{schema}.{table}'",
                    session._catalog_storage_options(catalog), catalog)
        # Surface the (new or grown) table immediately — no manual refresh() needed.
        session.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(catalog)}.{_qid(schema)}")
        session._register_view(catalog, schema, table)
        # Re-apply USE: on a previously-empty warehouse the schema didn't exist at connect, so the
        # USE silently no-op'd; now that it exists, unqualified names must resolve.
        session._use(session._current_catalog, session._current_database)
        return table

    def insertInto(self, name: str, overwrite: bool = False) -> str:
        """``df.write.insertInto(name)`` — append into an **existing** Delta table by catalog name
        (Spark's insertInto verb). Like Spark, the target must already exist — this errors instead
        of creating it (use :meth:`saveAsTable` to create). ``overwrite=True`` replaces all rows.
        Columns are matched as delta-rs appends them (by name); the configured ``mode()`` is ignored
        in favour of the insert/overwrite semantics."""
        session = self._df.session
        catalog, schema, table = session._resolve(name)
        path = session._table_path(schema, table, catalog)
        if not engine.table_exists(path, session._catalog_storage_options(catalog)):
            raise ValueError(
                f"insertInto target '{catalog}.{schema}.{table}' does not exist; create it first with "
                f"df.write.saveAsTable('{name}')."
            )
        self._mode = "overwrite" if overwrite else "append"
        return self.saveAsTable(name)


class Catalog:
    """A small ``Catalog`` over the discovered schemas/views."""

    def __init__(self, session: DuckSession):
        self.session = session

    _SKIP_SCHEMAS = {"information_schema", "pg_catalog", "main"}

    def listDatabases(self) -> List[str]:
        rows = self.session.con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = ? ORDER BY schema_name",
            [self.session._current_catalog],
        ).fetchall()
        return [r[0] for r in rows if r[0] not in self._SKIP_SCHEMAS]

    def listTables(self, dbName: Optional[str] = None) -> List[str]:
        schema = dbName or self.session._current_database
        rows = self.session.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name",
            [self.session._current_catalog, schema],
        ).fetchall()
        return [r[0] for r in rows]

    def currentDatabase(self) -> str:
        return self.session._current_database

    def setCurrentDatabase(self, dbName: str):
        self.session._current_database = dbName
        self.session._use(self.session._current_catalog, dbName)

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        self.session.refresh(quiet=True)  # safe: reflect on-store truth, not stale views
        catalog, schema, table = self.session._resolve(tableName)
        if dbName is not None:
            schema = dbName
        rows = self.session.con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
            [catalog, schema, table],
        ).fetchall()
        return len(rows) > 0

    def databaseExists(self, dbName: str) -> bool:
        self.session.refresh(quiet=True)  # safe: re-discover schema folders first
        return dbName in self.listDatabases()

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[str]:
        self.session.refresh(quiet=True)
        catalog, schema, table = self.session._resolve(tableName)
        if dbName is not None:
            schema = dbName
        rows = self.session.con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [catalog, schema, table],
        ).fetchall()
        return [r[0] for r in rows]

    # ---- multi-catalog (each attached lakehouse root is a catalog) -------------------------

    def listCatalogs(self) -> List[str]:
        """The attached catalogs (the primary from ``connect`` plus any ``conn.attach``ed roots)."""
        return list(self.session._catalogs.keys())

    def currentCatalog(self) -> str:
        return self.session._current_catalog

    def setCurrentCatalog(self, catalogName: str):
        """Make ``catalogName`` the current catalog, so unqualified / 2-part names resolve in it. The
        current database becomes that catalog's ``dbo`` (or its first schema)."""
        sess = self.session
        if catalogName not in sess._catalogs:
            raise ValueError(
                f"unknown catalog '{catalogName}'; attached catalogs: {list(sess._catalogs)}."
            )
        sess._current_catalog = catalogName
        schema_filter = sess._catalogs[catalogName].schema_filter
        if schema_filter is not None:
            sess._current_database = schema_filter  # the catalog was attached pinned to one schema
        else:
            rows = sess.con.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE catalog_name = ? ORDER BY schema_name",
                [catalogName],
            ).fetchall()
            schemas = [r[0] for r in rows if r[0] not in self._SKIP_SCHEMAS]
            sess._current_database = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
        sess._use(catalogName, sess._current_database)


def connect(path: str, storage_options: Optional[Dict[str, str]] = None,
            schema: Optional[str] = None, compaction_threshold: int = 100,
            read_only: bool = True, name: Optional[str] = None) -> DuckSession:
    """Open a storage-neutral, DataFrame-style session over a Delta lakehouse.

    The session binds to this one lakehouse root as the primary catalog. Catalog is first-class:
    tables are addressed ``catalog.schema.table`` (``schema.table`` / ``table`` resolve in the current
    catalog), so single- and multi-catalog sessions share one code path. Attach more lakehouses with
    :meth:`DuckSession.attach` to query across them.

    Args:
        path: the lakehouse root, or (OneLake) ``…/Tables`` or ``…/Tables/<schema>``. Works with a
            local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``.
        storage_options: forwarded to delta-rs (and used to mint DuckDB secrets). For OneLake you
            can omit it inside a Fabric notebook — a token is acquired automatically.
        schema: restrict to a single schema. Omit to discover every schema folder.
        compaction_threshold: file-count threshold for post-append/merge compaction.
        read_only: **default True** — the session refuses every Delta write (saveAsTable / insertInto
            / save / merge / insert / update / delete / replaceWhere) with a ``PermissionError``, so
            an accidental write can't mutate a shared lakehouse. Pass ``read_only=False`` to enable
            writes. Reads and native DuckDB scratch (``CREATE TEMP``/``CREATE VIEW``) are unaffected.
        name: the primary catalog's name. When omitted it's derived from the URL (the OneLake
            lakehouse name, or the local folder name); when nothing can be derived (a GUID-only
            OneLake path) it falls back to ``"data"`` — a non-reserved word, so ``data.schema.table``
            works bare. Pass one to address the catalog explicitly as ``<name>.schema.table``.

    Example:
        >>> conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables")
        >>> conn.sql("SHOW TABLES").show()                          # primary catalog 'sales'
        >>> w = duckrun.connect("…/Tables/dbo", read_only=False)   # opt in to write
        >>> w.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")
        >>> lh = duckrun.connect("…/<guid>/Tables", name="lakehouse")   # name a GUID-path catalog
    """
    check_runtime_versions()  # fail loud if Fabric's stale duckdb/deltalake are still loaded
    return DuckSession(path, storage_options, schema, compaction_threshold, read_only, name)
