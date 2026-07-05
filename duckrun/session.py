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
from . import auth, sortkey
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

# _get_rle profiles a reservoir SAMPLE of the table, not the whole thing: key selection only needs to
# rank columns by cardinality/skew and test functional dependencies, all of which survive sampling.
# The sample size is a byte-budgeted, width-aware plan (``sortkey.plan_sample``): a byte budget over
# the table's real average row width (from the Delta log), so a wide table samples fewer rows and a
# narrow one more, and a table that fits the budget is profiled EXACTLY (no sampling at all). This
# turns the ~dozen profiling scans from full-table passes (minutes on a remote 142M-row table) into
# millisecond local-temp-table scans.
_READ_ONLY_MSG = (
    "catalog '{catalog}' is read-only — cannot {op}. duckrun opens read-only by default; enable "
    "Delta writes (saveAsTable / save / merge / insert / update / delete / replaceWhere) "
    "with connect(read_only=False) for the primary, or conn.attach(path, name='{catalog}', "
    "read_only=False) for an attached catalog."
)


def _unsupported_dml(query: str) -> Optional[str]:
    """An error message if ``query`` is a DML form duckrun can't route to delta_rs, else None."""
    s = _strip_leading(query)
    low = s.lower()
    if low.startswith("update") and delta_dml._find_top_level(s, _TOP_FROM) != -1:
        return _UPDATE_FROM_MSG
    if low.startswith("delete") and delta_dml._find_top_level(s, _TOP_USING) != -1:
        return _DELETE_USING_MSG
    if re.match(r"(insert|update|delete|merge|create|alter|drop)\b", low) and _is_multi_statement(s):
        return _MULTI_MSG
    return None


def _is_multi_statement(s: str) -> bool:
    """True if ``s`` holds more than one statement (a top-level ``;`` with anything after it).

    Skips ``$tag$…$tag$`` dollar-quoted bodies (reusing delta_dml's scanner) so a ``;`` inside a
    dollar-quoted literal isn't mistaken for a statement separator — matching the other scanners."""
    depth, quote, i, n = 0, None, 0, len(s)
    while i < n:
        ch = s[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
        elif ch == "$":
            de = delta_dml._dollar_quote_end(s, i)
            if de is not None:
                i = de
                continue
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == ";" and depth == 0 and s[i + 1:].strip():
            return True
        i += 1
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
    return rel.project(", ".join(f'{_qid(c)} AS {_qid(n)}' for c, n in zip(cur, names)))


def _project_cast(rel, types: List[str]):
    cur = rel.columns
    if len(types) != len(cur):
        raise ValueError(f"schema lists {len(types)} types but data has {len(cur)} columns")
    return rel.project(", ".join(f'CAST({_qid(c)} AS {t}) AS {_qid(c)}' for c, t in zip(cur, types)))


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


def _norm_exts(file_extensions: Optional[List[str]]) -> Optional[set]:
    """Normalise a file-extension filter to a lowercase set with leading dots (``csv`` → ``.csv``),
    or ``None`` for no filter."""
    if not file_extensions:
        return None
    return {("" if e.startswith(".") else ".") + e.lower() for e in file_extensions}


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


# Scoped-secret minting lives in ``secret`` so the dbt adapter and this session share it.
_secret_name = secret.scoped_secret_name
_mint_scoped_secret = secret.mint_scoped_secret


# Detect a 3-part cross-catalog DML target. The matcher lives in delta_dml so the native session
# and the dbt adapter's cursor wrapper share one implementation.
_dml_target_catalog = delta_dml.dml_target_catalog


class DuckSession:
    """A session handle bound to one or more Delta lakehouse roots, each surfaced as a catalog."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]],
                 schema: Optional[str], read_only: bool = True,
                 name: Optional[str] = None):
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
        try:
            self._attach_catalog(catalog_name, root, storage_options, schema_filter, primary=True, quiet=False)
        except Exception:
            # Don't leak the DuckDB connection opened above if discovery/secret-mint fails
            # (e.g. fail-loud primary token error) — the half-built session is discarded.
            self.con.close()
            raise

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
        except Exception as exc:
            # The primary catalog's secret is load-bearing: without it every delta_scan fails
            # later with a cryptic 403 / "no tables". Fail loud at connect() instead. Attached
            # (secondary) catalogs stay best-effort — a warning, so one bad attach doesn't sink
            # an otherwise-usable session.
            if primary:
                raise RuntimeError(
                    f"could not mint OneLake secret for catalog '{name}': {exc}"
                ) from exc
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
                if self._register_view(name, schema, table):
                    registered.append(f"{schema}.{table}")

        schemas = list(mapping.keys())
        if name == self._current_catalog and self._current_database is None:
            self._current_database = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
        if name == self._current_catalog:
            self._use(self._current_catalog, self._current_database)

        if not quiet:
            lh = root.rstrip("/").rsplit("/", 1)[-1]
            print(f"Connected to {lh} (catalog '{name}') — discovered {len(registered)} table(s)"
                  + (": " + ", ".join(registered) if registered else ""))
        return registered

    def _register_view(self, catalog: str, schema: str, table: str) -> bool:
        """Register a discovered table as a ``delta_scan`` view. Returns True if registered, False if
        the directory turned out not to be a Delta table (no ``_delta_log``) and was skipped — so
        ``connect()`` tolerates ANY root (a Files section, a mixed dir), not only a clean Tables root."""
        entry = self._catalogs[catalog]
        path = f"{entry.root_path}/{schema}/{table}"
        try:
            self.con.execute(
                f"CREATE OR REPLACE VIEW {_qid(catalog)}.{_qid(schema)}.{_qid(table)} AS "
                f"SELECT * FROM delta_scan('{_qlit(path)}')"
            )
            return True
        except Exception as exc:
            # delta_scan failed. On abfss, discovery lists directory NAMES (it can't cheaply tell a
            # Delta table from a plain folder), so a dir with no `_delta_log` is simply not a table —
            # skip it silently rather than aborting the whole connect. But a dir that HAS a `_delta_log`
            # and still won't read is a real failure (e.g. the delta-kernel #307 friendly-name bug) →
            # fail loud, keeping the engine error but dropping DuckDB's echo of the CREATE VIEW we
            # generated (`from None` so the noisy SQL echo doesn't reappear in tracebacks).
            if remote.is_abfss(entry.root_path) and not remote.has_delta_log(path, entry.storage_options):
                return False
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
        (append), ``delete``/``update`` (delta_rs delete/update), ``alter table … add column``,
        ``drop table`` (tombstone — marks the table dropped without deleting data; a human purges
        the files), and ``merge into … using … on … when …`` (delta_rs upsert). After a DML
        statement the catalog is refreshed.

        A SQL ``merge`` must reference the literal ``target`` and ``source`` aliases in the ``ON``
        condition and ``WHEN`` clauses (``merge into t using s on target.id = source.id when matched
        then update set * when not matched then insert *``) — duckrun renames the merge relations to
        those names. It mirrors the DataFrame ``DeltaTable.forName(conn, name).merge(...)`` builder.
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
        # source_table + pristine: this frame IS the Delta table unchanged, so .optimize() can rewrite
        # it and the writer can catch a self-overwrite. .sort() keeps source_table but clears pristine.
        return DataFrame(
            self.con.sql(f"SELECT * FROM {_qid(catalog)}.{_qid(schema)}.{_qid(table)}"), self,
            source_table=name, pristine=True)

    def createDataFrame(self, data, schema=None) -> "DataFrame":
        """Create a :class:`DataFrame` from in-memory data — the ``createDataFrame`` API.

        ``data`` is a list of tuples/lists (a list of scalars is treated as a single column), a
        pandas ``DataFrame``, or a pyarrow ``Table`` / ``RecordBatchReader``. ``schema`` is
        ``None`` (names inferred — ``_1, _2, …`` for tuples, the frame's own names otherwise), a
        list of column names, or a DDL string (``"id int, name string"``; ``:`` between name and
        type is also accepted). The data is materialised as a relation on duckrun's own DuckDB
        connection — write it to Delta with ``df.write.saveAsTable(...)``.
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

    # ---- file transfer (OneLake Files / any store) -----------------------------------------

    def _files_base(self, remote_folder: str) -> str:
        """Resolve ``remote_folder`` to an absolute store URL. A full URL (``…://``) is used as-is.
        Otherwise it is relative to the current catalog's store: for a OneLake lakehouse root ending
        in ``…/Tables`` the sibling ``…/Files`` section is used (Tables holds Delta tables, Files holds
        loose files); for every other store the catalog root itself is the base."""
        rf = remote_folder.replace("\\", "/").strip("/")
        if "://" in remote_folder:
            return remote_folder.replace("\\", "/").rstrip("/")
        base = self.root_path.rstrip("/")
        if remote.is_abfss(base) and base.endswith("/Tables"):
            base = base[: -len("/Tables")] + "/Files"
        return f"{base}/{rf}" if rf else base

    def copy(self, local_folder: str, remote_folder: str,
             file_extensions: Optional[List[str]] = None, overwrite: bool = False) -> bool:
        """Upload every file under ``local_folder`` to ``remote_folder`` on the store, preserving the
        directory tree. Storage-neutral: files move via DuckDB's ``COPY (SELECT content FROM
        read_blob(local)) TO remote (FORMAT BLOB)`` over the secret ``connect()`` already minted — no
        extra auth, no obstore. ``remote_folder`` is relative to the lakehouse Files section on OneLake
        (or a full ``…://`` URL); ``file_extensions`` filters by suffix (``['.csv', '.parquet']``);
        ``overwrite=False`` (default) skips files already present remotely. Returns ``True`` on success.

        Each file is read whole into memory as one BLOB, so this suits ordinary files, not multi-GB blobs.
        """
        exts = _norm_exts(file_extensions)
        base = self._files_base(remote_folder)
        pairs = []
        for dirpath, _dirs, names in os.walk(local_folder):
            for n in names:
                if exts and os.path.splitext(n)[1].lower() not in exts:
                    continue
                local_path = os.path.join(dirpath, n)
                rel = os.path.relpath(local_path, local_folder).replace("\\", "/")
                pairs.append((local_path.replace("\\", "/"), f"{base}/{rel}"))
        if not pairs:
            print(f"⚠️  no files to upload from '{local_folder}'"
                  + (f" (filtered by {file_extensions})" if exts else ""))
            return True
        print(f"📁 Uploading {len(pairs)} file(s) to '{base}'...")
        for local_path, remote_path in pairs:
            if not overwrite and self._remote_exists(remote_path):
                print(f"  ⏭ exists: {remote_path}")
                continue
            if "://" not in remote_path:  # local target: object stores need no dirs, a local FS does
                os.makedirs(os.path.dirname(remote_path) or ".", exist_ok=True)
            # COMPRESSION 'none' is load-bearing: a file copy must be byte-verbatim. Without it,
            # COPY TO re-encodes by the destination extension (a '.gz'/'.zst' target would re-compress
            # an already-compressed blob → double-compressed, corrupt).
            self.con.execute(
                f"COPY (SELECT content FROM read_blob('{_qlit(local_path)}')) "
                f"TO '{_qlit(remote_path)}' (FORMAT BLOB, COMPRESSION 'none')")
            print(f"  ✓ {local_path} → {remote_path}")
        print("✅ upload complete")
        return True

    def download(self, remote_folder: str = "", local_folder: str = "./downloaded_files",
                 file_extensions: Optional[List[str]] = None, overwrite: bool = False) -> bool:
        """Download every file under ``remote_folder`` to ``local_folder``, preserving the directory
        tree. The mirror of :meth:`copy`: remote files are enumerated with ``glob`` and pulled via
        ``COPY (SELECT content FROM read_blob(remote)) TO local (FORMAT BLOB)`` over the existing secret.
        ``remote_folder`` is relative to the OneLake Files section (or a full ``…://`` URL);
        ``file_extensions`` filters by suffix; ``overwrite=False`` (default) skips files already present
        locally. Returns ``True`` on success."""
        base = self._files_base(remote_folder)
        pairs = [(fp, os.path.join(local_folder, *rel.split("/")))
                 for fp, rel in self._enumerate_remote(base, _norm_exts(file_extensions))]
        if not pairs:
            print(f"⚠️  no files to download from '{base}'"
                  + (f" (filtered by {file_extensions})" if file_extensions else ""))
            return True
        print(f"📁 Downloading {len(pairs)} file(s) to '{local_folder}'...")
        for remote_path, local_path in pairs:
            if not overwrite and os.path.exists(local_path):
                print(f"  ⏭ exists: {local_path}")
                continue
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            # COMPRESSION 'none' → byte-verbatim (see copy(): a '.gz'/'.zst' target must not re-compress).
            self.con.execute(
                f"COPY (SELECT content FROM read_blob('{_qlit(remote_path)}')) "
                f"TO '{_qlit(local_path.replace(chr(92), '/'))}' (FORMAT BLOB, COMPRESSION 'none')")
            print(f"  ✓ {remote_path} → {local_path}")
        print("✅ download complete")
        return True

    def list_files(self, remote_folder: str = "",
                   file_extensions: Optional[List[str]] = None) -> List[str]:
        """List the files under ``remote_folder`` on the store, as **relative path strings** (e.g.
        ``['csv/daily/a.csv.gz', 'csv/log.parquet']``) — the companion to :meth:`copy`/:meth:`download`
        (the returned paths feed straight back into ``download``). ``remote_folder`` is relative to the
        OneLake Files section (or a full ``…://`` URL); ``file_extensions`` filters by suffix. Recurses.
        Storage-neutral: abfss via the DFS REST API (DuckDB can't glob OneLake, duckdb-azure#174),
        local/s3/gcs/az via ``glob``."""
        base = self._files_base(remote_folder)
        return [rel for _fp, rel in self._enumerate_remote(base, _norm_exts(file_extensions))]

    def get_stats(self, source: Optional[str] = None, detailed: bool = False) -> "DataFrame":
        """Delta table statistics — the "why is my table slow / full of small files" view. Returns a
        :class:`DataFrame`, one row per table (``detailed=False``) or one row per parquet **row group**
        (``detailed=True``, the raw ``parquet_metadata`` columns).

        ``source``: ``None`` → every table in the current schema; a table name (1/2/3-part) → that
        table; a schema name → every table in it; a wildcard pattern (``fct_*``, ``mart.fct_*``) →
        every matching table in the current catalog. Aggregated columns: ``catalog, schema, table,
        total_rows, num_files, num_row_groups, avg_row_group, size_mb, vorder, compression``. Reads the
        Delta log for the active file list + size + VORDER, then the parquet footers for row-group
        shape — so it counts only live files (tombstoned ones are excluded)."""
        targets = self._resolve_stats_targets(source)
        if not targets:
            raise ValueError(
                f"get_stats: nothing to describe for source={source!r} "
                f"(catalog '{self._current_catalog}', schema '{self._current_database}').")
        parts = []
        for cat, sch, tbl in targets:
            entry = self._catalogs[cat]
            files, size_bytes, vorder = engine.delta_file_summary(
                self.con, f"{entry.root_path}/{sch}/{tbl}", entry.storage_options)
            pre = (f"'{_qlit(cat)}' AS catalog, '{_qlit(sch)}' AS schema, "
                   f"'{_qlit(tbl)}' AS \"table\"")
            if not files:
                if not detailed:
                    parts.append(f"SELECT {pre}, 0 AS total_rows, 0 AS num_files, 0 AS num_row_groups, "
                                 f"NULL::DOUBLE AS avg_row_group, 0.0 AS size_mb, {str(vorder).lower()} "
                                 f"AS vorder, 'EMPTY' AS compression")
                continue
            lit = "[" + ", ".join(f"'{_qlit(f)}'" for f in files) + "]"
            if detailed:
                parts.append(f"SELECT {pre}, m.* FROM parquet_metadata({lit}) m")
            else:
                parts.append(
                    f"SELECT {pre}, "
                    f"SUM(fm.num_rows) AS total_rows, COUNT(*) AS num_files, "
                    f"SUM(fm.num_row_groups) AS num_row_groups, "
                    f"ROUND(SUM(fm.num_rows)::DOUBLE / NULLIF(SUM(fm.num_row_groups), 0), 1) AS avg_row_group, "
                    f"ROUND({size_bytes} / 1048576.0, 2) AS size_mb, {str(vorder).lower()} AS vorder, "
                    f"(SELECT COALESCE(STRING_AGG(DISTINCT compression, ', '), 'UNCOMPRESSED') "
                    f"FROM parquet_metadata({lit})) AS compression "
                    f"FROM parquet_file_metadata({lit}) fm")
        if not parts:
            raise ValueError(f"get_stats: no files to describe for source={source!r}.")
        return DataFrame(self.con.sql(" UNION ALL ".join(parts)), self)


    def _resolve_stats_targets(self, source: Optional[str]) -> List[tuple]:
        """Resolve a ``get_stats`` source to ``(catalog, schema, table)`` targets: ``None`` → every
        table in the current schema; a known table (1/2/3-part) → itself; a schema name → its tables;
        a wildcard pattern (``*``/``?``/``[...]``, e.g. ``fct_*`` or ``mart.fct_*``) → every matching
        table in the current catalog."""
        if source is None:
            db = self._current_database
            return [(self._current_catalog, db, t) for t in self.catalog.listTables(db)]
        if any(ch in source for ch in "*?["):
            return self._glob_stats_targets(source)
        if self.catalog.tableExists(source):
            return [self._resolve(source)]
        if "." not in source and self.catalog.databaseExists(source):
            return [(self._current_catalog, source, t) for t in self.catalog.listTables(source)]
        raise ValueError(
            f"get_stats: '{source}' is neither a known table nor a schema in catalog "
            f"'{self._current_catalog}'.")

    def _glob_stats_targets(self, source: str) -> List[tuple]:
        """Wildcard-match ``source`` (fnmatch ``*``/``?``/``[...]``, case-insensitive) against tables in
        the current catalog. Accepts ``table``, ``schema.table``, or ``catalog.schema.table`` patterns;
        a missing leading segment defaults to ``*`` (schema) / the current catalog. Returns the matched
        ``(catalog, schema, table)`` targets (possibly empty — the caller reports the miss)."""
        import fnmatch
        parts = source.split(".")
        if len(parts) == 1:
            cat_pat, schema_pat, table_pat = self._current_catalog, "*", parts[0]
        elif len(parts) == 2:
            cat_pat, schema_pat, table_pat = self._current_catalog, parts[0], parts[1]
        elif len(parts) == 3:
            cat_pat, schema_pat, table_pat = parts
        else:
            raise ValueError(
                f"get_stats: too many segments in pattern {source!r} "
                f"(use table, schema.table, or catalog.schema.table).")
        cat = self._current_catalog
        if not fnmatch.fnmatchcase(cat.lower(), cat_pat.lower()):
            return []
        sp, tp = schema_pat.lower(), table_pat.lower()
        out = []
        for sch in self.catalog.listDatabases():
            if not fnmatch.fnmatchcase(sch.lower(), sp):
                continue
            for tbl in self.catalog.listTables(sch):
                if fnmatch.fnmatchcase(tbl.lower(), tp):
                    out.append((cat, sch, tbl))
        return out

    def _get_rle(self, table: str, sort_key_cap: int = 4, min_gain_pct: float = 1.0,
                 key_sort_below_pct: float = 10.0, null_excl: float = 0.5,
                 fd_band: float = 0.12, grain_frac: float = 0.5,
                 seed: Optional[int] = None) -> "DataFrame":
        """EXPERIMENTAL / PRIVATE — parked, not part of the public API. Recommend a short Delta **sort
        key** that minimises a table's estimated **in-memory columnar** footprint, and
        return a per-column :class:`DataFrame`. Recommendation-only — it never rewrites the table.

        Profiled on a reservoir SAMPLE (materialised once into a temp table) rather than the whole
        table: the key model only ranks columns by cardinality/skew and tests functional
        dependencies, which survive sampling, and this keeps the ~dozen profiling scans off the
        (possibly remote) full table. The sample size is a byte-budgeted, width-aware plan
        (``sortkey.plan_sample`` over the Delta log's real average row width), so a table that fits
        the byte budget is profiled EXACTLY (no ``USING SAMPLE`` at all) and larger ones sample fewer
        rows the wider they are. ``ndv``/``skew``/``current_runs`` are SAMPLE estimates otherwise.
        Uniqueness is only asserted when the profile was exact — a sample can't tell a unique key from
        a merely higher-than-sample-cardinality column. ``seed`` makes the reservoir sample
        reproducible (``REPEATABLE``): two runs on the same table return byte-identical rows.

        The target is an in-memory columnar encoding, **not** parquet-on-disk: each column is value- or hash/dictionary-encoded
        (indices bit-packed at ``ceil(log2 ndv)`` bits) and then RLE is kept only when it beats the
        bit-packed form. There is **no general-purpose byte compressor** (no ZSTD), so a column that is
        not part of the sort key can only shrink through RLE, and for a column left in ~arbitrary order
        its runs are governed by value **skew**: ``E[runs] ≈ N·(1 − Σ p_v²)`` where ``Σ p_v²`` is the
        Simpson/Herfindahl index of the value histogram. Uniform columns (``Σp_v² ≈ 1/ndv``) get ≈N runs
        and fall back to bit-packing; skewed columns RLE well in any order. The target encoding itself only benefits from sorting by
        a short prefix, so this picks **1..sort_key_cap** columns from the eligible **dimensions/keys** —
        a **measure** (DECIMAL/FLOAT/DOUBLE — a continuous value you aggregate, never filter or sort on)
        is excluded, and a costly one is flagged to shrink by cutting precision / splitting instead. The
        chosen dimensions are ordered by **ascending cardinality** (the classic rule, which also respects
        natural hierarchies — a coarse ``date`` leads the finer ``time`` nested within it rather than being
        stranded behind it); columns are added while each still compresses at its position and dropped once
        the prefix reaches the table's grain (marginal gain below ``min_gain_pct``). High-cardinality hash
        columns whose cost is dominated by their dictionary are flagged — the sort key can't shrink those,
        only cutting cardinality can.

        **Key-organized tables** (a dimension, or a table at its grain) are handled specially: if a
        (near-)unique key column exists and the best compression sort saves less than ``key_sort_below_pct``
        percent, the recommendation is ``ORDER BY <key>`` — the canonical join / segment-locality layout —
        rather than the marginal compression sort, because a unique key leaves nothing for RLE to group so
        compression is already at its floor. The compression alternative is still printed. A genuine unique
        key is never flagged "cut cardinality" (you can't shrink a key).

        One row per column: ``table, in_sort_key, sort_position, column, data_type, encoding, ndv,
        skew_pct`` (``100·Σp_v²``) ``, current_runs, est_kb_current, est_kb_sorted, saved_pct``. Also
        prints the recommended ``ORDER BY`` and the estimated size before/after. **Single table only** —
        a schema name / ``None`` raises."""
        if not isinstance(table, str) or not table.strip():
            raise ValueError("_get_rle is single-table; pass one table name.")
        if not self.catalog.tableExists(table):
            if "." not in table and self.catalog.databaseExists(table):
                raise ValueError(f"_get_rle is single-table; '{table}' is a schema — pass one table.")
            raise ValueError(f"_get_rle: table '{table}' not found.")
        cat, sch, tbl = self._resolve(table)
        path = f"{self._catalogs[cat].root_path}/{sch}/{tbl}"
        plit = _qlit(path)
        # Partition columns lead the physical ORDER BY but are NOT compression-key candidates: Delta
        # strips them from the data files (zero RLE value), yet ordering by them first keeps ~one
        # delta-rs partition writer open at a time (less write memory). Discover them from the Delta
        # metadata; best-effort — an unreadable log just means "treat as unpartitioned". The same
        # add_actions carry each file's size_bytes; sum them here (in the SAME best-effort block, no
        # extra open) to size the profiling sample below by the table's real average row width.
        file_bytes = 0
        try:
            _dt = engine._delta_table(path, self._catalog_storage_options(cat))
            partition_cols = list(_dt.metadata().partition_columns or [])
            add_actions = _dt.get_add_actions(flatten=True)  # noqa: F841 - DuckDB replacement scan
            file_bytes = int(self.con.sql(
                "select coalesce(sum(size_bytes), 0)::bigint from add_actions").fetchone()[0] or 0)
        except Exception:
            partition_cols = []
        desc = self.con.sql(f"DESCRIBE SELECT * FROM delta_scan('{plit}')").fetchall()
        cols = [r[0] for r in desc]
        types = {r[0]: r[1] for r in desc}
        partition_cols = [c for c in partition_cols if c in cols]
        if not cols:
            raise ValueError(f"_get_rle: table '{table}' has no columns.")

        # Delta-LOG column stats (no data scan): the sample gives ndv/skew but not each column's null
        # share, so a mostly-null column can wrongly win a scarce sort-key slot. get_add_actions carries
        # per-file null_count/min/max — sum them once here and hand the profiler a per-column null_frac
        # (and constancy, reserved). Best-effort: an unreadable/statless log just yields {} and the
        # profiler runs exactly as before. total_rows also sizes the sample below.
        stats, _, total_rows = engine.delta_column_stats(
            self.con, path, cols, types, self._catalog_storage_options(cat))
        # Byte-budgeted, width-aware sample plan. avg_row_bytes = real on-disk bytes/row × a
        # decompression factor (the in-memory form is larger than parquet); fall back to a schema
        # width estimate when the log gave nothing. plan_sample returns the row count AND whether the
        # table fits the budget — if it does, profile it EXACTLY (no USING SAMPLE) so small tables
        # stay exact and uniqueness can be trusted.
        avg_row_bytes = ((file_bytes / total_rows) * sortkey._DECOMPRESSION_FACTOR
                         if file_bytes and total_rows else sortkey.estimate_row_bytes(types))
        plan_rows, exact = sortkey.plan_sample(total_rows or None, avg_row_bytes)
        src = "_rle_src"
        if exact:
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {src} AS SELECT * FROM delta_scan('{plit}')")
        else:
            samp = (f"reservoir({plan_rows} ROWS) REPEATABLE ({int(seed)})"
                    if seed is not None else f"{plan_rows} ROWS")
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {src} AS "
                f"SELECT * FROM delta_scan('{plit}') USING SAMPLE {samp}")
        try:
            rows, schema, lines = sortkey.recommend_sort_key(
                self.con, sch, tbl, src, cols, types, partition_cols,
                sort_key_cap=sort_key_cap, min_gain_pct=min_gain_pct,
                key_sort_below_pct=key_sort_below_pct, stats=stats, null_excl=null_excl,
                fd_band=fd_band, grain_frac=grain_frac, sample_rows=plan_rows, exact=exact)
        finally:
            self.con.execute(f"DROP TABLE IF EXISTS {src}")
        for line in lines:   # the module is pure; the caller prints the advisory
            print(line)
        return self.createDataFrame(rows, schema)

    def _auto_sort_cols(self, relation, seed: Optional[int] = None) -> List[str]:
        """Run the sort-key recommender over an arbitrary relation (no Delta table behind it, so no
        partitions and no log stats) and return the recommended ORDER BY columns, or ``[]`` if nothing
        pays off. Backs the no-arg ``DataFrame.sort()``. Profiles a bounded reservoir sample, not the
        whole relation — sized by a schema-only width estimate (no Delta log to read real bytes), and
        NEVER exact (a relation's row count is unknown without evaluating it, so uniqueness is never
        claimed here). ``seed`` makes the reservoir sample reproducible."""
        cols = list(relation.columns)
        types = {n: str(t) for n, t in zip(relation.columns, relation.types)}
        if not cols:
            return []
        # No Delta log → estimate the in-memory row width from the schema alone; total_rows unknown
        # (None) → plan_sample never returns exact, so this path always samples.
        plan_rows, exact = sortkey.plan_sample(None, sortkey.estimate_row_bytes(types))
        samp = (f"reservoir({plan_rows} ROWS) REPEATABLE ({int(seed)})"
                if seed is not None else f"{plan_rows} ROWS")
        view, src = "_rle_relation_src", "_rle_src"
        relation.create_view(view, replace=True)
        try:
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {src} AS "
                f"SELECT * FROM {view} USING SAMPLE {samp}")
            rows, _, lines = sortkey.recommend_sort_key(
                self.con, "df", "sort()", src, cols, types, [], sample_rows=plan_rows, exact=exact)
        finally:
            self.con.execute(f"DROP TABLE IF EXISTS {src}")
            self.con.execute(f"DROP VIEW IF EXISTS {view}")
        # The no-arg sort() path prints ONLY the ORDER BY line (not the full advisory block).
        for line in lines:
            if line.strip().startswith("ORDER BY"):
                print(line)
        # rows follow sortkey._SCHEMA: [1]=in_sort_key, [2]=sort_position, [3]=column.
        return [r[3] for r in sorted((x for x in rows if x[1]), key=lambda x: x[2])]

    def _auto_sort_cols_from_table(self, source_table: str, seed: Optional[int] = None) -> List[str]:
        """No-arg ``df.sort()`` key for a frame that carries a source TABLE: profile it via ``_get_rle``,
        which sizes its sample from the Delta **log**'s real row width (exact when the table fits the
        byte budget) rather than a schema-only estimate — the same profiler the sort rewrite uses.
        Returns the recommended ORDER BY columns, or ``[]`` if nothing pays off."""
        prof = self._get_rle(source_table, seed=seed)
        recs = [dict(zip(prof.columns, row)) for row in prof.collect()]
        return [r["column"] for r in sorted((x for x in recs if x["in_sort_key"]),
                                            key=lambda x: x["sort_position"])]

    def _enumerate_remote(self, base: str, exts) -> List[tuple]:
        """Enumerate files recursively under the resolved store URL ``base``, as ``(full_path,
        relative_path)`` pairs honouring the extension filter. Shared by ``list_files``/``download``."""
        if remote.is_abfss(base):
            raw = remote.list_files(base, self.storage_options)
        else:
            raw = [r[0] for r in self.con.execute(f"SELECT file FROM glob('{_qlit(base)}/**')").fetchall()]
        out = []
        for p in raw:
            fp = p.replace("\\", "/")
            if exts and os.path.splitext(fp)[1].lower() not in exts:
                continue
            rel = fp[len(base):].lstrip("/") if fp.startswith(base) else os.path.basename(fp)
            out.append((fp, rel))
        return out

    def _remote_exists(self, path: str) -> bool:
        """True if a single file exists at ``path``. OneLake can't be globbed (duckdb-azure#174), so
        abfss uses a REST HEAD; local/s3/gcs/az use ``glob`` on the exact path."""
        if remote.is_abfss(path):
            return remote.file_exists(path, self.storage_options)
        return bool(self.con.execute(f"SELECT 1 FROM glob('{_qlit(path)}') LIMIT 1").fetchall())

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    def stop(self):
        """Close the underlying DuckDB connection (Spark's ``SparkSession.stop()``). The session is
        unusable afterwards — registered views and the minted secret go with the connection."""
        self.con.close()

    def __enter__(self) -> "DuckSession":
        return self

    def __exit__(self, exc_type, exc, tb):
        """Close the connection on ``with`` exit — ``with duckrun.connect(...) as conn:``."""
        self.stop()
        return False

    @property
    def _connection(self):
        """The underlying DuckDB connection (internal escape hatch)."""
        return self.con


class StructField:
    """One column of a :class:`StructType`. Mirrors Spark's ``StructField`` surface (``name``,
    ``dataType``, ``nullable``); ``dataType`` is the **DuckDB** type as a string — duckrun is
    DuckDB-native and doesn't remap to Spark type objects (same stance as ``df.dtypes``)."""

    def __init__(self, name: str, dataType: str, nullable: bool = True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable

    def simpleString(self) -> str:
        return f"{self.name}:{self.dataType}"

    def __repr__(self) -> str:
        return f"StructField('{self.name}', '{self.dataType}', {self.nullable})"


class StructType:
    """A :class:`DataFrame`'s schema — a list of :class:`StructField`, built from the DuckDB
    relation's columns and types. Mirrors Spark's ``StructType`` surface (``fields``, ``names``,
    iteration, ``simpleString()``)."""

    def __init__(self, fields: List[StructField]):
        self.fields = list(fields)

    @property
    def names(self) -> List[str]:
        return [f.name for f in self.fields]

    def __iter__(self):
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def simpleString(self) -> str:
        return f"struct<{','.join(f.simpleString() for f in self.fields)}>"

    def treeString(self) -> str:
        lines = ["root"]
        for f in self.fields:
            lines.append(f" |-- {f.name}: {f.dataType} (nullable = {str(f.nullable).lower()})")
        return "\n".join(lines) + "\n"

    def __repr__(self) -> str:
        return f"StructType([{', '.join(repr(f) for f in self.fields)}])"


class DataFrame:
    """Wraps a DuckDB relation; exposes a DataFrame-style ``.write`` plus a few DataFrame aliases.

    Anything not defined here falls through to the underlying DuckDB relation, so ``.df()``,
    ``.arrow()``, ``.fetchall()``, ``.fetchnumpy()`` etc. all keep working.
    """

    def __init__(self, relation, session: DuckSession, source_table: Optional[str] = None,
                 pristine: bool = False):
        self.relation = relation
        self.session = session
        # Lineage back to a catalog table, in two fields:
        #  _source_table: the Delta table this frame reads from — set by conn.table(name) and PRESERVED
        #    through a projection like .sort() (a sorted frame still reads table X). None for a
        #    createDataFrame / reader / conn.sql() frame with no single source. The self-overwrite guard
        #    and the no-arg sort() profiler key off this.
        #  _pristine: True only while the relation IS the table unchanged (straight conn.table(name));
        #    .sort() clears it (the rows are reordered/projected). .optimize() requires BOTH — a rewrite
        #    key is chosen by profiling the table, not implied by a frame's current order.
        self._source_table = source_table
        self._pristine = pristine and source_table is not None

    @property
    def write(self) -> "DataFrameWriter":
        return DataFrameWriter(self)

    def optimize(self, *keys: str, rewrite: bool = False, where: Optional[str] = None,
                 analyze: bool = False, seed: Optional[int] = None):
        """Maintain this table. Only available on ``conn.table(name)`` — a derived/query DataFrame has
        no Delta table to touch.

        **The safe button** — ``optimize()`` (no arguments) compacts small files and vacuums; it
        **never rewrites data**. It applies a byte trigger (bin-pack only partitions carrying real
        small-file debt), commits ``dataChange=false``, and is idempotent — a clean table is a
        ``noop``. Safe to run on a schedule and safe under concurrent writers.

        **The deliberate rewrite** — pass a key, ``rewrite=True``, or ``where`` to sort-rewrite the
        table physically ordered for compression / read pruning (returns the REAL measured on-disk
        ``sizeBytesBefore`` / ``sizeBytesAfter`` / ``savedPct``). This is a full rewrite that commits
        ``dataChange=true``, so run it occasionally, not on every load:

        - ``optimize(rewrite=True)`` — full rewrite; the sort key is auto-picked by profiling.
        - ``optimize("region", "status")`` — full rewrite sorted by exactly those columns.
        - ``optimize("region", where="year = 2026")`` — rewrite ONLY the partitions matching the
          predicate (a CAST-free delta_rs SQL expression), as one atomic snapshot-fenced commit.

        **Advisory** — ``optimize(analyze=True)`` returns the sort-key recommendation as a DataFrame
        (the profiler promoted to public) and prints the small-file debt, and commits nothing.

        ``seed`` makes the profiling sample reproducible (the auto-key paths — ``analyze=True`` and
        the auto-picked ``rewrite=True``); two runs with the same seed pick the same key.

        Partition columns always lead the physical order and are preserved."""
        if self._source_table is None:
            raise ValueError(
                "optimize() is only available on conn.table(name); a derived/query DataFrame has no "
                "Delta table to optimize.")
        if not self._pristine:
            raise ValueError(
                f"optimize() on a sorted frame is refused — sorting a frame does not choose the "
                f"rewrite key. Use conn.table('{self._source_table}').optimize('<col>', ...) to "
                f"rewrite in place, or write the sorted frame to a new table.")
        from .delta_table import DeltaTable
        dt = DeltaTable.forName(self.session, self._source_table)
        if analyze:
            if keys or where or rewrite:
                raise ValueError(
                    "optimize(analyze=True) is advisory-only and commits nothing — drop the "
                    "keys / rewrite / where.")
            return dt._analyze(seed=seed)
        if keys or where or rewrite:
            return dt._sort_rewrite(keys=list(keys) or None, where=where, seed=seed)
        return dt._maintain()

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

    def first(self):
        """First row as a tuple, or ``None`` if empty (Spark's ``DataFrame.first()``)."""
        return self.relation.limit(1).fetchone()

    def head(self, n=None):
        """``head()`` → the first row (or ``None``); ``head(n)`` → a list of the first ``n`` rows
        (Spark's ``DataFrame.head([n])``)."""
        if n is None:
            return self.relation.limit(1).fetchone()
        return self.relation.limit(n).fetchall()

    def take(self, n: int):
        """The first ``n`` rows as a list (Spark's ``DataFrame.take(n)``)."""
        return self.relation.limit(n).fetchall()

    def isEmpty(self) -> bool:
        """``True`` if the DataFrame has no rows (Spark's ``DataFrame.isEmpty()``)."""
        return self.relation.limit(1).fetchone() is None

    def sort(self, *cols, ascending=None, seed: Optional[int] = None) -> "DataFrame":
        """A new DataFrame globally sorted by ``cols`` (Spark's ``DataFrame.sort``; ``orderBy`` is its
        alias). ``cols`` are column names (or a single list of them); ``ascending`` is a bool or a
        list of bools matching ``cols`` (default all ascending). This is a native DuckDB ``ORDER BY``,
        so writing the result lands physically sorted Delta files — nothing sort-specific in the write
        path. Without this, ``df.sort(...)`` fell through to the bare DuckDB relation and lost ``.write``.

        The no-arg ``df.sort()`` auto-picks a run-length-friendly key by profiling; ``seed`` makes
        that profiling sample reproducible."""
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = tuple(cols[0])
        if not cols:
            # No-arg auto-key: profile the SOURCE TABLE via its Delta log (real row width, exact when
            # the table fits the byte budget) when this frame carries one; otherwise a schema-only
            # estimate over the bare relation. Needs source_table only — a sorted frame still profiles
            # its table.
            names = (self.session._auto_sort_cols_from_table(self._source_table, seed=seed)
                     if self._source_table is not None
                     else self.session._auto_sort_cols(self.relation, seed=seed))
            if not names:
                return self  # profiler found no key worth sorting by — leave order untouched
            dirs = ["ASC"] * len(names)
        else:
            names = [str(c) for c in cols]
            if ascending is None:
                dirs = ["ASC"] * len(names)
            elif isinstance(ascending, (list, tuple)):
                if len(ascending) != len(names):
                    raise ValueError("ascending list length must match the number of sort columns.")
                dirs = ["ASC" if a else "DESC" for a in ascending]
            else:
                dirs = ["ASC" if ascending else "DESC"] * len(names)
        order_expr = ", ".join(f"{_qid(n)} {d}" for n, d in zip(names, dirs))
        # Keep the lineage (source_table) so a self-overwrite is caught, but drop pristine — the rows
        # are reordered now, so .optimize() must refuse (it profiles the table, not this order).
        return DataFrame(self.relation.order(order_expr), self.session,
                         source_table=self._source_table, pristine=False)

    orderBy = sort  # Spark: orderBy is an alias of sort

    @property
    def schema(self) -> StructType:
        """The schema as a :class:`StructType` of :class:`StructField` (Spark's ``DataFrame.schema``).
        Types are the DuckDB types (as in ``df.dtypes``); the relation doesn't carry nullability, so
        every field reports ``nullable=True`` — Spark's own default for an inferred schema."""
        rel = self.relation
        return StructType([StructField(n, str(t)) for n, t in zip(rel.columns, rel.types)])

    def printSchema(self) -> None:
        """Print the schema as a tree (Spark's ``DataFrame.printSchema``)."""
        print(self.schema.treeString(), end="")

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
        self._schema = None

    def format(self, fmt: str) -> "DataFrameReader":
        self._format = fmt.lower()
        return self

    def option(self, key: str, value) -> "DataFrameReader":
        self._options[key] = value
        return self

    def schema(self, schema) -> "DataFrameReader":
        """Supply an explicit read schema (Spark's ``read.schema``) — a DDL string
        (``"id int, name string"``) or a :class:`StructType`. Applies to ``csv`` / ``json``, where
        it both **names and types** the columns and turns off type sniffing (and skips the header
        row), matching Spark's override. ``delta`` / ``parquet`` carry their own schema, so setting
        one for them is rejected rather than silently ignored."""
        self._schema = schema
        return self

    def _columns_arg(self) -> str:
        """Render the stored schema as a DuckDB ``columns={'n': 'TYPE', …}`` argument. A StructType
        maps field→type directly; a DDL string is parsed robustly by letting DuckDB build a throwaway
        temp table and reading back its column names and types (handles ``DECIMAL(10,2)``, nested
        types, etc. that naive comma-splitting would break)."""
        s = self._schema
        if isinstance(s, StructType):
            pairs = [(f.name, f.dataType) for f in s.fields]
        elif isinstance(s, str):
            con = self.session.con
            tmp = "__duckrun_schema_probe"
            con.execute(f'create or replace temp table "{tmp}" ({s})')
            try:
                rel = con.sql(f'select * from "{tmp}" limit 0')
                pairs = list(zip(rel.columns, [str(t) for t in rel.types]))
            finally:
                con.execute(f'drop table if exists "{tmp}"')
        else:
            raise ValueError("read.schema(...) must be a DDL string or a StructType.")
        cols = ", ".join(f"'{_qlit(n)}': '{_qlit(t)}'" for n, t in pairs)
        return f"columns={{{cols}}}"

    def load(self, path: str) -> DataFrame:
        fmt = self._format
        if self._schema is not None and fmt in ("delta", "parquet"):
            raise ValueError(
                f"read.schema(...) applies to csv/json only; {fmt} carries its own schema."
            )
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
        elif fmt == "json":
            if self._schema is not None:
                scan = f"read_json('{_qlit(path)}', {self._columns_arg()})"
            else:
                scan = f"read_json_auto('{_qlit(path)}')"
        elif fmt == "csv":
            opts = "".join(f", {k}={_csv_opt(v)}" for k, v in self._options.items())
            if self._schema is not None:
                scan = f"read_csv('{_qlit(path)}', {self._columns_arg()}{opts})"
            else:
                scan = f"read_csv_auto('{_qlit(path)}'{opts})"
        else:
            raise ValueError(f"Unsupported read format '{fmt}'. Use 'delta', 'parquet', 'json', or 'csv'.")
        return DataFrame(self.session.con.sql(f"SELECT * FROM {scan}"), self.session)

    def parquet(self, path: str) -> DataFrame:
        return self.format("parquet").load(path)

    def json(self, path: str) -> DataFrame:
        return self.format("json").load(path)

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

    ``append_if_unchanged`` / ``overwrite_if_unchanged`` are the fenced (compare-and-swap) siblings of
    the unfenced ``append`` / ``overwrite`` modes: they commit only if the table version has not moved
    since the call, else raise ``CommitFailedError``."""

    _MODES = {"overwrite", "append", "append_if_unchanged", "overwrite_if_unchanged",
              "ignore", "error", "errorifexists"}

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
        if m == "safeappend":
            raise ValueError("mode 'safeappend' was renamed — use mode('append_if_unchanged')")
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

    def _write(self, path: str, descr: str, so=None, catalog=None, target=None) -> None:
        """Apply the configured mode to the Delta table at ``path`` (storage-neutral). ``descr``
        names the target in the mode='error' message. ``so`` / ``catalog`` are the target catalog's
        storage_options / name (default to the current catalog's). ``target`` is the resolved
        ``(catalog, schema, table)`` for saveAsTable — used to refuse a self-overwrite; save(path)
        has no catalog name and passes None. Shared by saveAsTable and save."""
        session = self._df.session
        session._require_writable("write a Delta table", catalog)
        # Self-overwrite guard: a full overwrite of a table with a frame READ FROM that same table
        # (compare resolved names, not strings) is an unfenced read-modify-write — the classic
        # conn.table("t").sort(...).write.mode("overwrite").saveAsTable("t"). Only overwrite modes
        # clobber the whole table; append / *_if_unchanged / replaceWhere don't. optimize() is the
        # fenced, measured way to rewrite in place.
        if (target is not None and self._df._source_table is not None
                and self._replace_where is None
                and self._mode in ("overwrite", "overwrite_if_unchanged")
                and session._resolve(self._df._source_table) == target):
            raise ValueError(
                "overwriting a table with a sort/projection of itself is an unfenced rewrite — use "
                f"conn.table('{self._df._source_table}').optimize(...) which is snapshot-fenced and "
                "measured.")
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
                cur=session.con,
            )
            return

        mode = self._mode
        replacing_tombstone = False
        if mode in ("error", "errorifexists"):
            # A drop-tombstone (a table `drop table` marked deleted, data not yet purged) is ABSENT to
            # the SQL surface — `create table` recreates it. mode='error' must agree: only a LIVE table
            # blocks. Reuse the same is_dropped predicate discovery/SQL use, so both surfaces see one
            # truth. Recreating over the tombstone forces overwrite_schema (its marker column differs).
            exists = engine.table_exists(path, so)
            if exists and not delta_dml.is_dropped(session.con, path, so):
                raise ValueError(
                    f"{descr} already exists (mode='error'). "
                    f"Use mode('overwrite'), mode('append'), mode('append_if_unchanged'), or mode('ignore')."
                )
            replacing_tombstone = exists
            mode = "overwrite"

        if mode == "append_if_unchanged":
            # Optimistic append (the dbt append_if_unchanged strategy): pin to the version
            # now and CAS the commit, so a writer that lands between this read and the commit fails
            # the append (fail loud) instead of duplicating. On a missing table there is nothing to
            # fence against, so create it via a plain append (matches dbt's first-run create).
            if engine.table_exists(path, so):
                engine.append_if_unchanged(
                    path,
                    self._df.relation,
                    read_version=engine.table_version(path, so),
                    partition_by=self._partition_by,
                    merge_schema=self._merge_schema,
                    storage_options=so,
                    cur=session.con,
                )
            else:
                engine.write_delta(
                    path,
                    self._df.relation,
                    mode="append",
                    partition_by=self._partition_by,
                    merge_schema=self._merge_schema,
                    storage_options=so,
                    cur=session.con,
                )
        elif mode == "overwrite_if_unchanged":
            # Optimistic FULL overwrite (the overwrite sibling of append_if_unchanged): pin + CAS so
            # a concurrent write fails the overwrite instead of being clobbered. A missing table has
            # nothing to fence — create it via a plain overwrite.
            if engine.table_exists(path, so):
                engine.overwrite_if_unchanged(
                    path,
                    self._df.relation,
                    read_version=engine.table_version(path, so),
                    partition_by=self._partition_by,
                    overwrite_schema=self._overwrite_schema,
                    storage_options=so,
                )
            else:
                engine.write_delta(
                    path,
                    self._df.relation,
                    mode="overwrite",
                    partition_by=self._partition_by,
                    overwrite_schema=self._overwrite_schema,
                    storage_options=so,
                    cur=session.con,
                )
        else:
            engine.write_delta(
                path,
                self._df.relation,
                mode=mode,
                partition_by=self._partition_by,
                merge_schema=self._merge_schema,
                overwrite_schema=self._overwrite_schema or replacing_tombstone,
                storage_options=so,
                cur=session.con,
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
                    session._catalog_storage_options(catalog), catalog,
                    target=(catalog, schema, table))
        # Surface the (new or grown) table immediately — no manual refresh() needed.
        session.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(catalog)}.{_qid(schema)}")
        session._register_view(catalog, schema, table)
        # Re-apply USE: on a previously-empty warehouse the schema didn't exist at connect, so the
        # USE silently no-op'd; now that it exists, unqualified names must resolve.
        session._use(session._current_catalog, session._current_database)
        return table


# Spark's catalog.getTable / getDatabase return Table / Database objects; we mirror their fields with
# a plain namedtuple rather than inventing classes. duckrun tables are always managed Delta tables
# materialized under the catalog root, never temporary.
Table = namedtuple("Table", ["name", "catalog", "database", "description", "tableType", "isTemporary"])
Database = namedtuple("Database", ["name", "catalog", "description", "locationUri"])


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

    def getTable(self, tableName: str, dbName: Optional[str] = None) -> Table:
        """Return a :class:`Table` record for ``tableName`` (Spark's ``catalog.getTable``), or raise
        ``ValueError`` if it doesn't exist — the peer of :meth:`tableExists` / :meth:`listTables`.
        duckrun tables are always managed Delta tables, never temporary."""
        catalog, schema, table = self.session._resolve(tableName)
        if dbName is not None:
            schema = dbName
        if not self.tableExists(tableName, dbName):
            raise ValueError(f"table '{tableName}' not found in '{catalog}.{schema}'.")
        return Table(name=table, catalog=catalog, database=schema, description=None,
                     tableType="MANAGED", isTemporary=False)

    def createTable(self, tableName: str, schema) -> "DataFrame":
        """Create an empty managed Delta table and return it as a :class:`DataFrame` (Spark's
        ``catalog.createTable``). ``schema`` is a DDL string (``"id int, name string"``) or a
        :class:`StructType` (e.g. from another frame's ``df.schema``). Routes through the same
        Delta-backed ``CREATE TABLE`` the SQL path uses, so the table is queryable immediately.

        Note: unlike Spark there's no ``path`` / ``source`` argument — duckrun tables are always
        managed Delta under the catalog root; read foreign data by path with ``conn.read…load()``."""
        if isinstance(schema, StructType):
            ddl = ", ".join(f'"{f.name}" {f.dataType}' for f in schema.fields)
        elif isinstance(schema, str):
            ddl = schema
        else:
            raise ValueError("createTable: schema must be a DDL string or a StructType.")
        self.session.sql(f"CREATE TABLE {tableName} ({ddl})")
        return self.session.table(tableName)

    def getDatabase(self, dbName: str) -> Database:
        """Return a :class:`Database` record for ``dbName`` (Spark's ``catalog.getDatabase``), or
        raise ``ValueError`` if it doesn't exist — the peer of :meth:`databaseExists` /
        :meth:`listDatabases`. ``locationUri`` is the schema folder under the catalog root."""
        catalog = self.session._current_catalog
        if not self.databaseExists(dbName):
            raise ValueError(f"database '{dbName}' not found in catalog '{catalog}'.")
        location = f"{self.session._catalogs[catalog].root_path}/{dbName}"
        return Database(name=dbName, catalog=catalog, description=None, locationUri=location)

    def refreshTable(self, tableName: str) -> None:
        """Rebuild the cached view for a single table from the current on-store Delta snapshot
        (Spark's ``catalog.refreshTable``). The per-table peer of ``conn.refresh()``, which
        rediscovers the whole store; use this after an out-of-band write to one table."""
        catalog, schema, table = self.session._resolve(tableName)
        self.session._register_view(catalog, schema, table)

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

    def dropTempView(self, viewName: str) -> bool:
        """Drop a view registered by :meth:`DataFrame.createOrReplaceTempView` (the inverse). Returns
        ``True`` if the view existed and was dropped, ``False`` if there was nothing to drop — like
        Spark's ``catalog.dropTempView``. These views are native, ephemeral DuckDB views, not Delta
        tables, so this never touches storage."""
        con = self.session.con
        existed = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_type = 'VIEW'",
            [viewName],
        ).fetchone() is not None
        con.execute(f'DROP VIEW IF EXISTS "{viewName}"')
        return existed

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
            schema: Optional[str] = None,
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
        read_only: **default True** — the session refuses every Delta write (saveAsTable
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
    return DuckSession(path, storage_options, schema, read_only, name)
