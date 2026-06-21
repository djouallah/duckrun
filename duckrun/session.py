"""A PySpark-shaped, storage-neutral connection over a Delta lakehouse.

``duckrun.connect(path)`` opens a DuckDB connection, discovers the Delta tables under the store,
registers each as a ``delta_scan`` view, and hands back a :class:`DuckSession` whose surface
mirrors a ``SparkSession``: ``.sql()``, ``.table()``, ``.read``, ``.catalog``, and a
``DataFrame`` with a Spark ``.write…saveAsTable()``.

It is storage-neutral — local path, ``s3://``, ``gs://``, ``az://``, OneLake ``abfss://`` — because
every storage concern (token → secret, table discovery, the Delta write path) is delegated to the
``dbt.adapters.duckrun`` modules that already handle all of them. This module is glue.
"""
import re
from typing import Dict, List, Optional

import duckdb

from dbt.adapters.duckrun import delta_dml, engine, remote, secret
from . import auth
from ._runtime import check_runtime_versions


# Statements that would WRITE to a table — rejected by the read-only conn.sql() with a pointer to
# the Spark write API. INSERT/UPDATE/DELETE/MERGE against a read-only delta_scan view error in
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
    "conn.sql() can't run a SQL MERGE via delta_rs. Use the Spark write API: "
    "df.write.saveAsTable(...) to create/append, or "
    "conn.delta_table(name).merge(...)/.delete()/.update()/.replaceWhere()."
)
_UPDATE_FROM_MSG = (
    "conn.sql() can't run UPDATE … FROM via delta_rs. Rewrite the SET values as correlated "
    "subqueries, or use conn.delta_table(name).update(...)/.merge(...)."
)
_DELETE_USING_MSG = (
    "conn.sql() can't run DELETE … USING via delta_rs. Rewrite the predicate as a correlated "
    "subquery (DELETE … WHERE … IN (SELECT …)), or use conn.delta_table(name).delete(...)/.merge(...)."
)
_MULTI_MSG = (
    "conn.sql() runs one statement at a time — split the batch into separate conn.sql() calls."
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
    Spark write API instead). CREATE TEMP/TEMPORARY TABLE and CREATE VIEW are NOT writes."""
    s = _strip_leading(query)
    if _WRITE_KEYWORD_RE.match(s):
        return True
    return bool(_CREATE_TABLE_RE.match(s)) and not _CREATE_TEMP_RE.match(s)


def _delta_write_message(query: str) -> str:
    """The error for a raw-SQL write conn.sql() can't route to delta_rs. For an INSERT/UPDATE/DELETE
    whose target isn't a discovered Delta table — the common cause being a typo or a table written
    out-of-band before refresh() — name the table and give form-appropriate guidance, instead of the
    generic 'use the Spark write API' redirect (which misdirects: for UPDATE/DELETE the problem is the
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
        "Use the Spark write API: df.write.saveAsTable(...) to create/append, or "
        "conn.delta_table(name).merge(...)/.delete()/.update()/.replaceWhere()."
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


class DuckSession:
    """A SparkSession-like handle bound to one Delta lakehouse root."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]],
                 schema: Optional[str], compaction_threshold: int):
        self.root_path, self._schema = _split_root_schema(path, schema)
        self.storage_options = dict(storage_options) if storage_options else None
        self.compaction_threshold = compaction_threshold

        # OneLake with no caller-supplied token: acquire one (Fabric / env / azure-identity) so
        # both the DuckDB read secret and delta-rs writes can authenticate.
        if remote.is_abfss(self.root_path) and not secret.bearer_token(self.storage_options):
            self.storage_options = dict(self.storage_options or {})
            self.storage_options["bearer_token"] = auth.get_onelake_token()

        self.con = duckdb.connect()
        engine.configure_duckdb_session(self.con)
        # abfss:// / az:// reads need the DuckDB Azure secret minted before any delta_scan/glob.
        # No-op when there's no bearer token (e.g. local / s3 / gcs use env / DuckDB secrets).
        try:
            secret.ensure_azure_secret(self.con, self.storage_options)
        except Exception as exc:  # surfaced, not swallowed as "no tables" later
            print(f"⚠️  could not mint OneLake secret: {exc}")

        self.catalog = Catalog(self)
        self._current_database = None
        self.refresh()

    # ---- discovery -------------------------------------------------------------------------

    def _list_tables(self, schema: str) -> List[str]:
        if remote.is_abfss(self.root_path):
            return remote.list_delta_tables(self.root_path, str(schema), self.storage_options)
        return remote.list_delta_tables_via_glob(self.con, self.root_path, str(schema))

    def _list_schemas(self) -> List[str]:
        if remote.is_abfss(self.root_path):
            # Immediate sub-directories under the root (…/Tables) are the schema folders.
            return remote.list_delta_tables(self.root_path, "", self.storage_options)
        # Local / s3 / gcs / az: glob two levels deep and collect the schema segment.
        pattern = _qlit(self.root_path.rstrip("/") + "/*/*/_delta_log/*.json")
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

    def refresh(self, quiet: bool = False):
        """Re-discover the Delta tables and (re)register them as ``delta_scan`` views.

        Call after writing tables out-of-band (or from another process) to surface them.
        Pass ``quiet=True`` to skip the connection banner (used by the catalog existence checks,
        which refresh on every call).
        """
        if self._schema is not None:
            mapping = {self._schema: self._list_tables(self._schema)}
        else:
            mapping = {s: self._list_tables(s) for s in self._list_schemas()}

        registered = []
        for schema, tables in mapping.items():
            if not tables:
                continue
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(schema)}")
            for table in tables:
                # Hide drop-tombstones (a `drop table` overwrites the table to a one-column marker;
                # no data is deleted, the files persist, but the table must not surface).
                if delta_dml.is_dropped(self.con, self.table_path(schema, table), self.storage_options):
                    continue
                self._register_view(schema, table)
                registered.append(f"{schema}.{table}")

        schemas = list(mapping.keys())
        if self._current_database is None:
            self._current_database = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
        self._set_search_path(self._current_database)

        if not quiet:
            lh = self.root_path.rstrip("/").rsplit("/", 1)[-1]
            print(f"🔌 Connected to {lh} — discovered {len(registered)} table(s)"
                  + (": " + ", ".join(registered) if registered else ""))
        return self

    def _register_view(self, schema: str, table: str):
        path = f"{self.root_path.rstrip('/')}/{schema}/{table}"
        try:
            self.con.execute(
                f"CREATE OR REPLACE VIEW {_qid(schema)}.{_qid(table)} AS "
                f"SELECT * FROM delta_scan('{_qlit(path)}')"
            )
        except Exception as exc:
            # delta_scan failed reading the table. Keep the real engine error (it's the signal —
            # e.g. the OneLake "No files in log segment" delta-kernel bug), but drop DuckDB's echo
            # of the CREATE VIEW statement *we* generated, and say which table/path it was. Suppress
            # the chained original (`from None`) so the noisy SQL echo doesn't reappear in tracebacks.
            hint = _onelake_guid_hint(self.root_path)
            raise RuntimeError(
                f"duckrun: could not read Delta table {schema}.{table} at '{path}':\n"
                f"{_strip_query_context(str(exc))}"
                + (f"\n\n{hint}" if hint else "")
            ) from None

    def _set_search_path(self, schema: str):
        try:
            self.con.execute(f"SET search_path = {_qid(schema)}")
        except Exception:  # an empty/absent schema shouldn't abort connect
            pass

    def table_path(self, schema: str, table: str) -> str:
        return f"{self.root_path.rstrip('/')}/{schema}/{table}"

    def resolve(self, name: str):
        """Split a possibly-qualified ``db.table`` into ``(schema, table)`` using the current db."""
        if "." in name:
            schema, table = name.split(".", 1)
            return schema, table
        return self._current_database, name

    # ---- Spark-shaped surface --------------------------------------------------------------

    def sql(self, query: str) -> "DataFrame":
        """Run a query and return a :class:`DataFrame`.

        Reads pass straight through to DuckDB over the ``delta_scan`` views (time-travel works for
        free — ``conn.sql("from delta_scan('path', version => 0)")``).

        Delta **DML** is applied to the Delta table via delta_rs (works local AND on OneLake):
        ``create table … as select`` (overwrite), ``insert into … select``/``insert into … values``
        (append), ``delete``/``update`` (delta_rs delete/update), ``alter table … add column``, and
        ``drop table`` (tombstone — marks the table dropped without deleting data; a human purges
        the files). After a DML statement the catalog is refreshed.

        ``merge`` isn't expressible via delta_rs DML here — use the Spark write surface instead:
        ``df.write.saveAsTable(...)`` or
        ``conn.delta_table(name).merge(...)/.delete()/.update()/.replaceWhere()``.
        ``CREATE TEMP/VIEW`` and other DuckDB-local scratch DDL pass through to DuckDB.
        """
        unsupported = _unsupported_dml(query)
        if unsupported:
            raise ValueError(unsupported)
        if delta_dml.handle(self.con, self.root_path, self.storage_options, query,
                            default_schema=self._current_database):
            self.refresh(quiet=True)
            return DataFrame(self.con.sql("SELECT 'ok' AS status"), self)
        if _is_delta_write(query):
            raise ValueError(_delta_write_message(query))
        return DataFrame(self.con.sql(query), self)

    def table(self, name: str) -> "DataFrame":
        schema, table = self.resolve(name)
        return DataFrame(self.con.sql(f"SELECT * FROM {_qid(schema)}.{_qid(table)}"), self)

    def delta_table(self, name: str) -> "DeltaTable":
        """A Spark ``DeltaTable`` handle for an existing table: ``.merge(...)``, ``.delete()``,
        ``.update()``, ``.replaceWhere()``, ``.version()``. (Reads still go through ``conn.sql`` /
        ``conn.table``; this is the write/mutate side.) Shortcut for ``DeltaTable.forName(conn, name)``."""
        from .delta_table import DeltaTable  # local import: delta_table imports nothing from session
        return DeltaTable.forName(self, name)

    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)

    @property
    def connection(self):
        """The underlying DuckDB connection (escape hatch)."""
        return self.con


class DataFrame:
    """Wraps a DuckDB relation; exposes a Spark-style ``.write`` plus a few Spark aliases.

    Anything not defined here falls through to the underlying DuckDB relation, so ``.df()``,
    ``.arrow()``, ``.fetchall()``, ``.fetchnumpy()`` etc. all keep working.
    """

    def __init__(self, relation, session: DuckSession):
        self.relation = relation
        self.session = session

    @property
    def write(self) -> "DataFrameWriter":
        return DataFrameWriter(self)

    # Spark aliases over the DuckDB relation.
    def show(self, *a, **k):
        return self.relation.show(*a, **k)

    def toPandas(self):
        return self.relation.df()

    def collect(self):
        return self.relation.fetchall()

    def count(self) -> int:
        return self.relation.aggregate("count(*)").fetchone()[0]

    def createOrReplaceTempView(self, name: str) -> "DataFrame":
        """Register this DataFrame as a session-scoped view named ``name``, so it can be queried by
        name via ``conn.sql("select * from name")`` (Spark ``createOrReplaceTempView``).

        This is the path-read counterpart to ``saveAsTable``: ``conn.read.delta(path)`` returns a
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
    """Spark ``DataFrameReader``: read a path/table into a :class:`DataFrame` without it having to
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
            scan = f"delta_scan('{_qlit(path)}')"
        elif fmt == "parquet":
            scan = f"read_parquet('{_qlit(path)}')"
        elif fmt == "csv":
            opts = "".join(f", {k}={_csv_opt(v)}" for k, v in self._options.items())
            scan = f"read_csv_auto('{_qlit(path)}'{opts})"
        else:
            raise ValueError(f"Unsupported read format '{fmt}'. Use 'delta', 'parquet', or 'csv'.")
        return DataFrame(self.session.con.sql(f"SELECT * FROM {scan}"), self.session)

    def delta(self, path: str) -> DataFrame:
        return self.format("delta").load(path)

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
    """Spark ``DataFrameWriter`` over delta-rs (the adapter's :func:`engine.write_delta`).

    Beyond Spark's modes it adds ``"safeappend"`` — the same optimistic, fail-loud append as the
    dbt adapter's incremental strategy: it commits only if the table version has not moved since
    the call (compare-and-swap), else raises ``CommitFailedError``. Non-standard, but identical
    behaviour to ``safeappend`` in dbt."""

    _MODES = {"overwrite", "append", "safeappend", "ignore", "error", "errorifexists"}

    def __init__(self, df: DataFrame):
        self._df = df
        self._format = "delta"
        self._mode = "error"  # Spark's default SaveMode
        self._merge_schema = False
        self._overwrite_schema = False
        self._partition_by: Optional[List[str]] = None

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
        truthy = str(value).lower() in ("true", "1")
        if key == "mergeSchema":
            self._merge_schema = truthy
        elif key == "overwriteSchema":
            self._overwrite_schema = truthy
        else:
            raise ValueError(
                f"Unsupported write option '{key}'. Supported: 'mergeSchema', 'overwriteSchema'."
            )
        return self

    def partitionBy(self, *cols) -> "DataFrameWriter":
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = tuple(cols[0])
        self._partition_by = list(cols)
        return self

    def _write(self, path: str, descr: str) -> None:
        """Apply the configured mode to the Delta table at ``path`` (storage-neutral). ``descr``
        names the target in the mode='error' message. Shared by saveAsTable and save."""
        session = self._df.session
        so = session.storage_options

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
        """Spark ``df.write.save(path)`` — write to a Delta table by PATH, not catalog name.

        Storage-neutral (local / s3:// / gs:// / az:// / abfss://). Unlike :meth:`saveAsTable`,
        the result is addressed only by ``path`` — there is no schema.table name to register a
        view for — so it is read back with ``conn.read.delta(path)`` / ``delta_scan('<path>')``,
        not as an unqualified table. Returns ``path``."""
        self._write(path, f"delta table at '{path}'")
        return path

    def saveAsTable(self, name: str) -> str:
        session = self._df.session
        schema, table = session.resolve(name)
        path = session.table_path(schema, table)
        self._write(path, f"table '{schema}.{table}'")
        # Surface the (new or grown) table immediately — no manual refresh() needed.
        session.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(schema)}")
        session._register_view(schema, table)
        # Re-apply the search_path: on a previously-empty warehouse the schema didn't exist at
        # connect, so SET search_path silently no-op'd; now that it exists, unqualified names
        # (e.g. `select * from <table>`) must resolve.
        session._set_search_path(session._current_database)
        return table


class Catalog:
    """A small Spark ``Catalog`` over the discovered schemas/views."""

    def __init__(self, session: DuckSession):
        self.session = session

    def listDatabases(self) -> List[str]:
        rows = self.session.con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = current_database() ORDER BY schema_name"
        ).fetchall()
        skip = {"information_schema", "pg_catalog", "main"}
        return [r[0] for r in rows if r[0] not in skip]

    def listTables(self, dbName: Optional[str] = None) -> List[str]:
        schema = dbName or self.session._current_database
        rows = self.session.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name",
            [schema],
        ).fetchall()
        return [r[0] for r in rows]

    def currentDatabase(self) -> str:
        return self.session._current_database

    def setCurrentDatabase(self, dbName: str):
        self.session._current_database = dbName
        self.session._set_search_path(dbName)

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        self.session.refresh(quiet=True)  # safe: reflect on-store truth, not stale views
        schema, table = self.session.resolve(tableName)
        if dbName is not None:
            schema = dbName
        rows = self.session.con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchall()
        return len(rows) > 0

    def databaseExists(self, dbName: str) -> bool:
        self.session.refresh(quiet=True)  # safe: re-discover schema folders first
        return dbName in self.listDatabases()

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[str]:
        self.session.refresh(quiet=True)
        schema, table = self.session.resolve(tableName)
        if dbName is not None:
            schema = dbName
        rows = self.session.con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        return [r[0] for r in rows]


def connect(path: str, storage_options: Optional[Dict[str, str]] = None,
            schema: Optional[str] = None, compaction_threshold: int = 100) -> DuckSession:
    """Open a storage-neutral, Spark-shaped session over a Delta lakehouse.

    Args:
        path: the lakehouse root, or (OneLake) ``…/Tables`` or ``…/Tables/<schema>``. Works with a
            local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``.
        storage_options: forwarded to delta-rs (and used to mint DuckDB secrets). For OneLake you
            can omit it inside a Fabric notebook — a token is acquired automatically.
        schema: restrict to a single schema. Omit to discover every schema folder.
        compaction_threshold: file-count threshold for post-append/merge compaction.

    Example:
        >>> conn = duckrun.connect("abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/dbo")
        >>> conn.sql("SHOW TABLES").show()
        >>> conn.sql("select * from orders").write.mode("overwrite").saveAsTable("orders_copy")
    """
    check_runtime_versions()  # fail loud if Fabric's stale duckdb/deltalake are still loaded
    return DuckSession(path, storage_options, schema, compaction_threshold)
