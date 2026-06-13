"""A PySpark-shaped, storage-neutral connection over a Delta lakehouse.

``duckrun.connect(path)`` opens a DuckDB connection, discovers the Delta tables under the store,
registers each as a ``delta_scan`` view, and hands back a :class:`DuckSession` whose surface
mirrors a ``SparkSession``: ``.sql()``, ``.table()``, ``.read``, ``.catalog``, and a
``DataFrame`` with a Spark ``.write…saveAsTable()``.

It is storage-neutral — local path, ``s3://``, ``gs://``, ``az://``, OneLake ``abfss://`` — because
every storage concern (token → secret, table discovery, the Delta write path) is delegated to the
``dbt.adapters.duckrun`` modules that already handle all of them. This module is glue.
"""
from typing import Dict, List, Optional

import duckdb

from dbt.adapters.duckrun import engine, remote, secret
from . import auth


def _qid(name: str) -> str:
    """Quote a SQL identifier (schema/table/view name)."""
    return '"' + str(name).replace('"', '""') + '"'


def _qlit(text: str) -> str:
    """Escape a SQL string literal body (the path inside ``delta_scan('...')``)."""
    return str(text).replace("'", "''")


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

    def refresh(self):
        """Re-discover the Delta tables and (re)register them as ``delta_scan`` views.

        Call after writing tables out-of-band (or from another process) to surface them.
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
                self._register_view(schema, table)
                registered.append(f"{schema}.{table}")

        schemas = list(mapping.keys())
        if self._current_database is None:
            self._current_database = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
        self._set_search_path(self._current_database)

        lh = self.root_path.rstrip("/").rsplit("/", 1)[-1]
        print(f"🔌 Connected to {lh} — discovered {len(registered)} table(s)"
              + (": " + ", ".join(registered) if registered else ""))
        return self

    def _register_view(self, schema: str, table: str):
        path = f"{self.root_path.rstrip('/')}/{schema}/{table}"
        self.con.execute(
            f"CREATE OR REPLACE VIEW {_qid(schema)}.{_qid(table)} AS "
            f"SELECT * FROM delta_scan('{_qlit(path)}')"
        )

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
        return DataFrame(self.con.sql(query), self)

    def table(self, name: str) -> "DataFrame":
        schema, table = self.resolve(name)
        return DataFrame(self.con.sql(f"SELECT * FROM {_qid(schema)}.{_qid(table)}"), self)

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
    """Spark ``DataFrameWriter`` over delta-rs (the adapter's :func:`engine.write_delta`)."""

    _MODES = {"overwrite", "append", "ignore", "error", "errorifexists"}

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

    def saveAsTable(self, name: str) -> str:
        session = self._df.session
        schema, table = session.resolve(name)
        path = session.table_path(schema, table)
        so = session.storage_options

        mode = self._mode
        if mode in ("error", "errorifexists"):
            if engine.table_exists(path, so):
                raise ValueError(
                    f"table '{schema}.{table}' already exists (mode='error'). "
                    f"Use mode('overwrite'), mode('append'), or mode('ignore')."
                )
            mode = "overwrite"

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
        # Surface the (new or grown) table immediately.
        session.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(schema)}")
        session._register_view(schema, table)
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
    return DuckSession(path, storage_options, schema, compaction_threshold)
