"""A storage-neutral, SQL-first connection over a Delta lakehouse.

``duckrun.connect(path)`` opens a DuckDB connection, discovers the Delta tables under the store,
registers each as a ``delta_scan`` view, and hands back a :class:`DuckSession`. The surface is SQL:
``conn.sql(query)`` returns DuckDB's native relation for reads and routes write DML
(``CREATE TABLE … AS`` / ``INSERT`` / ``UPDATE`` / ``DELETE`` / ``MERGE`` / ``VACUUM`` /
``REPLACE WHERE``) to delta-rs.

It is storage-neutral — local path, ``s3://``, ``gs://``, ``az://``, OneLake ``abfss://`` — because
every storage concern (token → secret, table discovery, the Delta write path) is delegated to the
``dbt.adapters.duckrun`` modules that already handle all of them. This module is glue.
"""
import os
import re
from collections import namedtuple
from typing import Dict, List, Optional

import duckdb

from dbt.adapters.duckrun import delta_dml, engine, objectstore, remote, secret, sortkey
from ._runtime import check_runtime_versions


# Statements that would WRITE to a table — rejected by a read-only conn.sql().
# INSERT/UPDATE/DELETE/MERGE against a read-only delta_scan view error in
# DuckDB anyway; CREATE [OR REPLACE] TABLE … is the dangerous one — it silently makes an ephemeral
# DuckDB-local table that never reaches Delta — so it must be caught BEFORE executing. CREATE
# TEMP/TEMPORARY TABLE and CREATE VIEW are DuckDB-local scratch by design and pass through.
_WRITE_KEYWORD_RE = re.compile(r"^(insert|update|delete|merge)\b", re.IGNORECASE)
_CREATE_TABLE_RE = re.compile(r"^create\s+(or\s+replace\s+)?table\b", re.IGNORECASE)
_VACUUM_WRITE_RE = re.compile(r"^vacuum\s+(?:analyze\s+)?[\"\w]", re.IGNORECASE)
_RESTORE_RE = re.compile(r"^restore\s+table\b", re.IGNORECASE)   # RESTORE writes a new commit
# `describe detail <t>` / `describe history <t>` — Delta introspection (plain `describe <t>` is DuckDB's).
_DESCRIBE_EXT_RE = re.compile(r"^describe\s+(?P<kind>detail|history)\s+(?P<rel>.+?)\s*;?\s*$", re.IGNORECASE)
# A bare `SELECT * FROM <table>` — nothing else (no WHERE/JOIN/LIMIT/projection). Its output is that
# table verbatim, so SORTED BY AUTO can profile it EXACTLY from the Delta log instead of sampling.
_SELECT_STAR_FROM = re.compile(
    r"\s*select\s+\*\s+from\s+(?P<rel>(?:\"[^\"]+\"|\w+)(?:\.(?:\"[^\"]+\"|\w+))*)\s*", re.IGNORECASE)
# A body whose top-level projection is a bare `*` (or `alias.*`) — every output column is a
# passthrough of its source column, so decimal narrowing (which rewrites the projection) is safe.
# Any explicit projection (including a user-written CAST — an instruction we must not override) is
# NOT matched, so it is left untouched.
_SELECT_STAR_BODY = re.compile(r"\s*select\s+(?:\*|\"?\w+\"?\.\*)\s+from\b", re.IGNORECASE)
_DML_TARGET_RE = re.compile(
    r"^(?:insert\s+into|delete\s+from|update)\s+(?P<rel>\"?[\w.]+\"?)", re.IGNORECASE)
_CREATE_TEMP_RE = re.compile(r"^create\s+(or\s+replace\s+)?(temp|temporary)\b", re.IGNORECASE)
# Explicit transaction-control verbs. DuckDB owns the native transaction, but Delta writes auto-commit
# via delta_rs and can't join it — so we track whether one is open and reject Delta DML inside it.
_TXN_VERB_RE = re.compile(r"^(begin|start|commit|rollback|end|abort)\b", re.IGNORECASE)

# DML forms that genuinely can't be expressed through delta_rs (delta_dml.handle never applies them):
# rejected up front with a form-specific pointer rather than letting DuckDB raise a cryptic error on
# the read-only delta_scan view (or, for UPDATE … FROM, silently mangling the SET clause).
# leading `\b`: _find_top_level probes every depth-0 index (see delta_dml._find_top_level).
_TOP_FROM = re.compile(r"\bfrom\b", re.IGNORECASE)
_TOP_USING = re.compile(r"\busing\b", re.IGNORECASE)
_TOP_RETURNING = re.compile(r"\breturning\b", re.IGNORECASE)
_strip_leading = delta_dml._strip_leading  # shared comment/whitespace stripper

_UPDATE_FROM_MSG = (
    "conn.sql() can't run UPDATE … FROM via delta_rs. Rewrite the SET values as correlated "
    "subqueries, or express the join as MERGE INTO … USING …."
)
_DELETE_USING_MSG = (
    "conn.sql() can't run DELETE … USING via delta_rs. Rewrite the predicate as a correlated "
    "subquery (DELETE … WHERE … IN (SELECT …)), or express the join as MERGE INTO … USING …."
)
_RETURNING_MSG = (
    "conn.sql() can't run a RETURNING clause via delta_rs — a Delta write commits through the "
    "transaction log and does not hand back the affected rows. Drop RETURNING, then read the "
    "table back with a follow-up SELECT."
)
_MULTI_MSG = (
    "conn.sql() runs one statement at a time — split the batch into separate conn.sql() calls."
)
_TXN_WRITE_MSG = (
    "Delta writes auto-commit (delta_rs) and cannot run inside an explicit transaction — a ROLLBACK "
    "would report success while the write persisted. Commit or roll back the open transaction first, "
    "then run the write; or run the write outside BEGIN … COMMIT."
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
    "Delta writes (INSERT / UPDATE / DELETE / MERGE / REPLACE WHERE / CREATE / DROP / VACUUM) "
    "with connect(read_only=False) for the primary, or conn.attach(path, name='{catalog}', "
    "read_only=False) for an attached catalog."
)


def _unsupported_dml(query: str) -> Optional[str]:
    """An error message if ``query`` is a DML form duckrun can't route to delta_rs, else None."""
    # Strip interior comments too (not just leading ones) so a `--`/`/* */` can't inject a false
    # FROM/USING/RETURNING boundary or a bogus statement-separator `;`. _strip_comments is quote/
    # dollar-quote aware; _strip_leading then peels the leading whitespace/comment off the front.
    s = _strip_leading(delta_dml._strip_comments(query))
    low = s.lower()
    if low.startswith("update") and delta_dml._find_top_level(s, _TOP_FROM) != -1:
        return _UPDATE_FROM_MSG
    if low.startswith("delete") and delta_dml._find_top_level(s, _TOP_USING) != -1:
        return _DELETE_USING_MSG
    # RETURNING (INSERT/UPDATE/DELETE/MERGE) — delta_rs commits via the log and can't return the
    # affected rows. Caught here so the SET-clause parser can't mis-read the RETURNING list as more
    # assignments (it otherwise reports a bogus "sets unknown column(s)" for the projected columns).
    if re.match(r"(insert|update|delete|merge)\b", low) and \
            delta_dml._find_top_level(s, _TOP_RETURNING) != -1:
        return _RETURNING_MSG
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


def _is_vacuum(query: str) -> bool:
    """True if ``query`` is a ``VACUUM <table>`` — duckrun repurposes DuckDB's VACUUM verb to compact
    and vacuum a Delta table, which WRITES (compacted files + tombstone GC) and so must be caught by
    the read-only gate. A bare ``VACUUM`` / ``VACUUM ANALYZE`` (no operand) is a DuckDB no-op, not gated."""
    return bool(_VACUUM_WRITE_RE.match(_strip_leading(query)))


def _is_restore(query: str) -> bool:
    """True if ``query`` is ``RESTORE TABLE …`` — it commits a new (restored) version, so the read-only
    gate must catch it."""
    return bool(_RESTORE_RE.match(_strip_leading(query)))


_MERGE_RE = re.compile(r"(?is)^\s*merge\s+into\b")


def _is_merge(query: str) -> bool:
    """True if ``query`` is a ``MERGE INTO …`` — the write that shares the process with delta_rs's own
    merge pool, so DuckDB's memory_limit is tightened to its split share (not just the write share)."""
    return bool(_MERGE_RE.match(_strip_leading(query)))


def _delta_write_message(query: str) -> str:
    """The error for a raw-SQL write conn.sql() can't route to delta_rs. For an INSERT/UPDATE/DELETE
    whose target isn't a discovered Delta table — the common cause being a typo or a table written
    out-of-band before refresh() — name the table and give form-appropriate guidance, instead of the
    generic redirect (which misdirects: for UPDATE/DELETE the problem is the missing table)."""
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
            f"CREATE TABLE {rel} AS SELECT …, then insert."
        )
    return (  # a CREATE … AS that didn't resolve, or any other unrouted Delta write
        "conn.sql() can't write a Delta table from this SQL. Use CREATE TABLE … AS SELECT to create "
        "or overwrite, INSERT / UPDATE / DELETE / MERGE against a discovered table, "
        "INSERT INTO t REPLACE WHERE <pred> … to overwrite a slice, or VACUUM t to compact."
    )


def _qid(name: str) -> str:
    """Quote a SQL identifier (schema/table/view name)."""
    return '"' + str(name).replace('"', '""') + '"'


def _qlit(text: str) -> str:
    """Escape a SQL string literal body (the path inside ``delta_scan('...')``)."""
    return str(text).replace("'", "''")


def _case_collision(names):
    """The first ``(earlier, later)`` pair of ``names`` that are equal ignoring case but differ in
    spelling — a case-fold collision DuckDB's case-INSENSITIVE catalog would silently merge (so one
    table shadows the other) — else ``None``. Used to fail discovery loud when a store holds two
    tables (or schemas) that differ only by case, e.g. an external engine wrote both ``Foo`` and
    ``foo`` as separate directories on a case-sensitive filesystem."""
    seen = {}
    for n in names:
        prior = seen.get(n.lower())
        if prior is not None and prior != n:
            return prior, n
        seen.setdefault(n.lower(), n)
    return None


def _case_clash_msg(catalog, schema, clash) -> str:
    """The fail-loud message for a case-fold collision found during discovery."""
    a, b = clash
    where = (f"schema '{schema}' of catalog '{catalog}'" if schema is not None
             else f"catalog '{catalog}'")
    kind = "tables" if schema is not None else "schemas"
    return (
        f"duckrun: {where} has two {kind} that differ only by case — '{a}' and '{b}'. DuckDB's "
        f"catalog is case-insensitive, so it can expose only one and the other would be silently "
        f"hidden. They are separate directories on the store (written by another engine); rename or "
        f"remove one so the name is unambiguous.")


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


# The one strict GUID shape test, shared with the dbt adapter and fabric_remote (see remote.py).
_GUID = remote._GUID


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


# The OneLake `<workspace>/<item>` shorthand expander lives in ``remote`` (next to the other abfss
# parsing) so this session and the dbt adapter's root_path resolve it identically.
_expand_onelake_shorthand = remote.expand_onelake_shorthand


def _split_root_schema(path: str, schema: Optional[str]):
    """Normalize ``path`` into ``(root_path, schema)``.

    ``root_path`` is the directory that *contains* schema folders (so a table lives at
    ``root_path/<schema>/<table>``). When ``schema`` is passed explicitly it wins and ``path`` is
    taken as the root verbatim. Otherwise, for OneLake (``abfss://``) we honor the ``…/Tables`` and
    ``…/Tables/<schema>`` convention — the segment after ``Tables`` (if any) becomes the schema and
    the root is truncated to ``…/Tables``. For every other store an omitted schema means
    "discover all schema folders under ``path``".

    The OneLake shorthand (``ws/lh.Lakehouse``, ``<ws-guid>/<item-guid>``) is expanded first, so
    both :func:`connect` and :meth:`DuckSession.attach` accept it through this one seam.
    """
    p = _expand_onelake_shorthand(path).rstrip("/")
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
        self._in_explicit_txn = False  # an explicit BEGIN … is open on self.con (Delta writes can't join it)

        self.con = duckdb.connect()
        engine.configure_duckdb_session(self.con)
        # DuckDB's memory_limit as configured (its host-RAM default; configure_duckdb_session leaves it
        # alone). A Delta write clamps DOWN from this and restores to it — so a write can't OOM on a
        # container's host-RAM default while interactive reads keep full memory. Mirrors the dbt path.
        self._baseline_memory_limit: Optional[str] = engine.read_memory_limit(self.con)

        # The catalog registry: name -> _CatEntry (root / creds / read-only). "Which catalog & schema
        # is current" is NOT tracked here — DuckDB owns it (current_database() / current_schema(),
        # surfaced by the _current_catalog / _current_database properties), so a user's `USE` steers
        # reads AND write-routing with no parallel state to drift.
        self._catalogs: Dict[str, _CatEntry] = {}
        # The primary catalog keeps the unscoped OneLake secret (attached ones are path-scoped); the
        # token-refresh guard needs to know which is which to re-mint the right secret.
        self._primary_catalog: Optional[str] = None

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

    @property
    def _current_catalog(self) -> str:
        """The current catalog — DuckDB's own ``current_database()``. Single source of truth: a bare
        ``conn.sql("USE …")`` moves it, so reads and write-routing never disagree."""
        return self.con.execute("SELECT current_database()").fetchone()[0]

    @property
    def _current_database(self) -> str:
        """The current schema — DuckDB's own ``current_schema()``."""
        return self.con.execute("SELECT current_schema()").fetchone()[0]

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
        # the DuckDB read secret and delta-rs writes can authenticate. Shared with the dbt path via
        # secret.with_onelake_token, so the two surfaces can't drift.
        so = secret.with_onelake_token(root, so)
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
            print(f"[warn] could not mint OneLake secret for catalog '{name}': {exc}")

        self.con.execute(f"ATTACH ':memory:' AS {_qid(name)}")
        self._catalogs[name] = _CatEntry(name, root, so, schema_filter, ro)
        if primary:
            self._primary_catalog = name
        try:
            schemas = self._refresh_catalog(name, quiet=quiet)
        except Exception:
            # Roll back the half-built catalog. Otherwise a failed secondary attach() (e.g. the
            # case-collision error) leaves the DuckDB ATTACH and the registry entry behind:
            # refresh() would iterate a broken catalog, and re-attaching the corrected name/root
            # would die on "already attached".
            self._catalogs.pop(name, None)
            try:
                self.con.execute(f"DETACH {_qid(name)}")
            except Exception:
                pass
            raise
        if primary:
            # Make the primary the current catalog (+ a sensible default schema) via DuckDB's own USE —
            # the ONE place "current" is set; every reader derives it back from current_database().
            # Ensure the default schema exists first (an empty catalog has none yet) so USE can't fail
            # and silently leave the DuckDB default ('memory') current.
            default_db = "dbo" if "dbo" in schemas else (schemas[0] if schemas else "dbo")
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(name)}.{_qid(default_db)}")
            self._use(name, default_db)

    def attach(self, path: str, name: Optional[str] = None,
               storage_options: Optional[Dict[str, str]] = None,
               schema: Optional[str] = None, read_only: Optional[bool] = None) -> "DuckSession":
        """Attach a second+ lakehouse as a named catalog, so ``catalog.schema.table`` resolves across
        lakehouses.

        ``name`` is derived from a friendly path when omitted, but is **mandatory** for a GUID-only
        OneLake path (raises, since there is no readable name to derive). The mapping is bijective:
        re-attaching a URL (under any name), or reusing a name, raises. ``schema`` restricts discovery
        to a single schema, exactly as in :func:`connect`, and ``path`` takes the same OneLake
        shorthand (``ws/lh.Lakehouse``, ``<ws-guid>/<item-guid>``). ``read_only`` fences writes to *this* catalog
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

        # Fail loud when the store holds two schemas/tables that differ ONLY by case: DuckDB's catalog
        # folds them to one name, so exposing both is impossible and one would silently shadow the
        # other. This happens on a case-sensitive store (Linux / OneLake) when an external engine
        # (Spark/Fabric) wrote both — duckrun can't fix it, but it must not hide it.
        clash = _case_collision(list(mapping))
        if clash:
            raise RuntimeError(_case_clash_msg(name, None, clash))

        registered = []
        for schema, tables in mapping.items():
            if not tables:
                continue
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qid(name)}.{_qid(schema)}")
            # Open every table's Delta log concurrently for the tombstone checks — serialized
            # per-table opens were the dominant connect()/refresh() cost on OneLake (the twin of
            # the adapter's issue-#16 discovery fix). View registration stays serial on the
            # shared connection.
            dts = engine.open_delta_tables([(f"{root}/{schema}/{t}", so) for t in tables])
            live = []
            for table, dt in zip(tables, dts):
                # Hide drop-tombstones (a `drop table` overwrites the table to a one-column marker;
                # no data is deleted, the files persist, but the table must not surface).
                if dt is not None:
                    if delta_dml.is_dropped_dt(dt):
                        continue
                # delta-rs couldn't open it (a plain folder, or a credential only DuckDB holds) —
                # fall back to the original DuckDB-side probe so a tombstone there is still hidden.
                elif delta_dml.is_dropped(self.con, f"{root}/{schema}/{table}", so):
                    continue
                if self._register_view(name, schema, table):
                    live.append(table)
                    registered.append(f"{schema}.{table}")
            clash = _case_collision(live)
            if clash:
                raise RuntimeError(_case_clash_msg(name, schema, clash))

        if not quiet:
            lh = root.rstrip("/").rsplit("/", 1)[-1]
            print(f"Connected to {lh} (catalog '{name}') - discovered {len(registered)} table(s)"
                  + (": " + ", ".join(registered) if registered else ""))
        return list(mapping.keys())

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
        parts = delta_dml._split_dotted(name)  # quote-aware: a dot inside "a.b" stays one part
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
        Delta-write entry point (raw write-DML in sql()). ``read_only`` is per-catalog, so a read-only
        attached store fails loud here even when the session/primary is writable."""
        cat = catalog if catalog is not None else self._current_catalog
        if self._catalogs[cat].read_only:
            raise PermissionError(_READ_ONLY_MSG.format(op=op, catalog=cat))

    # ---- catalog introspection (internal; SQL users use SHOW TABLES / information_schema) -------

    _SKIP_SCHEMAS = {"information_schema", "pg_catalog", "main"}

    def _cat_databases(self, catalog: Optional[str] = None) -> List[str]:
        """Schemas in ``catalog`` (default: the current catalog) — the discovered Delta schema
        folders, minus DuckDB internals."""
        cat = catalog or self._current_catalog
        rows = self.con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = ? ORDER BY schema_name", [cat]).fetchall()
        return [r[0] for r in rows if r[0] not in self._SKIP_SCHEMAS]

    def _cat_tables(self, dbName: Optional[str] = None, catalog: Optional[str] = None) -> List[str]:
        """Tables (registered delta_scan views) in ``dbName`` (default: the current database) of
        ``catalog`` (default: the current catalog)."""
        cat = catalog or self._current_catalog
        schema = dbName or self._current_database
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name",
            [cat, schema]).fetchall()
        return [r[0] for r in rows]

    def _cat_table_exists(self, tableName: str) -> bool:
        """True iff ``tableName`` is a LIVE Delta table (read from the store; a drop-tombstone is False)."""
        catalog, schema, table = self._resolve(tableName)
        return self._live_table_exists(self._table_path(schema, table, catalog),
                                       self._catalog_storage_options(catalog))

    def _cat_database_exists(self, dbName: str) -> bool:
        self.refresh(quiet=True)  # re-discover schema folders first
        return dbName in self._cat_databases()

    def _refresh_tokens(self) -> None:
        """Refresh each catalog's near-expiry OneLake token and re-mint its DuckDB secret, so a
        long-lived session's reads/writes don't 401 past the token's ~1h life. The primary keeps the
        unscoped secret; attached catalogs re-mint their path-scoped secret. Shared with the dbt cursor
        guard via secret.refresh_catalog_secret. No-op unless a token is genuinely near expiry, and for
        local / token-less catalogs."""
        for name, entry in list(self._catalogs.items()):
            fresh = secret.refresh_catalog_secret(
                self.con, name, entry.storage_options,
                is_default=(name == self._primary_catalog), root=entry.root_path,
            )
            if fresh is not entry.storage_options:
                self._catalogs[name] = entry._replace(storage_options=fresh)

    def _handle_delta_write(self, entry, query: str) -> bool:
        """Route a Delta write through ``delta_dml.handle`` with the DuckDB ``memory_limit`` clamped to
        the write-path share (and tightened to the merge split for a ``MERGE``, which shares the process
        with delta_rs's merge pool), then restored to the baseline. Without this the write inherits
        DuckDB's host-physical-RAM default and OOM-kills the session on a container — the same clamp the
        dbt path applies in ``store()``, reusing the same engine helpers so the two can't drift. The
        restore keeps interactive reads at full memory once the write is done."""
        baseline = self._baseline_memory_limit
        engine.set_write_memory_limit(self.con, baseline)
        if _is_merge(query):
            engine.set_merge_memory_limit(self.con)
        try:
            return delta_dml.handle(self.con, entry.root_path, entry.storage_options,
                                    query, default_schema=self._current_database)
        finally:
            engine.restore_memory_limit(self.con, baseline)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Run a query and return DuckDB's native ``DuckDBPyRelation`` — unwrapped, with all of its
        own methods (``.show()``, ``.df()``, ``.arrow()``, ``.pl()``, ``.fetchall()``, ``.filter()``,
        …), maintained upstream by DuckDB.

        Reads pass straight through to DuckDB over the ``delta_scan`` views (time-travel works for
        free — ``conn.sql("from delta_scan('path', version => 0)")``).

        Delta **DML** is applied to the Delta table via delta_rs (works local AND on OneLake):
        ``create table … as select`` (overwrite), ``insert into … select``/``insert into … values``
        (append), ``insert with schema evolution into …`` (append that widens the table with the
        source's new columns), ``delete``/``update`` (delta_rs delete/update), ``alter table … add /
        drop / rename column``, ``drop table`` (tombstone — marks the table dropped without deleting
        data; a human purges the files), and ``merge into … using … on … when …`` (delta_rs upsert).
        After a DML statement the catalog is refreshed.

        ``vacuum <table>`` repurposes DuckDB's VACUUM verb for Delta maintenance: it compacts the
        table's small files (delta_rs ``optimize.compact``, dataChange=false) and vacuums files
        tombstoned past the retention window. (Compaction also runs automatically after writes; this
        is the manual button.)

        ``insert into <t> replace where <pred> select …`` (delta_rs ``replaceWhere``, the Spark/Delta
        spelling): atomically overwrite ONLY the rows matching ``<pred>`` with the SELECT's rows, in a
        single fenced commit (pinned to the version read — a concurrent write fails it loud). ``<pred>``
        is a CAST-free expression over the target's columns; partition columns are preserved.

        A SQL ``merge`` must reference the literal ``target`` and ``source`` aliases in the ``ON``
        condition and ``WHEN`` clauses (``merge into t using s on target.id = source.id when matched
        then update set * when not matched then insert *``) — duckrun renames the merge relations to
        those names. ``CREATE TEMP/VIEW`` and other DuckDB-local scratch DDL pass through to DuckDB.

        A DML statement returns a one-row ``status`` relation (there is no result set to hand back);
        the write has already been applied to Delta.

        ``describe detail <table>`` and ``describe history <table>`` (the Spark/Delta
        introspection verbs) return the table's ``location`` / ``numFiles`` / ``sizeInBytes`` /
        ``version`` and its commit history — read from the Delta log. (Plain ``describe <table>``
        passes through to DuckDB for column info.) ``restore table <t> to version as of <n>`` (or
        ``to timestamp as of '…'``) rolls the table back — a new commit on top of history, itself
        revertible.
        """
        # OneLake token freshness — the universal guard, mirroring the dbt cursor wrapper. A session
        # outliving the token's ~1h life would otherwise 401 on the next delta_scan (read) or delta-rs
        # write. EVERY statement funnels through here, so this is the one place that covers them all.
        # Cheap: refresh_catalog_secret only parses the JWT expiry and re-mints at most once per token
        # lifetime, and is a no-op for local / token-less catalogs.
        self._refresh_tokens()
        # One statement in, one relation out — for EVERY statement, not just DML. Routing keys off the
        # leading verb, so a `;`-batch led by a read (`select 1; create table foo as select 2`) would
        # otherwise slip past the DML router into raw DuckDB and silently make an ephemeral native table.
        # Reject the whole batch up front. Comment-stripped so a `;` inside a comment isn't miscounted.
        if _is_multi_statement(delta_dml._strip_comments(query)):
            raise ValueError(_MULTI_MSG)
        # DESCRIBE DETAIL / DESCRIBE HISTORY — the Delta introspection verbs. DuckDB has plain
        # DESCRIBE (columns) but rejects these, so answer them from the Delta log as a native relation.
        mdesc = _DESCRIBE_EXT_RE.match(_strip_leading(query))
        if mdesc:
            rel = mdesc.group("rel").strip()
            return (self._describe_detail(rel) if mdesc.group("kind").lower() == "detail"
                    else self._describe_history(rel))
        # Explicit transaction control passes through to DuckDB; we only track whether one is open so a
        # Delta write inside it can fail loud below (delta_rs auto-commits — it can't be rolled back).
        mtxn = _TXN_VERB_RE.match(_strip_leading(query))
        if mtxn:
            result = self.con.sql(query)
            self._in_explicit_txn = mtxn.group(1).lower() in ("begin", "start")
            return result
        # Raw DML routes through delta_rs against ONE root. A 3-part target names which catalog that
        # is (``_dml_target_catalog`` → its name); an unqualified/2-part target is the current catalog.
        # Reads (SELECT) are unaffected — DuckDB resolves every attached catalog natively.
        target_cat = _dml_target_catalog(query)
        if target_cat is not None and target_cat not in self._catalogs:
            raise ValueError(
                f"unknown catalog '{target_cat}'; attached catalogs: {list(self._catalogs)}. "
                f"Attach it with conn.attach(path, name='{target_cat}')."
            )
        # The current catalog is DuckDB's own current_database() (the _current_catalog property), so a
        # bare `USE cat.schema` steers write routing exactly as it steers reads — no parallel state.
        write_cat = target_cat if target_cat is not None else self._current_catalog
        is_write = _is_delta_write(query) or _is_vacuum(query) or _is_restore(query)
        # A Delta write auto-commits via delta_rs, so it can't participate in an open explicit
        # transaction — reject it loud rather than let a later ROLLBACK silently fail to undo it.
        if is_write and self._in_explicit_txn:
            raise ValueError(_TXN_WRITE_MSG)
        entry = self._catalogs.get(write_cat)
        # A write whose current/target catalog isn't duckrun-managed (e.g. after `USE memory`, or a
        # DuckDB-native catalog) has no Delta root to land in — fail loud instead of KeyError.
        if is_write and entry is None:
            raise ValueError(
                f"catalog '{write_cat}' is not a duckrun-managed catalog; USE one of "
                f"{list(self._catalogs)}, or qualify the write with catalog.schema.table.")
        # The read-only gate is the *target* catalog's — a read-only attached store fails loud even
        # when the current catalog is writable. _WRITE_KEYWORD_RE covers insert/update/delete/merge.
        if entry is not None and entry.read_only and is_write:
            op = "vacuum (compact) a table" if _is_vacuum(query) else "run write DML"
            raise PermissionError(_READ_ONLY_MSG.format(op=op, catalog=write_cat))
        unsupported = _unsupported_dml(query)
        if unsupported:
            raise ValueError(unsupported)
        query = self._resolve_auto_sort(query)
        # A Delta write is routed with the DuckDB memory_limit clamped (and restored after); a read runs
        # handle() unwrapped — it returns False for non-DML and falls through to DuckDB below. is_write
        # implies entry is not None here (the None case raised above).
        if entry is not None:
            handled = (self._handle_delta_write(entry, query) if is_write else
                       delta_dml.handle(self.con, entry.root_path, entry.storage_options,
                                        query, default_schema=self._current_database))
        else:
            handled = False
        if handled:
            self.refresh(quiet=True, catalog=write_cat)
            return self.con.sql("SELECT 'ok' AS status")
        if _is_delta_write(query):
            raise ValueError(_delta_write_message(query))
        return self.con.sql(query)

    def _resolve_auto_sort(self, query: str) -> str:
        """Resolve ``CREATE TABLE … SORTED BY AUTO AS <query>`` (a duckrun extension) into an explicit
        ``SORTED BY (cols)`` by profiling the query's result with the sort-key recommender, so the
        router only ever handles explicit layout clauses. If nothing pays off, the clause is dropped.
        ``SORTED BY (cols)`` / ``PARTITIONED BY (cols)`` (native DuckDB syntax) and every non-CREATE
        statement pass straight through untouched.

        When the body is a bare ``SELECT * FROM <one delta table>`` (the re-cluster-this-table case),
        the key is profiled EXACTLY from that table's Delta log — real row width, per-column null
        stats, and uniqueness (a unique key is left PLAIN and picked as the ORDER BY key) — which a
        sampled relation profile can't assert. Any filter / projection / join / non-Delta source falls
        back to sampling the result relation, whose distribution isn't a table's on-disk layout."""
        stripped = delta_dml._strip_comments(delta_dml._strip_leading(query)).rstrip().rstrip(";").rstrip()
        m = delta_dml._CREATE_AS.fullmatch(stripped)
        if not m:
            return query
        _rel, sort, _part = delta_dml._split_create_layout(m.group("rel").strip())
        if sort != "AUTO":
            return query
        body = m.group("body")
        src = self._auto_sort_single_table(body)
        cols = (self._auto_sort_cols_from_table(src) if src is not None
                else self._auto_sort_cols(self.con.sql(body)))
        new_body = self._narrow_wide_decimals(body)
        if new_body != body:
            bstart, bend = m.span("body")
            stripped = stripped[:bstart] + new_body + stripped[bend:]
        replacement = ("SORTED BY (" + ", ".join(_qid(c) for c in cols) + ")") if cols else ""
        return delta_dml._SORTED_BY_RE.sub(replacement, stripped, count=1)

    def _narrow_wide_decimals(self, body: str) -> str:
        """Rewrite a ``SELECT *`` body to narrow wide-``DECIMAL`` columns so they regain dictionary
        encoding, or return it unchanged. A ``DECIMAL(p, s)`` with ``p > 18`` maps to a 16-byte
        FIXED_LEN_BYTE_ARRAY, which arrow-rs (the delta-rs writer) NEVER dictionary-encodes — the
        column is written PLAIN even when its value domain is tiny (measured: ~1 GB and a 10× cold
        cliff on the Contoso price columns). Narrowing precision to 18 (scale unchanged) restores
        INT64, hence dictionary/RLE and a cheap transcode. See ``sortkey.decimal_narrow_target``.

        Only ``SELECT *`` bodies are eligible (every output column is a passthrough, so the outer
        ``* REPLACE`` is safe) — any explicit projection, including a user-written CAST (an
        instruction), is left untouched. Gated by ``DUCKRUN_NARROW_DECIMALS`` (default on). Uses the
        EXACT per-column max (one aggregate scan) so the unconditional cast can never overflow at
        write time. Prints one advisory per wide-decimal column (narrowed or kept)."""
        if os.environ.get("DUCKRUN_NARROW_DECIMALS", "1") == "0":
            return body
        if not _SELECT_STAR_BODY.match(body.strip()):
            return body
        try:
            desc = self.con.sql(f"DESCRIBE SELECT * FROM ({body}) _d").fetchall()
        except Exception:
            return body  # un-describable body (e.g. a macro DuckDB can't plan yet) — leave it alone
        wide = []
        for row in desc:
            col, typ = row[0], str(row[1])
            dm = sortkey._DECIMAL_RE.fullmatch(typ.strip())
            if dm and int(dm.group(1)) > 18:
                wide.append((col, typ))
        if not wide:
            return body
        aggs = ", ".join(f"max(abs({_qid(c)}))" for c, _ in wide)
        maxes = self.con.sql(f"SELECT {aggs} FROM ({body}) _m").fetchone()
        max_abs = dict(zip((c for c, _ in wide), maxes))
        repl = []
        for col, typ in wide:
            target = sortkey.decimal_narrow_target(typ, max_abs[col])
            if target:
                repl.append(f"CAST({_qid(col)} AS {target}) AS {_qid(col)}")
                mv = max_abs[col]
                print(f"  {col} {typ} -> {target} "
                      f"(max {mv if mv is not None else 'NULL'}; FLBA has no dictionary in arrow-rs)")
            else:
                print(f"  {col} {typ} kept: max too large to narrow - no dictionary encoding")
        if not repl:
            return body
        return f"SELECT * REPLACE ({', '.join(repl)}) FROM ({body}) _n"

    def _auto_sort_single_table(self, body: str) -> Optional[str]:
        """If ``body`` is a bare ``SELECT * FROM <table>`` over a single duckrun-managed Delta table,
        return that table name so SORTED BY AUTO profiles it exactly from the Delta log; else None (a
        general query — sample its result relation, whose output distribution is unknown)."""
        m = _SELECT_STAR_FROM.fullmatch(body.strip())
        if not m:
            return None
        name = m.group("rel")
        try:
            if self._cat_table_exists(name):
                return name
        except Exception:
            pass
        return None

    def register(self, name: str, obj) -> None:
        """Register an in-memory object (pandas / polars / pyarrow / a DuckDB relation) as ``name`` so
        SQL can read it: ``conn.register("df", df); conn.sql("SELECT * FROM df")``.

        Registration is explicit because a bare ``conn.sql("FROM df")`` can't find a caller-local
        ``df`` — DuckDB's replacement scan only inspects the immediate calling frame, which is this
        method, not the user's. Forwards to DuckDB's
        native ``register``; it's an in-memory view (no Delta write), so a read-only session allows it.
        Persist it with the normal write path: ``conn.sql("CREATE TABLE t AS SELECT * FROM df")``."""
        self.con.register(name, obj)

    def _live_table_exists(self, path: str, so=None) -> bool:
        """True iff a LIVE Delta table exists at ``path``. A drop-tombstone counts as NONEXISTENT — the
        same ``is_dropped`` predicate discovery and the raw-DML router use — so every existence check
        (the writer's error/ignore mode and the reader) agrees a dropped table is
        gone. One oracle, no per-surface duplication."""
        return engine.table_exists(path, so) and not delta_dml.is_dropped(self.con, path, so)

    def _relation_from(self, rows, ddl: str) -> "duckdb.DuckDBPyRelation":
        """Materialise recommender ``rows`` (a list of same-width tuples) as a native relation whose
        columns and types are the ``name type`` fields of ``ddl`` — DuckDB parses the type spellings
        directly (``string`` → VARCHAR, ``int`` → INTEGER, …). The one caller is :meth:`_get_rle`,
        which passes :data:`sortkey._SCHEMA`."""
        fields = [f.strip().split(None, 1) for f in ddl.split(",")]
        names = [f[0] for f in fields]
        types = [f[1] for f in fields]
        if not rows:
            cols = ", ".join(f'CAST(NULL AS {t}) AS "{n}"' for n, t in zip(names, types))
            return self.con.sql(f"SELECT {cols} WHERE 1=0")
        ncols = len(names)
        placeholders = ", ".join("(" + ", ".join(["?"] * ncols) + ")" for _ in rows)
        src = ", ".join(f'"_{i + 1}"' for i in range(ncols))
        proj = ", ".join(f'CAST("_{i + 1}" AS {t}) AS "{n}"'
                         for i, (n, t) in enumerate(zip(names, types)))
        flat = [v for r in rows for v in r]
        return self.con.sql(
            f"SELECT {proj} FROM (VALUES {placeholders}) AS t({src})", params=flat)

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
        directory tree. Storage-neutral: files stream via obstore over the same ``storage_options``
        ``connect()`` already holds — no extra auth. ``remote_folder`` is relative to the lakehouse
        Files section on OneLake (or a full ``…://`` URL); ``file_extensions`` filters by suffix
        (``['.csv', '.parquet']``); ``overwrite=False`` (default) skips files already present remotely.
        Returns ``True`` on success.

        Each file is streamed from an open handle (multipart PUT for large files), so this handles
        multi-GB blobs, not just ordinary files."""
        exts = _norm_exts(file_extensions)
        base = self._files_base(remote_folder)
        pairs = []
        for dirpath, _dirs, names in os.walk(local_folder):
            for n in names:
                if exts and os.path.splitext(n)[1].lower() not in exts:
                    continue
                local_path = os.path.join(dirpath, n)
                rel = os.path.relpath(local_path, local_folder).replace("\\", "/")
                pairs.append((local_path, rel))
        if not pairs:
            print(f"[warn] no files to upload from '{local_folder}'"
                  + (f" (filtered by {file_extensions})" if exts else ""))
            return True
        print(f"Uploading {len(pairs)} file(s) to '{base}'...")
        store = objectstore.build_store(base, secret.refreshed(self.storage_options))
        for local_path, rel in pairs:
            if not overwrite and objectstore.exists(store, rel):
                print(f"  [skip] exists: {base}/{rel}")
                continue
            objectstore.upload(store, rel, local_path)
            print(f"  [ok] {local_path} -> {base}/{rel}")
        print("upload complete")
        return True

    def download(self, remote_folder: str = "", local_folder: str = "./downloaded_files",
                 file_extensions: Optional[List[str]] = None, overwrite: bool = False) -> bool:
        """Download every file under ``remote_folder`` to ``local_folder``, preserving the directory
        tree. The mirror of :meth:`copy`: remote files are enumerated and streamed back via obstore
        over the existing ``storage_options``. ``remote_folder`` is relative to the OneLake Files
        section (or a full ``…://`` URL); ``file_extensions`` filters by suffix; ``overwrite=False``
        (default) skips files already present locally. Returns ``True`` on success."""
        exts = _norm_exts(file_extensions)
        base = self._files_base(remote_folder)
        store = objectstore.build_store(base, secret.refreshed(self.storage_options))
        pairs = [(key, os.path.join(local_folder, *key.split("/")))
                 for key in objectstore.list_keys(store)
                 if not exts or os.path.splitext(key)[1].lower() in exts]
        if not pairs:
            print(f"[warn] no files to download from '{base}'"
                  + (f" (filtered by {file_extensions})" if file_extensions else ""))
            return True
        print(f"Downloading {len(pairs)} file(s) to '{local_folder}'...")
        for key, local_path in pairs:
            if not overwrite and os.path.exists(local_path):
                print(f"  [skip] exists: {local_path}")
                continue
            objectstore.download(store, key, local_path)
            print(f"  [ok] {base}/{key} -> {local_path}")
        print("download complete")
        return True

    def list_files(self, remote_folder: str = "",
                   file_extensions: Optional[List[str]] = None) -> List[str]:
        """List the files under ``remote_folder`` on the store, as **relative path strings** (e.g.
        ``['csv/daily/a.csv.gz', 'csv/log.parquet']``) — the companion to :meth:`copy`/:meth:`download`
        (the returned paths feed straight back into ``download``). ``remote_folder`` is relative to the
        OneLake Files section (or a full ``…://`` URL); ``file_extensions`` filters by suffix. Recurses.
        Storage-neutral: obstore lists every backend (local / s3 / gcs / az / OneLake) natively."""
        exts = _norm_exts(file_extensions)
        base = self._files_base(remote_folder)
        store = objectstore.build_store(base, secret.refreshed(self.storage_options))
        return [key for key in objectstore.list_keys(store)
                if not exts or os.path.splitext(key)[1].lower() in exts]

    def convert_to_delta(self, identifier: str, partition_schema=None) -> str:
        """Convert an existing parquet directory to Delta **in place, zero-copy** — a ``_delta_log`` is
        written over the parquet, the data files are not rewritten. Unlike the table verbs this is a
        session op: the table doesn't exist yet, there's nothing to fence. ``identifier`` is the
        delta-spark form ``"parquet.`<path>`"`` (a bare ``<path>`` is also accepted); ``partition_schema``
        is a pyarrow ``Schema`` of the Hive-partition columns for a partitioned dir, or ``None``. Returns
        the converted path; ``conn.refresh()`` then surfaces it
        as a discoverable table when it sits under a catalog root. Storage-neutral (local / s3 / gs / az /
        OneLake) — uses the session's already-minted credentials."""
        from .delta_table import _parse_parquet_identifier
        path = _parse_parquet_identifier(identifier).replace("\\", "/").rstrip("/")
        self._require_writable("convert parquet to Delta")
        engine.convert_to_delta(path, self.storage_options, partition_by=partition_schema)
        return path

    def get_stats(self, source: Optional[str] = None, detailed: bool = False) -> "duckdb.DuckDBPyRelation":
        """Delta table statistics — the "why is my table slow / full of small files" view. Returns a
        native DuckDB relation, one row per table (``detailed=False``) or one row per parquet **row
        group** (``detailed=True``, the raw ``parquet_metadata`` columns).

        ``source``: ``None`` → every table across all attached catalogs; a table name (1/2/3-part)
        → that table; a schema name → every matching table in it (any catalog); a wildcard pattern
        (``fct_*``, ``mart.fct_*``, ``*summary*``) → every matching table across all attached
        catalogs. Aggregated columns: ``catalog, schema, table,
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
        return self.con.sql(" UNION ALL ".join(parts))

    def _describe_detail(self, name: str) -> "duckdb.DuckDBPyRelation":
        """``describe detail <table>`` — the Delta table's ``format`` / ``id`` / ``name`` /
        ``location`` (its storage path) / ``partitionColumns`` / ``numFiles`` / ``sizeInBytes`` /
        ``version``, read from the Delta log. One row, as a native relation."""
        cat, sch, tbl = self._resolve(name)
        entry = self._catalogs[cat]
        path = f"{entry.root_path}/{sch}/{tbl}"
        dt = engine._delta_table(path, entry.storage_options)
        md = dt.metadata()
        files, size_bytes, _ = engine.delta_file_summary(self.con, path, entry.storage_options)
        parts = list(md.partition_columns or [])
        part_lit = "[" + ", ".join(f"'{_qlit(p)}'" for p in parts) + "]"
        return self.con.sql(
            f"SELECT 'delta' AS format, '{_qlit(str(md.id))}' AS id, '{_qlit(f'{sch}.{tbl}')}' AS name, "
            f"'{_qlit(path)}' AS location, {part_lit} AS \"partitionColumns\", "
            f"{len(files)} AS \"numFiles\", {int(size_bytes)} AS \"sizeInBytes\", "
            f"{int(dt.version())} AS version")

    def _describe_history(self, name: str) -> "duckdb.DuckDBPyRelation":
        """``describe history <table>`` — one row per Delta commit (``version``, ``timestamp``,
        ``operation``, ``operationMetrics``), newest first, read from the Delta log."""
        cat, sch, tbl = self._resolve(name)
        entry = self._catalogs[cat]
        path = f"{entry.root_path}/{sch}/{tbl}"
        dt = engine._delta_table(path, entry.storage_options)
        rows = []
        for h in dt.history():
            ver = h.get("version")
            ts = h.get("timestamp")
            op = _qlit(str(h.get("operation", "")))
            metrics = _qlit(str(h.get("operationMetrics", "") or ""))
            ver_expr = str(int(ver)) if ver is not None else "NULL"
            ts_expr = f"epoch_ms({int(ts)})" if ts is not None else "NULL::timestamp"
            rows.append(f"({ver_expr}, {ts_expr}, '{op}', '{metrics}')")
        if not rows:
            return self.con.sql(
                "SELECT NULL::bigint AS version, NULL::timestamp AS timestamp, "
                "NULL::varchar AS operation, NULL::varchar AS \"operationMetrics\" WHERE 1=0")
        body = ", ".join(rows)
        return self.con.sql(
            f'SELECT * FROM (VALUES {body}) AS h(version, timestamp, operation, "operationMetrics") '
            f"ORDER BY version DESC")


    def _resolve_stats_targets(self, source: Optional[str]) -> List[tuple]:
        """Resolve a ``get_stats`` source to ``(catalog, schema, table)`` targets: ``None`` → every
        table across all attached catalogs; a known table (1/2/3-part) → itself; a schema name →
        its tables in every catalog that has it; a wildcard pattern (``*``/``?``/``[...]``, e.g.
        ``fct_*`` or ``mart.fct_*``) → every matching table across all attached catalogs."""
        if source is None:
            return [(c, s, t) for c in self._catalogs
                    for s in self._cat_databases(c) for t in self._cat_tables(s, c)]
        if any(ch in source for ch in "*?["):
            return self._glob_stats_targets(source)
        if self._cat_table_exists(source):
            return [self._resolve(source)]
        if "." not in source:                          # a bare schema name, in any attached catalog
            self.refresh(quiet=True)                   # re-discover schema folders first
            hits = [(c, source, t) for c in self._catalogs
                    if source in self._cat_databases(c) for t in self._cat_tables(source, c)]
            if hits:
                return hits
        raise ValueError(
            f"get_stats: '{source}' is neither a known table nor a schema in any attached catalog "
            f"({list(self._catalogs)}).")

    def _glob_stats_targets(self, source: str) -> List[tuple]:
        """Wildcard-match ``source`` (fnmatch ``*``/``?``/``[...]``, case-insensitive) against tables
        across all attached catalogs. Accepts ``table``, ``schema.table``, or
        ``catalog.schema.table`` patterns; a missing leading segment defaults to ``*`` (matches any
        schema / any catalog). Returns the matched ``(catalog, schema, table)`` targets (possibly
        empty — the caller reports the miss)."""
        import fnmatch
        parts = delta_dml._split_dotted(source)  # quote-aware split (matches _resolve / the router)
        if len(parts) == 1:
            cat_pat, schema_pat, table_pat = "*", "*", parts[0]
        elif len(parts) == 2:
            cat_pat, schema_pat, table_pat = "*", parts[0], parts[1]
        elif len(parts) == 3:
            cat_pat, schema_pat, table_pat = parts
        else:
            raise ValueError(
                f"get_stats: too many segments in pattern {source!r} "
                f"(use table, schema.table, or catalog.schema.table).")
        cp, sp, tp = cat_pat.lower(), schema_pat.lower(), table_pat.lower()
        out = []
        for cat in self._catalogs:
            if not fnmatch.fnmatchcase(cat.lower(), cp):
                continue
            for sch in self._cat_databases(cat):
                if not fnmatch.fnmatchcase(sch.lower(), sp):
                    continue
                for tbl in self._cat_tables(sch, cat):
                    if fnmatch.fnmatchcase(tbl.lower(), tp):
                        out.append((cat, sch, tbl))
        return out

    def _get_rle(self, table: str, sort_key_cap: int = 4, min_gain_pct: float = 1.0,
                 key_sort_below_pct: float = 10.0, null_excl: float = 0.5,
                 fd_band: float = 0.12, grain_frac: float = 0.5,
                 seed: Optional[int] = None) -> "duckdb.DuckDBPyRelation":
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
        if not self._cat_table_exists(table):
            if "." not in table and self._cat_database_exists(table):
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
        return self._relation_from(rows, schema)

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
        recs = [dict(zip(prof.columns, row)) for row in prof.fetchall()]
        return [r["column"] for r in sorted((x for x in recs if x["in_sort_key"]),
                                            key=lambda x: x["sort_position"])]

    def close(self):
        """Close the underlying DuckDB connection. The session is unusable afterwards — registered
        views and the minted secret go with the connection."""
        self.con.close()

    def __enter__(self) -> "DuckSession":
        return self

    def __exit__(self, exc_type, exc, tb):
        """Close the connection on ``with`` exit — ``with duckrun.connect(...) as conn:``."""
        self.close()
        return False

    @property
    def _connection(self):
        """The underlying DuckDB connection (internal escape hatch)."""
        return self.con


def connect(path: str, storage_options: Optional[Dict[str, str]] = None,
            schema: Optional[str] = None,
            read_only: bool = True, name: Optional[str] = None) -> DuckSession:
    """Open a storage-neutral, SQL-only session over a Delta lakehouse.

    The session binds to this one lakehouse root as the primary catalog. Catalog is first-class:
    tables are addressed ``catalog.schema.table`` (``schema.table`` / ``table`` resolve in the current
    catalog), so single- and multi-catalog sessions share one code path. Attach more lakehouses with
    :meth:`DuckSession.attach` to query across them.

    Args:
        path: the lakehouse root, or (OneLake) ``…/Tables`` or ``…/Tables/<schema>``. Works with a
            local path, ``s3://``, ``gs://``, ``az://``, or OneLake ``abfss://``. OneLake also takes
            the shorthand ``<workspace>/<item>[/<schema>]`` — ``"ws/sales.Lakehouse"`` or
            ``"<ws-guid>/<lakehouse-guid>"`` — expanded to the full ``abfss://…/Tables`` URL. Only
            those two shapes are shorthand: a suffix-less ``"ws/lh"`` is still a local relative path.
        storage_options: forwarded to delta-rs (and used to mint DuckDB secrets). For OneLake you
            can omit it inside a Fabric notebook — a token is acquired automatically.
        schema: restrict to a single schema. Omit to discover every schema folder.
        read_only: **default True** — the session refuses every Delta write (INSERT / UPDATE
            / DELETE / MERGE / REPLACE WHERE / CREATE / DROP / VACUUM) with a ``PermissionError``, so
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
        >>> w.sql("CREATE OR REPLACE TABLE orders_copy AS SELECT * FROM orders")
        >>> lh = duckrun.connect("…/<guid>/Tables", name="lakehouse")   # name a GUID-path catalog
        >>> s = duckrun.connect("ws/sales.Lakehouse/dbo")           # OneLake shorthand, catalog 'sales'
        >>> g = duckrun.connect("<ws-guid>/<lakehouse-guid>")       # GUID shorthand, catalog 'data'
    """
    check_runtime_versions()  # fail loud if Fabric's stale duckdb/deltalake are still loaded
    return DuckSession(path, storage_options, schema, read_only, name)
