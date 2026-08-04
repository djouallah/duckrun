"""Debug a dbt model from a notebook and get a DuckDB relation back (issue #29).

    from duckrun import dbt_project

    p = dbt_project("dbt/", target="dev")

    p.show("orders_enriched")        # DuckDBPyRelation — real types, lazy
    p.sql("select * from {{ ref('stg_orders') }} where year = 2026")
    p.compiled("orders_enriched")    # the SQL text

``dbt show`` serializes through agate to JSON, so a model's schema is gone by the time the rows
arrive: decimals come back as strings or floats, dates as strings, and the dataframe library has to
guess a type per column. It is also eager — ``--limit -1`` pulls the whole result through JSON
before you can filter it. Neither is fixable downstream; the type information was discarded two
layers earlier.

duckrun does not have that problem, because the adapter runs DuckDB in-process: a model's compiled
SQL is ordinary DuckDB SQL whose ``ref()``s resolve to ``delta_scan`` views. dbt never has to
EXECUTE anything to give you a debuggable result — it only has to COMPILE. So this module compiles
with dbt and executes on duckrun, and what comes back is a ``DuckDBPyRelation``: real types end to
end, and lazy, so ``rel.filter("customer = 'X'").limit(100)`` pushes into the ``delta_scan``
instead of materializing the model first.

Three things about how this is wired, each of which was a decision rather than an accident:

**The connection is the run path minus the write.** It is never reconstructed from a
``duckrun.connect()`` session with hand-aligned catalog names — that is a copy of the adapter that
can silently drift from it, and in a multi-Lakehouse project (``catalogs:``) the drift is invisible
until a read comes back from the wrong Lakehouse. Instead the session runs on a
``DuckrunEnvironment`` built from the profile, reusing the very one dbt just used when it is
compatible, so secrets, ATTACHes, catalog aliases and the lazy bind are identical to a real run by
construction.

**Read-only is structural.** The cursor is a ``DuckrunDebugCursor``, which has no route to delta_rs
anywhere in its class hierarchy — see environment.py. A debug session built from the real profile
otherwise sits one typo away from a production write.

**A stale manifest is never served.** Keeping the parsed manifest warm matters for notebook
ergonomics (a re-parse costs seconds on a real project), but a warm manifest hands back the SQL as
it was parsed — so after you edit a model, ``show()`` would show you the version from before your
edit, silently. That is the worst possible failure for a debugging tool, so the project's files are
mtime-checked on every call (single-digit milliseconds) and re-parsed only when something changed.
"""
import contextlib
import io
import os
import time
from pathlib import Path

__all__ = ["dbt_project", "DbtProject", "CompileResult", "DbtProjectError"]

# Suffixes whose edit invalidates a parsed manifest: models and macros (.sql), schema/config/profile
# (.yml/.yaml), seeds (.csv), docs blocks (.md) and python models (.py).
_WATCHED_SUFFIXES = frozenset({".sql", ".yml", ".yaml", ".csv", ".md", ".py"})
# Directories that change constantly without changing what dbt would parse. `target/` in particular
# is rewritten by every compile, so watching it would make the project look edited on every call.
_SKIP_DIRS = frozenset({"target", "logs", "dbt_packages", ".git", "__pycache__", ".venv", ".ipynb_checkpoints"})
# dbt writes `.user.yml` (its anonymous-usage id) into the PROFILES directory — which, in a duckrun
# project and in most Fabric notebooks, is the project directory. dbt's own first parse creates it,
# so without this the very next call sees "the project changed" and re-parses for nothing.
_SKIP_FILES = frozenset({".user.yml"})


class DbtProjectError(RuntimeError):
    """A dbt command issued by the debug session failed, or the project/profile is unusable."""


class CompileResult:
    """What the last compile actually did.

    ``show()`` deliberately returns the bare ``DuckDBPyRelation`` — it is what you want to filter
    and convert, and wrapping it would tax every call. But a relation is a DuckDB object with
    nowhere to hang metadata, so the answers to "what did I just run, and against which branch" live
    here instead, on ``DbtProject.last_compile``.
    """

    __slots__ = ("model", "node_id", "sql", "full_refresh", "cte")

    def __init__(self, model, node_id, sql, full_refresh, cte=None):
        self.model = model
        self.node_id = node_id
        #: The SQL that actually RAN — for a ``cte()`` call that is the sliced query, not the
        #: model's full compiled text, so this always answers "what did I just execute".
        self.sql = sql
        #: True when the compile was forced down the ``is_incremental() == False`` branch.
        self.full_refresh = full_refresh
        #: The CTE this was sliced down to, or None for a whole model.
        self.cte = cte

    def __repr__(self):
        extra = f", cte={self.cte!r}" if self.cte else ""
        extra += ", full_refresh=True" if self.full_refresh else ""
        return f"<CompileResult {self.model!r}{extra}, {len(self.sql or '')} chars of SQL>"


class DbtProject:
    """A read-only debug session over a dbt project. Build one with :func:`dbt_project`."""

    def __init__(self, project_dir=".", target=None, profiles_dir=None):
        self.project_dir = Path(project_dir).expanduser().resolve()
        self.target = target
        self.profiles_dir = self._resolve_profiles_dir(profiles_dir)
        #: :class:`CompileResult` for the most recent compile, or None.
        self.last_compile = None
        self._manifest = None
        self._signature = None
        self._compiled = {}
        self._creds = None
        self._env = None
        self._handle = None
        self._cursor = None
        self._check_project()

    def __repr__(self):
        state = "parsed" if self._manifest is not None else "not parsed yet"
        target = f", target={self.target!r}" if self.target else ""
        return f"<DbtProject {str(self.project_dir)!r}{target} — {state}>"

    # ── public API ─────────────────────────────────────────────────────────────────────────────

    def compiled(self, model, incremental=None) -> str:
        """The compiled SQL text of ``model``.

        ``model`` is passed to dbt as a selector, untouched — so ``"orders"``,
        ``"path:models/marts/orders.sql"`` and any other dbt selector syntax all work, and stay
        correct as dbt's selection grows. Reimplementing node selection here would be a second
        implementation to keep in sync with dbt's, for no gain.

        ``incremental=False`` forces the ``is_incremental() == False`` branch (dbt's
        ``--full-refresh``), so both branches of a branching model can be compared side by side
        instead of guessed at. ``None`` (the default) lets dbt decide, exactly as a run would."""
        return self._compile(selector=model,
                             full_refresh=self._full_refresh_for(incremental)).sql

    def show(self, model, incremental=None):
        """``model`` as a ``DuckDBPyRelation`` — real DuckDB types, and lazy.

        Nothing is read until you consume the relation, so ``p.show("orders").filter("id = 7")``
        pushes the filter into the ``delta_scan`` rather than materializing the model first."""
        return self._relation(self.compiled(model, incremental=incremental))

    def sql(self, query):
        """Ad-hoc SQL with ``{{ ref() }}`` / ``{{ source() }}`` rendered, as a relation.

        Compiled by dbt itself (``dbt compile --inline``), so refs resolve exactly the way they do
        in a model — including ephemeral models, which have no standalone compiled form but are
        injected as CTEs into whatever selects from them."""
        return self._relation(self._compile(inline=query).sql)

    def ctes(self, model, incremental=None) -> list:
        """The names of ``model``'s CTEs, in the order they are defined.

        Read off the COMPILED SQL, not the model source, so it also lists what you cannot see in
        the file: CTEs a macro generated, and ephemeral models, which dbt injects as
        ``__dbt__cte__<name>``."""
        from dbt.adapters.duckrun.delta_dml import split_cte_list
        sql = self.compiled(model, incremental=incremental)
        return [name for name, _ in split_cte_list(sql)[1]]

    def cte(self, model, name, incremental=None):
        """Run ``model`` only as far as the CTE ``name``, as a relation.

        When a model runs clean but returns nonsense, the move is always the same: take the CTEs one
        at a time and find where the row count or a key goes wrong. Done by hand that means copying
        blocks out of the compiled SQL, twenty times, getting it slightly wrong once. This is the
        same rewrite mechanically — keep the WITH list up to and including ``name``, then select
        from it — with real types at every intermediate step, which is what makes comparing two
        steps trustworthy.

        The CTE text is spliced from the compiled SQL verbatim; nothing is re-parsed or
        re-generated, so what runs is character-for-character what dbt produced."""
        sql = self.compiled(model, incremental=incremental)
        sliced = self._slice_to_cte(sql, name, model)
        self.last_compile.sql = sliced       # last_compile describes what RAN, not what was compiled
        self.last_compile.cte = name
        return self._relation(sliced)

    @staticmethod
    def _slice_to_cte(sql, name, model) -> str:
        from dbt.adapters.duckrun.delta_dml import split_cte_list

        head, parts, _ = split_cte_list(sql)
        names = [n for n, _ in parts]
        if not names:
            raise DbtProjectError(
                f"{model!r} has no CTEs — its compiled SQL is a single SELECT. Use show() instead.")
        if name not in names:
            raise DbtProjectError(
                f"{model!r} has no CTE {name!r}. It has: {', '.join(names)}")
        kept = parts[:names.index(name) + 1]
        # Later CTEs are dropped rather than left in place. DuckDB would not evaluate them anyway —
        # it does not even bind an unused CTE — so this is for what you READ back in
        # last_compile.sql: exactly the query that ran, and nothing that did not.
        quoted = '"' + name.replace('"', '""') + '"'
        return head + ",".join(text for _, text in kept) + f"\nselect * from {quoted}"

    def reload(self):
        """Re-parse the project now. Rarely needed — an edit is detected automatically — but useful
        after something the mtime check cannot see, such as an ``env_var`` changing in the kernel."""
        self._invalidate()
        self._ensure_manifest()
        return self

    # ── project / profile validation (cheap, and without touching dbt internals) ────────────────

    def _resolve_profiles_dir(self, explicit):
        """Where ``profiles.yml`` lives: what the caller said, else next to ``dbt_project.yml``
        (how duckrun's own projects and most Fabric notebooks are laid out), else dbt's usual
        ``DBT_PROFILES_DIR`` / ``~/.dbt``. Resolved here so both the eager check below and every
        dbt invocation use one answer."""
        if explicit is not None:
            return Path(explicit).expanduser().resolve()
        if (self.project_dir / "profiles.yml").is_file():
            return self.project_dir
        env = os.environ.get("DBT_PROFILES_DIR")
        return Path(env).expanduser().resolve() if env else Path.home() / ".dbt"

    def _check_project(self):
        """Fail on a wrong directory or a typo'd target NOW, at construction, rather than several
        cells later inside a ``show()`` — where it would be indistinguishable from the model being
        broken, which is the thing you are actually trying to debug.

        Deliberately a plain YAML read rather than dbt's own profile loader: that loader needs dbt's
        process-global flags and invocation context, which only a dbtRunner invoke sets up, and
        bootstrapping them by hand would be a copy of dbt/cli/requires.py that breaks the day dbt
        reorders it. Nothing is rendered here — we only check that keys exist — so this cannot drift
        from dbt's interpretation of the file. The real, rendered credentials come later, from the
        parse. Anything unreadable is skipped: this is a fast path to a good error message, never a
        gate."""
        if not (self.project_dir / "dbt_project.yml").is_file():
            raise DbtProjectError(
                f"no dbt_project.yml in {self.project_dir}\n"
                "  dbt_project() takes the project directory (the one holding dbt_project.yml)."
            )
        try:
            import yaml
            project = yaml.safe_load((self.project_dir / "dbt_project.yml").read_text(
                encoding="utf-8")) or {}
            profile_name = project.get("profile")
            profiles_path = self.profiles_dir / "profiles.yml"
            if not profile_name or not profiles_path.is_file():
                return
            profiles = yaml.safe_load(profiles_path.read_text(encoding="utf-8")) or {}
            entry = profiles.get(profile_name)
            if not isinstance(entry, dict):
                raise DbtProjectError(
                    f"profiles.yml in {self.profiles_dir} has no profile {profile_name!r} "
                    f"(dbt_project.yml asks for it). Found: {', '.join(sorted(profiles)) or 'nothing'}"
                )
            outputs = entry.get("outputs") or {}
            wanted = self.target or entry.get("target")
            if wanted and outputs and wanted not in outputs:
                raise DbtProjectError(
                    f"profile {profile_name!r} has no target {wanted!r}. "
                    f"Valid targets: {', '.join(sorted(outputs))}"
                )
        except DbtProjectError:
            raise
        except Exception:
            return  # best effort only — the parse will report anything real

    # ── manifest lifecycle ─────────────────────────────────────────────────────────────────────

    def _signature_now(self):
        """A cheap fingerprint of everything dbt would parse. Measured at ~2 ms for a small project
        and single-digit ms for a large one — far below the cost of the re-parse it avoids, and far
        below the cost of debugging against SQL that no longer matches the file on disk."""
        out = []
        for dirpath, dirnames, filenames in os.walk(self.project_dir):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
            for name in filenames:
                if name in _SKIP_FILES:
                    continue
                if Path(name).suffix.lower() in _WATCHED_SUFFIXES:
                    p = os.path.join(dirpath, name)
                    try:
                        st = os.stat(p)
                    except OSError:
                        continue
                    out.append((p, st.st_mtime_ns, st.st_size))
        profiles = self.profiles_dir / "profiles.yml"
        if profiles.is_file() and self.profiles_dir != self.project_dir:
            st = profiles.stat()
            out.append((str(profiles), st.st_mtime_ns, st.st_size))
        return tuple(sorted(out))

    def _invalidate(self):
        """Drop everything derived from the manifest. The cursor goes too: a changed profile means
        different roots, secrets and ATTACHes, and silently reading through the old connection is
        exactly the class of stale answer this session exists to prevent."""
        if self._handle is not None:
            with contextlib.suppress(Exception):
                self._handle.close()   # our own child cursor; the shared connection stays up
        self._manifest = self._signature = self._creds = None
        self._compiled = {}
        self._env = self._handle = self._cursor = None

    def _changed_since(self, signature):
        """Up to three file names that differ from ``self._signature``. Naming them is what makes
        the re-parse message trustworthy: an unexplained "files changed" on a call where you
        changed nothing reads as a bug, and one that names ``target/…`` would BE one."""
        was = {path: stamp for path, *stamp in (self._signature or ())}
        now = {path: stamp for path, *stamp in signature}
        names = sorted({os.path.basename(p) for p in set(was) ^ set(now)}
                       | {os.path.basename(p) for p in set(was) & set(now) if was[p] != now[p]})
        return ", ".join(names[:3]) + (f" (+{len(names) - 3} more)" if len(names) > 3 else "")

    def _ensure_manifest(self):
        signature = self._signature_now()
        if self._manifest is not None and signature == self._signature:
            return
        why = "first use" if self._manifest is None else self._changed_since(signature) + " changed"
        self._invalidate()
        started = time.perf_counter()
        result = self._invoke(["parse"])
        self._manifest = result.result
        self._signature = signature
        print(f"[duckrun] dbt project parsed ({why}, {time.perf_counter() - started:.1f}s)")

    # ── running dbt ────────────────────────────────────────────────────────────────────────────

    def _invoke(self, args, manifest=None):
        """One in-process dbt command against this project.

        dbt's own output is captured rather than printed: ``dbt compile`` prints the whole compiled
        node to stdout, which in a notebook buries the relation you asked for under the SQL you
        didn't. It is re-emitted only when the command fails, where it is the diagnosis."""
        from dbt.cli.main import dbtRunner

        argv = [*args, "--project-dir", str(self.project_dir),
                "--profiles-dir", str(self.profiles_dir)]
        if self.target:
            argv += ["--target", self.target]
        runner = dbtRunner(manifest=manifest) if manifest is not None else dbtRunner()
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            result = runner.invoke(argv)
        if not result.success:
            detail = result.exception if result.exception is not None else ""
            raise DbtProjectError(
                f"dbt {' '.join(args)} failed\n{detail}\n{captured.getvalue().strip()}".strip())
        return result

    def _full_refresh_for(self, incremental):
        if incremental is None:
            return False
        if incremental is False:
            return True
        raise DbtProjectError(
            "incremental=True cannot be forced: is_incremental() is true whenever the target table "
            "exists and --full-refresh was not passed, so it is already the default here. Use "
            "incremental=False to compile the full-refresh branch instead."
        )

    def _compile(self, selector=None, inline=None, full_refresh=False) -> CompileResult:
        """Compile one node, at most once per manifest generation, and record what it was.

        The cache is a CORRECTNESS requirement, not a speed-up — though it is also that. dbt marks a
        node ``extra_ctes_injected`` once it has spliced an ephemeral parent in as a CTE, so a
        SECOND compile against the same manifest object regenerates ``compiled_code`` from
        ``raw_code`` and does not re-inject. The result still REFERENCES ``__dbt__cte__<parent>``
        while no longer defining it: silently broken SQL, from the second call onward, for any
        project with ephemeral models. Compiling once per generation sidesteps that entirely instead
        of reaching into dbt to reset the flag.

        Safe to cache because ``_ensure_manifest`` runs first: any edit drops the manifest and this
        cache with it, so a hit can only ever be SQL that is still current."""
        self._ensure_manifest()
        key = (selector, inline, full_refresh)
        if key not in self._compiled:
            self._compiled[key] = self._run_compile(selector, inline, full_refresh)
        name, node_id, sql = self._compiled[key]
        # A fresh CompileResult per call: cte() rewrites .sql on it, and that must not reach back
        # into the cache and corrupt the next caller's answer.
        self.last_compile = CompileResult(
            model=name, node_id=node_id, sql=sql, full_refresh=full_refresh)
        return self.last_compile

    def _run_compile(self, selector, inline, full_refresh):
        args = ["compile"]
        args += ["--inline", inline] if inline is not None else ["--select", selector]
        if full_refresh:
            args += ["--full-refresh"]
        result = self._invoke(args, manifest=self._manifest)

        nodes = [r.node for r in (getattr(result.result, "results", None) or [])
                 if getattr(r, "node", None) is not None]
        if not nodes:
            raise DbtProjectError(
                f"the selector {selector!r} matched no model. "
                "It is passed to dbt untouched, so any dbt selector works — "
                "'my_model', 'path:models/marts/my_model.sql', 'tag:daily', …")
        if len(nodes) > 1:
            names = ", ".join(sorted(getattr(n, "name", "?") for n in nodes))
            raise DbtProjectError(
                f"the selector {selector!r} matched {len(nodes)} nodes: {names}\n"
                "  show() needs exactly one — narrow the selector.")
        node = nodes[0]
        return getattr(node, "name", selector), getattr(node, "unique_id", None), node.compiled_code

    # ── the connection ─────────────────────────────────────────────────────────────────────────

    def _credentials(self):
        """The profile's rendered credentials.

        Only ever called after :meth:`_ensure_manifest`, and that ordering is load-bearing:
        ``load_profile`` reads dbt's process-global flags and invocation context, which are set by a
        dbtRunner invoke and are absent in a fresh kernel. The parse is what sets them up."""
        if self._creds is None:
            from dbt.config.runtime import load_profile
            self._creds = load_profile(
                str(self.project_dir), {}, profile_name_override=None,
                target_override=self.target).credentials
        return self._creds

    def _environment(self):
        """The ``DuckrunEnvironment`` to run on — dbt's own when it fits, else a fresh one.

        Reuse is not just an optimization. Every DuckDB instance pins ``memory_limit`` to a large
        share of available RAM, so a second environment alongside dbt's means two databases each
        claiming most of the machine — an OOM in a Fabric notebook. And dbt's ``_ENV`` after a
        compile IS a ``DuckrunEnvironment`` built from this profile, so reusing it is the most
        literal reading of "build the connection from the profile through DuckrunEnvironment":
        it is not a lookalike of the run's connection, it is that connection."""
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
        from dbt.adapters.duckrun.environment import DuckrunEnvironment

        creds = self._credentials()
        env = getattr(DuckDBConnectionManager, "_ENV", None)
        if (isinstance(env, DuckrunEnvironment) and getattr(env, "conn", None) is not None
                and env.creds == creds):
            return env
        return DuckrunEnvironment(creds)

    def _relation(self, sql):
        """Run already-compiled SQL on the read-only cursor. No freshness check here: the compile
        that produced ``sql`` just did one, and re-checking could only re-parse for a change that
        this SQL predates — a confusing message and a dropped connection for no gain."""
        if self._cursor is None:
            # Both env and handle are held: a collected env closes its connection out from under
            # the cursor, and the handle is what we close again on invalidate.
            self._env = self._environment()
            self._handle = self._env.debug_handle()
            self._cursor = self._handle.cursor()
        return self._cursor.sql(sql)


def dbt_project(project_dir=".", target=None, profiles_dir=None) -> DbtProject:
    """Open a read-only debug session over a dbt project.

    ``project_dir`` is the directory holding ``dbt_project.yml`` (default: the current one).
    ``target`` overrides the profile's default target. ``profiles_dir`` is only needed when
    ``profiles.yml`` sits neither in the project directory nor in the usual dbt location.

    Returns immediately: the directory, profile and target are checked right away, but the parse —
    the part that costs seconds on a real project — is deferred to the first ``show()`` / ``sql()``
    / ``compiled()`` call, and then kept warm until you edit something."""
    return DbtProject(project_dir=project_dir, target=target, profiles_dir=profiles_dir)
