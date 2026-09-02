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

**A ``view`` model is exposed as a view.** duckrun persists only Delta tables, so a `view` model
exists only inside the session that built it — and a cold debug session would find nothing under
that name, making everything downstream of a staging layer unreadable. The manifest already says
which parents are views and dbt can compile them, so they are registered as DuckDB views before the
query runs, in dependency order, by default rather than in reaction to a catalog error. A view, not
a materialization: the relation stays lazy either way.

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

from dbt.adapters.duckrun.engine import quote_ident

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
# dbt's own prefix for an ephemeral model injected as a CTE. Not configurable in dbt, and the same
# string its compiled SQL carries — so matching on it is reading dbt's output, not guessing a name.
_EPHEMERAL_CTE = "__dbt__cte__"


class DbtProjectError(RuntimeError):
    """A dbt command issued by the debug session failed, or the project/profile is unusable."""


def _materialization(node) -> str:
    return getattr(getattr(node, "config", None), "materialized", None) or ""


class CompileResult:
    """What the last compile actually did.

    ``show()`` deliberately returns the bare ``DuckDBPyRelation`` — it is what you want to filter
    and convert, and wrapping it would tax every call. But a relation is a DuckDB object with
    nowhere to hang metadata, so the answers to "what did I just run, and against which branch" live
    here instead, on ``DbtProject.last_compile``.
    """

    __slots__ = ("model", "node_id", "sql", "full_refresh", "cte", "incremental", "parents")

    def __init__(self, model, node_id, sql, full_refresh, cte=None, incremental=None, parents=()):
        self.model = model
        self.node_id = node_id
        #: The SQL that actually RAN — for a ``cte()`` call that is the sliced query, not the
        #: model's full compiled text, so this always answers "what did I just execute".
        self.sql = sql
        #: True when the compile was forced down the ``is_incremental() == False`` branch.
        self.full_refresh = full_refresh
        #: The CTE this was sliced down to, or None for a whole model.
        self.cte = cte
        #: Which ``is_incremental()`` branch this SQL came from — True or False — or None when the
        #: model does not branch on it at all and there is therefore nothing to be ambiguous about.
        self.incremental = incremental
        #: The compiled node's direct manifest parents. For an inline ``sql()`` compile the node
        #: itself is gone from the manifest by the time anything runs (dbt removes it on success,
        #: :meth:`DbtProject._drop_inline_node` on failure), so these are what view-ancestor
        #: binding starts from.
        self.parents = tuple(parents or ())

    def __repr__(self):
        extra = f", cte={self.cte!r}" if self.cte else ""
        extra += f", incremental={self.incremental}" if self.incremental is not None else ""
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
        self._compiled_nodes = set()
        self._bound_views = set()
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
        sql = self._compile(inline=query).sql
        self._reject_ddl_over_injected_ctes(query, sql)
        return self._relation(sql)

    def ctes(self, model, incremental=None, ephemeral=True) -> list:
        """The names of ``model``'s CTEs, in the order they are defined.

        Read off the COMPILED SQL, not the model source, so it also lists what you cannot see in
        the file: CTEs a macro generated, and ephemeral models, which dbt injects as
        ``__dbt__cte__<name>``.

        ``ephemeral=False`` leaves the injected ones out. On a project with a staging layer they
        can be most of the list — eighteen of thirty is a real number from a real project — and
        then the model's own steps, which is what you came to read, are hard to find.

        It hides them from the LISTING only. They are genuinely in the compiled SQL, ``cte()``
        still takes their names, and slicing at one is often exactly right: it shows a staging
        model in the context of the model that consumes it."""
        from dbt.adapters.duckrun.delta_dml import has_leading_with, split_cte_list
        sql = self.compiled(model, incremental=incremental)
        parts = split_cte_list(sql)[1]
        if not parts and has_leading_with(sql):
            # The compiled SQL opens with WITH but the splitter declined to take it apart.
            # Reporting [] here would claim the model has no CTEs, which is false.
            raise DbtProjectError(
                f"{model!r} has a WITH list this splitter cannot take apart — read it with "
                f"compiled() instead.")
        names = [name for name, _ in parts]
        return names if ephemeral else [n for n in names if not n.startswith(_EPHEMERAL_CTE)]

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
        from dbt.adapters.duckrun.delta_dml import has_leading_with, split_cte_list

        head, parts, _ = split_cte_list(sql)
        names = [n for n, _ in parts]
        if not names:
            if has_leading_with(sql):
                raise DbtProjectError(
                    f"{model!r} has a WITH list this splitter cannot take apart — read it with "
                    f"compiled() instead.")
            raise DbtProjectError(
                f"{model!r} has no CTEs — its compiled SQL is a single SELECT. Use show() instead.")
        if name not in names:
            # DuckDB resolves unquoted identifiers case-insensitively, so `cte(m, 'totals')` must
            # find a CTE spelled TOTALS. Exact match first; fold only when it is unambiguous.
            folded = [n for n in names if n.lower() == str(name).lower()]
            if len(folded) != 1:
                raise DbtProjectError(
                    f"{model!r} has no CTE {name!r}. It has: {', '.join(names)}")
            name = folded[0]
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
        # The views we registered for `view`-materialized models go first, while the cursor is
        # still open. They live in the shared in-memory catalog, so they would otherwise outlive
        # the manifest that defined them — and a model whose materialization changed from view to
        # table would then be read through yesterday's view. Dropped through the RAW cursor: the
        # read-only classifier reads `drop` as a write, which is true of a Delta table and not of a
        # catalog object we created ourselves.
        raw = getattr(self._cursor, "_cursor", None)
        for relation in self._bound_views if raw is not None else ():
            with contextlib.suppress(Exception):
                raw.execute(f"drop view if exists {relation}")
        self._bound_views = set()
        if self._handle is not None:
            with contextlib.suppress(Exception):
                self._handle.close()   # our own child cursor; the shared connection stays up
        self._manifest = self._signature = self._creds = None
        self._compiled = {}
        self._compiled_nodes = set()
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

    @staticmethod
    def _reject_ddl_over_injected_ctes(query, sql):
        """Say what went wrong when an inline DDL statement refs an ephemeral model.

        dbt injects an ephemeral model by prepending ``with __dbt__cte__<name> as (...)`` to the
        query — which is only valid in front of a SELECT. Put a ``create temp table`` there and the
        compiled text reads ``) create temp table ...``, and DuckDB reports ``syntax error at or
        near "create"`` pointing at line 83 of SQL the caller never wrote. That reads like a typo
        in their own statement, so they look in the wrong place.

        Detected from BOTH sides, and deliberately not with ``split_cte_list``: that scanner locates
        the final SELECT of a compiled model, so on this shape it hands back the SELECT and drops
        the ``create temp table ... as`` in front of it — correct for what it is for, and blind to
        exactly the thing being detected here. The compiled text says an ephemeral model was
        injected; the caller's own query says the statement is not a SELECT. Both are needed:
        neither DDL alone nor an ephemeral ref alone is a problem.

        A leading comment makes the keyword check miss, and that is fine — the guard simply does
        not fire and DuckDB reports it as before. A project without ephemeral models never reaches
        this at all, which is why it took a real one to find."""
        head = query.lstrip()
        if _EPHEMERAL_CTE not in sql or not head or head.startswith("("):
            return
        keyword = head.split(None, 1)[0].lower()
        if keyword in ("select", "with"):
            return
        raise DbtProjectError(
            f"{query.strip()[:60]}...\n"
            f"  This refs an ephemeral model. dbt injects those as a WITH clause in front of the "
            f"query, which only parses before a SELECT -- not before {keyword.upper()}.\n"
            "  Build the relation first, then materialise it:\n"
            "    rel = p.sql('select ...')\n"
            "    rel.create('scratch_name')        # or rel.create_view('scratch_name')")

    def _drop_inline_node(self):
        """Take the node ``--inline`` parsed into our manifest back out.

        A SUCCESSFUL inline compile removes it itself. A FAILED one does not — and what it leaves
        is named ``inline_query``, so the NEXT sql() parses a second one and dbt refuses the
        duplicate name. One failed sql() therefore turned every later sql() in the session into
        "dbt found two sql_operations with the name inline_query": an error about our leftovers,
        which says nothing about what the caller actually got wrong, and which a notebook user can
        only escape by restarting the kernel.

        Only reachable because the manifest is kept WARM. dbt's own CLI parses a fresh one per
        invocation, so it never sees this. Cleaning up in a finally is the whole fix: it costs a
        dict lookup, and it makes a failed sql() cost exactly the failed sql()."""
        nodes = getattr(self._manifest, "nodes", None) or {}
        for key in [k for k, n in nodes.items() if getattr(n, "name", None) == "inline_query"]:
            del nodes[key]

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
        """Compile one node, once per (selector, branch) per manifest generation, and record what it
        was.

        The cache keeps ``ctes()`` followed by ``cte()`` to one compile, and is safe because
        ``_ensure_manifest`` runs first: any edit drops the manifest and this cache with it, so a hit
        can only ever be SQL that is still current.

        It is NOT what makes a repeat compile safe — see :meth:`_reset_injected_ctes`. It cannot be:
        the ``is_incremental()`` probe deliberately compiles the same node under a second key."""
        self._ensure_manifest()
        key = (selector, inline, full_refresh)
        if key not in self._compiled:
            self._compiled[key] = self._run_compile(selector, inline, full_refresh)
        name, node_id, sql, branches, parents = self._compiled[key]
        incremental = self._which_branch(selector, inline, full_refresh, sql, branches)
        # A fresh CompileResult per call: cte() rewrites .sql on it, and that must not reach back
        # into the cache and corrupt the next caller's answer.
        self.last_compile = CompileResult(
            model=name, node_id=node_id, sql=sql, full_refresh=full_refresh,
            incremental=incremental, parents=parents)
        if branches and not full_refresh:
            self._report_branch(self.last_compile)
        return self.last_compile

    def _which_branch(self, selector, inline, full_refresh, sql, branches):
        """Which ``is_incremental()`` branch produced ``sql`` — True, False, or None for a model
        that does not branch.

        Answered by ASKING DBT rather than by re-deriving its rule. ``is_incremental()`` is true when
        the model is incremental, the target relation exists, and ``--full-refresh`` was not passed;
        working that out here would mean duplicating dbt's relation lookup, which is exactly the kind
        of copy that drifts from the adapter without anyone noticing. Instead: compile the same model
        with ``--full-refresh``, which forces the branch false, and compare. Different text means the
        default compile took the incremental branch.

        Costs one extra compile — but only for a model whose source actually contains
        ``is_incremental``, only on the default path, and only once per manifest generation, because
        the second compile lands in the same cache. A model that does not branch pays nothing at all.
        Identical texts on a branching model mean the false branch was taken anyway, which is worth
        reporting too.

        Comparing TEXTS is only a valid question to ask because :meth:`_reset_injected_ctes` makes the
        second compile of a node produce what a first one would. Without it the two texts differ for a
        model with an ephemeral parent no matter which branch was taken, and this reports True for
        every one of them — the exact misreading the hint exists to prevent."""
        if not branches:
            return None
        if full_refresh:
            return False        # forced false by construction — no second compile needed
        other = (selector, inline, True)
        if other not in self._compiled:
            self._compiled[other] = self._run_compile(selector, inline, True)
        return self._compiled[other][2] != sql

    @staticmethod
    def _report_branch(result):
        """Say which branch was compiled, every time, for a model that has two.

        Not once-per-session: a hint you scrolled past three cells ago is the same as no hint, and
        the failure it prevents — reading a delta as if it were the table — costs an hour."""
        # ASCII only, and that is not a style choice: issue #15: a non-ASCII progress print raises
        # UnicodeEncodeError on a stock Windows cp1252 console and takes the caller down with it.
        # tests/connection_api/test_connection_api.py asserts it for every print in the package.
        if result.incremental:
            print(f"[duckrun] {result.model}: is_incremental() = True -- this is the incremental "
                  f"branch,\n          i.e. the rows a run would write INTO the existing table, "
                  f"not the table's\n          contents. The other branch: incremental=False")
        else:
            # What was OBSERVED is that both compiles produced the same text -- not WHY. The usual
            # cause is a target table that does not exist yet, but two branches can also render
            # identically; naming a cause we never checked is the one thing a hint must not do.
            print(f"[duckrun] {result.model}: is_incremental() = False -- this is the full-refresh "
                  f"branch.\n          The model does branch, but both branches compile to the same "
                  f"SQL right now\n          (typically: the target table does not exist yet), so a "
                  f"run today\n          produces this.")

    def _reset_injected_ctes(self):
        """Let a node that was already compiled in this manifest generation be compiled again.

        dbt injects an ephemeral parent as a CTE exactly once per node and records that in
        ``extra_ctes_injected``. ``compile_node`` always re-renders ``compiled_code`` from
        ``raw_code``, but ``_recursively_prepend_ctes`` then returns early on that flag — so a SECOND
        compile of the same node against the same manifest hands back SQL that still REFERENCES
        ``__dbt__cte__<parent>`` while no longer defining it. The manifest we pass to ``dbtRunner`` is
        used as-is, never copied, so the node and its flag are shared across invocations.

        The ``is_incremental()`` probe has to compile the same node twice (once with
        ``--full-refresh``), so clearing the flag first is what keeps both branches whole. dbt then
        re-injects from ``extra_ctes``, which still carries the parent ids, rebuilding each CTE from
        the parent's own ``_pre_injected_sql`` — the same text a first compile produced. Only the
        consumer's flag is touched; an ephemeral parent keeps its state and is reused as-is.

        (dbt 1.8-1.11: compilation.py ``compile_node`` / ``_recursively_prepend_ctes``,
        cli/requires.py ``setup_manifest``.)"""
        nodes = getattr(self._manifest, "nodes", None) or {}
        for node_id in self._compiled_nodes:
            node = nodes.get(node_id)
            if getattr(node, "extra_ctes_injected", False):
                node.extra_ctes_injected = False

    def _run_compile(self, selector, inline, full_refresh):
        args = ["compile"]
        args += ["--inline", inline] if inline is not None else ["--select", selector]
        if full_refresh:
            args += ["--full-refresh"]
        self._reset_injected_ctes()
        try:
            result = self._invoke(args, manifest=self._manifest)
        finally:
            if inline is not None:
                self._drop_inline_node()

        nodes = [r.node for r in (getattr(result.result, "results", None) or [])
                 if getattr(r, "node", None) is not None]
        # dbt selects tests INDIRECTLY: with the default `eager` indirect selection, `--select
        # my_model` returns the model and every generic test hanging off it. Any real project puts
        # not_null/unique on its models, so a perfectly unambiguous selector arrived at the guard
        # below as "matched 3 nodes". Tests are the only thing dbt adds indirectly, so dropping
        # them resolves it -- unless tests are ALL that matched, which means one was named
        # outright, and reading a failing test back as a relation is one of the better uses of
        # this. Filtered here rather than passed as --indirect-selection=empty because `compile`
        # only grew that flag after 1.8, and duckrun supports dbt-core >=1.8.
        selected = [n for n in nodes
                    if getattr(getattr(n, "resource_type", None), "value", None) != "test"]
        nodes = selected or nodes
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
        # Whether the model branches at all is read off its SOURCE, before compilation erases the
        # `{% if %}` — after rendering there is nothing left to tell two branches apart. This is only
        # a gate: it decides whether asking dbt the exact question is worth a second compile, so a
        # false positive costs 0.9s and a false negative costs a hint, never a wrong answer.
        raw = getattr(node, "raw_code", "") or ""
        branches = selector is not None and "is_incremental" in raw
        node_id = getattr(node, "unique_id", None)
        if node_id is not None:
            self._compiled_nodes.add(node_id)   # so a repeat compile of it re-injects; see above
        # The node's direct parents, taken NOW: for an inline compile the node object outlives the
        # manifest entry (dbt removes it on success, _drop_inline_node on failure), and these ids
        # are all view-ancestor binding has left to start from.
        parents = tuple(getattr(getattr(node, "depends_on", None), "nodes", None) or ())
        return (getattr(node, "name", selector), node_id, node.compiled_code, branches, parents)

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

    def _bind_view_ancestors(self, node_id, parents=()):
        """Register every ``view``-materialized model the compiled node reads — as a VIEW — before
        running it. By default, on every call; never in reaction to an error.

        duckrun persists only Delta tables. A ``view`` model is a plain DuckDB view that lived in
        the catalog of the session that built it and died with it, so a cold debug session finds
        nothing under that name: no Delta table for the lazy bind to attach, and no view in a fresh
        in-memory catalog. Everything downstream of a staging layer is then unreadable — which on a
        real project is most of the project.

        None of that is unknowable, and that is the point. The manifest says which parents are views
        and dbt can compile each one, so this walks the node's ancestry depth-first and creates them
        in dependency order. The alternative — let DuckDB fail, then parse the missing name back out
        of its message — is how the RUN path's lazy bind works, and it is right there because on a
        build dbt creates the views itself and the bind is only a rescue. A debug session has the
        manifest in hand; rediscovering from an error string what is already in memory is slower and
        breaks the day the message changes.

        It costs nothing extra: a view is catalog metadata, and binding its body resolves exactly
        the parents the query had to resolve anyway. Only the ancestry of what you asked for —
        registering the whole project eagerly would NOT be free, since it would open every Delta
        ancestor's ``_delta_log`` for models you never open.

        A VIEW, never a materialization: ``create or replace view``, so the relation stays lazy and
        ``.filter(...)`` still pushes through it into the ``delta_scan`` underneath.

        ``parents`` is the fallback root set for a node the manifest no longer holds — an inline
        ``sql()`` compile, whose node dbt removes on success and :meth:`_drop_inline_node` on
        failure. Walking from its recorded parents is the same traversal one level down; without it
        an inline ``{{ ref() }}`` to a view model found nothing to register and died in the lazy
        bind, which only knows Delta tables (a view model never wrote one)."""
        nodes = getattr(self._manifest, "nodes", None) or {}
        seen = set()

        def walk(nid):
            if nid in seen:
                return
            seen.add(nid)
            node = nodes.get(nid)
            if node is None:
                return          # a source, or anything not in `nodes` — delta_scan binding covers it
            for parent in getattr(getattr(node, "depends_on", None), "nodes", None) or ():
                walk(parent)    # deepest first, so each view's body binds when it is created
            # The requested node itself is never created: its compiled SQL is what we are about to
            # run, and a view over it would be a second name for the same query.
            if nid != node_id and _materialization(node) == "view":
                self._create_view(node)

        if node_id in nodes or not parents:
            walk(node_id)
        else:
            for parent in parents:
                walk(parent)

    def _create_view(self, node):
        """``create or replace view`` for one view-materialized model, at the name dbt renders for
        it. Once per manifest generation — an edit invalidates the manifest, which drops these
        views, so a re-created one is always the current definition."""
        relation = getattr(node, "relation_name", None)
        if not relation or relation in self._bound_views:
            return
        # By full fqn, never by bare name: `--select v_boosted` selects EVERY node of that name, so
        # one same-named model in an installed package aborted show() on a dependency the caller
        # never typed ("matched 2 nodes ... narrow the selector" — about a selector they never
        # wrote). The dotted fqn is unique per node; the id check makes a selector-semantics drift
        # loud instead of quietly compiling somebody else's SQL into this view.
        selector = "fqn:" + ".".join(str(part) for part in (getattr(node, "fqn", None) or [node.name]))
        key = (selector, None, False)
        if key not in self._compiled:
            self._compiled[key] = self._run_compile(selector, None, False)
        compiled_id = self._compiled[key][1]
        if compiled_id != getattr(node, "unique_id", compiled_id):
            raise DbtProjectError(
                f"binding the view ancestor {node.name!r}: {selector!r} compiled "
                f"{compiled_id!r} instead — please report this.")
        sql = self._compiled[key][2]
        schema, database = getattr(node, "schema", None), getattr(node, "database", None)
        if schema:
            # A custom-schema model has no schema in a fresh in-memory DuckDB (same reason the
            # adapter's Delta bind creates one).
            # quote_ident doubles an embedded `"` — node.schema/database are raw manifest strings.
            prefix = f"{quote_ident(database)}." if database else ""
            self._cursor.execute(f"create schema if not exists {prefix}{quote_ident(schema)}")
        # Through the debug cursor, not the raw one: `create or replace view` classifies as
        # passthrough, and going this way means a Delta-backed parent still gets its lazy bind.
        self._cursor.execute(f"create or replace view {relation} as {sql}")
        self._bound_views.add(relation)

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
        # No guard here against a CREATE VIEW taking over a model's name. It is possible, and it
        # does change what a direct cursor read returns — but it does not survive: every method on
        # this class compiles through dbt first, and dbt's compile re-registers the delta_scan
        # views, overwriting the override. Measured: shadow, direct read gives the override; one
        # p.sql() later the delta_scan view is back and reads are correct again. Warning about a
        # hazard that repairs itself before it can mislead would be a false statement in a message
        # whose whole job is to be trusted.
        if self.last_compile is not None and (self.last_compile.node_id or self.last_compile.parents):
            self._bind_view_ancestors(self.last_compile.node_id, self.last_compile.parents)
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
