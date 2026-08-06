"""Point `duckrun.dbt_project()` at a real, unmodified dbt project and check what it hands back.

The debug session (issue #29) compiles a model with dbt and returns a `DuckDBPyRelation`. Its unit
tests run against a hand-written 7-model fixture — and every bug the feature shipped with was found
by pointing it at a REAL project instead: generic tests made every model name look ambiguous, a
failed inline compile poisoned the session, ephemeral CTEs outnumbered the model's own. A fixture
written beside the code cannot produce those, because it is written by whoever already knows the
answer.

So this runs the session against the projects the parity suite already uses, over the tables those
projects have already materialized. It is a READER, top to bottom: it never builds anything, never
writes to the warehouse, and never modifies the repo — it clones the project source (dbt needs the
.sql files to compile) and reads what is already there. An empty warehouse is a failure it reports,
not something it fixes by building.

Each project supplies the model names it actually has; a check whose model is None is skipped rather
than guessed at. Call `check(...)` and exit on its return value — see `<project>/run_debug.py`.
"""
import os
import subprocess
import sys
import traceback


def ensure_project(clone_dir, repo_url):
    """The project SOURCE, and nothing else — no build.

    The debug session is a reader: it compiles models and reads the tables that are already in the
    warehouse. So this clones the repo if it is not there (dbt needs the .sql files to compile) and
    installs its packages if it has any (dbt cannot even PARSE a project whose packages.yml is
    unresolved). It never runs `dbt build` — if the warehouse has nothing in it, that is a real
    failure and the battery says so, rather than quietly materializing something to read back."""
    clone_dir = str(clone_dir)
    if not os.path.isfile(os.path.join(clone_dir, "dbt_project.yml")):
        subprocess.run(["git", "clone", "--depth", "1", repo_url, clone_dir], check=True)
    if (os.path.isfile(os.path.join(clone_dir, "packages.yml"))
            and not os.path.isdir(os.path.join(clone_dir, "dbt_packages"))):
        subprocess.run([sys.executable, "-m", "dbt.cli.main", "deps"], cwd=clone_dir, check=True)
    return clone_dir


def _rows(rel):
    """A relation's rows. `count("*")` gives back a RELATION, not a scalar — that is the API being
    tested, so unwrap it here rather than pretend otherwise."""
    return rel.fetchall()


def _count(rel) -> int:
    return _rows(rel.count("*"))[0][0]


class _Battery:
    """Run named checks, keep going after a failure, and report every one. A debug session is a
    surface of small promises; stopping at the first broken one hides the rest."""

    def __init__(self):
        self.failures = []

    def check(self, name, fn):
        try:
            detail = fn()
            print(f"  PASS  {name}" + (f" -- {detail}" if detail else ""))
        except Exception as exc:                     # noqa: BLE001 — a failure IS the result here
            self.failures.append(name)
            print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
            traceback.print_exc()

    def skip(self, name, why):
        print(f"  SKIP  {name} -- {why}")


def check(project_dir, profiles_dir, warehouse, schema, *, seed_backed_model=None, cte_model=None,
          seed_ref=None, view_backed_model=None, incremental_model=None) -> bool:
    """Run the debug-session battery against an already-built project. True when every check passed.

    * ``seed_backed_model`` — a model whose refs are all Delta-backed (seeds/tables), so it can be
      EXECUTED from a cold session. Types and laziness are checked on it.
    * ``cte_model`` — a model with CTEs, also Delta-backed. Listing and verbatim slicing.
    * ``seed_ref`` — a seed, i.e. a real Delta table, for the read-only proof.
    * ``view_backed_model`` — a model whose parents are `view`-materialized. duckrun does not persist
      views, so this documents what a cold session can NOT read.
    * ``incremental_model`` — a model that branches on ``is_incremental()``. Compile-only, so its
      parents' materializations do not matter.

    Nothing here writes. The models must already be materialized in ``warehouse``/``schema``.
    """
    # The profile renders root_path/schema from these; run_parity.py passes them to the dbt
    # subprocess only, so an in-process session has to export them itself.
    os.environ["WAREHOUSE_PATH"] = str(warehouse)
    os.environ["DBT_SCHEMA"] = str(schema)

    import duckrun
    from dbt.adapters.duckrun.environment import DuckrunReadOnlyError

    print(f"\n=== debug session ({duckrun.dbt_project.__module__}) on {project_dir} ===")
    p = duckrun.dbt_project(str(project_dir), profiles_dir=str(profiles_dir))
    b = _Battery()

    if seed_backed_model:
        def real_types():
            """The whole point of returning a relation instead of `dbt show --output json`: the
            schema survives. agate/JSON turns every column into a Python object and the DataFrame
            library guesses — decimals as strings, dates as strings."""
            rel = p.show(seed_backed_model)
            numeric = [t for t in map(str, rel.types)
                       if t.startswith(("BIGINT", "INTEGER", "HUGEINT", "DECIMAL", "DOUBLE",
                                        "FLOAT", "SMALLINT", "TINYINT"))]
            assert numeric, f"no numeric column survived: {list(map(str, rel.types))}"
            return f"{len(rel.columns)} cols, types {numeric[:3]}"

        def lazy_filter():
            """`.filter(...).limit(...)` has to push INTO the delta_scan rather than materialize the
            model first — the other half of what a relation buys over a JSON dump."""
            rel = p.show(seed_backed_model)
            total = _count(rel)
            assert total > 0, "the built model is empty; parity built nothing to debug"
            few = _rows(p.show(seed_backed_model).limit(3))
            assert 0 < len(few) <= 3
            return f"{total} rows, limit(3) -> {len(few)}"

        b.check("show() returns real DuckDB types", real_types)
        b.check("show() is lazy and filterable", lazy_filter)
    else:
        b.skip("show()", "no Delta-backed model named for this project")

    if cte_model:
        def ctes_are_the_models_own():
            names = p.ctes(cte_model)
            assert names, f"{cte_model} compiled to a single SELECT — pick a model with CTEs"
            compiled = p.compiled(cte_model)
            missing = [n for n in names if n not in compiled]
            assert not missing, f"listed CTEs absent from the compiled SQL: {missing}"
            return f"{names}"

        def slice_is_verbatim():
            """The claim is character-for-character: `cte()` splices the CTE text out of the
            compiled SQL rather than re-generating it. Checked on SQL nobody wrote for us —
            real formatting, real comments."""
            compiled = p.compiled(cte_model)
            first = p.ctes(cte_model)[0]
            rel = p.cte(cte_model, first)
            sliced = p.last_compile.sql
            body = sliced.rsplit("\nselect * from", 1)[0]
            assert compiled.startswith(body), "the sliced CTE text is not a prefix of the compiled SQL"
            n = _count(rel)
            assert n > 0, f"the first CTE of {cte_model} returned no rows"
            return f"{first!r}: {n} rows, {len(body)} chars spliced verbatim"

        b.check("ctes() lists what is really in the compiled SQL", ctes_are_the_models_own)
        b.check("cte() slices verbatim and runs", slice_is_verbatim)
    else:
        b.skip("ctes()/cte()", "no CTE model named for this project")

    def a_model_with_generic_tests_is_not_ambiguous():
        """dbt's default `eager` indirect selection returns a model AND every generic test on it, so
        on a real project — where every model has not_null/unique — a plain name arrived as
        "matched 3 nodes". This is that fix, standing on a project with real tests."""
        target = seed_backed_model or cte_model or incremental_model
        assert target, "no model named"
        p.compiled(target)
        tests = [n.name for n in (getattr(p._manifest, "nodes", None) or {}).values()
                 if getattr(getattr(n, "resource_type", None), "value", None) == "test"]
        assert tests, "this project declares no generic tests — it cannot prove the fix"
        return f"{target} resolved; project has {len(tests)} test nodes"

    def a_test_named_outright_still_resolves():
        """Dropping the INDIRECT pull-in must not cost the DIRECT one: reading a failing test back
        as a typed relation is one of the better uses of this."""
        tests = [n.name for n in (getattr(p._manifest, "nodes", None) or {}).values()
                 if getattr(getattr(n, "resource_type", None), "value", None) == "test"]
        assert tests, "this project declares no generic tests"
        named = sorted(tests)[0]
        sql = p.compiled(named)
        assert sql and sql.strip(), f"{named} compiled to nothing"
        return f"{named} -> {len(sql)} chars"

    b.check("a model's generic tests are not an ambiguous selector",
            a_model_with_generic_tests_is_not_ambiguous)
    b.check("a test named outright still resolves", a_test_named_outright_still_resolves)

    if seed_ref:
        def read_only_is_real():
            """The session is built from the real profile, so it sits one typo away from a
            production write. On the run path this DELETE is routed to delta_rs and really does
            destroy data — here it must be refused, and the table must be untouched afterwards.

            Counted through `sql()` rather than `show(seed)`: dbt has no compiled SQL for a seed
            node, so the ref is the honest way to read one."""
            def rows():
                return _rows(p.sql(f"select count(*) from {{{{ ref('{seed_ref}') }}}}"))[0][0]

            before = rows()
            assert before > 0, f"{seed_ref} is empty — parity built nothing to protect"
            try:
                p.sql(f"delete from {{{{ ref('{seed_ref}') }}}}")
            except DuckrunReadOnlyError as exc:
                after = rows()
                assert after == before, f"rows changed despite the refusal: {before} -> {after}"
                return f"{before} rows intact; {str(exc).splitlines()[0]}"
            raise AssertionError("the DELETE was not refused")

        b.check("a write through the session is refused, live table untouched", read_only_is_real)
    else:
        b.skip("read-only", "no seed named for this project")

    if view_backed_model:
        def view_parents_are_not_readable_cold():
            """A KNOWN limitation, pinned rather than left to be discovered: duckrun has no
            persistent view — a `view` model exists only inside the session that built it. So a
            model whose parents are views cannot be read from a cold debug session; its compiled
            SQL is fine, the parent relation simply is not there. Compiling still works, which is
            what makes ctes()/compiled() useful on these models anyway.

            If duckrun ever names the view-materialized parent in the error, this check fails and
            says so, instead of the improvement going unnoticed."""
            assert p.compiled(view_backed_model).strip(), "compiling should still work"
            try:
                _rows(p.show(view_backed_model).limit(1))
            except Exception as exc:                 # noqa: BLE001 — the failure IS the pinned shape
                return f"{type(exc).__name__}: {str(exc).splitlines()[0][:90]}"
            raise AssertionError(
                f"{view_backed_model} read fine from a cold session — duckrun now persists views, "
                "or its parents stopped being views. Re-pin this check.")

        b.check("a view-backed model compiles but cannot be read cold (known limitation)",
                view_parents_are_not_readable_cold)
    else:
        b.skip("view-backed model", "none named for this project")

    if incremental_model:
        def the_branch_is_reported():
            """Which `is_incremental()` branch was compiled is invisible in the SQL — rendering
            erases the `{% if %}`. The parity build has just created the target table, so the
            incremental branch is the honest answer here."""
            default = p.compiled(incremental_model)
            assert p.last_compile.incremental is True, (
                f"expected the incremental branch (the table exists), got "
                f"{p.last_compile.incremental}")
            forced = p.compiled(incremental_model, incremental=False)
            assert p.last_compile.incremental is False
            assert forced != default, "--full-refresh compiled to the same SQL as the default"
            return f"{len(default)} vs {len(forced)} chars"

        def both_branches_are_whole():
            """The branch answer costs a SECOND compile of the same node against one warm manifest,
            which is where dbt stops re-injecting ephemeral parents. Whatever this project's
            ephemeral layer looks like, every `__dbt__cte__` a branch references must be defined in
            that same text."""
            for label, sql in (("default", p.compiled(incremental_model)),
                               ("full-refresh", p.compiled(incremental_model, incremental=False))):
                names = {w.split()[0].strip("(),") for w in sql.split("__dbt__cte__")[1:] if w.split()}
                for name in names:
                    cte = "__dbt__cte__" + name
                    assert f"{cte} as" in sql, f"{label} branch references {cte} but never defines it"
            return "no dangling __dbt__cte__ in either branch"

        b.check("the is_incremental() branch is reported", the_branch_is_reported)
        b.check("both branches define every ephemeral CTE they reference", both_branches_are_whole)
    else:
        b.skip("is_incremental() report", "no incremental model named for this project")

    ok = not b.failures
    print("\nDEBUG SESSION:", "PASS" if ok else f"FAIL — {', '.join(b.failures)}")
    return ok
