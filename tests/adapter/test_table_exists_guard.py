"""WS1 — the ``table_exists`` data-loss window.

Before this fix, ``engine.table_exists`` swallowed *every* exception and returned False. A
transient storage error (ADLS/OneLake 503, expired token) at store time therefore looked like
"no table", sending an incremental write — whose SQL had already filtered to only-new rows —
down the overwrite branch, replacing the whole table with just the increment. Silent loss, green
run.

The fix: ``table_exists`` catches only ``TableNotFoundError`` (genuine absence) and re-raises
everything else, and ``Plugin.store()`` refuses to overwrite when dbt's run-start discovery
believed the table existed but it can't be opened now.
"""
import types

import duckdb
import pyarrow as pa
import pytest

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from dbt.adapters.duckrun import engine
from dbt.adapters.duckrun.delta_plugin import Plugin

try:
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


# ----------------------------------------------------------------- unit: table_exists

def test_table_exists_reraises_generic_error(monkeypatch):
    """A non-TableNotFound error (e.g. a transient OSError) must propagate, not become False."""
    def boom(path, storage_options):
        raise OSError("transient ADLS 503")

    monkeypatch.setattr(engine, "_delta_table", boom)
    with pytest.raises(OSError):
        engine.table_exists("/some/path")


def test_table_exists_false_on_table_not_found(monkeypatch):
    """A genuinely-absent table (TableNotFoundError) is the only case that returns False."""
    def missing(path, storage_options):
        raise TableNotFoundError("no log here")

    monkeypatch.setattr(engine, "_delta_table", missing)
    assert engine.table_exists("/some/path") is False


def test_table_exists_true_for_real_table(tmp_path):
    path = str(tmp_path / "t")
    engine.write_delta(path, pa.table({"id": pa.array([1, 2, 3], pa.int64())}), "overwrite")
    assert engine.table_exists(path) is True


# ----------------------------------------------------------------- unit: delta_version

def test_delta_version_reraises_generic_error(monkeypatch):
    """delta_version feeds append_if_unchanged's start-of-build pin; a real error must NOT silently
    become None (which would degrade the pin to HEAD-at-write and reopen the race)."""
    from dbt.adapters.duckrun.impl import DuckrunAdapter

    def boom(location, so):
        raise OSError("transient storage error")

    monkeypatch.setattr(engine, "_delta_table", boom)
    # Build a bare adapter shell: delta_version only touches self.config.credentials.
    adapter = DuckrunAdapter.__new__(DuckrunAdapter)
    adapter.config = types.SimpleNamespace(
        credentials=types.SimpleNamespace(
            storage_options=None,
            storage_options_for_location=lambda location: None,
        )
    )
    with pytest.raises(OSError):
        adapter.delta_version("/some/path")


# ----------------------------------------------------------------- integration: store() guard

def _fake_target_config(path, relation_name, cfg):
    return types.SimpleNamespace(
        location=types.SimpleNamespace(path=path),
        config=cfg,
        relation=types.SimpleNamespace(render=lambda: relation_name),
    )


def _store_plugin(con):
    p = Plugin.__new__(Plugin)
    p._storage_options = None
    p._conn = con
    p._cursor_handle = con
    p._baseline_memory_limit = None
    p._microbatch_seen = set()
    return p


def test_store_refuses_overwrite_when_table_vanishes_midrun(tmp_path, monkeypatch):
    """End-to-end of the loss window: a full table exists on disk; on the 'second run' the open
    fails transiently. store() must fail loudly AND leave the full table (and its version) intact
    — never overwrite it with the filtered increment."""
    path = str(tmp_path / "events")
    full = pa.table({"id": pa.array([1, 2, 3, 4, 5], pa.int64())})
    engine.write_delta(path, full, "overwrite")
    version_before = DeltaTable(path).version()

    con = duckdb.connect()
    con.execute("create view increment as select 99 as id")
    plugin = _store_plugin(con)

    # Simulate the transient storage error the fix stops swallowing. store()'s existence check is
    # engine.open_if_exists (it keeps the handle for the drop-tombstone check); same fail-loud
    # contract as table_exists, which delegates to it.
    monkeypatch.setattr(engine, "open_if_exists", lambda *a, **k: (_ for _ in ()).throw(OSError("503")))

    tc = _fake_target_config(
        path, "increment",
        {"incremental": True, "full_refresh": False, "dbt_believes_exists": True},
    )
    with pytest.raises(OSError):
        plugin.store(tc)

    # The table on disk is untouched: same version, full contents.
    assert DeltaTable(path).version() == version_before
    ids = sorted(DeltaTable(path).to_pyarrow_table().column("id").to_pylist())
    assert ids == [1, 2, 3, 4, 5]


def test_assert_not_null_raises_with_contract_message():
    """The contract NOT NULL guard fires on a staged null and uses dbt's phrasing."""
    con = duckdb.connect()
    con.execute("create view staged as select * from (values (1,'a'),(null,'b')) t(id, color)")
    with pytest.raises(CompilationError, match="NOT NULL constraint failed"):
        Plugin._assert_not_null(con, "staged", ["id"])


def test_assert_not_null_passes_when_no_nulls():
    con = duckdb.connect()
    con.execute("create view staged as select * from (values (1,'a'),(2,'b')) t(id, color)")
    # No exception for a fully-populated column.
    Plugin._assert_not_null(con, "staged", ["id", "color"])


def test_validate_merge_rejects_unsupported_semantics():
    """merge_on_using_columns has no delta_rs equivalent and is REJECTED (not silently ignored) so
    a green run can't quietly diverge from what the user asked for."""
    with pytest.raises(CompilationError, match="duckrun cannot honor"):
        Plugin._validate_merge_config({"merge_on_using_columns": ["id"]})


def test_validate_merge_allows_clause_configs():
    """merge_clauses and merge_update_set_expressions ARE honored now (translated to delta_rs's full
    TableMerger clause list — see _custom_merge_clauses), so they pass validation."""
    Plugin._validate_merge_config({"merge_clauses": {"when_matched": [{"action": "update"}]}})
    Plugin._validate_merge_config({"merge_update_set_expressions": {"v": "v + 1"}})
    # when_not_matched_by_source (full-sync delete/update) is a valid clause group too.
    Plugin._validate_merge_config(
        {"merge_clauses": {"when_not_matched_by_source": [{"action": "delete"}]}})


def test_merge_clauses_translation_full_surface():
    """_specs_from_merge_clauses maps a full CDC/sync merge_clauses dict onto ordered delta_rs clause
    specs — matched update+delete, not-matched insert, and not-matched-by-source delete/update."""
    cols = ["id", "name", "amount"]
    specs = Plugin._specs_from_merge_clauses({
        "when_matched": [
            {"action": "delete", "condition": "DBT_INTERNAL_SOURCE.amount < 0"},
            {"action": "update", "mode": "by_name"},
        ],
        "when_not_matched": [{"action": "insert", "mode": "by_name"}],
        "when_not_matched_by_source": [{"action": "delete"}],
    }, cols, "id")
    kinds = [(s["clause"], s["action"]) for s in specs]
    assert kinds == [
        ("matched", "delete"), ("matched", "update_all"),
        ("not_matched", "insert_all"), ("not_matched_by_source", "delete"),
    ]
    # DBT_INTERNAL_SOURCE alias rewritten to delta_rs's 'source' for the matched-delete predicate.
    assert specs[0]["predicate"] == "source.amount < 0"

    # by-source UPDATE needs an explicit set map (no source row to copy from).
    upd = Plugin._specs_from_merge_clauses(
        {"when_not_matched_by_source": [{"action": "update", "set": {"name": "'departed'"}}]},
        cols, "id")
    assert upd == [{"clause": "not_matched_by_source", "action": "update",
                    "updates": {"name": "'departed'"}, "predicate": None}]
    with pytest.raises(CompilationError, match="requires a 'set' map"):
        Plugin._specs_from_merge_clauses(
            {"when_not_matched_by_source": [{"action": "update"}]}, cols, "id")


def test_merge_clauses_do_nothing_is_dbt_duckdbs_insert_only():
    """#20: dbt-duckdb spells insert-only as `when_matched: [{'action': 'do_nothing'}]` — which duckrun
    used to REJECT, forcing a project that targets both adapters to branch the strategy on
    target.name. It now translates to the same thing duckrun's own `incremental_strategy='insert'`
    produces: one unconditional WHEN NOT MATCHED THEN INSERT * (the matched clause folds away, the
    omitted when_not_matched key takes dbt-duckdb's implicit insert default) — the shape the engine
    routes to the cheap DuckDB anti-join."""
    cols = ["id", "name", "amount"]
    specs = engine.resolve_do_nothing(
        Plugin._specs_from_merge_clauses({"when_matched": [{"action": "do_nothing"}]}, cols, "id"))
    assert specs == [{"clause": "not_matched", "action": "insert_all", "predicate": None}]
    assert engine._insert_only_shape(specs)

    # A CONDITIONAL do_nothing is first-match-wins, not a dropped clause: it gates the later
    # same-kind clause with `IS NOT TRUE` (same fold a raw SQL `THEN DO NOTHING` gets).
    gated = engine.resolve_do_nothing(Plugin._specs_from_merge_clauses({
        "when_matched": [{"action": "do_nothing", "condition": "DBT_INTERNAL_SOURCE.amount < 0"},
                         {"action": "update"}],
    }, cols, "id"))
    assert gated[0] == {"clause": "matched", "action": "update_all",
                        "predicate": "(source.amount < 0) IS NOT TRUE"}

    # do_nothing everywhere folds to nothing at all — a no-op merge (engine.merge_delta_clauses
    # commits nothing rather than raising "merge has no clauses").
    assert engine.resolve_do_nothing(Plugin._specs_from_merge_clauses(
        {"when_matched": [{"action": "do_nothing"}],
         "when_not_matched": [{"action": "do_nothing"}]}, cols, "id")) == []


def test_resolve_do_nothing_does_not_mutate_the_callers_clauses():
    """The fold runs at the shared engine seam on clause dicts the CALLER still owns (the dbt
    translator's spec list, the raw-SQL parser's), so it must copy rather than stamp guards onto them
    — a mutated input would leak a stale predicate into a retry."""
    clauses = [{"clause": "matched", "action": "do_nothing", "predicate": "source.a > 0"},
               {"clause": "matched", "action": "update_all", "predicate": None}]
    engine.resolve_do_nothing(clauses)
    assert clauses[1]["predicate"] is None and "_dead" not in clauses[1]


def test_merge_clauses_applies_dbt_duckdbs_implicit_clause_defaults():
    """dbt-duckdb's merge macro defaults an OMITTED key to that key's clause (when_matched ->
    UPDATE BY NAME, when_not_matched -> INSERT BY NAME), so a one-key merge_clauses dict is a full
    upsert there. duckrun mirrors it: same config, same merge on both adapters."""
    cols = ["id", "name", "amount"]
    for one_key in ({"when_matched": [{"action": "update"}]},
                    {"when_not_matched": [{"action": "insert"}]}):
        assert Plugin._specs_from_merge_clauses(one_key, cols, "id") == [
            {"clause": "matched", "action": "update_all", "predicate": None},
            {"clause": "not_matched", "action": "insert_all", "predicate": None},
        ]

    # EXCEPTION: when_not_matched_by_source is duckrun's own extension (dbt-duckdb's merge_clauses has
    # no such key), so there is no upstream default to mirror and a full-sync clause list stays
    # explicit — no implicit upsert is bolted onto a CDC config.
    assert Plugin._specs_from_merge_clauses(
        {"when_matched": [{"action": "delete"}], "when_not_matched_by_source": [{"action": "delete"}]},
        cols, "id") == [
        {"clause": "matched", "action": "delete", "predicate": None},
        {"clause": "not_matched_by_source", "action": "delete", "predicate": None},
    ]


def test_merge_clauses_accepts_the_remaining_dbt_duckdb_spellings():
    """The rest of dbt-duckdb's clause surface, which duckrun previously mistranslated in silence."""
    cols = ["id", "name", "amount"]

    # A list `condition` is AND-ed (upstream joins with `) AND (`); duckrun used to stringify the list
    # straight into the predicate.
    assert Plugin._specs_from_merge_clauses({"when_matched": [
        {"action": "update", "condition": ["DBT_INTERNAL_SOURCE.amount > 0",
                                          "DBT_INTERNAL_DEST.amount < 5"]}]}, cols, "id"
    )[0]["predicate"] == "(source.amount > 0) AND (target.amount < 5)"

    # mode star / by_position mean "every column", like by_name — not an explicit column list (which
    # would also have cost the insert-only routing).
    for mode in ("star", "by_position"):
        assert Plugin._specs_from_merge_clauses(
            {"when_not_matched": [{"action": "insert", "mode": mode}]}, cols, "id"
        )[1] == {"clause": "not_matched", "action": "insert_all", "predicate": None}

    # Explicit INSERT is spelled insert: {columns, values} upstream; duckrun read include/exclude only,
    # so it silently inserted every non-key column and left the key NULL.
    assert Plugin._specs_from_merge_clauses({"when_not_matched": [
        {"action": "insert", "mode": "explicit",
         "insert": {"columns": ["id", "name"],
                    "values": ["DBT_INTERNAL_SOURCE.id", "upper(DBT_INTERNAL_SOURCE.name)"]}}]},
        cols, "id")[1]["updates"] == {"id": "source.id", "name": "upper(source.name)"}
    with pytest.raises(CompilationError, match="pair up one-to-one"):
        Plugin._specs_from_merge_clauses({"when_not_matched": [
            {"action": "insert", "mode": "explicit",
             "insert": {"columns": ["id", "name"], "values": ["DBT_INTERNAL_SOURCE.id"]}}]},
            cols, "id")

    # Explicit UPDATE honors set_expressions on top of include/exclude.
    assert Plugin._specs_from_merge_clauses({"when_matched": [
        {"action": "update", "mode": "explicit",
         "update": {"include": ["name"],
                    "set_expressions": {"amount": "DBT_INTERNAL_DEST.amount + 1"}}}]},
        cols, "id")[0]["updates"] == {"name": "source.name", "amount": "target.amount + 1"}

    # `by: source` inside when_not_matched is upstream's portable spelling of duckrun's by-source
    # group — and being portable, it still gets the implicit matched default.
    assert Plugin._specs_from_merge_clauses(
        {"when_not_matched": [{"by": "source", "action": "delete"}]}, cols, "id") == [
        {"clause": "matched", "action": "update_all", "predicate": None},
        {"clause": "not_matched_by_source", "action": "delete", "predicate": None},
    ]

    # dbt-duckdb's `error` action has no delta_rs equivalent — refused loudly, never dropped.
    with pytest.raises(CompilationError, match="no delta_rs equivalent"):
        Plugin._specs_from_merge_clauses(
            {"when_matched": [{"action": "error", "error_message": "nope"}]}, cols, "id")


def test_validate_merge_allows_supported_conditions():
    """Conditions duckrun honors as delta_rs predicates must pass validation."""
    Plugin._validate_merge_config({
        "merge_update_condition": "DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age",
        "merge_insert_condition": "DBT_INTERNAL_SOURCE.age > 25",
        "merge_update_columns": ["name", "age"],
    })


def test_rewrite_merge_aliases():
    assert Plugin._rewrite_merge_aliases(None) is None
    assert (
        Plugin._rewrite_merge_aliases("DBT_INTERNAL_DEST.age < DBT_INTERNAL_SOURCE.age")
        == "target.age < source.age"
    )


def test_store_contradiction_guard_when_exists_false(tmp_path, monkeypatch):
    """If table_exists returns False (no table found) but dbt's discovery believed it existed and
    this is an incremental non-full-refresh run, store() refuses rather than overwriting with the
    increment."""
    path = str(tmp_path / "events")
    con = duckdb.connect()
    con.execute("create view increment as select 99 as id")
    plugin = _store_plugin(con)

    monkeypatch.setattr(engine, "table_exists", lambda *a, **k: False)

    tc = _fake_target_config(
        path, "increment",
        {"incremental": True, "full_refresh": False, "dbt_believes_exists": True},
    )
    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        plugin.store(tc)
