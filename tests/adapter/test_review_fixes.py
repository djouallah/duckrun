"""Regression tests for the code-review work order (duckrun_review_for_opus.md).

Pure-Python unit tests for the small, self-contained fixes — the ones that don't need a real
``dbt run`` to exercise. Grouped by review item number.
"""
import duckdb
import pytest

from dbt.adapters.duckrun import delta_dml, engine, policy, secret, sqlscan
from dbt.adapters.duckrun.credentials import DuckrunCredentials
from dbt.adapters.duckrun.delta_plugin import Plugin
from duckrun.session import _is_multi_statement
from duckrun import auth

try:
    from dbt_common.exceptions import CompilationError
except Exception:  # pragma: no cover - older layouts
    CompilationError = ValueError


# ------------------------------------------------------- #13 not-null guard: one pass

def test_not_null_guard_single_pass_raises_on_null():
    con = duckdb.connect()
    con.execute("create view v as select * from (values (1,'a'),(2,null),(3,null)) t(id, name)")
    with pytest.raises(CompilationError) as exc:
        Plugin._assert_not_null(con, "v", ["id", "name"])
    # Reports the offending column and its null count.
    assert "name" in str(exc.value)
    assert "2 null" in str(exc.value)


def test_not_null_guard_clean_and_empty():
    con = duckdb.connect()
    con.execute("create view v as select * from (values (1,'a')) t(id, name)")
    Plugin._assert_not_null(con, "v", ["id", "name"])  # no raise
    Plugin._assert_not_null(con, "v", [])  # empty column list is a no-op


def test_not_null_guard_quotes_exotic_column():
    con = duckdb.connect()
    con.execute('create view v as select * from (values (cast(null as int))) t("weird ""col")')
    with pytest.raises(CompilationError):
        Plugin._assert_not_null(con, "v", ['weird "col'])


# ------------------------------------------------------- #21 merge ON predicate quoting

def test_merge_on_predicate_quotes_keys():
    pred = Plugin._merge_on_predicate(["Id", "region key"], {}, ["Id", "region key"])
    assert pred == 'target."Id" = source."Id" AND target."region key" = source."region key"'


def test_merge_on_predicate_strips_user_quotes():
    pred = Plugin._merge_on_predicate('"id"', {}, ["id"])
    assert pred == 'target."id" = source."id"'


# ------------------------------------------------------- #17 trailing-slash normalization

def test_root_path_trailing_slash_stripped():
    c = DuckrunCredentials(
        database="db", schema="main",
        root_path="abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/",
        catalogs={"other": {"root_path": "s3://bucket/warehouse/"}},
    )
    assert c.root_path == "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables"
    assert c.catalog_locations == {"other": "s3://bucket/warehouse"}
    assert c.catalogs["other"]["root_path"] == "s3://bucket/warehouse"


def test_root_path_without_slash_unchanged():
    c = DuckrunCredentials(database="db", schema="main", root_path="./warehouse")
    assert c.root_path == "./warehouse"


# ------------------------------------------------------- #22 dollar-quote multi-statement

def test_multi_statement_skips_dollar_quote():
    assert _is_multi_statement("update t set x = $tag$a;b$tag$ where id=1") is False
    assert _is_multi_statement("select $$one;two$$") is False


def test_multi_statement_still_detects_real_split():
    assert _is_multi_statement("select 1; select 2") is True
    assert _is_multi_statement("select ';'") is False
    assert _is_multi_statement("select 1") is False


# ------------------------------------------------------- #16 memory limit doesn't self-throttle

def test_effective_mem_limit_adds_rss_back(monkeypatch):
    # Our own 6 GiB RSS dragged the available term down to 3 GiB; adding it back gives 9 GiB, so the
    # per-model cap doesn't ratchet down by counting the process against itself.
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: 3 * 2 ** 30)
    monkeypatch.setattr(engine, "_proc_rss_bytes", lambda: 6 * 2 ** 30)
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: 64 * 2 ** 30)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: 10 * 2 ** 30)
    assert engine._effective_mem_limit_bytes() == 9 * 2 ** 30


def test_effective_mem_limit_still_clamped_to_cgroup(monkeypatch):
    # Adding RSS back can never exceed the real container ceiling: min() re-clamps to the cgroup.
    monkeypatch.setattr(engine, "_available_ram_bytes", lambda: 8 * 2 ** 30)
    monkeypatch.setattr(engine, "_proc_rss_bytes", lambda: 6 * 2 ** 30)
    monkeypatch.setattr(engine, "_total_ram_bytes", lambda: 64 * 2 ** 30)
    monkeypatch.setattr(engine, "_cgroup_mem_limit_bytes", lambda: 10 * 2 ** 30)
    assert engine._effective_mem_limit_bytes() == 10 * 2 ** 30


# ------------------------------------------------------- #10 non-interactive token refresh

def test_refresh_never_reaches_interactive_browser(monkeypatch):
    """refresh_storage_token must call _azure_identity_token(interactive=False) — never a browser."""
    seen = {}

    def fake_identity(interactive=True):
        seen["interactive"] = interactive
        return "tok"

    monkeypatch.setattr(auth, "_fabric_token", lambda: None)
    monkeypatch.setattr(auth, "_github_oidc_token", lambda scope=auth._STORAGE_SCOPE: None)
    monkeypatch.setattr(auth, "_azure_identity_token", fake_identity)
    assert auth.refresh_storage_token() == "tok"
    assert seen["interactive"] is False


# ------------------------------------------ Fabric/Power BI tokens self-acquired via OIDC by scope

@pytest.fixture(autouse=True)
def _clear_token_cache():
    """get_*_token caches per scope for the process; clear it so each test acquires fresh."""
    auth._TOKEN_CACHE.clear()
    yield
    auth._TOKEN_CACHE.clear()


def test_get_token_caches_and_reuses(monkeypatch):
    # A burst of get_onelake_token calls (a multi-catalog dbt run) must mint the token ONCE, not hammer
    # the OIDC endpoint — the second call returns the cached token without re-acquiring.
    calls = {"n": 0}

    def acquire():
        calls["n"] += 1
        return "TOKEN-XYZ"  # non-JWT → cached for the process

    monkeypatch.setattr(auth, "_fabric_token", lambda: None)
    monkeypatch.setattr(auth, "_github_oidc_token", lambda scope=auth._STORAGE_SCOPE: acquire())
    monkeypatch.setattr(auth, "_azure_identity_token", lambda interactive=True: None)
    monkeypatch.delenv("AZURE_STORAGE_TOKEN", raising=False)
    assert auth.get_onelake_token() == "TOKEN-XYZ"
    assert auth.get_onelake_token() == "TOKEN-XYZ"
    assert calls["n"] == 1  # minted once, reused


def _capture_oidc_scope(monkeypatch):
    """Stub _github_oidc_token to record the scope it's asked for and return a scope-tagged token."""
    seen = {}

    def fake_oidc(scope=auth._STORAGE_SCOPE):
        seen["scope"] = scope
        return f"OIDC::{scope}"

    monkeypatch.setattr(auth, "_github_oidc_token", fake_oidc)
    return seen


def test_get_fabric_token_uses_oidc_with_fabric_scope(monkeypatch):
    # Off a Fabric notebook and with no FABRIC_TOKEN env, the control-plane token comes from the OIDC
    # exchange at the Fabric scope — no az/env spoon-feeding needed.
    monkeypatch.delenv("FABRIC_TOKEN", raising=False)
    monkeypatch.setattr(auth, "_notebook_fabric_api_token", lambda: None)
    seen = _capture_oidc_scope(monkeypatch)
    assert auth.get_fabric_token() == f"OIDC::{auth._FABRIC_SCOPE}"
    assert seen["scope"] == auth._FABRIC_SCOPE


def test_get_powerbi_token_uses_oidc_with_powerbi_scope(monkeypatch):
    monkeypatch.delenv("POWERBI_TOKEN", raising=False)
    monkeypatch.setattr(auth, "_notebook_fabric_api_token", lambda: None)
    seen = _capture_oidc_scope(monkeypatch)
    assert auth.get_powerbi_token() == f"OIDC::{auth._POWERBI_SCOPE}"
    assert seen["scope"] == auth._POWERBI_SCOPE


def test_notebook_token_still_wins_over_oidc(monkeypatch):
    # In a Fabric notebook the runtime token is used first — the OIDC branch is a pure fallback, so
    # the notebook path is unchanged.
    monkeypatch.setattr(auth, "_notebook_fabric_api_token", lambda: "NBTOK")
    monkeypatch.setattr(auth, "_github_oidc_token",
                        lambda scope=auth._STORAGE_SCOPE: pytest.fail("OIDC must not be reached"))
    assert auth.get_fabric_token() == "NBTOK"
    assert auth.get_powerbi_token() == "NBTOK"


# ------------------------------------------------------- #4 quote-aware predicate rewriting

def test_qualify_predicate_leaves_string_literal_untouched():
    # A column name inside a string literal must NOT be qualified.
    got = Plugin._qualify_predicate("status != 'archived status'", ["status"])
    assert got == "target.status != 'archived status'"


def test_qualify_predicate_skips_qualified_and_functions():
    assert Plugin._qualify_predicate("x.status = 1", ["status"]) == "x.status = 1"
    assert Plugin._qualify_predicate("current_date > id", ["id"]) == "current_date > target.id"


def test_delete_insert_predicates_strip_is_quote_aware():
    got = Plugin._delete_insert_predicates(
        ["DBT_INTERNAL_DEST.id = 1 and note = 'DBT_INTERNAL_DEST.x'"]
    )
    assert got == ["id = 1 and note = 'DBT_INTERNAL_DEST.x'"]


def test_dollar_quote_reexport_is_shared():
    assert delta_dml._dollar_quote_end is sqlscan._dollar_quote_end


# ------------------------------------------------------- #5 structural UPDATE / ALTER splits

def test_update_where_inside_set_literal_not_missplit():
    sql = "update t set note = 'apply where needed', qty = 2 where id = 1"
    m = delta_dml._fullmatch(delta_dml._UPDATE, sql)
    body = m.group("body")
    w = delta_dml._find_top_level(body, delta_dml._TOP_WHERE)
    set_clause, where = body[:w], body[w + len("where"):].strip()
    assert where == "id = 1"
    assert "'apply where needed'" in set_clause


@pytest.mark.parametrize("coldef, expected", [
    ("varchar default 'not null'", "varchar"),   # 'not null' is a literal, not a NOT NULL clause
    ("integer not null", "integer"),
    ("varchar null", "varchar"),
    ("decimal(10,2)", "decimal(10,2)"),          # no tail; parens don't confuse the scanner
])
def test_alter_add_type_split_is_quote_aware(coldef, expected):
    t = delta_dml._find_top_level(coldef, delta_dml._ALTER_TAIL)
    got = (coldef if t == -1 else coldef[:t]).strip()
    assert got == expected


def test_identity_token_no_browser_without_tty(monkeypatch):
    """Even the initial path skips InteractiveBrowserCredential when not attached to a TTY."""
    calls = []

    class FakeCli:
        def get_token(self, scope):
            calls.append("cli")
            raise RuntimeError("no cli session")

    import types
    fake_azure = types.SimpleNamespace(AzureCliCredential=FakeCli)
    monkeypatch.setitem(__import__("sys").modules, "azure.identity", fake_azure)
    monkeypatch.setattr(auth.sys.stdin, "isatty", lambda: False)
    # No InteractiveBrowserCredential is imported/used; returns None after CLI fails.
    assert auth._azure_identity_token(interactive=True) is None
    assert calls == ["cli"]


# ------------------------------------------------------- 2026-07 full review: P1 fixes

# --- token never leaks into a CREATE SECRET failure (secret._execute_secret_sql)

class _SecretEchoConn:
    """A conn whose CREATE SECRET fails the way DuckDB does: echoing the statement text."""

    def execute(self, sql):
        if "ACCESS_TOKEN" in sql and sql.startswith("CREATE OR REPLACE SECRET"):
            raise RuntimeError(f"Parser Error: something broke\nLINE 1: {sql}\n   ^")
        return None  # INSTALL/LOAD/SET are no-ops


def test_secret_mint_failure_redacts_token():
    tok = "eyJfake'TOKEN-SECRET-VALUE"
    with pytest.raises(RuntimeError) as exc:
        secret.ensure_azure_secret(_SecretEchoConn(), {"bearer_token": tok})
    msg = str(exc.value)
    assert tok not in msg and tok.replace("'", "''") not in msg
    assert "<redacted>" in msg
    # The original (token-bearing) exception must not ride along on the chain either.
    assert exc.value.__cause__ is None and exc.value.__context__ is None


def test_scoped_secret_mint_failure_redacts_token():
    tok = "eyJfakeSCOPED-SECRET-VALUE"
    with pytest.raises(RuntimeError) as exc:
        secret.mint_scoped_secret(_SecretEchoConn(), "duckrun_cat_x", "abfss://w@host/l/Tables", {"token": tok})
    assert tok not in str(exc.value)


def test_secret_mint_success_still_returns_true():
    class _OkConn:
        def __init__(self):
            self.sqls = []

        def execute(self, sql):
            self.sqls.append(sql)

    con = _OkConn()
    assert secret.ensure_azure_secret(con, {"bearer_token": "tok"}) is True
    assert any("CREATE OR REPLACE SECRET" in s for s in con.sqls)


# --- sqlscan skips SQL comments (line, block, nested block)

def test_qualify_identifiers_skips_line_comment():
    got = sqlscan.qualify_identifiers("amount > 5 -- amount is checked\n and amount < 9", ["amount"])
    assert got == "target.amount > 5 -- amount is checked\n and target.amount < 9"


def test_qualify_identifiers_skips_block_comment():
    got = sqlscan.qualify_identifiers("/* amount */ amount > 5", ["amount"])
    assert got == "/* amount */ target.amount > 5"


def test_qualify_identifiers_skips_nested_block_comment():
    got = sqlscan.qualify_identifiers("/* outer /* amount */ still comment */ amount = 1", ["amount"])
    assert got == "/* outer /* amount */ still comment */ target.amount = 1"


def test_strip_qualifier_skips_comments():
    got = sqlscan.strip_qualifier(
        "DBT_INTERNAL_DEST.id = 1 -- DBT_INTERNAL_DEST.id stays\n", "DBT_INTERNAL_DEST")
    assert got == "id = 1 -- DBT_INTERNAL_DEST.id stays\n"


def test_unterminated_block_comment_swallows_rest():
    # An unterminated /* runs to end-of-string, like in real SQL: nothing after it is rewritten.
    got = sqlscan.qualify_identifiers("id = 1 /* trailing id", ["id"])
    assert got == "target.id = 1 /* trailing id"


# --- MaintenancePolicy decisions (previously untested pure logic)

def _mb(n):
    return n * 1024 * 1024


def test_policy_compact_needs_count_and_bytes():
    pol = policy.MaintenancePolicy(target_file_size=_mb(256))
    small = _mb(64)
    # 7 small files: count floor not met, even with plenty of bytes.
    assert not pol.should_compact([small] * 7)
    # 8 tiny files: count met, byte floor (2 x target = 512MB) not met.
    assert not pol.should_compact([1024] * 8)
    # 8 x 64MB = 512MB: both floors met.
    assert pol.should_compact([small] * 8)
    # Target-sized files never count as small, whatever their number.
    assert not pol.should_compact([_mb(256)] * 100)


def test_policy_small_file_threshold_is_half_target():
    pol = policy.MaintenancePolicy(target_file_size=_mb(256))
    assert pol.small_file_threshold == _mb(128)
    # A file exactly at the threshold is NOT small (strict <).
    assert not pol.should_compact([_mb(128)] * 100)


def test_policy_partitions_to_compact_only_offending():
    pol = policy.MaintenancePolicy(target_file_size=_mb(256))
    parts = pol.partitions_to_compact([("p1", _mb(1)), ("p2", _mb(256)), ("p3", _mb(1))])
    assert parts == {"p1", "p3"}


def test_policy_vacuum_gated_on_compaction_and_age():
    pol = policy.MaintenancePolicy(min_vacuum_interval_s=100)
    assert not pol.should_vacuum(False, 1e9)     # no compaction -> never
    assert not pol.should_vacuum(True, 99)       # too recent
    assert pol.should_vacuum(True, 100)


def test_policy_run_maintenance_swallows_lost_race_only():
    from deltalake.exceptions import CommitFailedError as CFE
    pol = policy.MaintenancePolicy()
    ran = []
    pol.run_maintenance(lambda: ran.append("c"), lambda: ran.append("v"), True)
    assert ran == ["c", "v"]
    pol.run_maintenance(lambda: ran.append("skipped"), lambda: None, False)
    assert "skipped" not in ran
    # A maintenance CommitFailedError is swallowed (the data commit already succeeded)...
    pol.run_maintenance(lambda: (_ for _ in ()).throw(CFE("lost race")), lambda: None, True)
    # ...but any other exception propagates: a real fault, not a lost race.
    with pytest.raises(ValueError):
        pol.run_maintenance(lambda: (_ for _ in ()).throw(ValueError("real")), lambda: None, True)


# --- update_rows validates SET columns (SQL == DataFrame parity)

def test_update_rows_rejects_unknown_column(tmp_path):
    import pyarrow as pa
    from deltalake import write_deltalake

    p = str(tmp_path / "t")
    write_deltalake(p, pa.table({"id": [1, 2], "v": [10, 20]}))
    v = engine.table_version(p)
    with pytest.raises(ValueError, match="unknown column"):
        engine.update_rows(p, {"nope": "1"}, "id = 1", read_version=v)
    # Fails loud with NO commit: the log did not advance.
    assert engine.table_version(p) == v
    # A valid column still updates (case-insensitive match, like the SQL path).
    engine.update_rows(p, {"V": "99"}, "id = 1", read_version=v)
    assert engine.table_version(p) == v + 1
