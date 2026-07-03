"""Regression tests for the code-review work order (duckrun_review_for_opus.md).

Pure-Python unit tests for the small, self-contained fixes — the ones that don't need a real
``dbt run`` to exercise. Grouped by review item number.
"""
import duckdb
import pytest

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


# ------------------------------------------------------- #10 non-interactive token refresh

def test_refresh_never_reaches_interactive_browser(monkeypatch):
    """refresh_storage_token must call _azure_identity_token(interactive=False) — never a browser."""
    seen = {}

    def fake_identity(interactive=True):
        seen["interactive"] = interactive
        return "tok"

    monkeypatch.setattr(auth, "_fabric_token", lambda: None)
    monkeypatch.setattr(auth, "_github_oidc_token", lambda: None)
    monkeypatch.setattr(auth, "_azure_identity_token", fake_identity)
    assert auth.refresh_storage_token() == "tok"
    assert seen["interactive"] is False


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
