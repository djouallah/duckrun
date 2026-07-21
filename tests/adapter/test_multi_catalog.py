"""Unit tests for the multi-catalog (multi-Lakehouse) plumbing (issue #7).

The end-to-end behavior runs through real dbt in tests/correctness/multi_catalog/. These are the
fast, no-dbt-run guards on the individual pieces: the credential resolver (the single source of
truth every write/read/discovery path routes through), the DML catalog matcher, the scoped-secret
minting, and the plugin's write-token precedence.
"""
import types
from unittest import mock

import duckdb
import pytest

from dbt.adapters.duckrun import delta_dml, secret
from dbt.adapters.duckrun.credentials import DuckrunCredentials
from dbt.adapters.duckrun.delta_plugin import Plugin


def _creds(**kw):
    base = dict(database="silver", path=":memory:", root_path="/lh/silver",
                storage_options={"bearer_token": "SILVER"})
    base.update(kw)
    return DuckrunCredentials(**base)


BRONZE = {"lh_bronze": {"root_path": "/lh/bronze", "storage_options": {"bearer_token": "BRONZE"}}}


# --------------------------------------------------------------- resolver: root_for

def test_root_for_default_alias_and_fallback():
    c = _creds(catalogs=BRONZE)
    assert c.root_for("silver") == ("/lh/silver", {"bearer_token": "SILVER"})
    assert c.root_for("lh_bronze") == ("/lh/bronze", {"bearer_token": "BRONZE"})
    assert c.root_for(None) == ("/lh/silver", {"bearer_token": "SILVER"})   # default
    assert c.root_for("nope") == ("/lh/silver", {"bearer_token": "SILVER"})  # unknown -> default


def test_no_catalogs_is_zero_change():
    """A profile with no catalogs: exactly one entry equal to the top-level root_path/token, so
    every resolver behaves as it did before this feature."""
    c = _creds()
    assert c.catalogs is None
    assert c.catalog_locations is None
    assert list(c.catalog_roots()) == ["silver"]
    assert c.root_for(None) == ("/lh/silver", {"bearer_token": "SILVER"})
    assert c.root_for("anything") == ("/lh/silver", {"bearer_token": "SILVER"})


def test_catalog_locations_are_token_free():
    """The map surfaced on the Jinja target carries roots only — never tokens."""
    c = _creds(catalogs=BRONZE)
    assert c.catalog_locations == {"lh_bronze": "/lh/bronze"}
    assert "catalog_locations" in c._connection_keys()
    assert "catalogs" not in c._connection_keys()  # raw (tokened) map stays off the target


def test_storage_options_for_location_prefix_match():
    c = _creds(catalogs=BRONZE)
    assert c.storage_options_for_location("/lh/bronze/main/t") == {"bearer_token": "BRONZE"}
    assert c.storage_options_for_location("/lh/silver/main/t") == {"bearer_token": "SILVER"}
    assert c.storage_options_for_location("/elsewhere/t") == {"bearer_token": "SILVER"}  # default


def test_plugin_config_carries_catalogs():
    pc = [p for p in _creds(catalogs=BRONZE).plugins if getattr(p, "alias", None) == "duckrun"][0]
    assert pc.config["catalogs"] == BRONZE
    assert pc.config["storage_options"] == {"bearer_token": "SILVER"}
    assert pc.config["default_database"] == "silver"


# --------------------------------------------------------------- DML catalog routing

@pytest.mark.parametrize("sql,expected", [
    ("insert into lh_bronze.main.t select 1", "lh_bronze"),   # 3-part -> catalog
    ("delete from lh_bronze.main.t where id=1", "lh_bronze"),
    ('update "lh_bronze"."main"."t" set a=1', "lh_bronze"),
    ("insert into main.t select 1", None),                    # 2-part -> default
    ("insert into t select 1", None),                         # bare -> default
    ("with c as (select 1) insert into lh_bronze.main.t select * from c", "lh_bronze"),
])
def test_dml_target_catalog(sql, expected):
    assert delta_dml.dml_target_catalog(sql) == expected


# --------------------------------------------------------------- scoped secret

def test_mint_scoped_secret_coexists_with_default():
    con = duckdb.connect()
    assert secret.ensure_azure_secret(con, {"bearer_token": "DEFAULT"}) is True
    assert secret.mint_scoped_secret(
        con, secret.scoped_secret_name("lh_bronze"), "abfss://ws@onelake.dfs.fabric.microsoft.com/b/Tables",
        {"bearer_token": "BRONZE"},
    ) is True
    names = {r[0] for r in con.execute("select name from duckdb_secrets()").fetchall()}
    assert secret.SECRET_NAME in names          # default, unscoped
    assert "duckrun_cat_lh_bronze" in names     # aliased, scoped
    # The scoped secret carries the catalog's root as its scope so longest-prefix routing works.
    scopes = con.execute(
        "select scope from duckdb_secrets() where name = 'duckrun_cat_lh_bronze'"
    ).fetchone()[0]
    assert any("onelake" in str(s) for s in scopes)


def test_mint_scoped_secret_noop_without_token():
    con = duckdb.connect()
    assert secret.mint_scoped_secret(con, "duckrun_cat_x", "/local/root", None) is False
    names = {r[0] for r in con.execute("select name from duckdb_secrets()").fetchall()}
    assert "duckrun_cat_x" not in names  # no token -> nothing minted


# --------------------------------------------------------------- plugin write-token precedence

def _plugin(catalogs, storage_options):
    p = Plugin.__new__(Plugin)
    p.initialize({"catalogs": catalogs, "storage_options": storage_options,
                  "default_database": "silver"})
    return p


def _relation(database):
    return types.SimpleNamespace(database=database)


def test_store_token_precedence():
    p = _plugin(BRONZE, {"bearer_token": "SILVER"})
    # A model in the aliased catalog gets that catalog's token.
    assert p._catalog_storage_options("lh_bronze") == {"bearer_token": "BRONZE"}
    # A default-catalog model (or unknown db) gets the default token.
    assert p._catalog_storage_options("silver") == {"bearer_token": "SILVER"}
    assert p._catalog_storage_options(None) == {"bearer_token": "SILVER"}


def test_store_single_catalog_uses_default():
    p = _plugin({}, {"bearer_token": "SILVER"})
    assert p._catalog_storage_options("silver") == {"bearer_token": "SILVER"}
    assert p._catalog_storage_options("lh_bronze") == {"bearer_token": "SILVER"}


# ------------------------------------------------- format: iceberg -> stock attach:/secrets:

def _iceberg_creds(**kw):
    """Build credentials from a `format: iceberg` profile with the token acquisition stubbed —
    resolving it for real would reach OneLake, which CI can't do."""
    data = {"type": "duckrun", "format": "iceberg", "root_path": "ws/sales.Lakehouse",
            "schema": "mart", "threads": 1}
    data.update(kw)
    with mock.patch.object(secret, "with_onelake_token", return_value={"bearer_token": "TOK"}):
        return DuckrunCredentials.from_dict(data)


def test_iceberg_profile_expands_to_one_attachment():
    """The whole point: `format: iceberg` + the lakehouse renders the ATTACH a user would otherwise
    hand-write (endpoint, token, Fabric's flags, default schema) — no root_path left behind."""
    c = _iceberg_creds()
    assert c.root_path is None and c.database == "sales"
    assert len(c.attach) == 1
    sql = c.attach[0].to_sql()
    assert "'ws/sales.Lakehouse'" in sql and "AS sales" in sql
    assert "TYPE iceberg" in sql
    assert "ENDPOINT 'https://onelake.table.fabric.microsoft.com/iceberg'" in sql
    assert "TOKEN 'TOK'" in sql
    assert "DEFAULT_SCHEMA 'dbo'" in sql
    # Booleans must survive as options: dbt-duckdb DROPS an option whose value is Python False.
    assert "STAGE_CREATE_TABLES 'false'" in sql
    assert "SKIP_CREATE_TABLE_METADATA_UPDATES 'true'" in sql
    assert "ACCESS_DELEGATION_MODE 'none'" in sql
    # The storage credential DuckDB writes the data files with, from the same token.
    assert any("CREATE OR REPLACE SECRET duckrun_onelake" in s for s in c.secrets_sql())
    assert c.settings.get("preserve_insertion_order") is False


def test_iceberg_schema_and_alias_from_path():
    c = _iceberg_creds(root_path="ws/sales.Lakehouse/mart", database="ice")
    assert c.database == "ice"
    assert "DEFAULT_SCHEMA 'mart'" in c.attach[0].to_sql()
    assert "AS ice" in c.attach[0].to_sql()


def test_iceberg_native_catalog_has_no_delta_root():
    """The regression guard: an unknown database falls back to the DEFAULT root, so without this a
    natively-attached catalog's CREATE TABLE AS would be written as Delta into the wrong lakehouse."""
    c = _iceberg_creds()
    assert c.native_catalogs() == {"sales"}
    assert c.native_catalog_names == ["sales"]
    assert c.root_for("sales") == (None, None)
    assert c.root_for() == (None, None)
    # A hand-written attach: entry is native too, even next to a Delta root.
    from dbt.adapters.duckdb.credentials import Attachment
    d = _creds(attach=[Attachment(path="./ice.duckdb", alias="ice")])
    assert d.root_for("ice") == (None, None)
    assert d.root_for("silver") == ("/lh/silver", {"bearer_token": "SILVER"})


def test_iceberg_expansion_is_idempotent():
    """dbt deserializes a profile more than once and the expansion consumes root_path — a second
    pass must recognize its own output instead of erroring on the missing lakehouse."""
    data = {"type": "duckrun", "format": "iceberg", "root_path": "ws/sales.Lakehouse"}
    with mock.patch.object(secret, "with_onelake_token", return_value={"bearer_token": "TOK"}):
        once = DuckrunCredentials.__pre_deserialize__(dict(data))
        twice = DuckrunCredentials.__pre_deserialize__(dict(once))
    assert len(twice["attach"]) == 1


def test_unknown_format_fails_loud():
    with pytest.raises(Exception, match="unknown format"):
        DuckrunCredentials.from_dict({"type": "duckrun", "format": "hudi", "path": ":memory:"})


def test_no_format_is_zero_change():
    """A Delta profile (no `format`) keeps every existing behavior — no attach, no secrets."""
    c = _creds()
    assert c.format is None and c.attach in (None, [])
    assert c.native_catalogs() == set() and c.native_catalog_names is None
    assert c.root_for("anything") == ("/lh/silver", {"bearer_token": "SILVER"})
