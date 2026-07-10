"""Unit tests for the multi-catalog (multi-Lakehouse) plumbing (issue #7).

The end-to-end behavior runs through real dbt in tests/correctness/multi_catalog/. These are the
fast, no-dbt-run guards on the individual pieces: the credential resolver (the single source of
truth every write/read/discovery path routes through), the DML catalog matcher, the scoped-secret
minting, and the plugin's write-token precedence.
"""
import types

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
