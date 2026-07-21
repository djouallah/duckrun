"""Unit coverage for ``Plugin.source_scan_sql`` — the SQL a ``meta.plugin: duckrun`` source
resolves to when DuckrunEnvironment wraps it in a ``CREATE OR REPLACE VIEW``.

A source declares *where/what* (location + format) only. Format comes from ``meta.format`` or is
inferred from the file extension; ``delta_table_path`` forces Delta. csv/parquet/json/delta are the
supported readers, anything else is a hard error (no silent fallback).
"""
import pytest

from dbt.adapters.duckrun.delta_plugin import Plugin


def scan(**meta):
    return Plugin.source_scan_sql(meta)


# ----------------------------------------------------------------- explicit meta.format
def test_explicit_csv():
    assert scan(location="data.txt", format="csv") == "SELECT * FROM read_csv_auto('data.txt')"


def test_explicit_json():
    assert scan(location="data.txt", format="json") == (
        "SELECT * FROM read_json_auto('data.txt', maximum_object_size=2147483647)"
    )


def test_explicit_parquet():
    assert scan(location="data.bin", format="parquet") == "SELECT * FROM read_parquet('data.bin')"


def test_delta_table_path_forces_delta():
    sql = scan(delta_table_path="/lake/t", format="csv")  # delta_table_path wins over format
    assert sql == "SELECT * FROM delta_scan('/lake/t')"


# ----------------------------------------------------------------- extension inference
@pytest.mark.parametrize(
    "path,call",
    [
        ("https://x/communes.json", "read_json_auto('https://x/communes.json', maximum_object_size=2147483647)"),
        ("https://x/feed.ndjson", "read_json_auto('https://x/feed.ndjson', maximum_object_size=2147483647)"),
        ("https://x/feed.json.gz", "read_json_auto('https://x/feed.json.gz', maximum_object_size=2147483647)"),
        ("https://x/codes.csv", "read_csv_auto('https://x/codes.csv')"),
        ("https://x/dvf.csv.gz", "read_csv_auto('https://x/dvf.csv.gz')"),
        ("https://x/part.parquet", "read_parquet('https://x/part.parquet')"),
        ("/lake/dim_calendar", "delta_scan('/lake/dim_calendar')"),  # bare directory -> delta
    ],
)
def test_format_inferred_from_extension(path, call):
    assert scan(location=path) == f"SELECT * FROM {call}"


# ----------------------------------------------------------------- OneLake shorthand
def test_onelake_shorthand_source_path_expanded():
    # `{{ env_var('WAREHOUSE_PATH') }}/sources/dim_calendar` with the env holding the short form.
    assert scan(delta_table_path="ws/lh.Lakehouse/sources/dim_calendar") == (
        "SELECT * FROM delta_scan('abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/"
        "sources/dim_calendar')")
    # the Files side keeps its section, and the extension still drives the reader
    assert scan(location="ws/lh.Lakehouse/Files/csv/duid.csv") == (
        "SELECT * FROM read_csv_auto('abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/"
        "Files/csv/duid.csv')")


def test_local_source_path_not_mistaken_for_shorthand():
    assert scan(location="data/lake/codes.csv") == "SELECT * FROM read_csv_auto('data/lake/codes.csv')"


# ----------------------------------------------------------------- errors / safety
def test_unknown_format_is_a_hard_error():
    with pytest.raises(ValueError, match="Unsupported duckrun source format"):
        scan(location="x.shp", format="shape")


def test_missing_path_raises():
    with pytest.raises(ValueError, match="requires 'delta_table_path', 'location', or 'path'"):
        scan(format="csv")


def test_single_quote_in_path_is_escaped():
    assert scan(location="a'b.json") == (
        "SELECT * FROM read_json_auto('a''b.json', maximum_object_size=2147483647)"
    )
