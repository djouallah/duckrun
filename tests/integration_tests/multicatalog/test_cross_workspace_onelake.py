"""LIVE OneLake: two lakehouses in two workspaces, one session — attach the second as a catalog,
join across them, write the result back to the first. Each side writes into its own isolated schema,
removed at the end. Needs the CI's OIDC identity (duckrun self-acquires one token for both)."""
import os
import uuid

import pytest

import duckrun

# workspace `tpch`, lakehouse `duckrun`  (the primary: writable)
WS_A, LH_A = "1c52481c-0523-4a5a-bbde-fdc932bd77c2", "7cb28f47-65bd-4aac-96c0-f7dd4eeb40ff"
# workspace `analytics_as_code_dwh`, lakehouse `data`  (another workspace)
WS_B, LH_B = "d5f25039-7f33-4668-b909-2c397b2626fb", "9c0f9e72-e50f-40cd-aa94-1e7606c653a2"

pytestmark = pytest.mark.skipif(not os.environ.get("AZURE_CLIENT_ID"),
                                reason="live OneLake: needs the CI's OIDC identity")


def _drop_schema(conn, catalog, schema):
    """Delete the schema folder (DROP TABLE removes the tables, not the empty schema folder)."""
    from dbt.adapters.duckrun import objectstore
    url = conn._table_path(schema, "x", catalog).rstrip("/").rsplit("/", 1)[0]
    objectstore.delete_tree(url, conn.storage_options)


def run_cross_workspace(schema):
    conn = duckrun.connect(f"{WS_A}/{LH_A}", schema=schema, read_only=False)
    conn.attach(f"{WS_B}/{LH_B}", name="other", schema=schema, read_only=False)
    try:
        # one table in each workspace
        conn.sql("CREATE TABLE orders AS SELECT * FROM (VALUES "
                 "(1, 'north', 10.0), (2, 'south', 20.0), (3, 'north', 5.0)) t(id, region, amount)")
        conn.sql(f"CREATE TABLE other.{schema}.budgets AS SELECT * FROM (VALUES "
                 "('north', 12.0), ('south', 30.0)) t(region, budget)")
        # the join, across workspaces
        join = (f"SELECT o.region, sum(o.amount) AS sales, any_value(b.budget) AS budget "
                f"FROM {schema}.orders o JOIN other.{schema}.budgets b USING (region) "
                f"GROUP BY o.region ORDER BY o.region")
        assert conn.sql(join).fetchall() == [("north", 15.0, 12.0), ("south", 20.0, 30.0)]
        # written back to workspace A
        conn.sql(f"CREATE TABLE sales_vs_budget AS {join}")
        fresh = duckrun.connect(f"{WS_A}/{LH_A}", schema=schema)
        assert fresh.sql("SELECT region, sales, budget FROM sales_vs_budget ORDER BY region"
                         ).fetchall() == [("north", 15.0, 12.0), ("south", 20.0, 30.0)]
        # and a write into the other workspace's catalog, from a join that reads both
        conn.sql(f"INSERT INTO other.{schema}.budgets "
                 f"SELECT 'east', sum(amount) FROM {schema}.orders")
        assert conn.sql(f"SELECT budget FROM other.{schema}.budgets WHERE region = 'east'"
                        ).fetchall() == [(35.0,)]
    finally:
        for table, catalog in (("orders", None), ("sales_vs_budget", None), ("budgets", "other")):
            try:
                conn.sql(f"DROP TABLE IF EXISTS {catalog + '.' + schema + '.' if catalog else ''}{table}")
            except Exception:  # noqa: BLE001 - cleanup is best effort; the schema delete follows
                pass
        _drop_schema(conn, None, schema)
        _drop_schema(conn, "other", schema)


def test_cross_workspace_join_and_write_back():
    run_cross_workspace(f"cross_ws_{uuid.uuid4().hex[:8]}")
