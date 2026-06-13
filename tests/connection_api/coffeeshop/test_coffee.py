"""One coffee-shop scenario for ``duckrun.connect``, run against two storage targets.

The whole end-to-end flow lives in one function, :func:`run_coffee_scenario`, parameterized by the
number of fact rows. Two tests drive it:

  • ``test_coffee_local``   — a local-filesystem warehouse, **big** row count (stress: see how far a
                              GitHub runner / your laptop scales a partitioned Delta write + merge).
  • ``test_coffee_onelake`` — live OneLake over ``abfss://``, **small** row count (the network is the
                              bottleneck, not the engine), isolated schema, skips without creds.

Two knobs, both env-overridable so a CI job can crank them:

  COFFEE_LOCAL_ROWS     local fact rows   (default 1,000,000)
  COFFEE_ONELAKE_ROWS   OneLake fact rows (default 2,000)

OneLake target (no Azure ids hardcoded — same convention as the `aemo` workflow):
  WAREHOUSE_PATH   abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables
  ONELAKE_TOKEN    storage bearer token (resource https://storage.azure.com/)

Dimensions come from Josue Bogran's coffeeshopdatageneratorv2 CSVs (read over https); the fact is
generated locally in DuckDB. ``Dim_Products.csv`` is SCD2 (``product_id`` repeats across validity
windows), so the scenario first dedups it to a current, unique-key ``products`` table — which makes
the fact join 1:1 and gives the merge a legitimate key. Re-runnable: stable names + overwrite
(``overwriteSchema`` where a schema is intentionally reset).
"""
import os

import pytest

import duckrun
from duckrun import DeltaTable

GH = "https://raw.githubusercontent.com/JosueBogran/coffeeshopdatageneratorv2/main/"

LOCAL_ROWS = int(os.environ.get("COFFEE_LOCAL_ROWS", "1000000"))
ONELAKE_ROWS = int(os.environ.get("COFFEE_ONELAKE_ROWS", "2000"))

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")
ONELAKE_TOKEN = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
ONELAKE_SCHEMA = os.environ.get("DUCKRUN_IT_SCHEMA", "duckrun_conn_it")


def run_coffee_scenario(conn, schema, n_rows):
    """Exercise the full connection-API surface against ``conn`` with an ``n_rows`` fact table."""
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731 — scalar helper
    conn.connection.execute("INSTALL httpfs; LOAD httpfs;")  # read the dim CSVs over https

    # ── ingest dimensions: DataFrameReader.csv → Delta ───────────────────────────────────────────
    conn.read.csv(GH + "Dim_Locations.csv").write.mode("overwrite").saveAsTable("dim_locations")
    conn.read.csv(GH + "Dim_Products.csv").write.mode("overwrite").saveAsTable("dim_products")
    conn.refresh()
    assert conn.table("dim_locations").count() == 1000
    assert conn.table("dim_products").count() == 26   # SCD2 rows (product_id repeats)

    # ── dedup the SCD2 product dim to a current, unique-key 'products' table ──────────────────────
    conn.sql("""
        select product_id, name, category, subcategory, standard_cost, standard_price
        from (
            select *, row_number() over (partition by product_id order by to_date desc) as rn
            from dim_products
        ) where rn = 1
    """).write.mode("overwrite").saveAsTable("products")
    conn.refresh()
    n_products = conn.table("products").count()
    assert n_products == q("select count(distinct product_id) from dim_products")

    # ── generate an n_rows coffee-shop fact table → Delta, partitioned by region ─────────────────
    # Dense-index products & locations and sample by index so every order line joins exactly once.
    conn.sql(f"""
        with prods as (select *, row_number() over (order by product_id) as prn from products),
             locs  as (select location_id, region,
                              row_number() over (order by record_id) as lrn from dim_locations),
             raw as (
                select i as order_line_id,
                       1 + (i / 3)::int as order_id,
                       floor(random() * (select count(*) from prods))::int + 1 as prn,
                       floor(random() * (select count(*) from locs))::int  + 1 as lrn,
                       (random()*4)::int + 1 as quantity,
                       case when random() < 0.8 then 0 else (random()*14)::int + 1 end as discount_percentage,
                       date '2023-01-01' + (random()*364)::int as order_date
                from range(1, {n_rows + 1}) t(i)
             )
        select r.order_id, r.order_line_id, r.order_date,
               case when month(r.order_date) in (12,1,2) then 'Winter'
                    when month(r.order_date) in (3,4,5)  then 'Spring'
                    when month(r.order_date) in (6,7,8)  then 'Summer' else 'Fall' end as season,
               l.location_id, l.region, p.name as product_name, r.quantity,
               round(p.standard_price * ((100 - r.discount_percentage) / 100.0) * r.quantity, 2) as sales_amount,
               r.discount_percentage, p.product_id
        from raw r
        join prods p on p.prn = r.prn
        join locs  l on l.lrn = r.lrn
    """).write.mode("overwrite").partitionBy("region").saveAsTable("fact_sales")
    conn.refresh()
    assert conn.table("fact_sales").count() == n_rows   # 1:1 joins → no rows dropped

    # ── catalog (Spark Catalog) ──────────────────────────────────────────────────────────────────
    assert conn.catalog.currentDatabase() == schema
    assert schema in conn.catalog.listDatabases()
    assert {"products", "fact_sales", "dim_locations"} <= set(conn.catalog.listTables())
    assert {"products", "fact_sales"} <= {r[0] for r in conn.sql("SHOW TABLES").fetchall()}

    # ── analytics → a mart Delta table; revenue reconciles exactly (1:1 join to unique products) ──
    conn.sql("""
        select p.category, f.season, count(*) as order_lines, round(sum(f.sales_amount), 2) as revenue
        from fact_sales f join products p on p.product_id = f.product_id
        group by 1, 2
    """).write.mode("overwrite").saveAsTable("mart_revenue")
    conn.refresh()
    assert q("select count(*) from mart_revenue") >= 4   # (category x season) cells
    assert abs(q("select sum(revenue) from mart_revenue")
               - q("select sum(sales_amount) from fact_sales")) < 1.0   # tiny rounding only

    # DataFrame aliases over a top-products query
    top = conn.sql("select product_name, sum(sales_amount) rev from fact_sales group by 1 order by rev desc limit 3")
    assert len(top.collect()) == 3                                # .collect() → fetchall()
    assert list(top.toPandas().columns) == ["product_name", "rev"]  # .toPandas() → df()

    # ── write modes: append / ignore / Spark default 'error' ─────────────────────────────────────
    conn.sql("select * from fact_sales limit 1").write.mode("append").saveAsTable("fact_sales")
    assert conn.table("fact_sales").count() == n_rows + 1
    conn.sql("select * from fact_sales limit 0").write.mode("ignore").saveAsTable("fact_sales")
    assert conn.table("fact_sales").count() == n_rows + 1      # ignore = no-op when it exists
    with pytest.raises(ValueError):
        conn.sql("select 1 x").write.saveAsTable("products")   # default 'error' refuses clobber

    # ── schema evolution: overwriteSchema resets, then mergeSchema widens (re-runnable) ──────────
    conn.sql("select 1 id, 'A' grp") \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("evt")
    conn.refresh()
    assert conn.sql("select * from evt").columns == ["id", "grp"]
    conn.sql("select 2 id, 'B' grp, true as flagged") \
        .write.mode("append").option("mergeSchema", "true").saveAsTable("evt")
    conn.refresh()
    assert "flagged" in conn.sql("select * from evt").columns

    # ── read API straight off the store by path ──────────────────────────────────────────────────
    assert conn.read.delta(conn.table_path(schema, "products")).count() == n_products
    assert conn.read.format("delta").load(conn.table_path(schema, "fact_sales")).count() == n_rows + 1

    # ── MERGE / upsert: a price-list update (DeltaTable.merge → engine.merge_delta) on 'products' ─
    old_price = q("select standard_price from products where product_id = 1")
    new_prices = conn.sql("""
        select * from (values
            (1,  'Latte',             'Hot',  'Coffee',   2.0, 5.25),
            (99, 'Pumpkin Cold Brew', 'Cold', 'Seasonal', 2.5, 6.50)
        ) t(product_id, name, category, subcategory, standard_cost, standard_price)
    """)
    DeltaTable.forName(conn, f"{schema}.products").merge(new_prices, "target.product_id = source.product_id") \
        .whenMatchedUpdate(set={"standard_price": "source.standard_price"}) \
        .whenNotMatchedInsertAll().execute()
    assert q("select standard_price from products where product_id = 1") == 5.25  # matched updated
    assert q("select name from products where product_id = 99") == "Pumpkin Cold Brew"  # inserted
    assert conn.table("products").count() == n_products + 1
    assert old_price != 5.25  # sanity: the merge actually changed the price

    # insert-only: existing rows untouched, only the new key lands
    src = conn.sql("""
        select * from (values
            (1,   'IGNORED',          'Hot',  'Coffee',   0.0, 0.0),
            (200, 'Nitro Cold Brew',  'Cold', 'Seasonal', 2.5, 6.0)
        ) t(product_id, name, category, subcategory, standard_cost, standard_price)
    """)
    DeltaTable.forName(conn, f"{schema}.products").merge(src, "target.product_id = source.product_id") \
        .whenNotMatchedInsertAll().execute()
    assert conn.table("products").count() == n_products + 2            # only product_id=200 added
    assert q("select standard_price from products where product_id = 1") == 5.25  # untouched
    assert q("select name from products where product_id = 1") == "Latte"


def test_coffee_local(tmp_path):
    """Local filesystem, big fact table — stress the engine without the network in the way."""
    conn = duckrun.connect(str(tmp_path / "wh"), schema="dbo")
    run_coffee_scenario(conn, "dbo", LOCAL_ROWS)


@pytest.mark.skipif(
    not (WAREHOUSE_PATH and WAREHOUSE_PATH.startswith("abfss://") and ONELAKE_TOKEN),
    reason="OneLake not configured (set WAREHOUSE_PATH=abfss://…/Tables and ONELAKE_TOKEN)",
)
def test_coffee_onelake():
    """Live OneLake, small fact table — the network dominates, so keep the row count tiny."""
    conn = duckrun.connect(WAREHOUSE_PATH, storage_options={"bearer_token": ONELAKE_TOKEN}, schema=ONELAKE_SCHEMA)
    run_coffee_scenario(conn, ONELAKE_SCHEMA, ONELAKE_ROWS)
