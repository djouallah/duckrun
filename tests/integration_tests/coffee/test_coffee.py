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

Dimensions come from Josue Bogran's coffeeshopdatageneratorv2 CSVs, vendored under data/ (MIT — see
data/README.md) so the scenario never touches the network; the fact is
generated locally in DuckDB. ``Dim_Products.csv`` is SCD2 (``product_id`` repeats across validity
windows), so the scenario first dedups it to a current, unique-key ``products`` table — which makes
the fact join 1:1 and gives the merge a legitimate key. Re-runnable: stable names + overwrite
(``overwriteSchema`` where a schema is intentionally reset).
"""
import os
import sys
import time
from contextlib import contextmanager

import pytest

import duckrun
from duckrun import DeltaTable

# The scenario (and session.refresh) print Unicode; force utf-8 so a Windows cp1252 console doesn't
# crash on it when watching the run with `pytest -s`.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# Dimension CSVs are vendored under data/ (see data/README.md) so the scenario — and the
# coffee-stress release gate that runs it — never depend on the network. Forward-slash path so the
# DuckDB read_csv_auto string is valid on Windows too.
DATA = (os.path.dirname(os.path.abspath(__file__)) + "/data").replace(os.sep, "/")


@contextmanager
def _step(n, label):
    """Narrate one stage of the scenario — what it does — and time it. Run pytest with ``-s`` to
    watch the coffee-shop pipeline build, stage by stage (the local stress test prints to the CI
    job log)."""
    print(f"\n[{n}] {label}", flush=True)
    t = time.perf_counter()
    yield lambda detail: print(f"      -> {detail}", flush=True)
    print(f"      ({time.perf_counter() - t:.2f}s)", flush=True)

LOCAL_ROWS = int(os.environ.get("COFFEE_LOCAL_ROWS", "1000000"))
ONELAKE_ROWS = int(os.environ.get("COFFEE_ONELAKE_ROWS", "2000"))

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH")
ONELAKE_TOKEN = os.environ.get("ONELAKE_TOKEN") or os.environ.get("AZURE_STORAGE_TOKEN")
ONELAKE_SCHEMA = os.environ.get("DUCKRUN_IT_SCHEMA", "duckrun_conn_it")


def run_coffee_scenario(conn, schema, n_rows):
    """Exercise the full connection-API surface against ``conn`` with an ``n_rows`` fact table."""
    q = lambda sql: conn.sql(sql).fetchone()[0]  # noqa: E731 — scalar helper

    print(f"\n=== coffee-shop scenario | schema='{schema}' | {n_rows:,} fact rows "
          f"| warehouse={getattr(conn, 'root_path', '?')} ===", flush=True)

    # ── ingest dimensions: DataFrameReader.csv → Delta ───────────────────────────────────────────
    with _step(1, "ingest dimensions: read vendored Dim_Locations / Dim_Products CSVs → Delta") as say:
        conn.read.csv(DATA + "/Dim_Locations.csv").write.mode("overwrite").saveAsTable("dim_locations")
        conn.read.csv(DATA + "/Dim_Products.csv").write.mode("overwrite").saveAsTable("dim_products")
        assert conn.table("dim_locations").count() == 1000
        assert conn.table("dim_products").count() == 26   # SCD2 rows (product_id repeats)
        say("dim_locations=1,000 rows, dim_products=26 SCD2 rows")

    # ── dedup the SCD2 product dim to a current, unique-key 'products' table ──────────────────────
    with _step(2, "dedup SCD2 dim_products → current, unique-key 'products' "
                  "(row_number() over product_id, keep latest to_date)") as say:
        conn.sql("""
            select product_id, name, category, subcategory, standard_cost, standard_price
            from (
                select *, row_number() over (partition by product_id order by to_date desc) as rn
                from dim_products
            ) where rn = 1
        """).write.mode("overwrite").saveAsTable("products")
        n_products = conn.table("products").count()
        assert n_products == q("select count(distinct product_id) from dim_products")
        say(f"{n_products} unique products (down from 26 SCD2 rows)")

    # ── generate an n_rows coffee-shop fact table → Delta, partitioned by region ─────────────────
    # Dense-index products & locations and sample by index so every order line joins exactly once.
    with _step(3, f"generate {n_rows:,} fact rows (random product/location/qty/discount/date), "
                  "join to dims 1:1, write partitioned by region") as say:
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
        assert conn.table("fact_sales").count() == n_rows   # 1:1 joins → no rows dropped
        regions = q("select count(distinct region) from fact_sales")
        say(f"fact_sales={n_rows:,} rows across {regions} region partitions (1:1 joins, no rows dropped)")

    # ── catalog (Catalog) ──────────────────────────────────────────────────────────────────
    with _step(4, "Catalog: currentDatabase / listDatabases / listTables / SHOW TABLES") as say:
        assert conn.catalog.currentDatabase() == schema
        assert schema in conn.catalog.listDatabases()
        assert {"products", "fact_sales", "dim_locations"} <= set(conn.catalog.listTables())
        assert {"products", "fact_sales"} <= {r[0] for r in conn.sql("SHOW TABLES").fetchall()}
        say(f"tables in '{schema}': {sorted(conn.catalog.listTables())}")

    # ── analytics → a mart Delta table; revenue reconciles exactly (1:1 join to unique products) ──
    with _step(5, "analytics: revenue by category x season -> mart_revenue (joins fact to products)") as say:
        conn.sql("""
            select p.category, f.season, count(*) as order_lines, round(sum(f.sales_amount), 2) as revenue
            from fact_sales f join products p on p.product_id = f.product_id
            group by 1, 2
        """).write.mode("overwrite").saveAsTable("mart_revenue")
        cells = q("select count(*) from mart_revenue")
        revenue = q("select sum(revenue) from mart_revenue")
        assert cells >= 4   # (category x season) cells
        assert abs(revenue - q("select sum(sales_amount) from fact_sales")) < 1.0   # tiny rounding only
        say(f"{cells} (category x season) cells, total revenue=${revenue:,.2f} (reconciles to fact_sales)")

        # DataFrame aliases: .collect() (→ fetchall), .columns, and .toPandas() (→ relation.df()).
        top = conn.sql("select product_name, sum(sales_amount) rev from fact_sales group by 1 order by rev desc limit 3")
        assert len(top.collect()) == 3
        assert top.columns == ["product_name", "rev"]
        assert list(top.toPandas().columns) == ["product_name", "rev"]
        say(f"top product by revenue: {top.collect()[0][0]} (${top.collect()[0][1]:,.2f})")

    # ── write modes: append / ignore / default 'error' ─────────────────────────────────────
    with _step(6, "write modes: append (+1 row), ignore (no-op), default 'error' (refuses clobber)") as say:
        conn.sql("select * from fact_sales limit 1").write.mode("append").saveAsTable("fact_sales")
        assert conn.table("fact_sales").count() == n_rows + 1
        conn.sql("select * from fact_sales limit 0").write.mode("ignore").saveAsTable("fact_sales")
        assert conn.table("fact_sales").count() == n_rows + 1      # ignore = no-op when it exists
        with pytest.raises(ValueError):
            conn.sql("select 1 x").write.saveAsTable("products")   # default 'error' refuses clobber
        say(f"append -> {n_rows + 1:,} rows; ignore -> still {n_rows + 1:,}; error mode raised as expected")

    # ── schema evolution: overwriteSchema resets, then mergeSchema widens (re-runnable) ──────────
    with _step(7, "schema evolution: overwriteSchema resets columns, then mergeSchema widens") as say:
        conn.sql("select 1 id, 'A' grp") \
            .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("evt")
        assert conn.sql("select * from evt").columns == ["id", "grp"]
        conn.sql("select 2 id, 'B' grp, true as flagged") \
            .write.mode("append").option("mergeSchema", "true").saveAsTable("evt")
        assert "flagged" in conn.sql("select * from evt").columns
        say("evt columns after mergeSchema append: " + str(conn.sql("select * from evt").columns))

    # ── read API straight off the store by path ──────────────────────────────────────────────────
    with _step(8, "read API by path: conn.read.format('delta').load(path)") as say:
        assert conn.read.format("delta").load(conn.table_path(schema, "products")).count() == n_products
        assert conn.read.format("delta").load(conn.table_path(schema, "fact_sales")).count() == n_rows + 1
        say(f"read products={n_products}, fact_sales={n_rows + 1:,} straight off the store")

    # ── MERGE / upsert: a price-list update (DeltaTable.merge → engine.merge_delta) on 'products' ─
    with _step(9, "MERGE upsert on products: update price for id=1, insert new id=99 "
                  "(whenMatchedUpdate + whenNotMatchedInsertAll)") as say:
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
        say(f"id=1 price {old_price} -> 5.25 (updated), id=99 inserted; products now {n_products + 1}")

    # insert-only: existing rows untouched, only the new key lands
    with _step(10, "MERGE insert-only on products: whenNotMatchedInsertAll — only new id=200 lands, "
                   "existing rows untouched") as say:
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
        say(f"id=200 inserted; id=1 untouched (still Latte @ 5.25); products now {n_products + 2}")

    print(f"\n=== scenario complete: {schema} ===", flush=True)


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


# A friendly-name abfss path (workspace/lakehouse names instead of GUIDs) over the SAME lakehouse.
# OneLake's delta_scan can't enumerate a valid table's _delta_log via friendly names
# (duckdb-delta#307); GUID paths read fine. This is the live gate for the connect() error hygiene:
# when delta_scan fails, the message must keep the real engine error but NOT echo the internal
# `CREATE OR REPLACE VIEW ... delta_scan(...)` SQL, and must carry the GUID workaround hint.
WAREHOUSE_PATH_FRIENDLY = os.environ.get("WAREHOUSE_PATH_FRIENDLY")


@pytest.mark.skipif(
    not (WAREHOUSE_PATH_FRIENDLY and WAREHOUSE_PATH_FRIENDLY.startswith("abfss://") and ONELAKE_TOKEN),
    reason="friendly-name OneLake path not configured (set WAREHOUSE_PATH_FRIENDLY=abfss://<name>@…/<name>.Lakehouse/Tables)",
)
def test_onelake_friendly_name_connect_error_is_clean():
    """Connect over a friendly-name OneLake path (no schema → discover every table). Either it
    connects (upstream fixed / nothing unreadable) or it fails with the duckrun-shaped error —
    in which case the message must NOT leak the generated CREATE VIEW SQL and MUST point at the
    GUID workaround. Locks the fix end-to-end on live OneLake, which the GUID-only jobs can't."""
    try:
        conn = duckrun.connect(WAREHOUSE_PATH_FRIENDLY, storage_options={"bearer_token": ONELAKE_TOKEN})
    except RuntimeError as exc:
        msg = str(exc)
        print(f"\nfriendly-name connect failed as expected (delta-kernel #307):\n{msg}", flush=True)
        assert "CREATE OR REPLACE VIEW" not in msg, "internal delta_scan SQL must not leak into the error"
        assert "duckdb-delta#307" in msg, "the GUID workaround hint must be present"
        assert "log segment" in msg or "IO Error" in msg, "the real engine error must be preserved"
        return
    print(f"\nfriendly-name connect succeeded — discovered: {conn.catalog.listTables()}", flush=True)
