# coffee_shop — a coffee-shop dbt project on duckrun

A small coffee-shop analytics pipeline, materialized as **Delta Lake** tables on duckrun. It exists
in two incarnations that share the same scenario:

| Incarnation | Where | What it covers |
|---|---|---|
| **dbt project** (this dir) | `integration_tests/coffee/` | the model DAG — ingest → dedup → fact → mart |
| **connection API** (`duckrun.connect`) | `integration_tests/coffee/test_coffee.py` | the DataFrame-style surface, plus a millions-of-rows stress run |

## Credit — dimension data

The two dimension tables (`Dim_Locations.csv`, `Dim_Products.csv`) come from **Josue Bogran's**
[coffeeshopdatageneratorv2](https://github.com/JosueBogran/coffeeshopdatageneratorv2)
(MIT License, © Josue Bogran). All credit for the coffee-shop dataset goes to him. The fact table is
generated locally (random order lines), not taken from upstream.

Two consumers, two ways of reading the same data:

- **Connection-API scenario** (`integration_tests/coffee/test_coffee.py`) reads **vendored** copies
  under [`data/`](data/) — so the scenario, and the `coffee-stress`
  release gate that runs it, never touch the network (legit, repeatable stress numbers).
- **This dbt project** reads the CSVs straight from upstream over `https`
  (`read_csv_auto('https://raw.githubusercontent.com/JosueBogran/coffeeshopdatageneratorv2/main/…')`).
  In CI it is only docs-built (`--empty-catalog`, models never execute), so it never actually fetches;
  a local `dbt build` does, and always gets the current upstream version.

## The DAG

```
dim_locations ─┐
               ├─► fact_sales ─► mart_revenue
dim_products ──► products ─┘ (also feeds mart_revenue)
```

| Model | Layer | What it does |
|---|---|---|
| `dim_locations` | staging | the location dimension, read from the upstream CSV over https |
| `dim_products` | staging | the raw **SCD2** product dimension over https (`product_id` repeats) |
| `products` | dimensions | dedup the SCD2 product dim to a current, unique-key table |
| `fact_sales` | marts | generate `COFFEE_ROWS` order lines, join 1:1 to the dims, partition by region |
| `mart_revenue` | marts | revenue by product category × season |

`schema.yml` adds descriptions plus `unique` / `not_null` / `relationships` tests that assert the
1:1 join.

## Run it

```bash
export WAREHOUSE_PATH=/tmp/coffee_wh    # local Delta warehouse (or an abfss:// Tables path)
export COFFEE_ROWS=100000               # fact rows to generate (default 1,000,000)
dbt build --project-dir integration_tests/coffee --profiles-dir integration_tests/coffee
```

To target OneLake instead, point `WAREHOUSE_PATH` at an `abfss://…/Tables` path and set
`ONELAKE_TOKEN`.

## CI

The DAG is published to the docs site (built by the `aemo` job in `integration.yml`, no table
build — `--static --empty-catalog`). The connection-API scenario is exercised by `coffee-smoke.yml`
(local, small N), the `coffee-onelake` job in `integration.yml` (live OneLake, small N), and the
manual `coffee` stress job (millions of rows).
