# Getting started

duckrun gives you a DuckDB SQL engine that **reads and writes Delta Lake** — locally or on
OneLake / S3 / GCS / ADLS. Everything below is plain `conn.sql(...)`: if you know SQL, you're
already productive.

## 1. Connect

```python
import duckrun

# Read-only by default — safe to explore, no accidental writes.
conn = duckrun.connect("./lakehouse/Tables")
# OneLake: duckrun.connect("abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/dbo")

# Opt into writes:
conn = duckrun.connect("./lakehouse/Tables", read_only=False)
```

## 2. Explore and query

```python
conn.sql("SHOW TABLES").show()
conn.sql("SELECT status, count(*) FROM orders GROUP BY status").show()

reader = conn.sql("SELECT * FROM orders").toArrow()   # streaming pyarrow.RecordBatchReader
```

## 3. Write a table — plain SQL

```python
# create (or replace) a Delta table from a query
conn.sql("CREATE OR REPLACE TABLE clean_orders AS SELECT * FROM orders WHERE amount > 0")

# append more rows
conn.sql("INSERT INTO clean_orders SELECT * FROM new_orders")

# update / delete in place
conn.sql("UPDATE clean_orders SET status = 'closed' WHERE amount = 0")
conn.sql("DELETE FROM clean_orders WHERE status = 'void'")
```

## 4. Upsert with `MERGE`

```python
conn.sql("""
    MERGE INTO clean_orders USING updates ON target.id = source.id
    WHEN MATCHED     THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

Every write commits **snapshot-pinned**: if another writer changed the table since you read it,
the commit fails loud (`CommitFailedError`) instead of silently overwriting — so concurrent jobs
never lose each other's updates.

## 5. Use more than one catalog

Attach another lakehouse — or a Fabric **Warehouse read-only** — and join across them by
three-part `catalog.schema.table` name:

```python
conn.attach(
    "abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>.Warehouse/Tables",
    name="wh", read_only=True,
)

conn.sql("""
    CREATE OR REPLACE TABLE daily_revenue AS
    SELECT d.order_date, sum(f.amount) AS revenue
    FROM wh.dbo.fact_sales f JOIN dim_date d ON d.date_id = f.date_id
    GROUP BY d.order_date
""")
```

The read-only fence refuses any write to `wh`. Works the same against a local path, `s3://`,
`gs://`, or `az://`.

## Building pipelines with dbt

For multi-model pipelines, point a dbt profile at a lakehouse and `dbt run` — duckrun
materializes each model as a Delta table:

```yaml
# ~/.dbt/profiles.yml
my_project:
  outputs:
    dev:
      type: duckrun
      root_path: "abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables"
```

---

**Next:** the full [Connection API](connection-api.md) reference · the [dbt adapter](dbt-adapter.md)
guide · runnable [Examples](examples.md).
