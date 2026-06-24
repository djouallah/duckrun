---
hide:
  - navigation
  - toc
---

# duckrun { .duck-hide }

<div class="duck-home-code" markdown>

```python
!pip install duckrun --upgrade
notebookutils.session.restartPython()
```

```python
import duckrun

# connect to a lakehouse (read-only by default — pass read_only=False to write)
conn = duckrun.connect("abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/dbo", read_only=False)

# attach a Fabric Warehouse read-only as another catalog
conn.attach("abfss://<ws>@onelake.dfs.fabric.microsoft.com/<warehouse>.Warehouse/Tables", name="wh", read_only=True)

# write a Delta table with plain SQL — CREATE TABLE AS SELECT routes to delta-rs
conn.sql("""
  CREATE OR REPLACE TABLE daily_revenue AS
  SELECT d.order_date, sum(f.amount) AS revenue
  FROM wh.dbo.fact_sales f JOIN dim_date d ON d.date_id = f.date_id
  GROUP BY d.order_date
""")
```

Works anywhere Delta lives — **local filesystem, ADLS, S3, GCS, OneLake**. All you give it is a
path; **no catalog or metastore required**.

</div>

<div class="duck-thanks" markdown>

duckrun is just glue — the real work is done by [DuckDB](https://duckdb.org/),
[delta-rs](https://github.com/delta-io/delta-rs), [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
and [Apache Arrow](https://arrow.apache.org/).

</div>

<p class="duck-pins">needs <code>duckdb&nbsp;&gt;=&nbsp;1.5.4</code> &middot; <code>deltalake&nbsp;==&nbsp;1.5.0</code></p>
