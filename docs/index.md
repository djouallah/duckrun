---
hide:
  - navigation
  - toc
---

# duckrun { .duck-hide }

<div class="duck-home-code" markdown>

```bash
pip install duckrun
```

```python
import duckrun

# read-only by default — explore safely, no accidental writes
conn = duckrun.connect("abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/dbo")
conn.sql("select status, count(*) from orders group by status").show()
reader = conn.table("orders").toArrow()   # streaming pyarrow.RecordBatchReader
```

Writing Delta, snapshot-pinned upserts, the dbt adapter, the design and live examples
are in the menu above — start with the **[Connection API](connection-api.md)** or the
**[dbt adapter](dbt-adapter.md)**.

</div>

<div class="duck-thanks" markdown>

duckrun is just glue — the real work is done by [DuckDB](https://duckdb.org/),
[delta-rs](https://github.com/delta-io/delta-rs), [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
and [Apache Arrow](https://arrow.apache.org/).

</div>
