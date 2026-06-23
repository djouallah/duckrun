---
hide:
  - navigation
  - toc
---

# duckrun { .duck-hide }

<div class="duck-hero" markdown>

<p class="tagline">Run SQL in <strong>DuckDB</strong>, read &amp; write <strong>Delta Lake</strong> via delta-rs — locally or object store.</p>

</div>

<div class="duck-home-code" markdown>

## Install

```bash
pip install duckrun
```

In a **Microsoft Fabric** notebook, upgrade and restart the kernel (duckrun needs
`duckdb` ≥ 1.5.4, newer than the bundled stable build):

```python
!pip install duckrun --upgrade
notebookutils.session.restartPython()
```

## Read a lakehouse

```python
import duckrun

# Read-only by default — explore safely, no chance of an accidental write.
conn = duckrun.connect("abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/dbo")

conn.sql("show tables").show()
conn.sql("select status, count(*) from orders group by status").show()
reader = conn.table("orders").toArrow()   # streaming pyarrow.RecordBatchReader
```

That's the whole on-ramp. Writing Delta from SQL, snapshot-pinned upserts, the dbt
adapter, the design, and live examples are one click away in the menu above — start
with the **[Connection API](connection-api.md)** or the **[dbt adapter](dbt-adapter.md)**.

</div>

<div class="duck-thanks" markdown>

duckrun is just glue — the real work is done by:

- [DuckDB](https://duckdb.org/)
- [delta-rs](https://github.com/delta-io/delta-rs)
- [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
- [Apache Arrow](https://arrow.apache.org/)

</div>
