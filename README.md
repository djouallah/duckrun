<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="duckrun">

[![PyPI](https://img.shields.io/pypi/v/duckrun?color=blue&label=PyPI&logo=pypi&logoColor=white)](https://pypi.org/project/duckrun/)
[![Downloads](https://static.pepy.tech/badge/duckrun)](https://pepy.tech/project/duckrun)
[![Downloads/month](https://img.shields.io/pypi/dm/duckrun?color=brightgreen&label=downloads%2Fmonth)](https://pypi.org/project/duckrun/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue?logo=python&logoColor=white)](https://pypi.org/project/duckrun/)
[![License](https://img.shields.io/pypi/l/duckrun?color=lightgrey)](https://github.com/djouallah/duckrun/blob/main/LICENSE)

> **Disclaimer:** This is a personal project. Not affiliated with, endorsed by, or supported by any
> employer or vendor. No warranty — use it at your own risk.

**duckrun** runs SQL in [DuckDB](https://duckdb.org/) and reads/writes
[**Delta Lake**](https://delta-io.github.io/delta-rs/) via delta-rs — locally or on OneLake / S3 /
GCS / ADLS. Query a lakehouse, attach more catalogs, and **write Delta tables with plain SQL**;
concurrent writes are snapshot-pinned and fail loud. It's also a [dbt adapter](docs/dbt-adapter.md).

## Install

```bash
pip install duckrun
```

In a **Microsoft Fabric** notebook, upgrade and restart the kernel (duckrun needs `duckdb` ≥ 1.5.4,
newer than the bundled stable build; it fails loud at `connect()` otherwise):

```python
!pip install duckrun --upgrade
notebookutils.session.restartPython()
```

## Pure SQL over Delta

Connect to a lakehouse, attach a Fabric **Warehouse read-only** as a second catalog, and write a
Delta table with a plain `CREATE TABLE AS SELECT` — no DataFrame API, just `conn.sql(...)`:

```python
import duckrun

# Connect to a lakehouse. Read-only by default — pass read_only=False to opt into writes.
conn = duckrun.connect(
    "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/dbo",
    read_only=False,
)

# Attach a Fabric Warehouse read-only (a write-locked lakehouse) as the catalog `wh`.
conn.attach(
    "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<warehouse>.Warehouse/Tables",
    name="wh", read_only=True,
)

# Write a Delta table with plain SQL: join the read-only warehouse to the lakehouse and
# land the result as Delta in the lakehouse (CREATE TABLE AS SELECT routes to delta-rs).
conn.sql("""
    CREATE OR REPLACE TABLE daily_revenue AS
    SELECT d.order_date, SUM(f.amount) AS revenue
    FROM wh.dbo.fact_sales f
    JOIN dim_date d ON d.date_id = f.date_id
    GROUP BY d.order_date
""")

conn.sql("SELECT * FROM daily_revenue ORDER BY order_date").show()
```

Everything is plain `conn.sql(...)`: reads pass straight through DuckDB; `CREATE TABLE AS SELECT`,
`INSERT`, `UPDATE`, `DELETE`, and `MERGE` route to delta-rs and commit **snapshot-pinned** — a
concurrent commit is rejected with `CommitFailedError` instead of silently overwriting a lost
update. The read-only fence refuses any write to `wh`. Works the same against a local path,
`s3://`, `gs://`, or `az://`.

Full surface (DataFrame writes, `DeltaTable` merge/time-travel, the per-method scorecard):
**[Connection API](docs/connection-api.md)** · **[Spark/Delta coverage](docs/spark-delta-parity.md)** ·
[live multi-catalog demo](https://djouallah.github.io/duckrun/multicatalog.html).

## dbt adapter

duckrun is also a dbt adapter — a thin wrapper around
[`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) that adds Delta-backed `table` / `incremental`
materializations (everything else dbt-duckdb gives you is inherited). Point a profile at a lakehouse
and `dbt run`:

```yaml
# ~/.dbt/profiles.yml
my_project:
  outputs:
    dev:
      type: duckrun
      root_path: "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables"
```

Materializations, incremental strategies (incl. `append_if_unchanged`), sources, and automatic
compaction/vacuum are in **[docs/dbt-adapter.md](docs/dbt-adapter.md)**. AI coding assistants:
`/plugin marketplace add djouallah/duckrun` then `/plugin install duckrun-projects@duckrun`
(Claude Code), or read [`AGENTS.md`](AGENTS.md).

## How it works

Two engines, split cleanly: DuckDB runs every query and reads Delta through `delta_scan` views,
delta-rs handles every write, and an Arrow C-stream bridges them. Writes are snapshot-pinned — the
read is fixed at `delta_scan(…, version => N)` and the write commits against `N`. More:
[Design document](docs/design_document.md) · [Snapshot isolation](docs/snapshot-isolation.md).

![duckrun architecture: DuckDB executes SQL and reads Delta via delta_scan; an Arrow C-stream bridges to delta-rs, which handles every write and commits against the read version (OCC); dbt orchestrates on top](https://raw.githubusercontent.com/djouallah/duckrun/main/docs/architecture.png)

## Docs

Rendered site: **[djouallah.github.io/duckrun](https://djouallah.github.io/duckrun/)**.

| Doc | What's in it |
|---|---|
| [Connection API](docs/connection-api.md) | The `duckrun.connect()` API + the live per-method scorecard. |
| [Spark / Delta coverage](docs/spark-delta-parity.md) | What the `connect()` surface maps to in PySpark / Delta. |
| [dbt adapter](docs/dbt-adapter.md) | Profiles, materializations, incremental strategies, sources, maintenance. |
| [Design document](docs/design_document.md) | Why delta-rs (not DuckDB's native writer), why Delta (not Iceberg), why a separate adapter. |
| [Snapshot isolation](docs/snapshot-isolation.md) | How a read-modify-write is fenced to the version you read. |
| [dbt conformance](docs/conformance.md) | Official `dbt-tests-adapter` results, regenerated on every push to `main`. |
| [MERGE](docs/merge-benchmark.md) · [TPC-H](docs/tpch.md) | The live benchmark scorecards. |

## License

MIT
