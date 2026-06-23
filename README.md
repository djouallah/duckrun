<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="duckrun">

[![PyPI](https://img.shields.io/pypi/v/duckrun?color=blue&label=PyPI&logo=pypi&logoColor=white)](https://pypi.org/project/duckrun/)
[![Downloads](https://static.pepy.tech/badge/duckrun)](https://pepy.tech/project/duckrun)
[![Downloads/month](https://img.shields.io/pypi/dm/duckrun?color=brightgreen&label=downloads%2Fmonth)](https://pypi.org/project/duckrun/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue?logo=python&logoColor=white)](https://pypi.org/project/duckrun/)
[![License](https://img.shields.io/pypi/l/duckrun?color=lightgrey)](https://github.com/djouallah/duckrun/blob/main/LICENSE)

> **Disclaimer:** This is a personal project. It is
> not affiliated with, endorsed by, or supported by any employer or vendor. No warranty —
> use it at your own risk.

**duckrun** runs SQL in [DuckDB](https://duckdb.org/) and reads/writes
[**Delta Lake**](https://delta-io.github.io/delta-rs/) via delta-rs — locally or on OneLake / S3 /
GCS / ADLS. It's just glue: **DuckDB executes · delta-rs materializes · Arrow bridges · dbt
orchestrates**. Two ways to use it:

- **`connect()`** — a notebook helper to query and write Delta straight from SQL (this page);
- a **[dbt adapter](docs/dbt-adapter.md)** that materializes models as Delta tables.

Concurrent writers are first-class: every write is snapshot-pinned and fails loud rather than
silently interleaving.

## Install

```bash
pip install duckrun
```

In a **Microsoft Fabric** notebook, upgrade and restart the kernel (duckrun needs `duckdb` ≥ 1.5.4,
which is newer than the bundled stable build; it fails loud at `connect()` otherwise):

```python
!pip install duckrun --upgrade
notebookutils.session.restartPython()
```

## Quickstart — OneLake in a notebook

```python
import duckrun

# Read-only by default — explore a lakehouse safely, no chance of an accidental write.
# Use the workspace + lakehouse GUIDs (friendly names hit an upstream OneLake read bug for now).
conn = duckrun.connect("abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/dbo")

conn.sql("SHOW TABLES").show()
conn.sql("select status, count(*) from orders group by status").show()
df = conn.table("orders").toPandas()          # or .toArrow() for a streaming RecordBatchReader

# Time travel: list the versions, then read one
from duckrun import DeltaTable
DeltaTable.forName(conn, "orders").history()   # newest-first commits: version, timestamp, operation, …
conn.read.format("delta").option("versionAsOf", 0).load(".../Tables/dbo/orders").show()
```

Need to **write**? Opt in with `read_only=False`:

```python
conn = duckrun.connect("abfss://…/Tables/dbo", read_only=False)

# write Delta straight from SQL
conn.sql("select * from orders where amount > 0") \
    .write.mode("overwrite").saveAsTable("clean_orders")

# raw DML routes to delta-rs (insert / update / delete / create table as / alter / drop)
conn.sql("delete from clean_orders where amount = 0")

# upsert — snapshot-pinned automatically, nothing extra to pass
from duckrun import DeltaTable
src = conn.sql("select * from updates")
DeltaTable.forName(conn, "clean_orders").merge(src, "target.id = source.id") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

conn.stop()
```

**Multiple catalogs** — attach more lakehouses and read/join across them by three-part name. In
Fabric a Warehouse is just a write-locked Lakehouse, so attach it `read_only=True` next to a writable
one:

```python
conn.attach("abfss://…/warehouse.Warehouse/Tables", name="warehouse", read_only=True)
conn.attach("/data/reference", name="local")
conn.sql("select * from warehouse.mart.facts f join local.dbo.lookup l on l.id = f.id").show()
```

Works the same against a local path, `s3://`, `gs://`, or `az://`. Full method map:
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
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables"
```

Profiles, materializations, incremental strategies (incl. `append_if_unchanged`), sources, and
automatic compaction/vacuum are all in **[docs/dbt-adapter.md](docs/dbt-adapter.md)**.

## Building with an AI assistant

duckrun ships a guide so AI coding assistants get the adapter's defaults right (several differ from
other dbt adapters). For **Claude Code**:

```
/plugin marketplace add djouallah/duckrun
/plugin install duckrun-projects@duckrun
```

Other assistants read the [`AGENTS.md`](AGENTS.md) at the repo root, which points to the full guide.
None of this is required to use duckrun.

## How it works

Two engines, split cleanly: DuckDB runs every query and reads Delta through `delta_scan` views,
delta-rs handles every write, an Arrow C-stream bridges them, and dbt orchestrates on top.

![duckrun architecture: DuckDB executes SQL and reads Delta via delta_scan; an Arrow C-stream bridges to delta-rs, which handles every write and commits against the read version (OCC); dbt orchestrates on top](https://raw.githubusercontent.com/djouallah/duckrun/main/docs/architecture.png)

Writes are snapshot-pinned: the read is fixed at `delta_scan(…, version => N)` and the write commits
against `N`, so a concurrent commit is rejected with `CommitFailedError` instead of silently
overwriting a lost update.

![Two writers race on one table: Writer A reads v5 and computes; Writer B commits v6 in between; A's commit against v5 is rejected with CommitFailedError instead of silently overwriting B](https://raw.githubusercontent.com/djouallah/duckrun/main/docs/snapshot-timeline.png)

More on the design: [Design document](docs/design_document.md) ·
[Snapshot isolation](docs/snapshot-isolation.md).

## Docs

Browse the rendered docs site at **[djouallah.github.io/duckrun](https://djouallah.github.io/duckrun/)**
— or read the markdown here:

| Doc | What's in it |
|---|---|
| [Connection API](docs/connection-api.md) | The `duckrun.connect()` notebook API + the live per-method scorecard. |
| [Spark / Delta coverage](docs/spark-delta-parity.md) | What the `connect()` surface maps to in PySpark / Delta. |
| [dbt adapter](docs/dbt-adapter.md) | Profiles, materializations, incremental strategies, sources, maintenance, limitations. |
| [Design document](docs/design_document.md) | Why delta-rs (not DuckDB's native Delta writer), why Delta (not Iceberg), why a separate adapter. |
| [Snapshot isolation](docs/snapshot-isolation.md) | How a read-modify-write is fenced to the version you read, and how it compares to delta-rs/Spark/SQL Server. |
| [dbt adapter conformance](docs/conformance.md) | Official `dbt-tests-adapter` results, regenerated on every push to `main`. |
| [Incremental MERGE benchmark](docs/merge-benchmark.md) | ~60M-row TPCH merge / append / overwrite scorecard — the release gate. |

## License

MIT
