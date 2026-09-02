# Snapshot isolation in duckrun

How duckrun keeps a **read-modify-write** correct when more than one writer can touch the same
Delta table, and how that maps onto Spark + Delta and an RDBMS.

## The problem

A lakehouse has no transaction manager and no single-writer guarantee. Two pipelines, a
double-fired job, or a notebook racing a scheduled run can all commit to the same table. The
dangerous pattern is a *read-modify-write*:

```
1. read table X  (you see version V)
2. compute new rows from what you read
3. write X
```

If someone commits to X between steps 1 and 3, a naive write at HEAD silently overwrites their
change: a **lost update**, no error. duckrun turns that into a loud failure (`CommitFailedError`).

## The guarantee

> A read-modify-write on a single table behaves as if it ran under **SNAPSHOT isolation**: pinned to
> the version you read, and rejected if a *conflicting* change landed since.

The mechanism: pin the operation to the version you read, and let delta-rs's optimistic concurrency
control (OCC) validate the commit against it. Only the table being **written** is fenced; a table you
merely read is irrelevant. It is single-table — no `BEGIN TRAN`, no lock, no multi-table atomicity.

## How it's enforced: two mechanisms

Both pin with `load_as_version(read_version)`. They differ in *how strict* the rejection is, and that
difference is forced by the operation:

| Operation | Mechanism | Fails when… |
|---|---|---|
| `MERGE`, `DELETE`, `UPDATE` | native OCC (`load_as_version` + the op) | a **conflicting** commit landed since `V` (same rows/files) |
| self-referential append, `REPLACE WHERE`, sort-rewrite overwrite | strict version CAS (`load_as_version` + `max_commit_retries=0`) | **any** commit landed since `V` |

Why the asymmetry: delete/update/merge have a real read-set, so delta-rs detects genuine conflicts and
rebases non-conflicting commits. An append/overwrite reads *nothing* from the target, so
`load_as_version` alone is inert (delta-rs would just rebase onto HEAD) — `max_commit_retries=0` is the
only thing that makes it fail-loud. So a `DELETE` tolerates an unrelated concurrent append, while a
fenced write fails on *any* movement. This is exactly how SQL Server SNAPSHOT behaves.

## What's fenced, through `conn.sql`

Every write is one SQL statement, fenced automatically — no handle to manage, no mode to pick:

| Statement | Fenced? |
|---|---|
| `MERGE INTO t …`, `DELETE FROM t …`, `UPDATE t …` | **yes** — the target version is captured and the commit validates against it (conflict-checked OCC) |
| `INSERT INTO t SELECT … FROM t` (read-modify-append on the **same** table) | **yes** — detected by name and committed compare-and-swap (any movement fails it) |
| `INSERT INTO t REPLACE WHERE <pred> …` (replaceWhere) | **yes** — single atomic commit, version CAS |
| `CREATE OR REPLACE TABLE t SORTED BY AUTO AS SELECT * FROM t` (re-cluster) | **yes** — the overwrite is pinned to the version read |
| `INSERT INTO t VALUES …` / `INSERT INTO t SELECT … FROM other` (append of new data) | **no** — additive, last-writer-safe; nothing to lose |
| `CREATE OR REPLACE TABLE t AS …` (full rebuild) | **no** — last-writer-wins by design |

## The dbt path

The incremental materialization captures the target version `vB` before the model runs, pins
`{{ this }}` to it (`delta_scan('…', version => vB)` — the reason for the duckdb 1.5.4 floor) and
pins the merge / overwrite commit to `vB`. Version-by-version proof through a real `dbt run`:
[snapshot-pin.md](snapshot-pin.md).

## Lazy reads

A `conn.sql(...)` read runs nothing until a write consumes it, so for a self-referential write the
read, the compute and the commit happen inside one statement and the CAS fences the whole window. A
result you materialize (`.df()` / `.arrow()`) and write back in a separate statement is back to the
read-modify-write problem: express it as a single `MERGE` / `REPLACE WHERE` instead.

## vs Spark + Delta

Same OCC model — delta-rs is Delta's commit protocol in Rust — so an append is non-conflicting,
`DELETE` / `UPDATE` / `MERGE` are conflict-checked, and a full overwrite is last-writer-wins. The
difference is where the read version comes from:

| | Spark + Delta | duckrun |
|---|---|---|
| `MERGE` / `DELETE` / `UPDATE` | reads HEAD at execution; OCC checks the commit instant | pinned to the version read, so a read-modify-write is fenced to *that* version |
| read-modify-append (watermark) | you write a `MERGE` | a self-referential `INSERT … SELECT … FROM t` is auto-fenced (version CAS) |
| replaceWhere | overwrite option | a single atomic `INSERT … REPLACE WHERE` commit + version CAS |
| append / overwrite of new data | unfenced | unfenced (identical) |
| multi-table transaction | ❌ | ❌ |

## vs an RDBMS

| System | Mechanism | Scope | Multi-table txn |
|---|---|---|---|
| **duckrun** | OCC on the Delta version log; per-statement pin | single table | ❌ |
| **delta-rs / Spark + Delta** | OCC on the `_delta_log` | single table | ❌ |
| **SQL Server** | transactions + isolation levels (lock or MVCC) | multi-statement, multi-table | ✅ |
| **Postgres** | MVCC; `SERIALIZABLE` (SSI) | multi-statement, multi-table | ✅ |

An RDBMS owns its storage and runs a transaction manager, so `BEGIN TRAN … COMMIT` spans statements
and tables. duckrun is a library over a shared lakehouse, so each statement's read version is its
transaction scope. It therefore does not provide:

- **Multi-table transactions** — Delta commits one table at a time.
- **Pessimistic locking** — writers fail and retry, never block.
- **Isolation across a materialized read** — see [Lazy reads](#lazy-reads).

## Further reading

- **[Snapshot pin — version by version](snapshot-pin.md)** — the dbt MERGE pin proved through a real
  `dbt run` (silent data loss vs a loud, safe failure).
- **[How far Python alone can take you on Delta](https://datamonkeysite.com/2026/05/24/how-far-python-alone-can-take-you-on-delta/)**
  — the manual, pure-Python version of this pattern (read the version, pin the read and the merge to
  it, catch `CommitFailedError`, retry). duckrun automates exactly this.
