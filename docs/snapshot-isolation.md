# Snapshot isolation in duckrun

How duckrun keeps a **read-modify-write** correct when more than one writer can touch the same
Delta table, and how that maps onto Spark + Delta and a classic RDBMS.

## The problem

A lakehouse has **no transaction manager and no single-writer guarantee**. Two pipelines, a
double-fired job, or a notebook racing a scheduled run can all commit to the same table. The
dangerous pattern is a *read-modify-write*:

```
1. read table X  (you see version V)
2. compute new rows from what you read
3. write X
```

If someone commits to X between step 1 and step 3, a naïve write at HEAD silently overwrites their
change — a **lost update**, no error. duckrun's job is to turn that into a **loud failure**
(`CommitFailedError`).

## The guarantee

> A read-modify-write on a single table behaves as if it ran under **SNAPSHOT isolation**: pinned to
> the version you read, and rejected if a *conflicting* change landed since.

It is a *mental model*, not a transaction manager — no `BEGIN TRAN`, no lock, no multi-table
atomicity. The whole trick: **pin the operation to the version you read, and let delta-rs's
optimistic concurrency control (OCC) validate the commit against it.**

Only the table being **written** is fenced. A table you merely read as an input is irrelevant — you
didn't modify it, so there is no lost update to prevent. `read_version` is **required** on every
fenced path; a brand-new table's first write is a plain create (`write_delta`), never fenced.

**Where the references sit.** SQL Server SNAPSHOT is the *behavioral* reference; **Spark + Delta is the
concurrency-model reference** — delta-rs is the Rust port of Delta's commit protocol, so the same OCC
semantics carry over (append is non-conflicting; delete/update/merge are conflict-checked). duckrun
copies that model, then — on the read-modify-write paths only — tightens the conflict window to the
version you **read**, deliberately rejecting some races that Spark's commit-instant OCC would accept, so
that no decision is committed on stale data. The fast, unfenced paths (a plain append/overwrite of new
data) stay identical to Spark's; the extra strictness applies only where a read feeds the write.

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

Every write is one SQL statement, and duckrun fences it **automatically** — there is no handle to
manage and no mode to pick:

| Statement | Fenced? |
|---|---|
| `MERGE INTO t …`, `DELETE FROM t …`, `UPDATE t …` | **yes** — the target version is captured and the commit validates against it (conflict-checked OCC) |
| `INSERT INTO t SELECT … FROM t` (read-modify-append on the **same** table) | **yes** — detected by name and committed compare-and-swap (any movement fails it) |
| `INSERT INTO t REPLACE WHERE <pred> …` (replaceWhere) | **yes** — single atomic commit, version CAS |
| `CREATE OR REPLACE TABLE t SORTED BY AUTO AS SELECT * FROM t` (re-cluster) | **yes** — the overwrite is pinned to the version read |
| `INSERT INTO t VALUES …` / `INSERT INTO t SELECT … FROM other` (append of new data) | **no** — additive, last-writer-safe; nothing to lose |
| `CREATE OR REPLACE TABLE t AS …` (full rebuild) | **no** — last-writer-wins by design |

A single `MERGE`/`DELETE`/`UPDATE` is self-contained: it reads and writes `t` in one statement, so its
own read version is the fence. A self-referential `INSERT … SELECT … FROM t` (e.g. a
`max(ts) FROM t` watermark) is recognised by name and fenced the same way. Only a plain append of
genuinely new data, or a full rebuild, is unfenced — correct, because neither can lose a concurrent
writer's change.

## The dbt path

The dbt incremental materialization does this every run: it captures the target version `vB` before
the model runs, pins the model's `{{ this }}` read to it (`delta_scan('…', version => vB)` — the
reason for the duckdb 1.5.4 floor), and pins the merge/overwrite commit to `vB`. Read and write agree
on one snapshot. Version-by-version proof through a real `dbt run` is in
**[snapshot-pin.md](snapshot-pin.md)**.

## Lazy reads

DuckDB relations are lazy (exactly like Spark DataFrames): a `conn.sql(...)` read runs nothing until a
write consumes it. So for a self-referential write, the read, the compute, and the commit all happen
inside one statement, and the CAS fences that whole window. The only time read and write sit at
genuinely different versions is when you **materialize** a result (`.df()` / `.arrow()`) and write it
back in a *separate* statement — then you are back to the read-modify-write problem, and you should
express it as a single `MERGE` / `REPLACE WHERE` so the fence applies.

## vs Spark + Delta

duckrun runs on the **same OCC model** as Spark + Delta — delta-rs is Delta's commit protocol in Rust —
so the semantics carry over: an append is non-conflicting, `DELETE`/`UPDATE`/`MERGE` are
conflict-checked, and a full overwrite is last-writer-wins.

The one real difference is **where the read version comes from**:

| | Spark + Delta | duckrun |
|---|---|---|
| `MERGE` / `DELETE` / `UPDATE` | reads HEAD at execution; OCC checks the commit instant | pinned to the version read, so a read-modify-write is fenced to *that* version |
| read-modify-append (watermark) | you write a `MERGE` | a self-referential `INSERT … SELECT … FROM t` is auto-fenced (version CAS) |
| replaceWhere | overwrite option | a single atomic `INSERT … REPLACE WHERE` commit + version CAS |
| append / overwrite of new data | unfenced | unfenced (identical) |
| multi-table transaction | ❌ | ❌ |

In short: Spark/Delta fence the **commit instant** and leave the read-to-write gap to `MERGE` or
job-level retry; duckrun makes the **read version** the thing you fence to, and recognises a
self-referential append so the watermark case is fenced without you having to write a `MERGE`.

## vs an RDBMS, and what duckrun does *not* provide

| System | Mechanism | Scope | Multi-table txn |
|---|---|---|---|
| **duckrun** | OCC on the Delta version log; per-statement pin | single table | ❌ |
| **delta-rs / Spark + Delta** | OCC on the `_delta_log` | single table | ❌ |
| **SQL Server** | transactions + isolation levels (lock or MVCC) | multi-statement, multi-table | ✅ |
| **Postgres** | MVCC; `SERIALIZABLE` (SSI) | multi-statement, multi-table | ✅ |

An RDBMS gets all of this "for free" because it **owns its storage and runs a transaction manager** —
`BEGIN TRAN … COMMIT` under `SNAPSHOT` spans statements and tables. duckrun owns none of that; it is a
library over a shared lakehouse. So each statement's read version *is* its transaction scope,
deliberately single-table. What a lakehouse can't honestly provide, and duckrun therefore doesn't:

- **Multi-table transactions** — Delta commits one table at a time.
- **Pessimistic locking / blocking** — lakehouses are optimistic-only; writers fail-and-retry, never block.
- **Isolation across a _materialized_ read** — a result you collect and write back in a later
  statement sits at a different version than you read; express the read-modify-write as a single
  `MERGE` / `REPLACE WHERE` statement so the fence applies.

## Further reading

- **[Snapshot pin — version by version](snapshot-pin.md)** — the dbt MERGE pin proved through a real
  `dbt run` (silent data loss vs a loud, safe failure).
- **[How far Python alone can take you on Delta](https://datamonkeysite.com/2026/05/24/how-far-python-alone-can-take-you-on-delta/)**
  — the manual, pure-Python version of this pattern (read the version, pin the read and the merge to
  it, catch `CommitFailedError`, retry). duckrun automates exactly this.
