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

**Where the references sit.** SQL Server SNAPSHOT is the *behavioral* reference; Spark + Delta is the
*API and OCC* reference. duckrun copies both surfaces, then — *on the read-modify-write paths only* —
tightens the conflict window to the version you **read**, deliberately rejecting some races that
Spark's commit-instant OCC would accept, so that no decision is ever committed on stale data. The
fast, Spark-identical unfenced paths (plain `append`/`overwrite`) stay available; the extra strictness
applies only when you opt in (a `DeltaTable` handle, a fenced writer mode, or the dbt path).

## How it's enforced: two mechanisms

Both pin with `DeltaTable.load_as_version(read_version)`. They differ in *how strict* the rejection
is, and that difference is forced by the operation:

| Operation | Mechanism | Fails when… |
|---|---|---|
| `merge`, `delete`, `update` | native OCC (`load_as_version` + the op) | a **conflicting** commit landed since `V` (same rows/files) |
| `append_if_unchanged`, `overwrite_if_unchanged`, `replaceWhere` | strict version CAS (`load_as_version` + `max_commit_retries=0`) | **any** commit landed since `V` |

Why the asymmetry: delete/update/merge have a real read-set, so delta-rs detects genuine conflicts
and rebases non-conflicting commits. An append/overwrite reads *nothing* from the target, so
`load_as_version` alone is inert (no read-set to validate, delta-rs would just rebase onto HEAD) —
`max_commit_retries=0` is the only thing that makes it fail-loud. So a `delete` tolerates an
unrelated concurrent append, while a fenced append fails on *any* movement. This is also exactly how
SQL Server SNAPSHOT behaves: abort on a write-write **conflict**, not on every concurrent commit.

## The `DeltaTable` handle is the snapshot scope

For the connection API the handle is the poor-man's `BEGIN TRAN`:

```python
dt = DeltaTable.forName(conn, "orders")   # captures version V here
# ... time passes, another writer may commit ...
dt.delete("status = 'cancelled'")          # pinned to V; fails loud if orders moved since V
```

- `conn.table(name)` / `DeltaTable.forName` capture the version **once**, when the handle is taken.
- `merge` / `delete` / `update` through that handle all pin to that captured `V`.
- After a *successful* mutation the handle **re-snapshots** to the new HEAD, so a second mutation on
  the same handle only fails on a *foreign* write, never on its own previous write.

`append`/`overwrite` are unfenced by design (below) and are not part of the handle.

## The dbt path

The dbt incremental materialization does this automatically every run: it captures the target
version `vB` before the model runs, pins the model's `{{ this }}` read to it
(`delta_scan('…', version => vB)` — the reason for the duckdb 1.5.4 floor), and pins the merge
commit to `vB`. Read and write agree on one snapshot. Version-by-version proof through a real
`dbt run` is in **[snapshot-pin.md](snapshot-pin.md)**.

## Fenced vs unfenced writer modes

`mode("append")` and `mode("overwrite")` are **unfenced by design**, matching Spark's `SaveMode`:
append never fails on a concurrent write; overwrite is last-writer-wins. Keeping them unfenced
preserves the fast, high-concurrency append path. Each has a fenced compare-and-swap sibling:

| Unfenced (Spark `SaveMode`) | Fenced sibling | Fails if the table moved since the read version |
|---|---|---|
| `mode("append")` | `mode("append_if_unchanged")` | yes (any movement) |
| `mode("overwrite")` | `mode("overwrite_if_unchanged")` | yes (any movement) |



**Lazy reads.** DuckDB relations are lazy: `conn.table("x")` / `conn.sql(...)` read nothing — the
relation runs only when `.write` does. So the read, the dedup, and the commit all happen inside the
one write call, and the fenced modes capture `read_version` at **write time**; the CAS fences that
whole window. duckrun cannot pin an append's *read* to a version (delta-rs has no such API) and
doesn't need to — the guarantee lives entirely in the commit. The only time read and write sit at
genuinely different versions is when you **materialize** and act later; that is what the handle is
for (it carries `V` explicitly).

## vs Spark + Delta

duckrun deliberately mirrors the Delta-on-Spark surface — `DeltaTable.forName`,
`.merge(...).whenMatched*/whenNotMatched*`, `delete`, `update`, `SaveMode` append/overwrite,
`replaceWhere` — and runs on the same OCC model (delta-rs is the Rust port of Delta's commit
protocol), so semantics carry over: append is non-conflicting, delete/update/merge are
conflict-checked, `SaveMode.Append`/`Overwrite` are unfenced.

The one real difference is **where the read version comes from**:

| | Spark + Delta | duckrun |
|---|---|---|
| `DeltaTable.merge/delete/update` | reads HEAD at execution; OCC checks only the commit instant | pinned to the version captured at handle construction (`vB`), so a read-modify-write **split across statements** is fenced |
| Fenced append (watermark insert) | no built-in; you write a `MERGE` | `append_if_unchanged` — a lighter version CAS |
| `replaceWhere` | overwrite option | single atomic commit + version CAS (`replace_where`) |
| `SaveMode.Append` / `Overwrite` | unfenced | unfenced (identical) |
| Multi-table transaction | ❌ | ❌ |

In short: Spark/Delta fence the **commit instant** and leave the read-to-write gap to `MERGE` or
job-level retry. duckrun makes the **read version** the thing you fence to — captured at the handle
or at `vB` — and adds the lighter `append_if_unchanged` for the watermark case Spark has no built-in for.

## vs an RDBMS, and what duckrun does *not* provide

| System | Mechanism | Scope | Multi-table txn |
|---|---|---|---|
| **duckrun** | OCC on the Delta version log; explicit handle / per-op pin | single table | ❌ |
| **delta-rs / Spark + Delta** | OCC on the `_delta_log` | single table | ❌ |
| **SQL Server** | transactions + isolation levels (lock or MVCC) | multi-statement, multi-table | ✅ |
| **Postgres** | MVCC; `SERIALIZABLE` (SSI) | multi-statement, multi-table | ✅ |

An RDBMS gets all of this "for free" because it **owns its storage and runs a transaction manager** —
`BEGIN TRAN … COMMIT` under `SNAPSHOT` spans statements and tables. duckrun owns none of that; it is
a library over a shared lakehouse. So the **handle is the stand-in for `BEGIN TRAN`**, deliberately
single-table. What a lakehouse can't honestly provide, and duckrun therefore doesn't:

- **Multi-table transactions** — Delta commits one table at a time.
- **Pessimistic locking / blocking** — lakehouses are optimistic-only; writers fail-and-retry, never block.
- **Isolation across a _materialized_ read** — DuckDB relations are lazy (exactly like Spark
  DataFrames), so a `conn.sql` read that feeds a fenced `.write` is pinned at **write** time even when
  the read and the write are separate calls — read and commit land on one snapshot, and the CAS fences
  the window. The gap opens only if you **materialize** (`.toPandas()` / `collect()`) and write the
  result back later, so the read and the write genuinely sit at different versions. That is what the
  `DeltaTable` handle is for — it carries the read version explicitly. Spark + Delta behaves
  identically: a lazy DataFrame read→write is fenced at the commit; a collected-then-written result is
  not, and you reach for a `MERGE` or an explicit version check.

## Further reading

- **[Snapshot pin — version by version](snapshot-pin.md)** — the dbt MERGE pin proved through a real
  `dbt run` (silent data loss vs a loud, safe failure).
- **[How far Python alone can take you on Delta](https://datamonkeysite.com/2026/05/24/how-far-python-alone-can-take-you-on-delta/)**
  — the manual, pure-Python version of this pattern (`vB = DeltaTable(path).version()`, pin the read
  and the merge to `vB`, catch `CommitFailedError`, retry). duckrun automates exactly this.
