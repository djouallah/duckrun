# Snapshot isolation in duckrun

How duckrun keeps a **read-modify-write** correct when more than one writer can touch the same
Delta table — and how that compares to delta-rs/Delta, Spark, and a classic RDBMS like SQL Server.

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
change — a **lost update**, with no error. duckrun's job is to turn that into a **loud failure**
(`CommitFailedError`) instead.

## The guarantee

> A read-modify-write on a single table behaves as if it ran under **SNAPSHOT isolation**: it is
> pinned to the version you read, and the commit is rejected if a *conflicting* change landed since.

This is a *mental model*, not a real transaction manager — there is no `BEGIN TRAN`, no lock, no
multi-table atomicity. It is faked with one trick: **pin the operation to the version you read, and
let delta-rs's optimistic concurrency control (OCC) validate the commit against that version.**

Only the table being **written** is fenced, to the version *it* was read at. A table you merely
read as an input is irrelevant — you didn't modify it, so there is no lost update to prevent (this
matches Delta/Spark per-table OCC; see [comparison](#how-this-compares)).

## The two enforcement mechanisms

Both pin with `DeltaTable.load_as_version(read_version)` so delta-rs validates the commit over the
window `(read_version, HEAD]`. They differ in *how strict* the rejection is, and the difference is
forced by the operation, not chosen:

| Operation | Mechanism | Fails when… | Why |
|---|---|---|---|
| `merge`, `delete`, `update` | native OCC (`load_as_version` + plain op) | a **conflicting** commit landed since `V` (touches the same rows/files) | delete/update/merge have a real read/write set, so delta-rs detects genuine conflicts and lets non-conflicting commits rebase |
| `append_if_unchanged` (a.k.a. `safeappend`), `replaceWhere` | strict version CAS (`load_as_version` + `max_commit_retries=0`) | **any** commit landed since `V` | a plain append is *non-conflicting* in delta-rs (it would auto-rebase and never fail), so the only way to make it fail-loud is to forbid all rebasing |

So a `delete` tolerates an unrelated concurrent append (correct — not a lost update), while a
`safeappend` fails on *any* movement (it has no finer notion of conflict). This asymmetry is
inherent to the operations; it is exactly how SQL Server SNAPSHOT isolation behaves too (abort on a
write-write **conflict**, not on every concurrent commit).

`read_version` is **required** on every fenced path — there is no blind-HEAD escape hatch. A
brand-new table's first write is a plain create (`write_delta`), never a fenced op.

## The `DeltaTable` handle is the snapshot scope

For the connection API, the handle is the poor-man's `BEGIN TRAN`:

```python
dt = DeltaTable.forName(conn, "orders")   # captures version V here
# ... time passes, another writer may commit ...
dt.delete("status = 'cancelled'")          # pinned to V; fails loud if orders moved since V
```

- `forName` / `forPath` capture the table version **once**, when the handle is taken.
- `merge` / `delete` / `update` through that handle all pin to that captured `V`.
- After a *successful* mutation, the handle **re-snapshots** to the new HEAD, so a second mutation
  on the same handle only fails on a *foreign* write — never on the handle's own previous write.

`append`/`overwrite` are **unsafe by design** (see below) and are not part of the handle.

## The dbt path

The dbt incremental materialization does the same thing, automatically, for every run: it captures
the target version `vB` before the model runs, pins the model's `{{ this }}` read to it
(`delta_scan('…', version => vB)` — the reason for the duckdb 1.5.4 floor), and pins the merge
commit to `vB`. Read and write agree on one snapshot. The version-by-version proof through a real
`dbt run` is in **[snapshot-pin.md](snapshot-pin.md)**.

## What is deliberately *not* fenced

`mode("append")` and `mode("overwrite")` are **unsafe by design**, matching Spark's `SaveMode`:

- `append` — "just add these rows." Non-conflicting; never fails on a concurrent write.
- `overwrite` — "replace the whole table." Last-writer-wins.

If you want a fenced version, use the read-modify-write modes (`safeappend`/`replaceWhere`, or a
`DeltaTable` handle for delete/update/merge). Keeping the plain modes unfenced is intentional — it
matches Spark and keeps the fast, high-concurrency append path available.

## A note on lazy reads

DuckDB relations are lazy, so duckrun **cannot pin an append's data read** to a version (delta-rs
has no such API), and does not need to: the guarantee lives entirely in the **commit**. If the lazy
read happens to see a newer version than `read_version`, the commit simply fails — nothing stale is
ever written. The version you pass is "the version I based my work on"; the CAS does the rest.

## How this compares

The "fail if it moved since I read it" idea is **universal** — it is snapshot/serializable
isolation. What differs between systems is *where* it lives.

| System | Mechanism | Scope | Plain append | Conflict granularity | Multi-table txn |
|---|---|---|---|---|---|
| **duckrun** | OCC on the Delta version log; explicit handle / per-op pin | single table | unfenced (opt-in `safeappend`) | row/file (delete/update/merge), version (safeappend) | ❌ |
| **delta-rs / Delta Lake** | OCC on the `_delta_log`; commit conflict checker | single table | non-conflicting → auto-rebase | row/file; append commutes | ❌ |
| **Spark + Delta** | same engine as delta-rs | single table | `SaveMode.Append` unfenced | uses `MERGE` for read-dependent inserts | ❌ |
| **SQL Server** | transactions + isolation levels (lock or MVCC) | multi-statement, multi-table | n/a (it's `INSERT` inside a txn) | row/range; SNAPSHOT aborts on conflict (err 3960) | ✅ |
| **Postgres** | MVCC; `SERIALIZABLE` (SSI) | multi-statement, multi-table | n/a | abort (err 40001) | ✅ |

The takeaways:

- **vs delta-rs / Spark:** duckrun builds *directly* on delta-rs's OCC and inherits its semantics
  (append non-conflicting; delete/update/merge conflict-checked). What duckrun adds is making the
  *read version* the thing you fence to — captured at the handle / at `vB` — so a read-modify-write
  split across statements is caught, not just a write-write race in the commit instant. Spark/Delta
  leave that to `MERGE` or to job-level retry/idempotency; duckrun also offers the lighter
  `safeappend` for the watermark-append case (which Spark has no built-in equivalent for — there you
  would write a `MERGE`).
- **vs SQL Server:** SQL Server gets all of this "for free" because it **owns its storage and runs a
  transaction manager** — `BEGIN TRAN … COMMIT` under `SNAPSHOT` isolation is the whole story, and
  it spans multiple statements and multiple tables. duckrun owns none of that: it is a library over
  a shared lakehouse. So the **handle is duckrun's stand-in for `BEGIN TRAN`**, and it is
  deliberately **single-table** — there are no multi-table atomic transactions, no pessimistic
  locking/blocking, and no implicit isolation without taking a handle. Those are the parts of "be
  like SQL Server" a lakehouse can't honestly provide.

## What duckrun does *not* provide

- **Multi-table transactions** — Delta commits one table at a time; there is no all-or-nothing
  commit across tables.
- **Pessimistic locking / blocking** — lakehouses are optimistic-only; concurrent writers never
  block, they fail-and-retry.
- **Implicit cross-statement isolation** — you must take a `DeltaTable` handle (or use the dbt
  path) to get the read-version fence; an ad-hoc `conn.sql` read followed by a separate write is not
  auto-isolated.

## Fenced writer modes

The `DataFrameWriter` exposes the fenced (compare-and-swap) siblings of the unfenced Spark modes:

| Unsafe (Spark SaveMode) | Fenced sibling | Fails if the table moved since the read version |
|---|---|---|
| `mode("append")` | `mode("append_if_unchanged")` | yes (any movement — appends are non-conflicting) |
| `mode("overwrite")` | `mode("overwrite_if_unchanged")` | yes (any movement) |

`safeappend` is kept as a **deprecated alias** for `append_if_unchanged` (the dbt incremental
strategy accepts both names too). `append_if_unchanged` is the clearer name and matches the engine
function.

## Roadmap

One plumbing refinement remains (the mechanism is unchanged):

- **Carry the read version on the DataFrame** — `conn.table(name)` / `conn.read.load(path)` will
  remember the version they read so a fenced writer mode fences to *that* version (the read version)
  instead of the version at write time, closing the read→write gap for the DataFrame writer the same
  way the handle already does for delete/update/merge. `conn.table` stays live for reads; it only
  *also remembers* the version.
