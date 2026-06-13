# Design: Delta-backed dbt materializations via delta_rs + `delta_scan` views

## Context

**Purpose:** add dbt support for Delta Lake using **delta_rs** for writes as a pragmatic
workaround until DuckDB's native Delta write support matures (today it is read-first, with
only blind `INSERT` and no `UPDATE`/`DELETE`/`MERGE`). Until then, delta_rs does the writing
and DuckDB does the reading; this design can be revisited once DuckDB Delta writes are
reliable enough to take over.

The duckrun dbt adapter keeps **all state in Delta Lake**. Writes always go through
**delta_rs** (overwrite / merge / append in `engine.py` + `delta_plugin.py`). DuckDB is
**never** used to write Delta — its writer (blind `INSERT` only, no `UPDATE`/`DELETE`) is
not reliable for this. DuckDB's job is purely to *read* and to run model logic.

Each Delta table is surfaced to dbt as a plain **`delta_scan` view** named to match dbt's
`database.schema.identifier`. The view is what makes `{{ this }}`, `ref()`, and
`is_incremental()` resolve against real Delta tables — across separate `dbt build`
processes. Views are recreated at run start by discovering Delta tables on disk, and a
table built mid-run becomes visible the moment its own materialization recreates the view.
`location` is deterministic: `root_path/<schema>/<identifier>` (or `config(location=…)`).

## Why these choices

### Why delta_rs, not DuckDB's native Delta writer

DuckDB's Delta write support is read-first: blind `INSERT` only, no `UPDATE`/`DELETE`/`MERGE`.
The project's direction also seems to be writing through Unity Catalog, which is a non-starter:
the whole point of Delta is filesystem simplicity. Once you require a catalog, Iceberg makes
more sense — there are far more providers for it. So delta_rs handles every write and DuckDB
is confined to reads and model logic.

### Why Delta, not Iceberg

Iceberg writers still need time to mature. A POC was built and table maintenance is a blocker.

### Why a separate adapter, not a dbt-duckdb PR

Writing Delta with delta_rs needs the `deltalake` package. dbt-duckdb deliberately keeps a
minimal dependency footprint and avoids external dependencies like this — for very good
reasons — so this doesn't belong upstream. duckrun keeps it isolated here instead.

(For history: the pre-0.3 `duckrun` on the `legacy` branch was a bespoke orchestrator, built
before dbt was adopted as the right tool for the DAG.)

### Why a `delta_scan` view, not per-table `ATTACH (TYPE delta)`

Attaching each table with `ATTACH '<path>' AS <name> (TYPE delta)` after creating it was
considered and rejected as the registration mechanism because:

- **Naming.** A single-table attach is its own catalog (`<name>`), referenced as `<name>`.
  It cannot be placed inside dbt's three-part `lake.mart.dim_duid`, so `{{ this }}` would
  not resolve. A `delta_scan` view can be created with the *exact* name dbt expects:
  `CREATE OR REPLACE VIEW lake.mart.dim_duid AS SELECT * FROM delta_scan('<path>')`.
- **Writes are irrelevant to the choice.** `ATTACH (TYPE delta)` only adds blind `INSERT`
  (no `UPDATE`/`DELETE`); since every write goes through delta_rs anyway, attach buys
  nothing over a view on the read path, while the view aligns names and is always-latest.

(The metadata-caching perf edge of attach is not worth the naming/complexity cost for the
test workloads; `delta_scan` reads the latest `_delta_log` snapshot on each query, which is
what `is_incremental()` needs.)

## The invariant

| Concern            | Mechanism                                                        |
|--------------------|-----------------------------------------------------------------|
| Write Delta        | delta_rs (`engine.write_delta` / `engine.merge_delta`) — unchanged |
| Read a Delta table | `delta_scan('<location>')`                                       |
| `{{ this }}` / `ref()` / `is_incremental()` | a DuckDB **view** `db.schema.id` over `delta_scan('<location>')` |
| New table mid-run  | just `CREATE OR REPLACE VIEW …` — no attach, no re-attach        |
| Cross-process state| views are recreated at run start by discovering Delta tables on disk |

## Design

### 1. Disk discovery → relation cache + read-path views  (`impl.py`)

`list_relations_without_caching` discovers tables from disk. dbt populates its relation cache
at run start by calling `list_relations_without_caching(schema_relation)` for every schema in
the manifest (even on a fresh in-memory DuckDB). For each call:

1. Compute `base = root_path/<schema_relation.schema>`.
2. Enumerate the Delta table directories under `base`. **The mechanism depends on the store**
   (`_discover_via_glob` vs `_discover_via_rest`):
   - **Local / `az://`** — DuckDB `glob` on the adapter's connection (azure autoloads, the
     plugin's secret is already configured):
     ```sql
     SELECT DISTINCT file FROM glob('<base>/*/_delta_log/*.json')
     ```
     `*` matches one segment (the table dir); use `*.json` (a table always has ≥1 commit log;
     `00…0.json` is unreliable after `cleanup_metadata()`). **Normalize separators**: `glob`
     returns OS-native paths (backslashes on Windows), so `replace("\\","/")` before splitting
     on `/_delta_log/` to get the table name (last segment before the marker).
   - **OneLake / `abfss://`** — DuckDB cannot glob `abfss://` (duckdb-azure#174), so the
     table directories are listed with the OneLake DFS REST API instead (`_discover_via_rest`).
     Same result — a set of table names — by a different path.
3. Return relations built with `self.Relation.create(database=<db>, schema=<schema>,
   identifier=<name>, type=RelationType.Table)` merged (de-duped) with `super()`'s result.

   **Type must be `Table`**: dbt-core's `is_incremental()` requires
   `relation.type == 'table'`. The physical object is a view, but it is advertised as a
   table so `is_incremental()` is true on the 2nd run. Use `db`/`schema` from
   `schema_relation` (no hardcoded `lake`).

**Discovery feeds the cache AND registers the read-path view.** It returns the relations so
dbt's Python relation cache makes `is_incremental()` true, and it also calls
`_register_delta_view` on each discovered table so the physical `delta_scan` view exists for
**read-only commands** (`dbt test` / `show` / `docs`), which run no materialization and would
otherwise have no view to query. What that registration does **not** do is make `{{ this }}`
resolve during a `dbt run`: views created on the cache-population connection do **not** survive
to the model-run phase (confirmed empirically — queryable at discovery time, gone when the
model runs). So the run-phase `{{ this }}` view is pre-registered separately in the
materialization (step 2). Guard: if `root_path` is unset or discovery finds nothing, return
`super()`'s result unchanged.

### 2. Materialization: pre-register `{{ this }}`, then view-after-write  (`_delta_core.sql`)

A single flow, no attach branches:

- `duckrun__delta_paths()`: `stage_db` is always `target_relation.database`. The `location`
  is deterministic.
- `duckrun__build_delta()`:
  - **Pre-register `{{ this }}`** at the very top, *before* `run_hooks`: when
    `adapter.delta_table_exists(location)`, `create_schema(target_relation)` and
    `create or replace view {{ this }} as select * from delta_scan('<location>')`. This runs
    on the stable run-phase connection, so pre-hooks and the model's own SQL
    (`is_incremental()` self-reference, e.g. `… NOT IN (SELECT … FROM {{ this }})`) resolve.
  - `create_schema(target_relation)`, stage the model as a view, hand off to the delta_rs
    plugin, drop the staging view.
  - **Step 4 (`main`) is always:**
    ```sql
    create or replace view {{ target_relation }} as
      select * from delta_scan('{{ location }}')
    ```
  - Always `persist_docs`.

A newly created table is just a `CREATE OR REPLACE VIEW` at the end of its own
materialization — immediately visible to every downstream `ref()` in the same run (the
run-phase connection is stable across models). Cross-process `{{ this }}` works via the
pre-register step above. (Refs to a Delta table that exists on disk but is *not* built in the
current run are not auto-registered — do a full/`+upstream` build, the normal dbt workflow.)

`table.sql`, `incremental.sql`, `delta.sql` wrappers call `duckrun__build_delta(...)`.

### 2b. Plugin reads on the model's cursor  (`delta_plugin.py`)

The model is staged as a **view** and streamed to delta_rs as an Arrow stream (no full
in-DuckDB materialization). But the staged view (and any `SET VARIABLE` a pre-hook set, used
by `getvariable()`/`read_csv(...)`) lives in the **session of the cursor dbt ran the model
on**. DuckDB session state is cursor-local: dbt-duckdb calls `configure_connection` once on
the shared connection but gives each model its own child cursor via `configure_cursor`. If
the plugin made *yet another* child via `self._conn.cursor()`, `getvariable(...)` would be
`NULL` and the model would fail with `read_csv cannot take NULL list as parameter`.

So the plugin overrides `configure_cursor(cursor)` to stash the live per-model cursor and
`store()`/`load()` read on **that** cursor (falling back to the shared connection). The
pre-hook variable, the staged view, and the delta_rs read all share one session.

### 3. Memory: one cap split across two engines  (`engine.py`)

DuckDB and delta_rs each manage their own memory and neither knows the other exists. On a
**merge** — the one path where both peak in the same process at the same time (DuckDB
producing the source relation, delta_rs running the join pool) — they share one RAM budget
with no shared allocator. duckrun therefore carves a single *effective* limit into static
shares (`engine.set_merge_memory_limit` / `_default_merge_spill_size`):

- DuckDB `memory_limit` → `_DUCKDB_MEM_FRACTION` (**0.3**),
- delta_rs `max_spill_size` → `_MERGE_SPILL_FRACTION` (**0.6**),
- the remaining **~10%** is slack for Python, Arrow buffers, and page cache.

Past its share each consumer **spills to disk** rather than OOM-killing the container. The
write path (overwrite/append/safeappend/microbatch) has no competing delta_rs pool, so DuckDB
gets the bulk — clamped to `_WRITE_MEM_FRACTION` (**0.7**) of the effective limit
(`engine.set_write_memory_limit`).

The effective limit (`_effective_mem_limit_bytes`) is the **tightest of physical RAM, the
cgroup/container cap, and the RAM actually free**, sampled fresh per job. It is cgroup-aware
on purpose: on Fabric/Spark/k8s, DuckDB's own default (80% of *physical* RAM) sees the whole
node, not our slice, so without this clamp the kernel OOM-kills us.

This is a coordination layer that exists *only* because two independent processes split one
RAM budget. A single engine would not need it — see [Tradeoffs](#tradeoffs).

## Cross-process state

State survives across separate `dbt build` processes with no persistent catalog:

1. **Empty store:** discovery finds nothing → `is_incremental()` false → delta_rs overwrites;
   each model ends as a `delta_scan` view. New tables created earlier in the run are visible
   to later models.
2. **Populated store (fresh process):** disk discovery caches existing tables as `table`s →
   `is_incremental()` true; the materialization pre-registers `{{ this }}` so it reads
   current state; incremental models `merge`/`append` via delta_rs.

## Tradeoffs

Two engines split across a write means paying for a handoff that a single native engine
wouldn't. The honest costs:

- **Managing memory across two independent systems is a hack.** This is the headline cost.
  Neither engine knows the other exists, yet on a merge both peak in one process against one
  RAM budget with no shared allocator. duckrun papers over that with a *static* `0.3` /
  `0.6` / `0.7` split (see [§3](#3-memory-one-cap-split-across-two-engines)) and a
  cgroup-derived limit sampled per job. There is no split that is right for every workload:
  pick wrong and you either starve the merge pool ("Resources exhausted") or overcommit and
  get OOM-killed. A single engine has **one allocator over one budget** and spills against
  its own true peak — it will always be more predictable than two processes dividing a number
  they each only estimate. A native engine needs no tuning constant here at all; this one
  does, and the constant is a guess.
- **The Arrow bridge isn't truly zero-copy.** The C-stream interface passes buffers by
  pointer, but DuckDB's internal vector format is *not* Arrow — producing the Arrow stream
  materializes DuckDB's results into Arrow buffers first. So "zero-copy" describes the
  handoff, not the whole path; there is a real conversion cost on every write.
- **Arrow in-memory is uncompressed.** Data crosses the boundary as raw uncompressed
  columnar buffers, then delta_rs re-encodes and compresses it into Parquet. A single engine
  that read and wrote the same format could keep data compressed end-to-end and skip a
  decode/re-encode round-trip.
- **Two engines, two Parquet readers.** DuckDB reads Delta via `delta_scan` and delta_rs
  reads/writes the log and data files independently — duplicated metadata and Parquet
  machinery that one engine would share.

So, plainly: **a single native engine reading and writing Delta would win** — on the write
path and, more importantly, on predictability. The split-memory coordination above is not a
clever optimization; it is a workaround for not having that engine.

The one thing worth saying in its defence is *not* that two engines are secretly better — it
is that **the single-engine path does not exist today.** DuckDB's Delta writer is blind
`INSERT` only — no `UPDATE`/`DELETE`/`MERGE` — so for the upsert/merge workloads this adapter
exists to serve, the comparison is against a capability we can't ship, not a slower-but-working
alternative.

Net: the boundary is a deliberate, temporary cost we accept to get correct Delta writes
*now*; it is not the long-term ideal, and the design is structured (writes isolated behind
delta_rs, reads behind `delta_scan`) so the writer can be swapped for a native one later
without touching the read/state model — at which point the cross-engine memory split goes away
entirely.
