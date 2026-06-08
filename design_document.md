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

### 1. Disk discovery → relation cache only  (`impl.py`)

`list_relations_without_caching` discovers tables from disk. dbt populates its relation cache
at run start by calling `list_relations_without_caching(schema_relation)` for every schema in
the manifest (even on a fresh in-memory DuckDB). For each call:

1. Compute `base = root_path/<schema_relation.schema>`.
2. Enumerate Delta tables via DuckDB `glob` on the adapter's connection (works for local,
   OneLake/abfss, S3 — azure autoloads and the plugin's secret is already configured):
   ```sql
   SELECT DISTINCT file FROM glob('<base>/*/_delta_log/*.json')
   ```
   `*` matches one segment (the table dir); use `*.json` (a table always has ≥1 commit log;
   `00…0.json` is unreliable after `cleanup_metadata()`). **Normalize separators**: `glob`
   returns OS-native paths (backslashes on Windows), so `replace("\\","/")` before splitting
   on `/_delta_log/` to get the table name (last segment before the marker).
3. Return relations built with `self.Relation.create(database=<db>, schema=<schema>,
   identifier=<name>, type=RelationType.Table)` merged (de-duped) with `super()`'s result.

   **Type must be `Table`**: dbt-core's `is_incremental()` requires
   `relation.type == 'table'`. The physical object is a view, but it is advertised as a
   table so `is_incremental()` is true on the 2nd run. Use `db`/`schema` from
   `schema_relation` (no hardcoded `lake`).

**Discovery does NOT create views.** dbt runs `list_relations_without_caching` during the
`before_run` cache-population phase; views created on that connection do **not** survive to
the model-run phase (confirmed empirically — the view is created and queryable at discovery
time but gone when the model runs). So discovery only feeds dbt's Python relation cache
(making `is_incremental()` true); the physical `delta_scan` view is created in the
materialization instead (step 2). Guard: if `root_path` is unset or `glob` finds nothing,
return `super()`'s result unchanged.

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

So yes: **a single native engine reading and writing Delta would generally win on the write
path**, and once DuckDB's native Delta writes mature this design should be revisited (see
[Context](#context)).

Where I'd push back on "a single engine is *always* better":

- **It isn't an option today.** DuckDB's Delta writer is blind `INSERT` only — no
  `UPDATE`/`DELETE`/`MERGE`. For the upsert/merge workloads this adapter exists to serve,
  the single-engine path doesn't exist yet, so the comparison is against a capability we
  can't ship, not a slower-but-working alternative.
- **Two specialized engines can beat one generalist.** DuckDB is an excellent vectorized
  reader/executor and delta_rs is a mature, correct Delta writer (concurrency, retries,
  spill-aware merge). A single engine that is merely adequate at both can lose to two that
  are each best-in-class at their half — the boundary cost has to exceed what you gain from
  specialization, and for these workloads it doesn't.
- **The cost is bounded and amortized.** The handoff is an Arrow stream, not a full
  in-DuckDB materialization, and the dominant cost on incremental tables is the Delta
  merge/scan itself — the conversion is a small slice of total runtime here.

Net: the boundary is a deliberate, temporary cost we accept to get correct Delta writes
*now*; it is not the long-term ideal, and the design is structured (writes isolated behind
delta_rs, reads behind `delta_scan`) so the writer can be swapped for a native one later
without touching the read/state model.
