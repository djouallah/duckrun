# Design: replace `delta_classic` attach with `delta_scan` views

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

Today the read/state layer uses the **`delta_classic`** community extension: the dbt
`database` (`lake`) is a read-only attach over the whole `Tables/` root, so `{{ this }}`
and `is_incremental()` resolve against existing Delta tables across separate `dbt build`
processes.

The problem: `delta_classic` only discovers tables **at attach time**. A table created
mid-run is invisible until the catalog is re-attached, and the re-attach is fragile:

- `refresh_delta_attach()` must `DETACH` + `ATTACH` the whole catalog (can't use
  `ATTACH OR REPLACE` — it corrupts delta_classic's internal per-table databases:
  *"Internal delta database … not found"*).
- You can't detach the active catalog, so it juggles `USE memory` first.
- Empty schemas raise *"Schema X not found"*, special-cased in `list_relations_without_caching`.

This is exactly why **newly created tables struggle**. We are removing `delta_classic`
entirely and exposing each Delta table as a plain **`delta_scan` view** named to match
dbt's `database.schema.identifier`. This is the approach used in the standalone duckrun
("create the delta table as a view, treat it as a real table, no materialization").

### Why `delta_scan` view, not per-table `ATTACH (TYPE delta)`

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

`location` is deterministic: `root_path/<schema>/<identifier>` (or `config(location=…)`).

## Design

### 1. Disk discovery → relation cache only  (`impl.py`)

Replace the `delta_classic` override of `list_relations_without_caching` with disk-based
discovery. dbt populates its relation cache at run start by calling
`list_relations_without_caching(schema_relation)` for every schema in the manifest (even on
a fresh in-memory DuckDB). For each call:

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

Strip the attach branches; there is now a single flow:

- `duckrun__delta_paths()`: drop `attach_mode`; `stage_db` is always
  `target_relation.database`. Keep the deterministic `location`.
- `duckrun__build_delta()`:
  - **Pre-register `{{ this }}`** at the very top, *before* `run_hooks`: when
    `adapter.delta_table_exists(location)`, `create_schema(target_relation)` and
    `create or replace view {{ this }} as select * from delta_scan('<location>')`. This runs
    on the stable run-phase connection, so pre-hooks and the model's own SQL
    (`is_incremental()` self-reference, e.g. `… NOT IN (SELECT … FROM {{ this }})`) resolve.
  - Drop `is_new_delta`, `refresh_delta_attach()`, and the `attach_mode` branches.
  - `create_schema(target_relation)`, stage the model as a view, hand off to the delta_rs
    plugin (unchanged), drop the staging view.
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

`table.sql`, `incremental.sql`, `delta.sql` wrappers are unchanged except that they keep
calling `duckrun__build_delta(...)`.

### 3. Remove `delta_classic` machinery

- **`impl.py`**: delete `delta_attach_alias`, `_attachment_for_alias`,
  `refresh_delta_attach`, `_is_readonly_attach_db`, and the `create_schema` / `drop_schema`
  overrides (revert to inherited). Keep `delta_table_exists` (still useful) and the new
  `list_relations_without_caching`.
- **`credentials.py`**: delete the `delta_attach` property; drop `"delta_attach"` from
  `_connection_keys`; keep `root_path`, `storage_options`, plugin auto-registration.
- **`integration_tests/profiles.yml`**: remove `database: lake`, the `delta_classic`
  extension entry, and the entire `attach:` block. Keep only `root_path` + `storage_options`
  (`bearer_token` + `use_fabric_endpoint`). No explicit `extensions:`/`secrets:` blocks are
  needed: the plugin runs `INSTALL delta; LOAD delta;` and creates the azure secret from
  `storage_options.bearer_token`, and azure autoloads. (An explicit
  `extensions: [{name: delta, repo: community}]` actually *breaks* — `delta` is already
  installed from the core repo, so re-installing from community errors with "origin is
  different".) Let `database` default (in-memory); discovery uses `schema_relation.database`,
  so naming stays consistent.
- **`.github/workflows/integration.yml`**: update the second-pass step comment (no longer
  "delta_classic attach exposes state" → "delta_scan views rebuilt from disk expose state").

### 4. Integration-test model follow-ups (required for green CI)

A few models assume a **writable** `{{ this }}`, which contradicts the invariant (DuckDB
never writes Delta). These must change to rely on delta_rs strategies instead:

- `models/dimensions/dim_duid.sql`: `pre_hook=["DELETE FROM {{ this }} WHERE 1=1"]` — a
  `DELETE` against a `delta_scan` view fails. Replace the "delete-all-then-rebuild on new
  DUIDs" pattern with either `materialized='table'` (delta_rs overwrites every run) or an
  incremental `merge` on `unique_key=['DUID']`.
- `models/marts/fct_summary.sql`: the `TRUNCATE TABLE {{ this }}` pre-hook (full-rebuild
  path) similarly can't run against a view. Drive the rebuild via `--full-refresh` /
  delta_rs overwrite, or restructure so the rebuild branch produces the full set and uses
  `merge` rather than truncate+append.

(Other models — `fct_scada`, `fct_price`, `*_today` — only *read* `{{ this }}`; they work
unchanged.)

## Files to change

- `dbt/adapters/duckrun/impl.py` — new disk-discovery `list_relations_without_caching`;
  remove attach methods.
- `dbt/adapters/duckrun/credentials.py` — remove `delta_attach`.
- `dbt/include/duckrun/macros/materializations/_delta_core.sql` — single view-after-write
  path; remove attach branches.
- `integration_tests/profiles.yml` — drop `lake`/`delta_classic`/`attach`.
- `.github/workflows/integration.yml` — comment only.
- `integration_tests/models/dimensions/dim_duid.sql`,
  `integration_tests/models/marts/fct_summary.sql` — drop writable-`this` pre-hooks.

(`delta_plugin.py`, `engine.py`, `table.sql`, `incremental.sql`, `delta.sql` unchanged.)

## Verification

**Local smoke test (no OneLake creds needed):** point `root_path` at a local dir and build
the offline-capable `dim_calendar` model (incremental, `delete+insert`, `unique_key=date`,
no external data) twice in separate processes:

```
WAREHOUSE_PATH=<localdir> FILES_PATH=dummy ONELAKE_TOKEN=dummy DBT_SCHEMA=mart \
  dbt build --select dim_calendar --project-dir integration_tests --profiles-dir integration_tests
```

Confirmed: pass 1 creates the table (delta_rs overwrite) + a `delta_scan` view; pass 2 (fresh
process) discovers it from disk → `is_incremental()` true (compiled SQL contains
`WHERE date NOT IN (SELECT date FROM …dim_calendar)`) → `{{ this }}` pre-registered → merge
runs idempotently (3197 rows / 3197 distinct dates, no dupes). `not_null`/`unique` tests pass.

**Full CI (`.github/workflows/integration.yml`):** two-pass `dbt build --exclude tag:heavy`
against OneLake:

1. **Pass 1 (empty store):** discovery finds nothing → `is_incremental()` false → delta_rs
   overwrites; each model ends as a `delta_scan` view. New tables created earlier in the run
   are visible to later models (the bug we're fixing).
2. **Pass 2 (fresh process, populated store):** disk discovery caches existing tables as
   `table`s → `is_incremental()` true; the materialization pre-registers `{{ this }}` so it
   reads current state; incremental models `merge`/`append` via delta_rs.

Green on both passes — with no `delta_classic`, no `refresh_delta_attach`, and no
"newly created table" failures — confirms the change.
