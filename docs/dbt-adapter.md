---
hide:
  - navigation
---

# duckrun dbt adapter

duckrun is a thin wrapper around [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb): you keep
everything dbt-duckdb gives you — views, seeds, sources, tests, snapshots, the full plugin ecosystem
— and gain a Delta-backed `table` / `incremental` materialization that writes real Delta tables via
delta_rs. The design rationale (why delta_rs, why Delta not Iceberg, why a separate adapter) is in
[design_document.md](design_document.md).

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # `threads:` works as usual (dbt defaults to 1). Concurrent models share one DuckDB
      # memory budget, so raise it for many small/network-bound models, not for one big merge.
      # DuckDB runs in-memory by default — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...).
      # OneLake — address by GUID, not friendly names (see "OneLake: use GUID paths" below):
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables"
      # Or any other store: './warehouse' (local), 's3://...', 'gs://...'.
      # storage_options: {}      # passed through to deltalake for remote stores
```

Persisted models are written to `<root_path>/<schema>/<model>` (e.g. `.../Tables/dbo/orders`), or to
an explicit `config(location=...)`.

### OneLake shorthand for `root_path`

On OneLake you can drop the `abfss://…@onelake.dfs.fabric.microsoft.com/…/Tables` boilerplate and
write `workspace/item` — the same shorthand `duckrun.connect()` accepts, expanded when the profile
loads:

```yaml
      root_path: "<workspace_id>/<lakehouse_id>"        # → abfss://…/<lakehouse_id>/Tables
      # or by name: root_path: "my_workspace/sales.Lakehouse"
```

It works for each entry under `catalogs:` and for a source's `location` / `delta_table_path` too
(spell `…/Files/…` explicitly to reach the file side). Only two shapes are shorthand: an item with a
`.Lakehouse`/`.Warehouse` suffix, or a workspace-GUID/item-GUID pair — a suffix-less `warehouse/tables`
is still an ordinary local relative path.

### OneLake: use GUID paths for now

Address OneLake tables by **workspace GUID + lakehouse GUID**, not friendly names —
`abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/...`. This sidesteps an
upstream `duckdb-delta` read bug ("No files in log segment") that is **already fixed upstream but
still rolling out to production OneLake**. Friendly-name paths will work again once the fix finishes
deploying.

### OneLake authentication — tokens are optional

On OneLake, `storage_options` can be left out entirely: when no `bearer_token` is supplied, duckrun
self-acquires one (`azure-identity` ships as a core dependency) and mints the matching DuckDB Azure
secret, so writes **and** `delta_scan` reads — including `dbt test` / `dbt show` /
`dbt docs generate` — work with no credential in the profile. Acquisition order: Fabric notebook
(`notebookutils`) → an `AZURE_STORAGE_TOKEN` env var → GitHub Actions OIDC workload-identity
federation (`AZURE_CLIENT_ID` + `AZURE_TENANT_ID` env vars and `id-token: write` job permission — no
client secret) → Azure CLI → interactive browser. Tokens are cached per scope and re-acquired near
expiry.

An explicit `storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }` still works and
takes precedence — use it when you want to pin exactly which identity writes.

### Fabric Lakehouse without a schema

A schema-less Lakehouse (tables straight under `Tables/`, no `Tables/<schema>/` grouping) is a **bad
pattern** — you lose the namespace that keeps a warehouse organized — but if you're stuck with one,
no special config is needed. Drop the trailing `Tables` from `root_path` and let the schema fill that
slot:

```yaml
      schema: Tables
      root_path: "abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>"
```

Since models are written to `<root_path>/<schema>/<model>`, this lands them at
`<lh>.Lakehouse/Tables/<model>` — exactly the flat layout the schema-less Lakehouse expects. Prefer a
schema-enabled Lakehouse (`root_path: .../Tables`, real schemas) whenever you can.

### Remote stores (S3 / GCS / ADLS)

Point `root_path` at the warehouse location and pass credentials through `storage_options` — these
flow straight to deltalake for writes and merges.

On Azure-backed stores, if `storage_options` carries a `bearer_token` (or `token` / `access_token`),
the adapter also auto-creates a matching DuckDB Azure secret, so `delta_scan()` reads work with no
extra config. In a notebook where the storage secret is already provided to DuckDB, you can leave
`storage_options` empty.

```yaml
    remote:
      type: duckrun
      schema: dbo
      root_path: "s3://my-bucket/warehouse"   # or abfss://... , gs://...
      storage_options:
        aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

Verified end-to-end against real remote object storage: `table` overwrite, `incremental` merge, and
`delta_scan` reads / tests.

### Multiple Lakehouses in one project

One dbt project can write across several Lakehouses (e.g. a medallion `LH_Bronze` / `LH_Silver` /
`LH_Gold`). Declare each extra write root as a named **catalog** under `catalogs:`, then send a model
to it with the standard dbt `+database: <alias>` config. The default catalog is `database`
(`root_path` / `storage_options` at the top level); a project with no `catalogs:` behaves exactly as
before.

```yaml
    dev:
      type: duckrun
      # The default catalog is the top-level root_path/storage_options (Silver here). Don't set a
      # `database:` — dbt-duckdb requires it to match the `:memory:` path; the default catalog is
      # DuckDB's own `memory`.
      root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
      catalogs:
        lh_bronze:
          root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
          storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
        lh_gold:
          root_path: "abfss://ws@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables"
```

```sql
-- models/bronze/raw_events.sql — lands in LH_Bronze
{{ config(materialized='incremental', database='lh_bronze', unique_key='id') }}
select ...
```

Each catalog carries its own `storage_options`, so a per-Lakehouse OneLake token works (the adapter
mints a path-scoped DuckDB secret per catalog). `ref()` resolves across catalogs, and
`is_incremental()` / `dbt docs generate` work per Lakehouse.

### Multiple environments (dev / PPE / prod)

A team promoting the same project across Fabric workspaces (dev → PPE → prod) needs one **target**
per environment, each pointing its `catalogs:` aliases at that environment's own Lakehouses. Reuse
the same alias names in every target — only the `root_path`s change — so model config
(`+database: lh_bronze`) doesn't need to differ per environment.

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      root_path: "abfss://dev_ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
      catalogs:
        lh_bronze:
          root_path: "abfss://dev_ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
          storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
    ppe:
      type: duckrun
      root_path: "abfss://ppe_ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
      catalogs:
        lh_bronze:
          root_path: "abfss://ppe_ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
          storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
    prod:
      type: duckrun
      root_path: "abfss://prod_ws@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"
      storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
      catalogs:
        lh_bronze:
          root_path: "abfss://prod_ws@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables"
          storage_options: { bearer_token: "{{ env_var('ONELAKE_TOKEN') }}" }
```

```bash
dbt run --target ppe
```

Model config (`+database: lh_bronze`) never mentions an environment, so the same project runs
unmodified against any target.

#### Following the target from a source

A source can resolve its path the same way, via the alias → root map the profile resolver exposes
on the Jinja `target` as `target.catalog_locations` (see [Config your profile](#configure-your-profile)) —
`{alias: root_path}` for every entry under `catalogs:`, no tokens. This lets a source pick up
whichever environment's Lakehouse the current target points at, instead of hard-coding one path:

```yaml
sources:
  - name: lake
    tables:
      - name: raw_events
        meta:
          plugin: duckrun
          delta_table_path: "{{ target.catalog_locations['lh_bronze'] }}/dbo/raw_events"
```

**The empty-string trap.** `target.catalog_locations['lh_bronze']` is a plain Jinja dict lookup: if
the current target has no `lh_bronze` entry under `catalogs:` — a typo, or a target that hasn't been
updated yet — Jinja renders it as an empty string instead of failing, so the source silently becomes
`/dbo/raw_events`, and the failure surfaces later as a confusing
`InvalidTableLocationError: Path does not exist` (or an empty read) rather than a clear "unknown
catalog" error. Catch it up front with an `on-run-start` guard that checks every alias your sources
rely on before anything runs:

```sql
-- macros/require_catalogs.sql
{% macro require_catalogs(aliases) %}
  {% for alias in aliases %}
    {% if alias not in (target.catalog_locations or {}) %}
      {{ exceptions.raise_compiler_error("target '" ~ target.name ~ "' has no `" ~ alias ~ "` catalog") }}
    {% endif %}
  {% endfor %}
{% endmacro %}
```

#### Guarding production

To stop an accidental `dbt run --target prod` (or a `--full-refresh` a prod run should never take),
add another `on-run-start` macro that checks `target.name` before anything executes:

```sql
-- macros/guard_prod.sql
{% macro guard_prod() %}
  {% if target.name == 'prod' and flags.FULL_REFRESH %}
    {{ exceptions.raise_compiler_error("--full-refresh is blocked against target 'prod'") }}
  {% endif %}
{% endmacro %}
```

```yaml
# dbt_project.yml — both guards run on every invocation, in order
on-run-start:
  - "{{ require_catalogs(['lh_bronze', 'lh_gold']) }}"
  - "{{ guard_prod() }}"
```

## Materializations

| materialized      | backed by                | notes                                                                 |
|-------------------|--------------------------|-----------------------------------------------------------------------|
| **`table`**       | Delta (overwrite)        | DuckDB runs the SQL; delta_rs writes the table fresh each run.         |
| **`incremental`** | Delta (merge / append)   | First run overwrites; later runs apply `incremental_strategy`.         |
| `view`            | in-memory DuckDB         | Ephemeral staging within a run (inherited from dbt-duckdb).            |
| `seed`            | Delta (overwrite)        | CSV loaded via DuckDB, then persisted as a Delta table — survives across processes like a model. |
| `delta`           | Delta                    | Alias for `table`; honors `incremental=true`. Kept for convenience.   |

The persisted materializations (`table`, `incremental`, `delta`, `seed`) register a `delta_scan` view
over the new Delta table, so downstream `ref()` works — and because a seed is now a real Delta table,
it's rediscovered from storage in a fresh process (e.g. `dbt docs generate`, or a partial
`--select`), not just within the run that loaded it.

### `table`

```sql
-- models/orders.sql
{{ config(materialized='table') }}

select status, count(*) as n, sum(amount) as total
from {{ ref('stg_orders') }}
group by status
```

### `incremental`

```sql
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

The first run (or `--full-refresh`, or a missing table) overwrites. Later runs apply the
`incremental_strategy`:

| `incremental_strategy`             | behavior                                  | requires     |
|------------------------------------|-------------------------------------------|--------------|
| `merge` (default with `unique_key`) | upsert — update matched, insert new       | `unique_key` |
| `insert`                           | insert only new keys (idempotent append) — computed in DuckDB, committed as a **plain append** | `unique_key` |
| `delete+insert`                    | delete the batch's keys, insert the whole batch (duplicates preserved) | `unique_key` |
| `append` (default without `unique_key`) | append; **auto-fenced** when the model reads `{{ this }}`, else a blind append | — |
| `microbatch`                       | `replaceWhere` per dbt-driven `event_time` window | `event_time` |

### `insert` — insert-only, computed in DuckDB

Insert-only is the one incremental shape that never *removes* a row, so it is genuinely a single
Delta append: duckrun computes "batch rows whose `unique_key` is not already present" as a DuckDB
anti-join and hands delta_rs a commit containing `add` actions only. **No existing data file is ever
rewritten**, and the target read is projected down to the key columns.

This matters on a large fact table. A delta_rs `MERGE` — even an insert-only one — plans a join
against the whole pinned target, so its cost scales with the target's partition span rather than the
size of the batch, and its join state is not fully spillable. The DuckDB anti-join spills, keeps the
full write memory share (the 30/60 merge split is not applied), and writes nothing but the new rows.

It is row-for-row the same table delta_rs's `when_not_matched_insert_all` produces, NULL keys
included: `target.k = source.k` is NULL for a NULL key, never TRUE, so the row counts as unmatched
and inserts — the same rule SQL `IN` follows.

Two consequences worth knowing:

- **A batch that adds nothing writes no commit at all.** The Delta version does not move, where a
  delta_rs `MERGE` commits a no-op version. Re-running an already-loaded backlog is free and leaves
  no log churn. History records the write as `WRITE`, not `MERGE`.
- **The append is always fenced.** The anti-join reads the target, so this is a read-modify-append:
  it commits only if the table version is unchanged since the model started, and fails with
  `CommitFailedError` otherwise. A concurrent writer that landed in between would have made the
  anti-join stale and let a duplicate through.

The probe does not scan blindly. For every column it joins on, duckrun folds a **constant** filter
into the read of the target: the batch's `min`/`max` for a high-cardinality key
(`"id" >= 19900000 AND "id" <= 20099999`), so the reader skips files whose Delta stats put them
outside the batch's range — the same early filter a delta-rs merge derives from source statistics.
Both are result-neutral, because the join already requires equality.

For a partition column you get something better — the exact value set — but only if you declare the
equality, the same predicate you would give a merge:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
    partition_by=['month_key'],
    incremental_predicates=['target.month_key = source.month_key'],
) }}
```

That gets `"month_key" IN (202601, 202602)` — the batch's actual partitions, so whole partition
directories are skipped. An `IN` set beats a range here: a source that unions an old backfill with the
current feed is bimodal, and a `min`/`max` bound would smear across every partition in between.
Without the declared equality duckrun will not prune on `month_key` at all, since doing so would no
longer be result-neutral.

One thing the append inherits from the plain `append` strategy: it leaves a small file per partition,
and the shared post-write maintenance will compact them once the table's byte debt trips its gate,
rewriting files the insert itself did not. On a table whose files are already at target size that gate
does not fire.

**The notebook API gets the same treatment.** `conn.sql("MERGE INTO t USING s ON … WHEN NOT MATCHED
THEN INSERT *")` is the same operation written differently, so it routes to the same anti-join —
duckrun decides this at the shared engine seam, not per surface, so a dbt model and the equivalent SQL
cannot execute two different ways. Any other clause shape (a matched update or delete, a by-source
clause, a partial `INSERT (cols)`) still runs on delta_rs, because removing or changing a row means
rewriting files.

Need delta_rs's merge for an insert-only shape anyway? Set `merge_streamed_exec: true` (or pass
`streamed_exec` on the connection API) — an explicit request for delta_rs's streaming source handling
forces that path.

### `append` that reads `{{ this }}` — the automatic fence

A cheap append for the common "load only what's new" pattern — when your model SQL **already
guarantees no duplicates** and you don't want to pay for a merge. Use `incremental_strategy='append'`
and dedup against the table itself:

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}

select * from read_csv(getvariable('new_files'))
{% if is_incremental() %}
  -- the dedup is your SQL's job: only load files not already in the table
  where file not in (select distinct file from {{ this }})
{% endif %}
```

**Why, reason 1 — performance.** `merge` / `insert` read the target and join on the key to find
what's new. If the SQL above already excludes rows that are present, that work is redundant: a plain
`append` does **no target data scan and no key join at all**, reading only one Delta log entry to get
the version. (`insert` is far cheaper than it used to be — it is a DuckDB anti-join over the key
columns now, not a delta_rs merge — but it still reads the target, and this reads nothing.)

**Why, reason 2 — an automatic concurrency guard.** Because the dedup is done in SQL against
`{{ this }}`, a naive append would be unsafe under concurrency: if another writer commits between your
`not in (... from {{ this }})` read and your write, the file it added isn't excluded and you get a
duplicate. duckrun closes that gap **automatically** — because the model **reads `{{ this }}`**, the
append commits **only if the table version is unchanged since the model started** (captured *before* it
reads `{{ this }}`); if anything committed in between, it fails with `CommitFailedError` so the run
re-runs against the new state. No duplicate slips in. An `append` that does *not* read `{{ this }}`
(appending genuinely new data) is left unfenced — there's nothing to lose.

This is **optimistic concurrency control** — it never locks the table or blocks other writers; it
appends, then validates at commit with a compare-and-swap on the version and aborts on a mismatch.
It's the strictest guard (abort on *any* concurrent change), applied automatically only when the
read-modify-append shape needs it. Re-running is safe and idempotent: the SQL dedup simply excludes
whatever the previous attempt already loaded.

> Earlier versions exposed this as a separate `append_if_unchanged` strategy. That's gone — the
> behavior is now automatic on `append` whenever the model reads `{{ this }}`, so there's no strategy
> to pick. First run (or `--full-refresh`, or a missing table) overwrites to create the table.

### Config options (`table` / `incremental` / `delta`)

| option                  | description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `location`              | Delta path. Defaults to `<root_path>/<schema>/<id>`.                        |
| `incremental_strategy`  | `merge` \| `insert` \| `append` \| `delete+insert` \| `microbatch` (incremental only). |
| `unique_key`            | column(s) to merge on.                                                       |
| `merge_update_columns`  | merge: update only these columns on match (others untouched).               |
| `merge_exclude_columns` | merge: update all columns **except** these on match.                        |
| `merge_update_condition` / `merge_insert_condition` | merge: extra predicate AND-ed onto the matched-update / not-matched-insert clause (use `target.`/`source.`, or dbt's `DBT_INTERNAL_DEST`/`DBT_INTERNAL_SOURCE`). `merge_insert_condition` also applies to `insert`, where it must reference only the source — an unmatched row has no target to read. |
| `merge_clauses` / `merge_update_set_expressions` | merge: dbt-duckdb-style custom clause list / per-column `SET` expressions — translated to delta_rs's full TableMerger clause list. The clause dict follows dbt-duckdb spelling for spelling, so one config works on both adapters: `action` `update` / `delete` / `insert` / `do_nothing`, `mode` `by_name` / `by_position` / `star` / `explicit`, a `condition` string **or** list, `insert: {columns, values}`, `update: {include, exclude, set_expressions}`, and `by: source` for a not-matched-**by-source** clause. An omitted `when_matched` / `when_not_matched` key gets dbt-duckdb's implicit default (update-by-name / insert-by-name) — so `merge_clauses={'when_matched': [{'action': 'do_nothing'}]}` is insert-only, and takes the same cheap append route as `incremental_strategy='insert'`. duckrun refuses only what delta_rs cannot express: `merge_on_using_columns` and `action: error`. |
| `when_not_matched_by_source` | merge: duckrun's own top-level `merge_clauses` key (`update` with a `set` map / `delete` / `do_nothing`) for rows the source doesn't carry — full-sync semantics. Being duckrun-only, a dict that uses it opts **out** of the implicit clause defaults above; use dbt-duckdb's portable `{'by': 'source', …}` entry inside `when_not_matched` if you want both. |
| `merge_max_spill_size`  | merge: memory ceiling in **bytes** for delta_rs's merge pool (not a disk budget). Defaults to ~60% of the **effective** limit — `min(physical RAM, container/cgroup limit, currently-free RAM)` — beyond which delta_rs spills the merge join to disk (like DuckDB's `memory_limit`). DuckDB itself is bounded once per session by a `memory_limit` pin at ~85% of the same effective limit (measurement shows the two never peak together — delta_rs holds ~99% of merge RSS); both log their chosen value at run start. Set `0` to disable. It bounds the merge pool, *not* the whole process (the Arrow source, read buffers, and spill-file page cache sit outside it), so on a tight container with a huge source the total can still exceed the cap — lower it if needed. A cap below the join's minimum (~hundreds of MB) makes the merge raise `Resources exhausted` instead of spilling. Requires deltalake 1.5.0 (pinned). |
| `merge_max_temp_directory_size` | merge: disk cap in bytes for delta_rs's merge spill files (default ~80% of free disk). |
| `merge_streamed_exec`   | merge: `true` streams a huge merge **source** instead of collecting it into memory — needed for very large sources (especially `WHEN NOT MATCHED BY SOURCE` custom clauses), at the cost of losing target-file pruning. Default `false` suits the normal small-batch-into-big-table case. |
| `incremental_predicates`| merge / insert: extra predicates AND-ed into the merge (or anti-join) condition (use `target.`/`source.`, or dbt's `DBT_INTERNAL_DEST`/`DBT_INTERNAL_SOURCE`). On `insert`, a `target.<part> = source.<part>` entry also unlocks literal partition pruning of the target probe. |
| `on_schema_change`      | `ignore` (default) \| `append_new_columns` \| `fail`. (`sync_all_columns` only *adds* — delta_rs can't drop columns.) |
| `partition_by`          | Delta partition column(s).                                                   |
| `sort_by`               | column(s) to physically ORDER the write by (long RLE runs / dictionary locality — a trailing `ORDER BY` in the model SQL is **not** honored, this config is). The scalar `'auto'` (case-insensitive, **experimental**) profiles the staged model result and picks the key itself — the same heuristic as the connection API's `SORTED BY AUTO`. It writes unsorted when nothing pays off, re-profiles every incremental batch (so the key can vary run to run), and is inert on the delta_rs `merge` / `microbatch` / `delete+insert` paths, which keep the table's existing layout. See [Automatic sorting](parquet-layout.md#automatic-sorting). |
| `max_row_group_size`    | **rows** (deltalake's `WriterProperties` spelling) — an explicit parquet row-group **ceiling** for this model's writes. Unset, duckrun sizes row groups adaptively from a planner estimate that can under-shoot on a first build of a big joined/aggregated model (see [the write layout](parquet-layout.md#the-write-layout)); an explicit value bypasses that estimate entirely and is preserved by post-write compaction. E.g. `max_row_group_size: 16000000` pins a large fact to full Direct Lake segments from its very first build. Applies wherever duckrun writes files (overwrite, append, `insert`, `delete+insert`, `microbatch`); the delta_rs `merge` write itself keeps delta_rs defaults, but the post-merge compaction folds merged files into this geometry. |
| `target_file_size_mb`   | **megabytes** — per-model target parquet file size (default 256 MB). A row group cannot span files, so this byte cap also bounds segment size; the same value drives this model's post-write compaction so maintenance doesn't undo it. Same write-path coverage (and merge caveat) as `max_row_group_size`. |
| `merge_schema`          | allow schema evolution on write.                                            |
| `storage_options`       | per-model override forwarded to deltalake.                                   |

## Reading existing tables/files as sources

A source routed to the `duckrun` plugin can be a Delta table, a CSV, or a Parquet file.
`delta_table_path` always reads Delta; otherwise the path comes from `location` and the format is
taken from `format` (`csv` | `parquet` | `delta`) or inferred from the extension.

```yaml
sources:
  - name: lake
    tables:
      - name: customers           # Delta table
        meta:
          plugin: duckrun
          delta_table_path: 's3://bucket/lake/customers'
      - name: events              # CSV (read_csv_auto)
        meta:
          plugin: duckrun
          format: csv
          location: 's3://bucket/raw/events.csv'
      - name: metrics             # Parquet
        meta:
          plugin: duckrun
          format: parquet
          location: 's3://bucket/raw/metrics.parquet'
```

## How it works

1. dbt compiles your model SQL.
2. The materialization stages it as a DuckDB view.
3. A `dbt-duckdb` plugin (a `store()` hook) hands that relation to deltalake over the Arrow C-stream
   interface (`__arrow_c_stream__`) — no pyarrow required — which `write_deltalake` /
   `DeltaTable.merge` consume natively.
4. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter is a thin subclass of dbt-duckdb declaring `dependencies=['duckdb']`, so `view`, `seed`,
tests, and the rest are inherited directly; only `table` and `incremental` are overridden to write
Delta.

## Table maintenance (compaction & vacuum)

**duckrun maintains your Delta tables automatically — no configuration, no scheduled job, no separate
`OPTIMIZE`/`VACUUM` run to remember.** It happens inline on every write.

This matters because delta_rs has **no** automatic, post-commit maintenance of its own — and it
ignores Databricks-style auto-optimize table properties (`delta.autoOptimize.*`). Left alone, an
incremental table fragments into many small Parquet files and keeps every superseded file version
forever. duckrun runs the maintenance for you, right after each write:

| write | maintenance |
|---|---|
| `table` / overwrite | `vacuum` + metadata cleanup every run |
| `append` | `optimize.compact` + `vacuum` + cleanup once the table exceeds **100 files** |
| `merge` / `insert` | same threshold-gated `compact` + `vacuum` + cleanup after the merge |
| `microbatch` / delete+insert | same threshold-gated maintenance |

Every `vacuum` uses delta_rs's **safe default retention (7 days / 168h)**, so files a concurrent
reader might still be reading are never deleted out from under it. The trade-off is that a superseded
file version lingers for the retention window before it can be reclaimed — duckrun favors read-safety
over immediate disk savings.

## Limitations

The adapter's trade-offs — what `threads` above 1 costs you, the shared two-engine memory pool, the
soft-tombstone `DROP TABLE`, rejected merge configs, add-only schema evolution — are consolidated
with everything else in the top-level [Limitations](limitations.md) page.
