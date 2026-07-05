---
name: duckrun-projects
description: How to build, configure, and run dbt projects on the duckrun adapter — dbt models executed in DuckDB and materialized as Delta Lake tables via delta-rs, locally or on S3/GCS/ADLS/OneLake (Microsoft Fabric). Use this skill whenever a data engineer is setting up a duckrun profile, writing models or sources for it, choosing an incremental strategy (merge, insert, append, append_if_unchanged, microbatch), pointing dbt at OneLake/a Fabric Lakehouse, debugging a duckrun run, or asking "dbt + DuckDB + Delta" questions in general. Consult it BEFORE writing profiles.yml or any incremental model config — several defaults differ from other dbt adapters.
---

# Building dbt projects with duckrun

duckrun is a dbt adapter where **DuckDB executes your model SQL, delta-rs writes the
result as a Delta table, and dbt orchestrates the DAG**. It wraps dbt-duckdb, so
everything dbt-duckdb gives you still works — `view`, `seed`, sources, tests,
snapshots, Python models — and `table` / `incremental` write real Delta tables. The
whole pipeline is pure Python: it runs identically on a laptop, in GitHub Actions, or
in a Fabric Python notebook.

Mental model for every model you write: the SQL runs in an in-memory DuckDB; the result
streams to delta-rs which commits it to `<root_path>/<schema>/<model>`; dbt then sees
that table through a `delta_scan` view, which is how `{{ this }}`, `ref()` and
`is_incremental()` resolve — including across separate dbt processes. The Delta tables
are the ONLY state; there is no database file to manage.

## Install and profile

```bash
pip install duckrun        # brings dbt-duckdb, duckdb, deltalake at the right pins
```

Do NOT separately pin `duckdb` or `deltalake` in your requirements — duckrun pins exact
versions for documented upstream-bug reasons, and overriding them is the most common
way to break a project.

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      schema: dbo
      root_path: './warehouse'        # local path, or s3:// gs:// abfss://
      # storage_options: {}           # credentials, passed straight to deltalake
```

Things that differ from other adapters:

- **No `threads:` needed.** duckrun always runs single-threaded and pins it internally.
  Don't tune it, don't document it to users.
- **No database file.** DuckDB is in-memory by default; don't add a `path:` expecting
  persistence — persistence is the Delta tables.
- Models land at `<root_path>/<schema>/<model>`; a per-model `config(location=...)`
  overrides that.

### Remote stores

Point `root_path` at the store and put credentials in `storage_options` — they flow to
deltalake for writes and merges:

```yaml
remote:
  type: duckrun
  schema: dbo
  root_path: "s3://my-bucket/warehouse"
  storage_options:
    aws_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
    aws_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

### OneLake / Microsoft Fabric

A bearer token is the whole credential. duckrun auto-creates the matching DuckDB Azure
secret from it, so `delta_scan` reads work with no extra config — including read-only
commands (`dbt test`, `dbt show`, `dbt docs generate`):

```yaml
fabric:
  type: duckrun
  schema: dbo
  root_path: "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables"
  storage_options:
    bearer_token: "{{ env_var('ONELAKE_TOKEN') }}"
```

In a Fabric Python notebook, get the token with
`notebookutils.credentials.getToken("storage")`. In CI, mint it via OIDC + the Azure
CLI (`az account get-access-token --resource https://storage.azure.com`). In a notebook
where the DuckDB storage secret is already configured, `storage_options` can stay
empty for reads — but writes still need the token.

**Schema-less Lakehouse** (tables directly under `Tables/`, no schema folders): it's a
layout to avoid, but if stuck with it, drop the trailing `Tables` from `root_path` and
let the schema fill that slot — `schema: Tables`,
`root_path: "abfss://.../<lh>.Lakehouse"`. Models then land at
`<lh>.Lakehouse/Tables/<model>`.

End-to-end reference project (Fabric + OneLake + Direct Lake + scheduled CI):
https://github.com/djouallah/dbt_fabric_python_delta

## Running in a Fabric Python notebook

The reference project's `run.Notebook` is the canonical shape. Reproduce its cell
structure exactly — the ordering is not stylistic.

**Cell 0 — session config first (optional):** `%%configure` must be the very first
cell if used at all. Parameterize `vCores` so a Data Pipeline can size the session per
run (small default, scale up for backfills).

**Cell 1 — install, then restart, nothing else:**

```python
!pip install -q duckrun --upgrade
notebookutils.session.restartPython()
```

This cell IS the workaround for the central Fabric problem: the runtime preinstalls
older `duckdb` and `deltalake`, and both are native extensions — pip replaces them on
disk but the running interpreter keeps the old binaries loaded. duckrun's install
force-upgrades them (its pins are exact for upstream-bug reasons), so a kernel restart
is mandatory before anything imports them. `restartPython()` restarts the interpreter
and execution continues at the NEXT cell, so the rules are:

- Install + restart live alone in the first code cell. Never put imports, config, or
  any work before or beside them — everything in memory is lost at the restart.
- Never `import duckdb`, `deltalake`, or dbt before this cell.
- Don't "optimize away" the restart when the notebook seems to work without it — it
  works until the preinstalled version drifts, then fails confusingly mid-run.

**Cell 2 — dual-environment bootstrap.** `try: import notebookutils` for Fabric;
`except ModuleNotFoundError:` falls back to local dev (`AzureCliCredential` for the
token, a yaml file for config). One notebook, runnable in Fabric and on a laptop —
keep this pattern so the dev loop doesn't need a Fabric session. In Fabric, pull
per-environment config from a Variable Library (workspace id, lakehouse name, limits)
rather than hardcoding, resolve the lakehouse **GUID** with
`notebookutils.lakehouse.get(name)['id']` — abfss paths use ids, not names — and get
the token with `notebookutils.credentials.getToken('storage')`. The dbt project itself
lives in the lakehouse `Files/dbt` area and is copied to `/tmp` each run with
`notebookutils.fs.cp(..., True)`: the notebook is a runner, not the project's home.

**Cell 3 — env vars are the only interface between notebook and dbt.** Export
`ONELAKE_TABLES_PATH`, `ONELAKE_TOKEN`, `FILES_PATH`, and any limits; keep
`profiles.yml` pure `env_var()` so the same profile serves notebook, laptop, and CI.

**Cell 4 — orchestrate in-process with `dbtRunner`,** not shell `!dbt`:

```python
from dbt.cli.main import dbtRunner
os.chdir(dbt_path)
dbt = dbtRunner()
base = ["--target", dbt_target, "--profiles-dir", "."]

dbt.invoke(["run", "--select", "stg_csv_archive_log", *base])          # 1. ingest log first
new_daily = not dbt.invoke(["run-operation", "check_new_daily", *base]).success   # 2. probe
result = dbt.invoke(["run", "--exclude", "stg_csv_archive_log",
                     "--exclude", "fct_summary", *base])               # 3. main build
if not result.success:
    dbt.invoke(["retry", *base])                                       #    retry failures once
dbt.invoke(["run", "--select", "fct_summary", *base]
           + (["--full-refresh"] if new_daily else []))                # 4. conditional rebuild
dbt.invoke(["test", *base])                                            # 5. tests
```

Patterns worth reusing from this:

- **run-operation as a boolean probe.** A macro that `raise_compiler_error(...)` to
  signal "yes" lets the runner branch on `.success` (or the exit code in bash). Inside
  such a macro, query **physical paths** directly (`delta_scan('<abfss path>')`,
  `read_parquet(...)`) — run-operations register no model views, so `ref()`/`{{ this }}`
  won't resolve there. Run-operations also skip `on-run-start` hooks, so re-apply any
  session settings the macro needs.
- **Conditional `--full-refresh` of a single model**, decided by the probe BEFORE the
  models that would change its answer get built — order the probe accordingly.
- **`dbt retry` once** on a failed main build, before failing the run.
- **Mirror the exact same step sequence in CI.** The reference repo's GitHub Actions
  job runs the identical five steps in bash, authenticating via OIDC →
  `az account get-access-token --resource https://storage.azure.com` (mask the token in
  logs), with `AZURE_TRANSPORT_OPTION_TYPE=curl` set for the runner environment. Parity
  between notebook and CI means a green laptop run predicts a green scheduled run.

## Materializations

| `materialized` | Backed by | Use when |
|---|---|---|
| `table` | Delta overwrite | Full rebuild every run |
| `incremental` | Delta merge / append | Grow or upsert; strategy below |
| `view` | in-memory DuckDB | Ephemeral staging within a run |
| `seed` | in-memory DuckDB | CSV fixtures |

`table`, `incremental` (and the `delta` alias) register a `delta_scan` view after
writing, so downstream `ref()` sees fresh data immediately.

## Choosing an incremental strategy — the decision that matters most

```sql
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}
select * from {{ ref('stg_orders') }}
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

| Strategy | Behavior | Requires | Pick it when |
|---|---|---|---|
| `merge` (default with `unique_key`) | upsert: update matched, insert new | `unique_key` | Rows change after first load |
| `insert` | insert only keys not present | `unique_key` | Append-only data but you want key-level idempotency |
| `append` (default without `unique_key`) | blind append | — | Event streams where duplicates are impossible or acceptable |
| `append_if_unchanged` | append, but only if the table version hasn't moved since the model started; else fail | — | Your SQL already dedups against `{{ this }}` and the table is big |
| `microbatch` | delete+insert per `event_time` window | `event_time` config; rejects `unique_key` | dbt-driven backfills by time window |

First run, `--full-refresh`, or a missing table always overwrites.

**Steer big tables toward `append_if_unchanged`** (alias `append_if_unchanged`). A `merge` scans the target and joins on the
key — expensive on a large fact table, and the merge path splits the memory budget
between DuckDB and delta-rs. If the model SQL already excludes rows present in
`{{ this }}` (the classic "load only files not yet seen" pattern), that join is pure
waste. `append_if_unchanged` is a plain append (no target scan, full DuckDB memory budget) plus
a compare-and-swap: it commits only if the table version is unchanged since the model
started — captured BEFORE the model read `{{ this }}` — so a concurrent writer makes it
fail with `CommitFailedError` instead of letting a duplicate slip in. Re-running is
safe and idempotent: the SQL dedup excludes whatever the previous attempt loaded. The
canonical shape:

```sql
{{ config(materialized='incremental', incremental_strategy='append_if_unchanged') }}
select * from read_csv(getvariable('new_files'))
{% if is_incremental() %}
  where file not in (select distinct file from {{ this }})  -- dedup is YOUR job here
{% endif %}
```

**Concurrency expectations to set with the user:** merge/insert are safe — a
conflicting concurrent write makes the run fail loudly (never silently wrong), and
delta-rs is strict: ANY concurrent write to the same table aborts a merge, even
unrelated rows. So overlapping schedules or external writers on a merged table mean
retries, not corruption — design schedules accordingly. `append` has no guard at all by
design. Microbatch's delete+insert is not protected against concurrent writers — don't
run other writers against a microbatch table during its window.

## Merge config options

| Option | What it does |
|---|---|
| `unique_key` | column(s) to merge on (list for composite) |
| `merge_update_columns` / `merge_exclude_columns` | update only these / all but these on match |
| `merge_update_condition` / `merge_insert_condition` | per-clause predicates (use `target.` / `source.` or dbt's `DBT_INTERNAL_DEST/SOURCE`) |
| `incremental_predicates` | extra predicates AND-ed into the merge condition — use to prune partitions on big targets |
| `on_schema_change` | `ignore` (default) / `append_new_columns` / `fail`. `sync_all_columns` only ADDS — delta-rs cannot drop columns |
| `partition_by` | Delta partition column(s) |
| `merge_schema` | allow schema evolution on write |
| `merge_max_spill_size` | bytes cap on delta-rs's merge pool before it spills to disk; sensible cgroup-aware default; `0` disables |
| `merge_streamed_exec` | `true` streams a HUGE source instead of collecting it — at the cost of losing target-file pruning. Default `false` is right for the normal small-delta-into-big-table case |
| `storage_options` | per-model override |

duckrun validates merge config up front and **refuses** options it cannot honor (e.g.
dbt-duckdb's `merge_clauses`, `merge_returning_columns`) rather than silently running a
plain upsert. If a run fails with "duckrun cannot honor these merge configs", that is
deliberate — rewrite the model with the supported controls above, don't look for a
bypass.

## Sources

Existing Delta tables, CSVs, or Parquet files become dbt sources via the plugin:

```yaml
sources:
  - name: lake
    tables:
      - name: customers
        meta:
          plugin: duckrun
          location: 's3://bucket/lake/customers'   # bare dir => Delta
      - name: raw_events
        meta:
          plugin: duckrun
          location: 's3://bucket/landing/events.parquet'
      - name: ref_codes
        meta:
          plugin: duckrun
          location: './seeds_ext/codes.csv'        # read via read_csv_auto
```

Format is inferred from the extension (`.csv`/`.csv.gz`, `.parquet`/`.pq`, else Delta)
or forced with `meta.format`. `delta_table_path` still works (back-compat, forces
Delta). A source declares location + format ONLY — if a CSV needs hand-tuned parse
options (delimiters, types, skip rows), do that in a model with `read_csv(...)`,
not in the source.

## Python models

Supported for `table` and `incremental`. The function returns a relation/DataFrame;
config goes through `dbt.config(...)`:

```python
def model(dbt, session):
    dbt.config(materialized="incremental",
               unique_key=["source_type", "source_filename"],
               incremental_strategy="merge", schema="source")
    return session.sql("select ...")
```

Good fit for ingestion steps (downloads, unzipping, API calls) that end in a relation —
`session` is a DuckDB connection, so heavy lifting stays in SQL.

## Maintenance: already handled — do not add OPTIMIZE/VACUUM jobs

duckrun compacts and vacuums inline on every write: overwrite vacuums every run;
append/merge compact + vacuum once the table exceeds 100 files. Vacuum uses the safe
7-day retention so concurrent readers are never broken — superseded files linger up to
a week before reclaim, which is the intended trade. If a user asks "how do I schedule
OPTIMIZE for these tables", the answer is: you don't, it's built in.

## Contracts and tests

- `contract: {enforced: true}` with `not_null` column constraints IS enforced — a
  pre-write guard query fails the model before anything is written, leaving the prior
  table version untouched. `check` / `primary_key` / `foreign_key` are NOT enforceable
  (models are `delta_scan` views, not DDL tables).
- Regular dbt tests, `store_failures`, unit tests, and snapshots all work (inherited
  from dbt-duckdb).
- `dbt docs generate` works in a fresh process: model/column descriptions are persisted
  into the Delta table metadata and re-applied as comments when views are rebuilt.

## Troubleshooting

- **Weird import errors, version mismatches, or delta-log read failures right after
  installing in a Fabric notebook**: the kernel is still running the preinstalled
  `duckdb`/`deltalake` binaries. The fix is the install cell pattern above —
  `pip install duckrun --upgrade` followed immediately by
  `notebookutils.session.restartPython()`, before any import.
- **"schema does not exist" on OneLake** for `dbt test`/`show`/`docs`: the bearer token
  is missing or expired — discovery needs it before anything runs. Check
  `storage_options.bearer_token` resolves (env var set?) and that the token is fresh
  (OneLake tokens expire ~1h; long runs in CI should mint per-run).
- **`CommitFailedError` on append_if_unchanged or merge**: a concurrent writer touched the
  table mid-run. Not a bug — re-run; the strategies are idempotent by design. If it's
  chronic, two schedules overlap: stagger them.
- **dbt resolved incremental but "Delta table is not found at store time"**: duckrun
  refusing to overwrite a whole table with one increment after a transient storage
  error or a mid-run delete. Re-run; pass `--full-refresh` only if the table was
  deliberately deleted.
- **Merge OOM / slow on a big target**: add `incremental_predicates` on the partition
  column to prune; check the logged "merge spill cap" line; lower
  `merge_max_spill_size` in a tight container; consider whether the model qualifies for
  `append_if_unchanged` instead.
- **Huge merge SOURCE (not target)**: set `merge_streamed_exec: true` so the source
  streams instead of being collected.
- **NULLs in `unique_key`**: SQL `NULL != NULL` — merge cannot match null keys and you
  will get duplicates. Filter or coalesce key columns in the model.
- **A re-run "did nothing"**: correct merge behavior is idempotent — re-merging the
  same batch changes nothing. Verify with the watermark/dedup predicate, not row counts.
- **Don't read the Delta path with other tools mid-run** expecting the new version
  until dbt reports the model done; the view flips to the new version atomically at
  registration.

## Project shape that works well

```
models/
  staging/      -- views (or python models for ingestion); cheap, rebuilt every run
  dimensions/   -- table or incremental merge on a natural key
  marts/        -- big facts: incremental, append_if_unchanged where SQL dedups, else merge
```

Wire file-driven ingestion as: a small `merge` log model tracking which files exist →
fact models that `append_if_unchanged` only files not yet in `{{ this }}` (use a `pre_hook` with
`SET VARIABLE` to build the file list, as in the reference project). Keep
`partition_by` low-cardinality (month keys, not timestamps).
