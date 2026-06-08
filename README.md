<img src="https://raw.githubusercontent.com/djouallah/duckrun/main/duckrun.png" width="400" alt="duckrun">

[![PyPI version](https://badge.fury.io/py/duckrun.svg)](https://badge.fury.io/py/duckrun)

**duckrun** is a [dbt](https://www.getdbt.com/) adapter that runs your model SQL in
**DuckDB** and writes the results to **Delta Lake** using
[`delta_rs`](https://delta-io.github.io/delta-rs/) (the `deltalake` Python package).
duckrun itself is just glue — it owns none of the heavy lifting. The real work is done
by **DuckDB** (executes the SQL), **delta-rs** (writes the Delta table), **Arrow** (the
zero-copy (kind of) bridge that hands query results from DuckDB to delta-rs), and **dbt** (orchestrates
the DAG). DuckDB is here for convenience as the SQL engine; the materialization is all
delta-rs and Arrow.

It is a thin wrapper around [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb). You
keep everything dbt-duckdb gives you — views, seeds, sources, tests, snapshots, the full
plugin ecosystem — and gain one extra thing: a Delta-backed `table` / `incremental`
materialization that writes real Delta tables

The design rationale — why delta_rs and not DuckDB's native Delta writer, why Delta and not
Iceberg, why a separate adapter — lives in [design_document.md](design_document.md).

> ### 0.3.0 is a breaking change
>
> Versions ≤ 0.2.x of `duckrun` were a Microsoft Fabric / OneLake helper library. From
> **0.3.0** onward `duckrun` is a dbt adapter. Need the old library? Pin
> `pip install "duckrun<0.3"`, or use the
> [`legacy`](https://github.com/djouallah/duckrun/tree/legacy) branch.

## How it fits together

DuckDB is a great query engine, Delta Lake is a great open table format, and dbt is the
right tool to orchestrate the DAG. duckrun wires the three together:

> **DuckDB executes · delta_rs materializes · dbt orchestrates.**

## Install

```bash
pip install duckrun
```

That single install pulls in `dbt-duckdb` (and therefore `duckdb`) plus `deltalake`.

> **On Microsoft Fabric notebooks**, `deltalake` is already imported by the runtime, so a
> `pip install`/upgrade won't take effect until you restart the Python session. After
> installing, run:
>
> ```python
> %pip install -q duckrun --upgrade
> notebookutils.session.restartPython()
> ```
>
> duckrun pins `deltalake==1.5.0` (the first release with MERGE disk-spill, which bounds
> merge memory — see `merge_max_spill_size`), so the restart is what actually swaps Fabric's
> preinstalled `deltalake` for the pinned one.

## Configure your profile

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: duckrun
      # No `threads:` needed — duckrun always runs single-threaded.
      # DuckDB runs in-memory by default — the Delta tables are the only state.
      # Default Delta location for models that don't set config(location=...).
      root_path: './warehouse'   # local path, or abfss://.../Tables, s3://..., gs://...
      # storage_options: {}      # passed through to deltalake for remote stores
```

Persisted models are written to `<root_path>/<schema>/<model>` (e.g.
`./warehouse/dbo/orders`), or to an explicit `config(location=...)`.

### Remote stores (Fabric OneLake / ADLS / S3 / GCS)

Point `root_path` at the warehouse location and pass credentials through
`storage_options` — these flow straight to deltalake for writes and merges.

If `storage_options` carries a `bearer_token` (or `token` / `access_token`), the adapter
also auto-creates a matching DuckDB Azure secret, so `delta_scan()` reads work with no
extra config. In a notebook where the storage secret is already provided to DuckDB, you
can leave `storage_options` empty.

```yaml
    onelake:
      type: duckrun
      schema: dbo
      root_path: "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables"
      storage_options:
        # az account get-access-token --resource https://storage.azure.com
        bearer_token: "{{ env_var('ONELAKE_TOKEN') }}"
```

Verified end-to-end against real Fabric OneLake: `table` overwrite, `incremental` merge,
and `delta_scan` reads / tests.

## Materializations

| materialized      | backed by                | notes                                                                 |
|-------------------|--------------------------|-----------------------------------------------------------------------|
| **`table`**       | Delta (overwrite)        | DuckDB runs the SQL; delta_rs writes the table fresh each run.         |
| **`incremental`** | Delta (merge / append)   | First run overwrites; later runs apply `incremental_strategy`.         |
| `view`            | in-memory DuckDB         | Ephemeral staging within a run (inherited from dbt-duckdb).            |
| `seed`            | in-memory DuckDB         | CSV fixtures (inherited from dbt-duckdb).                              |
| `delta`           | Delta                    | Alias for `table`; honors `incremental=true`. Kept for convenience.   |

The persisted materializations (`table`, `incremental`, `delta`) register a `delta_scan`
view over the new Delta table, so downstream `ref()` works.

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
| `insert`                           | insert only new keys (idempotent append)  | `unique_key` |
| `append` (default without `unique_key`) | blind append                          | —            |
| `safeappend`                       | append, but only if the table is unchanged since the model read it (else fail) — cheap, no dedup scan | — |

### `safeappend`

A cheap append for the common "load only what's new" pattern — when your model SQL **already
guarantees no duplicates** and you don't want to pay for a merge.

```sql
{{ config(materialized='incremental', incremental_strategy='safeappend') }}

select * from read_csv(getvariable('new_files'))
{% if is_incremental() %}
  -- the dedup is your SQL's job: only load files not already in the table
  where file not in (select distinct file from {{ this }})
{% endif %}
```

**Why, reason 1 — performance.** `merge` / `insert` scan the target and join on the key to find
what's new — expensive on a large table. If the SQL above already excludes rows that are present,
that work is redundant. `safeappend` is a plain append: **no target data scan, no key join, and
DuckDB keeps its full memory budget** (the merge memory split is never applied — same as `append`
/ `overwrite`). The only thing it reads from the target is one Delta log entry to get the version.

**Why, reason 2 — a concurrency guard a blind `append` doesn't have.** Because the dedup is done
in SQL against `{{ this }}`, a plain `append` is unsafe under concurrency: if another writer
commits between your `not in (... from {{ this }})` read and your write, the file it added isn't
excluded and you get a duplicate. `safeappend` closes that gap — it commits **only if the table
version is unchanged since the model started** (captured *before* it reads `{{ this }}`); if
anything committed in between, it fails with `CommitFailedError` so the run re-runs against the new
state. No duplicate slips in.

This is **optimistic concurrency control** — it never locks the table or blocks other writers; it
appends, then validates at commit with a compare-and-swap on the version and aborts on a mismatch.
Its policy is the strictest of the strategies (abort on *any* concurrent change, rather than
reconcile like `merge` or auto-rebase like `append`), but the mechanism is optimistic, not
pessimistic. Re-running is safe and idempotent: the SQL dedup simply excludes whatever the previous
attempt already loaded.

First run (or `--full-refresh`, or a missing table) overwrites to create the table; `safeappend`
applies on later runs. A real example is the AEMO
[`fct_scada`](integration_tests/models/marts/fct_scada.sql) model — the project's largest table,
which loads only not-yet-seen files and so uses `safeappend` instead of an expensive merge.

### Config options (`table` / `incremental` / `delta`)

| option                  | description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `location`              | Delta path. Defaults to `<root_path>/<schema>/<id>`.                        |
| `incremental_strategy`  | `merge` \| `insert` \| `append` \| `safeappend` (incremental only).          |
| `unique_key`            | column(s) to merge on.                                                       |
| `merge_update_columns`  | merge: update only these columns on match (others untouched).               |
| `merge_exclude_columns` | merge: update all columns **except** these on match.                        |
| `merge_max_spill_size`  | merge: memory ceiling in **bytes** for delta_rs's merge pool (not a disk budget). Defaults to ~60% of the **effective** limit — `min(physical RAM, container/cgroup limit, currently-free RAM)` — beyond which delta_rs spills the merge join to disk (like DuckDB's `memory_limit`). The other big consumer, DuckDB itself, is separately pinned to ~30% of the same effective limit on the merge path (it produces the merge source in the same process), so the two budgets sum under the cgroup cap; both log their chosen value at run start. Set `0` to disable. It bounds the merge pool, *not* the whole process (the Arrow source, read buffers, and spill-file page cache sit outside it), so on a tight container with a huge source the total can still exceed the cap — lower it if needed. A cap below the join's minimum (~hundreds of MB) makes the merge raise `Resources exhausted` instead of spilling. Requires deltalake 1.5.0 (pinned). |
| `incremental_predicates`| merge: extra predicates AND-ed into the merge condition (use `target.`/`source.`, or dbt's `DBT_INTERNAL_DEST`/`DBT_INTERNAL_SOURCE`). |
| `on_schema_change`      | `ignore` (default) \| `append_new_columns` \| `fail`. (`sync_all_columns` only *adds* — delta_rs can't drop columns.) |
| `partition_by`          | Delta partition column(s).                                                   |
| `merge_schema`          | allow schema evolution on write.                                            |
| `storage_options`       | per-model override forwarded to deltalake.                                   |

## Reading existing Delta tables as sources

```yaml
sources:
  - name: lake
    tables:
      - name: customers
        meta:
          plugin: duckrun
          delta_table_path: 's3://bucket/lake/customers'
```

## How it works

1. dbt compiles your model SQL.
2. The materialization stages it as a DuckDB view.
3. A `dbt-duckdb` plugin (a `store()` hook) hands that relation to deltalake over the
   Arrow C-stream interface (`__arrow_c_stream__`) — no pyarrow required — which
   `write_deltalake` / `DeltaTable.merge` consume natively.
4. The model relation becomes a `delta_scan` view over the new Delta table.

The adapter is a thin subclass of dbt-duckdb declaring `dependencies=['duckdb']`, so
`view`, `seed`, tests, and the rest are inherited directly; only `table` and
`incremental` are overridden to write Delta.

## Table maintenance (compaction & vacuum)

**duckrun maintains your Delta tables automatically — no configuration, no scheduled job, no
separate `OPTIMIZE`/`VACUUM` run to remember.** It happens inline on every write.

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

Every `vacuum` uses delta_rs's **safe default retention (7 days / 168h)**, so files a
concurrent reader might still be reading are never deleted out from under it. The trade-off
is that a superseded file version lingers for the retention window before it can be
reclaimed — duckrun favors read-safety over immediate disk savings.

## Development

The `integration_tests/` directory is a small dbt project exercised by CI
(`.github/workflows/integration.yml`): `dbt build` runs twice against a local Delta
`./warehouse` — a seed, a `view`, a `table`, and an `incremental` model — where the
second build exercises the incremental merge. Verified to run with **pyarrow not
installed**, on the minimum supported `duckdb` and `deltalake`.

`tests/conformance/` runs the official dbt adapter test suite
([`dbt-tests-adapter`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-tests-adapter))
against duckrun (`.github/workflows/conformance.yml`). The results card is published to the
job summary and rendered live into this README below — regenerated on every push to `main`.

## Conformance results

_The conformance and MERGE scorecards below are regenerated on every push to `main`, so they
reflect the latest `main` — which may be ahead of the published PyPI release._

<!-- CONFORMANCE:START -->

## dbt adapter conformance — duckrun

```
┌───────────────────────────────────────────────────────┐
│ ✅ 92 passed   ❌ 38 failed   💥 0 errors   ⏭️ 5 skipped │
│ 135 total · 68% passing                               │
└───────────────────────────────────────────────────────┘
```

### By suite

| Suite | Pass rate | ✅ | ❌ | 💥 | ⏭️ | Total |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `aliases` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `caching` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `concurrency` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `empty` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `ephemeral` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `fast_seed` | `██████████` 100% | 4 | 0 | 0 | 0 | 4 |
| `simple_snapshot` | `██████████` 100% | 6 | 0 | 0 | 0 | 6 |
| `store_test_failures` | `██████████` 100% | 1 | 0 | 0 | 0 | 1 |
| `unit_testing` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `utils` | `█████████░` 88% | 28 | 0 | 0 | 4 | 32 |
| `basic` | `████████░░` 81% | 13 | 3 | 0 | 0 | 16 |
| `incremental` | `█████░░░░░` 54% | 14 | 12 | 0 | 0 | 26 |
| `incremental_microbatch` | `█████░░░░░` 54% | 7 | 6 | 0 | 0 | 13 |
| `constraints` | `██░░░░░░░░` 24% | 4 | 13 | 0 | 0 | 17 |
| `persist_docs` | `██░░░░░░░░` 20% | 1 | 3 | 0 | 1 | 5 |
| `changing_relation_type` | `░░░░░░░░░░` 0% | 0 | 1 | 0 | 0 | 1 |
| **Total** | `███████░░░` **68%** | **92** | **38** | **0** | **5** | **135** |

### Incremental / write support

| Capability | | Notes |
| --- | :-: | --- |
| `materialized='table'` (overwrite) | ✅ | full rewrite each run (delta_rs overwrite) |
| first run / `--full-refresh` | ✅ | overwrites |
| `append` | ✅ | blind append; default when no `unique_key` |
| `safeappend` | ✅ | append only if the table version is unchanged since the model read it (else fail); cheap, no dedup scan |
| `merge` (upsert) | ✅ | update matched + insert new, on `unique_key`; default with `unique_key` |
| `insert` (insert-only) | ✅ | insert new keys only (idempotent / dedupe) |
| `merge_update_columns` | ✅ | update only the listed columns on match |
| `merge_exclude_columns` | ✅ | update every column except the listed ones |
| `incremental_predicates` | ✅ | AND-ed into the merge condition (merge strategy) |
| `on_schema_change='append_new_columns'` | ✅ | new columns added via delta_rs schema evolution |
| `on_schema_change='fail'` | ✅ | raises if the model's columns drift from the table |
| `partition_by` | ✅ | Delta partition columns |
| `on_schema_change='sync_all_columns'` | ⚠️ | **add-only** — delta_rs can't drop columns |
| `delete+insert` | ⚠️ | mapped to `merge` (not exact delete+insert semantics) |
| `microbatch` strategy | ✅ | per-batch delete+insert on the `event_time` window (delta_rs delete + append) |
| advanced merge clauses (conditions / set / returning / custom) | ❌ | dbt-duckdb-specific, not implemented |
| constraints / DDL enforcement | ❌ | models are `delta_scan` views, not physical tables |

### Not passing — details by suite

<details><summary><b>changing_relation_type</b> — 1 not passing (0/1 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestChangeRelationTypesDuckDB::test_changing_materialization_changes_relation_type` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>persist_docs</b> — 3 not passing (1/5 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestPersistDocs::test_has_comments_pglike` | AttributeError: 'NoneType' object has no attribute 'startswith' |
| ❌ | `TestPersistDocsColumnMissing::test_missing_column` | AttributeError: 'NoneType' object has no attribute 'startswith' |
| ❌ | `TestPersistDocsCommentOnQuotedColumn::test_quoted_column_comments` | AttributeError: 'NoneType' object has no attribute 'startswith' |

</details>
<details><summary><b>constraints</b> — 13 not passing (4/17 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestTableConstraintsColumnsEqual::test__constraints_wrong_column_names` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestTableConstraintsColumnsEqual::test__constraints_wrong_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestTableConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestViewConstraintsColumnsEqual::test__constraints_wrong_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestViewConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalConstraintsColumnsEqual::test__constraints_wrong_column_names` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalConstraintsColumnsEqual::test__constraints_wrong_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestTableConstraintsRuntimeDdlEnforcement::test__constraints_ddl` | AssertionError: assert 'create table... model_subq);' == 'create or re...l_identifier>' - create or replace view <model_identifier> as select * from <model_iden |
| ❌ | `TestTableConstraintsRollback::test__constraints_enforcement_rollback` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalConstraintsRuntimeDdlEnforcement::test__constraints_ddl` | AssertionError: assert 'create table... model_subq);' == 'create or re...l_identifier>' - create or replace view <model_identifier> as select * from <model_iden |
| ❌ | `TestIncrementalConstraintsRollback::test__constraints_enforcement_rollback` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestModelConstraintsRuntimeEnforcement::test__model_constraints_ddl` | AssertionError: assert 'create table... model_subq);' == 'create or re...l_identifier>' - create or replace view <model_identifier> as select * from <model_iden |

</details>
<details><summary><b>incremental</b> — 12 not passing (14/26 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestIncrementalPredicates::test__incremental_predicates` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalOnSchemaChange::test_run_incremental_sync_all_columns` | dbt_common.exceptions.base.DbtRuntimeError: Runtime Error Binder Error: Referenced column "field2" not found in FROM clause! Candidate bindings: "field1", "fiel |
| ❌ | `TestIncrementalOnSchemaChangeQuotingFalse::test__handle_identifier_quoting_config_false` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMerge::test_merge_with_set_expressions` | assert 1 == 2 |
| ❌ | `TestIncrementalMergeValidation::test_invalid_condition_type` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_invalid_columns_type` | AssertionError: assert 'merge_update_columns must be a list' in 'Generic DeltaTable error: External error: Generic DeltaTable error: Schema error: No field name |
| ❌ | `TestIncrementalMergeValidation::test_invalid_set_expressions_type` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_conflicting_configs` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_invalid_clauses_type` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_empty_clauses` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_invalid_clause_list` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_invalid_clause_element` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>incremental_microbatch</b> — 6 not passing (7/13 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestMicrobatchScenarios::test_microbatch_inserts_new_batches` | _duckdb.CatalogException: Catalog Error: microbatch_exec_input is not an table |
| ❌ | `TestMicrobatchScenarios::test_microbatch_supports_date_event_time` | _duckdb.CatalogException: Catalog Error: microbatch_event_date_input is not an table |
| ❌ | `TestMicrobatchScenarios::test_microbatch_supports_hour_batch_size` | _duckdb.CatalogException: Catalog Error: microbatch_batch_hour_input is not an table |
| ❌ | `TestMicrobatchScenarios::test_microbatch_supports_month_batch_size` | _duckdb.CatalogException: Catalog Error: microbatch_batch_month_input is not an table |
| ❌ | `TestMicrobatchScenarios::test_microbatch_reprocesses_existing_batch` | _duckdb.BinderException: Binder Error: Can only update base table |
| ❌ | `TestMicrobatchScenarios::test_microbatch_lookback_reprocesses_previous_batches` | _duckdb.BinderException: Binder Error: Can only update base table |

</details>
<details><summary><b>basic</b> — 3 not passing (13/16 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestSimpleMaterializationsDuckDB::test_base` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestDocsGenReferencesDuckDB::test_references` | AssertionError: Key 'metadata' in 'model.test.ephemeral_summary' did not match assert {'comment': N...r': None, ...} == {'comment': N...r': None, ...} Omitting  |
| ❌ | `TestCatalogRelationsDuckDB::test_get_catalog_relations` | AssertionError: dbt exit state did not match expected |

</details>

<!-- CONFORMANCE:END -->

## Incremental MERGE benchmark

The [`merge-spill`](.github/workflows/merge.yml) workflow builds a large TPCH `lineitem`
fact table (the release gate runs scale factor **20**, ~120M rows) and runs four merge
shapes against it — mixed upsert, insert-only, update-only, and an idempotent re-merge —
plus a plain `append`, `safeappend`, and `overwrite` of the same batch for comparison, on a
single machine with duckrun's shipping memory defaults (per-merge DuckDB `memory_limit` +
delta_rs `max_spill_size` + target pruning). It proves the merges stay within the runner's
RAM and apply every UPDATE/INSERT correctly, and lets you compare a MERGE's cost against a
plain write of the same batch. It gates every release; the latest scorecard is rendered live below.

<!-- MERGE:START -->

## 🔀 Incremental MERGE test — duckrun on Delta Lake

**What this checks:** that duckrun MERGEs incremental batches into a large Delta *fact* table on one machine — across four merge shapes — applying UPDATEs and INSERTs correctly without being OOM-killed, and how the same batch compares against a plain `append` / `safeappend` / `overwrite` on that same table (which never scan the target).

### Setup (the inputs)
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.1 &middot; delta_rs 1.5.0 |
| Target fact table | TPCH `lineitem`, scale factor **20.0** → **119,994,608 rows** |
| Primary key (merge on) | `(l_orderkey, l_linenumber)` |
| Effective memory | 14894 MB (runner RAM, no artificial limit) |
| DuckDB `memory_limit` | 12.4 GiB — set by duckrun (cgroup-aware) |
| Merge spill cap | 8936 MB — delta_rs `max_spill_size` |

### The operations (run in order, on the same growing table)
1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE (randomized measures), ~20% key-shifted → INSERT. _Expect:_ rows grow by the inserts; updated rows carry the new measures.
2. **Insert-only (~5% sample):** key-shifted past the max key so nothing matches, stamped with a future `l_shipdate` (2035). _Expect:_ every row INSERTed; exactly that many rows carry the 2035 date.
3. **Update-only (~5% sample):** existing keys, randomized measures, no key shift → 100% match. _Expect:_ row count unchanged; rows carry the new measures.
4. **Idempotent re-merge:** re-run scenario 3's exact batch. _Expect:_ a correct MERGE is idempotent — nothing changes (same row count, same values).
5. **Append (no merge):** the same batch appended to the table. _Expect:_ rows grow by the batch — far faster than a MERGE, because an append only lands files (no target scan/join) and DuckDB streams the source.
6. **Safeappend (no merge):** the same batch via `safeappend` — a plain append that commits only if the table version is unchanged since it was read (it is here). _Expect:_ same cheap append, now version-guarded against concurrent writers.
7. **Overwrite (no merge):** the same batch overwriting the table. _Expect:_ the table is replaced by the batch — also far faster than a MERGE (no target scan/join).

### Results (row counts in millions)
| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Time |
|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|
| Mixed upsert | 1.2M | 1.0M | 0.2M | 120.0M | 120.2M | 120.2M | ✅ | ✅ | 169.5s |
| Insert-only (future shipdate) | 6.0M | 0.0M | 6.0M | 120.2M | 126.2M | 126.2M | ✅ | ✅ | 5.5s |
| Update-only (100% match) | 6.0M | 6.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 181.3s |
| Idempotent re-merge | 6.0M | 6.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 197.8s |
| Append (no merge) | 6.0M | 0.0M | 6.0M | 126.2M | 132.2M | 132.2M | ✅ | ✅ | 4.2s |
| Safeappend (no merge) | 6.0M | 0.0M | 6.0M | 132.2M | 138.2M | 138.2M | ✅ | ✅ | 4.2s |
| Overwrite (no merge) | 6.0M | 0.0M | 6.0M | 138.2M | 6.0M | 6.0M | ✅ | ✅ | 4.1s |

_The last three rows are the same batch as a plain `append` / `safeappend` / `overwrite` — compare their time against the merges above to see the cost a MERGE pays to scan & join the target._

**Result: ✅ all operations correct.** Target grew to **126,237,275 rows** across the merges, peak memory **6,398 MB** — duckrun stayed within the runner's RAM and every update/insert landed as expected.

<!-- MERGE:END -->

## License

MIT
