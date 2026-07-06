# dbt adapter conformance

`tests/conformance/` runs the official dbt adapter test suite
([`dbt-tests-adapter`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-tests-adapter))
against duckrun ([`.github/workflows/cores.yml`](../.github/workflows/cores.yml), `conformance` job).
The card below is published to the job summary and regenerated on every push to `main`, so it
reflects the latest `main` — which may be ahead of the published PyPI release.

Every still-failing test in the card below falls into one of three categories:

- **no persistent views** — the open Delta format (and `deltalake==1.5.0`) has no *view* primitive;
  a view exists only in some engine's catalog, never on disk. duckrun's durable artifact is always a
  Delta *table*, so a `materialized='view'` model is just a transient DuckDB catalog view, and the
  tests that swap a model's materialization `table → view` (`TestSimpleMaterializationsDuckDB::test_base`,
  `changing_relation_type`) have no durable home to land in. Satisfying them would mean rewriting
  Delta data on a mere materialization change — deliberately not done.
- **deliberate rejection of silently-divergent merge configs** — `merge_clauses`,
  `merge_update_set_expressions`, and `merge_on_using_columns` are dbt-duckdb-specific and have no
  delta-rs equivalent. duckrun raises a clear error rather than accept them and quietly run a plain
  upsert (a silent-divergence data bug), so `test_merge_with_set_expressions`,
  `test_merge_custom_clauses`, and `test_ducklake_valid_single_update` stay red on purpose.
- **delta-rs capability limit** — `on_schema_change='sync_all_columns'` requires *dropping* columns,
  which delta-rs can't do (it's add-only), so duckrun's schema evolution is add-only; and
  `QuotingFalse` expects a hard compile error for unquoted identifiers with spaces, which DuckDB and
  delta-rs simply permit.

<!-- CONFORMANCE:START -->

## dbt adapter conformance — duckrun

```
┌───────────────────────────────────────────────────────┐
│ ✅ 145 passed   ❌ 4 failed   💥 0 errors   ⏭️ 5 skipped │
│ 154 total · 94% passing                               │
└───────────────────────────────────────────────────────┘
```

### By suite

| Suite | Pass rate | ✅ | ❌ | 💥 | ⏭️ | Total |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `aliases` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `caching` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `concurrency` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `constraints` | `██████████` 100% | 17 | 0 | 0 | 0 | 17 |
| `empty` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `ephemeral` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `fast_seed` | `██████████` 100% | 4 | 0 | 0 | 0 | 4 |
| `incremental_microbatch` | `██████████` 100% | 13 | 0 | 0 | 0 | 13 |
| `simple_snapshot` | `██████████` 100% | 8 | 0 | 0 | 0 | 8 |
| `store_test_failures` | `██████████` 100% | 1 | 0 | 0 | 0 | 1 |
| `unit_testing` | `██████████` 100% | 5 | 0 | 0 | 0 | 5 |
| `unit_testing_incremental` | `██████████` 100% | 15 | 0 | 0 | 0 | 15 |
| `basic` | `█████████░` 94% | 15 | 1 | 0 | 0 | 16 |
| `incremental` | `█████████░` 92% | 24 | 2 | 0 | 0 | 26 |
| `utils` | `█████████░` 88% | 28 | 0 | 0 | 4 | 32 |
| `persist_docs` | `████████░░` 80% | 4 | 0 | 0 | 1 | 5 |
| `changing_relation_type` | `░░░░░░░░░░` 0% | 0 | 1 | 0 | 0 | 1 |
| **Total** | `█████████░` **94%** | **145** | **4** | **0** | **5** | **154** |

### Incremental / write support

| Capability | | Notes |
| --- | :-: | --- |
| `materialized='table'` (overwrite) | ✅ | full rewrite each run (delta_rs overwrite) |
| first run / `--full-refresh` | ✅ | overwrites |
| `append` | ✅ | blind append; default when no `unique_key` |
| `append_if_unchanged` | ✅ | append only if the table version is unchanged since the model read it (else fail); cheap, no dedup scan |
| `merge` (upsert) | ✅ | update matched + insert new, on `unique_key`; default with `unique_key` |
| `insert` (insert-only) | ✅ | insert new keys only (idempotent / dedupe) |
| `merge_update_columns` | ✅ | update only the listed columns on match |
| `merge_exclude_columns` | ✅ | update every column except the listed ones |
| `incremental_predicates` | ✅ | AND-ed into the merge condition (merge strategy) |
| `merge_update_condition` / `merge_insert_condition` | ✅ | honored as delta_rs per-clause predicates (gate which rows update / insert) |
| `on_schema_change='append_new_columns'` | ✅ | new columns added via delta_rs schema evolution |
| `on_schema_change='fail'` | ✅ | raises if the model's columns drift from the table |
| `partition_by` | ✅ | Delta partition columns |
| `on_schema_change='sync_all_columns'` | ⚠️ | **add-only** — delta_rs can't drop columns |
| `delete+insert` | ✅ | true delete+insert (duplicate-tolerant): delete the matched keys, insert every incoming row, committed as one **fenced overwrite** pinned to the version read (delta_rs has no two-commit delete+insert) |
| `microbatch` strategy | ✅ | per-batch **atomic replaceWhere** on the `event_time` window (single Delta commit, snapshot-pinned) |
| `merge_clauses` / `merge_update_set_expressions` / `merge_on_using_columns` | ❌ | dbt-duckdb-specific, no delta_rs equivalent — **rejected** with a clear error, never silently ignored |
| model contracts — column name/type/count | ✅ | enforced via dbt's `assert_columns_equivalent` preflight before the write |
| constraints — `not null` | ✅ | pre-write guard on the staged rows; a null fails the run and leaves the prior table intact |
| constraints — `check` / `primary_key` / `foreign_key` | ❌ | not enforceable against a `delta_scan` view; declared but not checked |

### Not passing — details by suite

<details><summary><b>changing_relation_type</b> — 1 not passing (0/1 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestChangeRelationTypesDuckDB::test_changing_materialization_changes_relation_type` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>incremental</b> — 2 not passing (24/26 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestIncrementalOnSchemaChange::test_run_incremental_sync_all_columns` | dbt_common.exceptions.base.DbtRuntimeError: Runtime Error Binder Error: Referenced column "field2" not found in FROM clause! Candidate bindings: "field1", "fiel |
| ❌ | `TestIncrementalOnSchemaChangeQuotingFalse::test__handle_identifier_quoting_config_false` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>basic</b> — 1 not passing (15/16 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestSimpleMaterializationsDuckDB::test_base` | AssertionError: dbt exit state did not match expected |

</details>

<!-- CONFORMANCE:END -->
