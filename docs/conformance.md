# dbt adapter conformance

`tests/conformance/` runs the official dbt adapter test suite
([`dbt-tests-adapter`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-tests-adapter))
against duckrun ([`.github/workflows/conformance.yml`](../.github/workflows/conformance.yml)).
The card below is published to the job summary and regenerated on every push to `main`, so it
reflects the latest `main` — which may be ahead of the published PyPI release.

Every still-failing test in the card below falls into one of three categories:

- **delta-rs API gap** — the write the test needs isn't supported by `deltalake==1.5.0`, e.g.
  the `constraints` `correct_column_data_types` cases require writing a `TIMESTAMP`-without-timezone
  column (`timestampNtz`), a Delta writer feature we don't enable because it bumps the table
  protocol and can break DirectLake/older readers.
- **view-backed relation limit** — duckrun surfaces each Delta table as a `delta_scan` view, so
  tests that mutate the relation in place (several `incremental_microbatch` fixtures `UPDATE` the
  view; `changing_relation_type` swaps a table for a view) can't be satisfied without a physical
  table.
- **deliberate scope-out** — e.g. `TestCatalogRelationsDuckDB` forces a `type: duckdb` profile and
  exercises `get_catalog_relations`, which this pinned dbt-duckdb doesn't implement — outside the
  duckrun adapter entirely.

<!-- CONFORMANCE:START -->

## dbt adapter conformance — duckrun

```
┌────────────────────────────────────────────────────────┐
│ ✅ 120 passed   ❌ 10 failed   💥 0 errors   ⏭️ 5 skipped │
│ 135 total · 89% passing                                │
└────────────────────────────────────────────────────────┘
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
| `incremental_microbatch` | `██████████` 100% | 13 | 0 | 0 | 0 | 13 |
| `simple_snapshot` | `██████████` 100% | 6 | 0 | 0 | 0 | 6 |
| `store_test_failures` | `██████████` 100% | 1 | 0 | 0 | 0 | 1 |
| `unit_testing` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `basic` | `█████████░` 94% | 15 | 1 | 0 | 0 | 16 |
| `utils` | `█████████░` 88% | 28 | 0 | 0 | 4 | 32 |
| `constraints` | `████████░░` 82% | 14 | 3 | 0 | 0 | 17 |
| `incremental` | `████████░░` 81% | 21 | 5 | 0 | 0 | 26 |
| `persist_docs` | `████████░░` 80% | 4 | 0 | 0 | 1 | 5 |
| `changing_relation_type` | `░░░░░░░░░░` 0% | 0 | 1 | 0 | 0 | 1 |
| **Total** | `█████████░` **89%** | **120** | **10** | **0** | **5** | **135** |

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
| `merge_update_condition` / `merge_insert_condition` | ✅ | honored as delta_rs per-clause predicates (gate which rows update / insert) |
| `on_schema_change='append_new_columns'` | ✅ | new columns added via delta_rs schema evolution |
| `on_schema_change='fail'` | ✅ | raises if the model's columns drift from the table |
| `partition_by` | ✅ | Delta partition columns |
| `on_schema_change='sync_all_columns'` | ⚠️ | **add-only** — delta_rs can't drop columns |
| `delete+insert` | ✅ | intentional Spark-faithful **atomic MERGE**, snapshot-pinned (delta_rs has no two-commit delete+insert) |
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
<details><summary><b>incremental</b> — 5 not passing (21/26 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestIncrementalOnSchemaChange::test_run_incremental_sync_all_columns` | dbt_common.exceptions.base.DbtRuntimeError: Runtime Error Binder Error: Referenced column "field2" not found in FROM clause! Candidate bindings: "field1", "fiel |
| ❌ | `TestIncrementalOnSchemaChangeQuotingFalse::test__handle_identifier_quoting_config_false` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMerge::test_merge_with_set_expressions` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMerge::test_merge_custom_clauses` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalMergeValidation::test_ducklake_valid_single_update` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>constraints</b> — 3 not passing (14/17 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestTableConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestViewConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |
| ❌ | `TestIncrementalConstraintsColumnsEqual::test__constraints_correct_column_data_types` | AssertionError: dbt exit state did not match expected |

</details>
<details><summary><b>basic</b> — 1 not passing (15/16 pass)</summary>

| Outcome | Test | Message |
| --- | --- | --- |
| ❌ | `TestSimpleMaterializationsDuckDB::test_base` | AssertionError: dbt exit state did not match expected |

</details>

<!-- CONFORMANCE:END -->
