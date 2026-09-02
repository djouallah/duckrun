# dbt adapter conformance

`tests/conformance/` runs the official dbt adapter test suite
([`dbt-tests-adapter`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-tests-adapter))
against duckrun ([`cores.yml`](../.github/workflows/cores.yml), `conformance` job). The card is
regenerated on every push to `main`, so it may be ahead of the published PyPI release.

Every still-failing test falls into one of these categories:

- **No persistent views.** Delta has no view primitive, so a `materialized='view'` model is a
  transient DuckDB catalog view, and the tests that swap a model `table → view`
  (`TestSimpleMaterializationsDuckDB::test_base`, `changing_relation_type`) have nowhere durable to
  land. Rewriting Delta data on a materialization change is deliberately not done.
- **Deliberate rejection of silently-divergent merge configs.** `merge_on_using_columns` and a clause
  `action: error` have no delta-rs equivalent, so duckrun raises instead of running something else;
  `test_ducklake_valid_single_update` stays red on purpose. `merge_clauses` and
  `merge_update_set_expressions` are translated and pass.
- **delta-rs and DuckDB limits.** `sync_all_columns` needs column drops, which delta-rs cannot do
  (schema evolution is add-only); `QuotingFalse` expects a compile error for unquoted identifiers
  with spaces, which DuckDB permits.

<!-- CONFORMANCE:START -->

## dbt adapter conformance — duckrun

```
┌────────────────────────────────────────────────────────┐
│ ✅ 153 passed   ❌ 4 failed   💥 0 errors   ⏭️ 10 skipped │
│ 167 total · 92% passing                                │
└────────────────────────────────────────────────────────┘
```

### By suite

| Suite | Pass rate | ✅ | ❌ | 💥 | ⏭️ | Total |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `aliases` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `caching` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `concurrency` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `constraints` | `██████████` 100% | 19 | 0 | 0 | 0 | 19 |
| `drop_relation` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `empty` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `ephemeral` | `██████████` 100% | 3 | 0 | 0 | 0 | 3 |
| `fast_seed` | `██████████` 100% | 4 | 0 | 0 | 0 | 4 |
| `get_columns_in_relation` | `██████████` 100% | 2 | 0 | 0 | 0 | 2 |
| `incremental_microbatch` | `██████████` 100% | 14 | 0 | 0 | 0 | 14 |
| `simple_snapshot` | `██████████` 100% | 10 | 0 | 0 | 0 | 10 |
| `store_test_failures` | `██████████` 100% | 1 | 0 | 0 | 0 | 1 |
| `unit_testing` | `██████████` 100% | 5 | 0 | 0 | 0 | 5 |
| `unit_testing_incremental` | `██████████` 100% | 15 | 0 | 0 | 0 | 15 |
| `basic` | `█████████░` 94% | 15 | 1 | 0 | 0 | 16 |
| `utils` | `█████████░` 88% | 28 | 0 | 0 | 4 | 32 |
| `persist_docs` | `████████░░` 80% | 4 | 0 | 0 | 1 | 5 |
| `incremental` | `████████░░` 76% | 22 | 2 | 0 | 5 | 29 |
| `changing_relation_type` | `░░░░░░░░░░` 0% | 0 | 1 | 0 | 0 | 1 |
| **Total** | `█████████░` **92%** | **153** | **4** | **0** | **10** | **167** |

### Incremental / write support

| Capability | | Notes |
| --- | :-: | --- |
| `materialized='table'` (overwrite) | ✅ | full rewrite each run (delta_rs overwrite) |
| first run / `--full-refresh` | ✅ | overwrites |
| `append` | ✅ | append; auto-fenced when the model reads `{{ this }}` (else a blind append); default when no `unique_key` |
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
<details><summary><b>incremental</b> — 2 not passing (22/29 pass)</summary>

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
