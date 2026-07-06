"""
Bespoke duckrun unit-test matrix for INCREMENTAL models.

Why this exists (and what it really tests)
-------------------------------------------
A dbt unit test compiles the target model into a *self-contained* SELECT: every
input (`ref`, `source`, and `this`) is swapped for an ephemeral CTE built from the
fixture rows, `is_incremental()` returns the `overrides.macros.is_incremental`
value, and the result set is compared to `expect`. The model's *materialization*
(the merge / delete+insert / append write to Delta) is NOT executed by the unit
test itself. So the `incremental_strategy` does not, on its own, change a unit
test's pass/fail.

To make the strategy genuinely matter — and to stress duckrun's actual delta_rs
write path — every case here runs the model for REAL and then reads the resulting
Delta table back and asserts its rows. The shared upstream is *windowed* by a
`load` var so a second run delivers a genuine change (an existing key re-emitted
with a new value at a later timestamp, plus a brand-new key):

  * ``dbt run``                 — first load (var load=1): creates the model.
  * ``dbt build``               — unit tests + a first, idempotent incremental
                                  write (no new upstream rows yet); we assert the
                                  table is unchanged (catches double-writes).
  * ``dbt build --vars load=2`` — unit tests + the REAL incremental write of the
                                  change; we assert the final rows match what THIS
                                  strategy must produce.

That final assertion is what distinguishes the strategies — merge UPDATES the
re-emitted key, insert KEEPS the old value, delete+insert REPLACES it, and
append leaves a SECOND copy. Without reading the rows back these were
mere "does it crash" smoke tests; now wrong output fails the suite.

Matrix axes: incremental_strategy × is_incremental(true/false) × key shape
(none / single / composite) × incremental_predicates × key data type
(int / date / timestamp / timestamptz / decimal) × fixture format
(dict / csv / sql) × empty-`this`.

These are bespoke (NOT vendored from dbt-duckdb). If a combination fails, that is a
real duckrun finding — fix the adapter, never weaken the test.
"""
import pytest

from dbt.tests.util import relation_from_name, run_dbt


# Shared upstream, WINDOWED by a `load` var so a second run is a real change:
#   load 1 -> {id 1: region a, ts 2024-01-01, amount 10}                       (initial)
#   load 2 -> id 1 RE-EMITTED (region a, ts 2024-01-03, amount 99) + new id 2  (the change)
# Deduped to the latest load per id so each window yields one row per id. Its content drives the
# REAL build (the unit tests mock it); the columns/types are what the unit tests rely on.
STG_SQL = """
select id, region, ts, amount
from (
    select load, id, region, ts, amount,
           row_number() over (partition by id order by load desc) as rn
    from (
        values
          (1, 1, 'a', cast('2024-01-01' as date), 10),
          (2, 1, 'a', cast('2024-01-03' as date), 99),
          (2, 2, 'b', cast('2024-01-02' as date), 20)
    ) as t(load, id, region, ts, amount)
    where load <= {{ var('load', 1) }}
) ranked
where rn = 1
"""

# Real-output expectations after the windowed change (for the id/region/amount models).
_INIT_IRA = [("1", "a", "10")]                                  # after the idempotent re-run
_FINAL_UPSERT = [("1", "a", "99"), ("2", "b", "20")]           # merge / delete+insert / dedup / composite
_FINAL_INSERT = [("1", "a", "10"), ("2", "b", "20")]           # insert: existing key kept at old value
_FINAL_APPEND = [("1", "a", "10"), ("1", "a", "99"), ("2", "b", "20")]  # append: a second copy of id 1
_RESULT_IRA = "id, region, amount"


class _IncrBase:
    """Builds {stg_events, fct_events(+schema)} then exercises unit tests + REAL writes + output.

    A unit test that references `input: this` needs the model relation to already
    exist (dbt reads the model's column TYPES off it — see dbt-core's
    get_fixture_sql.sql). Real projects always have a prior build; we mirror that by
    running `dbt run` first (models only, no unit tests), so the Delta `this`
    relation exists before the unit tests resolve it. The two following `dbt build`s
    then run the unit tests AND the REAL incremental write for this strategy, and we
    read the Delta table back after each to assert the rows are exactly what the
    strategy must produce — so a green run means the whole combination held end to end.
    """

    model_files: dict = {}
    build_extra: list = []
    result_cols: str = _RESULT_IRA      # columns to read back from fct_events (schema-qualified at runtime)
    expected_initial: list = None       # rows after the idempotent re-run (first build)
    expected_final: list = None         # rows after the windowed change (second build)

    @pytest.fixture(scope="class")
    def models(self):
        m = {"stg_events.sql": STG_SQL}
        m.update(self.model_files)
        return m

    def _rows(self, project):
        rel = relation_from_name(project.adapter, "fct_events")
        rows = project.run_sql(f"select {self.result_cols} from {rel}", fetch="all")
        return sorted(tuple(str(c) for c in r) for r in rows)

    def test_unit_and_incremental(self, project):
        run_dbt(["run", *self.build_extra])                 # create the Delta model so `this` exists
        run_dbt(["build", *self.build_extra])               # unit tests + a first (idempotent) write
        assert self._rows(project) == sorted(self.expected_initial), "idempotent re-run changed the table"
        run_dbt(["build", *self.build_extra, "--vars", "{load: 2}"])  # unit tests + the REAL incr write
        assert self._rows(project) == sorted(self.expected_final), "incremental write produced wrong rows"


# ---------------------------------------------------------------------------
# 1. merge, single int key — is_incremental true / false / empty-`this`
# ---------------------------------------------------------------------------
_MERGE_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""

_MERGE_SCHEMA = """
unit_tests:
  - name: incr_true_filters_by_max_ts
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
          - {id: 2, region: 'b', ts: '2024-01-03', amount: 20}
      - input: this
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
    expect:
      rows:
          - {id: 2, region: 'b', ts: '2024-01-03', amount: 20}
  - name: incr_false_full_refresh
    model: fct_events
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
          - {id: 2, region: 'b', ts: '2024-01-03', amount: 20}
    expect:
      rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
          - {id: 2, region: 'b', ts: '2024-01-03', amount: 20}
  - name: incr_true_empty_this_takes_all
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
      - input: this
        rows: []
    expect:
      rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
"""


class TestIncrMergeIntKey(_IncrBase):
    model_files = {"fct_events.sql": _MERGE_MODEL, "schema.yml": _MERGE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT


# ---------------------------------------------------------------------------
# 2. merge, COMPOSITE key
# ---------------------------------------------------------------------------
_MERGE_COMPOSITE_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['id', 'region']) }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""

_COMPOSITE_SCHEMA = """
unit_tests:
  - name: incr_true_composite
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
          - {id: 1, region: 'b', ts: '2024-01-03', amount: 20}
      - input: this
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
    expect:
      rows:
          - {id: 1, region: 'b', ts: '2024-01-03', amount: 20}
  - name: incr_false_composite
    model: fct_events
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
    expect:
      rows:
          - {id: 1, region: 'a', ts: '2024-01-02', amount: 10}
"""


class TestIncrMergeCompositeKey(_IncrBase):
    model_files = {"fct_events.sql": _MERGE_COMPOSITE_MODEL, "schema.yml": _COMPOSITE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT


# ---------------------------------------------------------------------------
# 3. delete+insert WITH incremental_predicates
# ---------------------------------------------------------------------------
_DELINS_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    incremental_predicates=["ts >= cast('1900-01-01' as date)"]
) }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrDeleteInsertPredicates(_IncrBase):
    model_files = {"fct_events.sql": _DELINS_MODEL, "schema.yml": _MERGE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT


# ---------------------------------------------------------------------------
# 4. insert (insert-only new keys)
# ---------------------------------------------------------------------------
_INSERT_MODEL = """
{{ config(materialized='incremental', incremental_strategy='insert', unique_key='id') }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrInsertOnly(_IncrBase):
    model_files = {"fct_events.sql": _INSERT_MODEL, "schema.yml": _MERGE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_INSERT       # id 1 NOT updated; only the new key 2 is inserted


# ---------------------------------------------------------------------------
# 5. append (no unique_key)
# ---------------------------------------------------------------------------
_APPEND_MODEL = """
{{ config(materialized='incremental', incremental_strategy='append') }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrAppendNoKey(_IncrBase):
    model_files = {"fct_events.sql": _APPEND_MODEL, "schema.yml": _MERGE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_APPEND       # blind append: a SECOND copy of id 1 (no dedup)


# ---------------------------------------------------------------------------
# 7. dedup via QUALIFY/row_number (duplicate keys in `given` -> last wins)
# ---------------------------------------------------------------------------
_DEDUP_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, region, ts, amount from (
    select id, region, ts, amount,
           row_number() over (partition by id order by ts desc) as rn
    from {{ ref('stg_events') }}
) where rn = 1
"""

_DEDUP_SCHEMA = """
unit_tests:
  - name: dedup_last_per_id
    model: fct_events
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_events')
        rows:
          - {id: 1, region: 'a', ts: '2024-01-01', amount: 10}
          - {id: 1, region: 'a', ts: '2024-01-05', amount: 99}
          - {id: 2, region: 'b', ts: '2024-01-02', amount: 20}
    expect:
      rows:
          - {id: 1, region: 'a', ts: '2024-01-05', amount: 99}
          - {id: 2, region: 'b', ts: '2024-01-02', amount: 20}
"""


class TestIncrDedupQualify(_IncrBase):
    model_files = {"fct_events.sql": _DEDUP_MODEL, "schema.yml": _DEDUP_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT       # full re-scan + dedup, upserted -> id 1 updated


# ---------------------------------------------------------------------------
# 8. TIMESTAMP (ntz) incremental key — probes duckrun's timestamp_ntz handling
# ---------------------------------------------------------------------------
_STG_TS_NTZ = """
select id, event_ts, amount
from (
    select load, id, event_ts, amount,
           row_number() over (partition by id order by load desc) as rn
    from (
        values
          (1, 1, cast('2024-01-01 00:00:00' as timestamp), 10),
          (2, 1, cast('2024-01-02 12:30:00' as timestamp), 99),
          (2, 2, cast('2024-01-02 00:00:00' as timestamp), 20)
    ) as t(load, id, event_ts, amount)
    where load <= {{ var('load', 1) }}
) ranked
where rn = 1
"""

_TS_NTZ_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, event_ts, amount
from {{ ref('stg_ts') }}
{% if is_incremental() %}
where event_ts > (select coalesce(max(event_ts), cast('1900-01-01' as timestamp)) from {{ this }})
{% endif %}
"""

_TS_NTZ_SCHEMA = """
unit_tests:
  - name: ts_ntz_incremental
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_ts')
        rows:
          - {id: 1, event_ts: '2024-01-01 00:00:00', amount: 10}
          - {id: 2, event_ts: '2024-01-02 12:30:00', amount: 20}
      - input: this
        rows:
          - {id: 1, event_ts: '2024-01-01 00:00:00', amount: 10}
    expect:
      rows:
          - {id: 2, event_ts: '2024-01-02 12:30:00', amount: 20}
"""


class TestIncrTimestampNtzKey(_IncrBase):
    result_cols = "id, amount"
    expected_initial = [("1", "10")]
    expected_final = [("1", "99"), ("2", "20")]   # merge on a timestamp key: id 1 updated, id 2 added

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_ts.sql": _STG_TS_NTZ, "fct_events.sql": _TS_NTZ_MODEL, "schema.yml": _TS_NTZ_SCHEMA}


# ---------------------------------------------------------------------------
# 9. TIMESTAMPTZ incremental key
# ---------------------------------------------------------------------------
_STG_TS_TZ = """
select id, event_ts, amount
from (
    select load, id, event_ts, amount,
           row_number() over (partition by id order by load desc) as rn
    from (
        values
          (1, 1, cast('2024-01-01 00:00:00+00' as timestamptz), 10),
          (2, 1, cast('2024-01-02 12:30:00+00' as timestamptz), 99),
          (2, 2, cast('2024-01-02 00:00:00+00' as timestamptz), 20)
    ) as t(load, id, event_ts, amount)
    where load <= {{ var('load', 1) }}
) ranked
where rn = 1
"""

_TS_TZ_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, event_ts, amount
from {{ ref('stg_ts') }}
{% if is_incremental() %}
where event_ts > (select coalesce(max(event_ts), cast('1900-01-01 00:00:00+00' as timestamptz)) from {{ this }})
{% endif %}
"""

_TS_TZ_SCHEMA = """
unit_tests:
  - name: ts_tz_incremental
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_ts')
        rows:
          - {id: 1, event_ts: '2024-01-01 00:00:00+00', amount: 10}
          - {id: 2, event_ts: '2024-01-02 12:30:00+00', amount: 20}
      - input: this
        rows:
          - {id: 1, event_ts: '2024-01-01 00:00:00+00', amount: 10}
    expect:
      rows:
          - {id: 2, event_ts: '2024-01-02 12:30:00+00', amount: 20}
"""


class TestIncrTimestampTzKey(_IncrBase):
    result_cols = "id, amount"
    expected_initial = [("1", "10")]
    expected_final = [("1", "99"), ("2", "20")]   # merge on a timestamptz key: id 1 updated, id 2 added

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_ts.sql": _STG_TS_TZ, "fct_events.sql": _TS_TZ_MODEL, "schema.yml": _TS_TZ_SCHEMA}


# ---------------------------------------------------------------------------
# 10. DECIMAL measure — rounding/precision in fixture round-trip
# ---------------------------------------------------------------------------
_STG_DEC = """
select cast(1 as integer) as id,
       cast(3 as integer) as qty,
       cast(4.005 as decimal(10,3)) as price
"""

_DEC_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, cast(qty * price as decimal(12,2)) as revenue
from {{ ref('stg_dec') }}
"""

_DEC_SCHEMA = """
unit_tests:
  - name: decimal_revenue
    model: fct_events
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_dec')
        rows:
          - {id: 1, qty: 3, price: 4.005}
          - {id: 2, qty: 2, price: 9.99}
    expect:
      rows:
          - {id: 1, revenue: 12.02}
          - {id: 2, revenue: 19.98}
"""


class TestIncrDecimalMeasure(_IncrBase):
    # Static upstream (one row): the point is decimal precision surviving the real delta_rs
    # round-trip, so we assert the computed revenue rather than an incremental change.
    result_cols = "id, revenue"
    expected_initial = [("1", "12.02")]   # 3 * 4.005 = 12.015 -> decimal(12,2) -> 12.02
    expected_final = [("1", "12.02")]

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_dec.sql": _STG_DEC, "fct_events.sql": _DEC_MODEL, "schema.yml": _DEC_SCHEMA}


# ---------------------------------------------------------------------------
# 11. fixture format: CSV (inline)
# ---------------------------------------------------------------------------
_CSV_SCHEMA = """
unit_tests:
  - name: csv_format_incremental
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_events')
        format: csv
        rows: |
          id,region,ts,amount
          1,a,2024-01-02,10
          2,b,2024-01-03,20
      - input: this
        format: csv
        rows: |
          id,region,ts,amount
          1,a,2024-01-02,10
    expect:
      format: csv
      rows: |
        id,region,ts,amount
        2,b,2024-01-03,20
"""


class TestIncrFixtureCsv(_IncrBase):
    model_files = {"fct_events.sql": _MERGE_MODEL, "schema.yml": _CSV_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT


# ---------------------------------------------------------------------------
# 12. fixture format: SQL (inline)
# ---------------------------------------------------------------------------
_SQL_SCHEMA = """
unit_tests:
  - name: sql_format_incremental
    model: fct_events
    overrides:
      macros: {is_incremental: true}
    given:
      - input: ref('stg_events')
        format: sql
        rows: |
          select 1 as id, 'a' as region, cast('2024-01-02' as date) as ts, 10 as amount
          union all
          select 2 as id, 'b' as region, cast('2024-01-03' as date) as ts, 20 as amount
      - input: this
        format: sql
        rows: |
          select 1 as id, 'a' as region, cast('2024-01-02' as date) as ts, 10 as amount
    expect:
      format: sql
      rows: |
        select 2 as id, 'b' as region, cast('2024-01-03' as date) as ts, 20 as amount
"""


class TestIncrFixtureSql(_IncrBase):
    model_files = {"fct_events.sql": _MERGE_MODEL, "schema.yml": _SQL_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT


# ---------------------------------------------------------------------------
# 13. microbatch — event_time windowed incremental (highest-risk combination)
# ---------------------------------------------------------------------------
_STG_MB = """
{{ config(materialized='view', event_time='ts') }}
select 1 as id, cast('2024-01-01' as date) as ts, 10 as amount
union all
select 2 as id, cast('2024-01-02' as date) as ts, 20 as amount
"""

_MB_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='ts',
    batch_size='day',
    begin='2024-01-01',
    lookback=0
) }}
select id, ts, amount
from {{ ref('stg_mb') }}
"""

_MB_SCHEMA = """
unit_tests:
  - name: microbatch_unit
    model: fct_events
    given:
      - input: ref('stg_mb')
        rows:
          - {id: 1, ts: '2024-01-01', amount: 10}
          - {id: 2, ts: '2024-01-02', amount: 20}
    expect:
      rows:
          - {id: 1, ts: '2024-01-01', amount: 10}
          - {id: 2, ts: '2024-01-02', amount: 20}
"""


class TestIncrMicrobatch(_IncrBase):
    # Microbatch needs an event-time window; pin it so the batch set is deterministic. Two daily
    # batches (id 1 on day 1, id 2 on day 2) must both land in the real table. (Deep batching
    # behaviour — reprocess / new-batch / lookback — lives in test_incremental_microbatch.py.)
    build_extra = ["--event-time-start", "2024-01-01", "--event-time-end", "2024-01-03"]
    result_cols = "id, amount"
    expected_initial = [("1", "10"), ("2", "20")]
    expected_final = [("1", "10"), ("2", "20")]

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_mb.sql": _STG_MB, "fct_events.sql": _MB_MODEL, "schema.yml": _MB_SCHEMA}


# ---------------------------------------------------------------------------
# 14. PARTITIONED incremental — merge onto a partition_by table (delta_rs partitioned write path)
# ---------------------------------------------------------------------------
# duckrun supports partition_by (engine wires it through every write path) but nothing else in the
# suite exercises it. A partitioned merge stresses a distinct delta_rs code path: the cold run
# CREATES a Hive-partitioned table, and the later MERGE must upsert correctly across partitions.
_PARTITIONED_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by=['region']
) }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrPartitioned(_IncrBase):
    model_files = {"fct_events.sql": _PARTITIONED_MODEL, "schema.yml": _MERGE_SCHEMA}
    expected_initial = _INIT_IRA
    expected_final = _FINAL_UPSERT       # same rows as plain merge; the point is it works partitioned


# ---------------------------------------------------------------------------
# 15. NULL round-trip — UPDATE a value to NULL + INSERT a row with NULL dim & measure
# ---------------------------------------------------------------------------
# delta_rs/arrow null handling is a real correctness trap. The change build updates id 1's amount
# from 10 to NULL (a MERGE ... SET col = NULL) and inserts id 2 with a NULL region and NULL amount;
# we assert the nulls survive the round-trip (read back as None).
_STG_NULL = """
select id, region, ts, amount
from (
    values
      (1, 1, 'a', cast('2024-01-01' as date), 10),
      (2, 1, 'a', cast('2024-01-03' as date), cast(null as integer)),
      (2, 2, cast(null as varchar), cast('2024-01-02' as date), cast(null as integer))
    ) as t(load, id, region, ts, amount)
where load <= {{ var('load', 1) }}
"""

_NULL_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, region, ts, amount
from {{ ref('stg_nulls') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""

_NULL_SCHEMA = """
unit_tests:
  - name: null_dim_and_measure_round_trip
    model: fct_events
    overrides:
      macros: {is_incremental: false}
    given:
      - input: ref('stg_nulls')
        rows:
          - {id: 1, region: 'a',  ts: '2024-01-01', amount: 10}
          - {id: 2, region: null, ts: '2024-01-02', amount: null}
    expect:
      rows:
          - {id: 1, region: 'a',  ts: '2024-01-01', amount: 10}
          - {id: 2, region: null, ts: '2024-01-02', amount: null}
"""


class TestIncrNullRoundTrip(_IncrBase):
    expected_initial = [("1", "a", "10")]
    expected_final = [("1", "a", "None"), ("2", "None", "None")]  # id 1 amount->NULL; id 2 all-null inserted

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_nulls.sql": _STG_NULL, "fct_events.sql": _NULL_MODEL, "schema.yml": _NULL_SCHEMA}


# ---------------------------------------------------------------------------
# 16. merge REJECTS a source with duplicate unique_key rows (a documented contract)
# ---------------------------------------------------------------------------
# incremental.sql promises merge "rejects duplicate source keys" (unlike delete+insert). Nothing
# verified it. The first load creates the table; the change batch then carries TWO rows for id 1,
# which must FAIL the build rather than silently pick one.
_STG_DUPS = """
select id, region, ts, amount
from (
    values
      (1, 1, 'a', cast('2024-01-01' as date), 10),
      (2, 1, 'a', cast('2024-01-03' as date), 99),
      (2, 1, 'a', cast('2024-01-04' as date), 98)
    ) as t(load, id, region, ts, amount)
where load <= {{ var('load', 1) }}
"""

_MERGE_DUP_MODEL = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id') }}
select id, region, ts, amount
from {{ ref('stg_dups') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrMergeDuplicateSourceRejected(_IncrBase):
    """merge must REJECT a source carrying duplicate unique_key rows. First load creates the table;
    a second load whose batch has two rows for id 1 must fail the build, not silently pick one."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_dups.sql": _STG_DUPS, "fct_events.sql": _MERGE_DUP_MODEL}

    def test_unit_and_incremental(self, project):
        run_dbt(["run"])                                              # load 1: single id 1, table created
        res = run_dbt(["build", "--vars", "{load: 2}"], expect_pass=False)  # batch has 2x id 1
        # ...and it must fail for the RIGHT reason: the duplicate-key cardinality guard, not some
        # unrelated error. (delta_rs would otherwise silently produce two rows for id 1.) Scan every
        # node's message — the failing one is fct_events, not necessarily results[0].
        msgs = " ".join((r.message or "") for r in res.results).lower()
        assert "unique" in msgs or "duplicate" in msgs, [r.message for r in res.results]
