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
write path — every case here runs `dbt build` **twice**:
  * 1st build: creates the upstream model, runs the unit tests, creates the
    incremental model (first run = overwrite).
  * 2nd build: re-runs the unit tests AND performs the *real* incremental write
    for that strategy (merge/insert/delete+insert/append/safeappend/microbatch).
`run_dbt(["build"])` raises if any node (unit test, model, or data test) fails, so
a green call means the whole combination held end to end.

Matrix axes: incremental_strategy × is_incremental(true/false) × key shape
(none / single / composite) × incremental_predicates × key data type
(int / date / timestamp / timestamptz / decimal) × fixture format
(dict / csv / sql) × empty-`this` (simulated first load).

These are bespoke (NOT vendored from dbt-duckdb). If a combination fails, that is a
real duckrun finding — fix the adapter, never weaken the test.
"""
import pytest

from dbt.tests.util import run_dbt


# A tiny upstream model. Its content is only used by `dbt build` (the unit tests
# mock it); the columns/types are what matter. Safe types only (int/varchar/date).
STG_SQL = """
select
    cast(1 as integer)         as id,
    cast('a' as varchar)       as region,
    cast('2024-01-01' as date) as ts,
    cast(10 as integer)        as amount
"""


class _IncrBase:
    """Builds {stg_events, fct_events(+schema)} then exercises unit tests + writes.

    A unit test that references `input: this` needs the model relation to already
    exist (dbt reads the model's column TYPES off it — see dbt-core's
    get_fixture_sql.sql). Real projects always have a prior build; we mirror that by
    running `dbt run` first (models only, no unit tests), so the Delta `this`
    relation exists before the unit tests resolve it. The two following `dbt build`s
    then run the unit tests AND the REAL incremental write for this strategy (1st =
    already-created so it takes the incremental path; 2nd = a second incremental
    write), so a green run means the whole combination held end to end.
    """

    model_files: dict = {}
    build_extra: list = []

    @pytest.fixture(scope="class")
    def models(self):
        m = {"stg_events.sql": STG_SQL}
        m.update(self.model_files)
        return m

    def test_unit_and_incremental(self, project):
        run_dbt(["run", *self.build_extra])      # create the Delta model so `this` exists
        run_dbt(["build", *self.build_extra])    # unit tests (this resolves) + incremental write
        run_dbt(["build", *self.build_extra])    # again: a second real incremental write


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


# ---------------------------------------------------------------------------
# 6. safeappend (CAS append)
# ---------------------------------------------------------------------------
_SAFEAPPEND_MODEL = """
{{ config(materialized='incremental', incremental_strategy='safeappend') }}
select id, region, ts, amount
from {{ ref('stg_events') }}
{% if is_incremental() %}
where ts > (select coalesce(max(ts), cast('1900-01-01' as date)) from {{ this }})
{% endif %}
"""


class TestIncrSafeAppend(_IncrBase):
    model_files = {"fct_events.sql": _SAFEAPPEND_MODEL, "schema.yml": _MERGE_SCHEMA}


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


# ---------------------------------------------------------------------------
# 8. TIMESTAMP (ntz) incremental key — probes duckrun's timestamp_ntz handling
# ---------------------------------------------------------------------------
_STG_TS_NTZ = """
select cast(1 as integer) as id,
       cast('2024-01-01 00:00:00' as timestamp) as event_ts,
       cast(10 as integer) as amount
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
    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_ts.sql": _STG_TS_NTZ, "fct_events.sql": _TS_NTZ_MODEL, "schema.yml": _TS_NTZ_SCHEMA}


# ---------------------------------------------------------------------------
# 9. TIMESTAMPTZ incremental key
# ---------------------------------------------------------------------------
_STG_TS_TZ = """
select cast(1 as integer) as id,
       cast('2024-01-01 00:00:00+00' as timestamptz) as event_ts,
       cast(10 as integer) as amount
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


# ---------------------------------------------------------------------------
# 13. microbatch — event_time windowed incremental (highest-risk combination)
# ---------------------------------------------------------------------------
_STG_MB = """
{{ config(materialized='view', event_time='ts') }}
select cast(1 as integer) as id,
       cast('2024-01-01' as date) as ts,
       cast(10 as integer) as amount
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
    # Microbatch needs an event-time window; pin it so the batch set is deterministic.
    build_extra = ["--event-time-start", "2024-01-01", "--event-time-end", "2024-01-03"]

    @pytest.fixture(scope="class")
    def models(self):
        return {"stg_mb.sql": _STG_MB, "fct_events.sql": _MB_MODEL, "schema.yml": _MB_SCHEMA}
