{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
  )
}}

{% if is_incremental() %}

-- INCREMENTAL BATCH (every run after the first): replace id=1 (-> 111). Same shape as events.sql,
-- but the strategy is delete+insert instead of merge: delta_plugin reads the kept rows pinned to vB
-- (delta_scan('<location>', version => vB)) and writes the (kept rows UNION this batch) back. That
-- final write MUST be fenced to vB too (overwrite_if_unchanged / CAS) — otherwise a concurrent
-- writer that commits during the run is silently clobbered by the full-table overwrite. This model
-- is the dbt-path race that proves the fence (test_snapshot_pin.py), exactly mirroring the merge one.
--
-- The WHERE self-references {{ this }} (pinned to vB) so the read pin is exercised; `value < 0` never
-- matches, so the batch row always survives the filter.
select id::bigint as id, value::bigint as value
from (values (1, 111)) as t(id, value)
where id not in (select id from {{ this }} where value < 0)

{% else %}

-- SEED (first run only): ten rows, value = id * 10. Creates the table at version v0.
select i::bigint as id, (i * 10)::bigint as value
from range(1, 11) as t(i)

{% endif %}
