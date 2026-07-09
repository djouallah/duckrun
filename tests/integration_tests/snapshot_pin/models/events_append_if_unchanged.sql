{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
  )
}}

-- A plain `append` that READS {{ this }} auto-fences: because the model self-references its own target
-- (the `not in (select id from {{ this }})` below), the append is committed compare-and-swap
-- (max_commit_retries=0) against vB, so a concurrent writer that commits during the run makes the
-- append refuse rather than land a (possibly duplicate) row on a drifted HEAD. There is no strategy to
-- pick — the fence is automatic on the self-reference. The race test proves that through a real run.

{% if is_incremental() %}

-- INCREMENTAL BATCH: append one new row (id=11). Self-references {{ this }} (pinned to vB) so the
-- read pin is exercised; `value < 0` never matches, so the row always survives the filter.
select id::bigint as id, value::bigint as value
from (values (11, 110)) as t(id, value)
where id not in (select id from {{ this }} where value < 0)

{% else %}

-- SEED (first run only): ten rows, value = id * 10. Creates the table at version v0.
select i::bigint as id, (i * 10)::bigint as value
from range(1, 11) as t(i)

{% endif %}
