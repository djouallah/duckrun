{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
  )
}}

-- Plain append is UNFENCED by design: it rebases onto HEAD and only ADDS rows, so a concurrent
-- writer's change is never clobbered (both coexist). The race test proves the run still succeeds and
-- nothing is lost — the deliberate contrast with the fenced strategies (merge/delete+insert/append_if_unchanged).

{% if is_incremental() %}

-- INCREMENTAL BATCH: append one new row (id=11).
select i::bigint as id, (i * 10)::bigint as value
from range(11, 12) as t(i)

{% else %}

-- SEED (first run only): ten rows, value = id * 10. Creates the table at version v0.
select i::bigint as id, (i * 10)::bigint as value
from range(1, 11) as t(i)

{% endif %}
