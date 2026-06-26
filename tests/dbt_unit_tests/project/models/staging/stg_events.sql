-- Shared upstream for the incremental-strategy marts. A var-controlled window over raw_events lets
-- the harness drive a deterministic two-run incremental scenario without mutating fixtures:
--   --vars '{load: 1}'  -> first load only  (events 1-3 at their original amounts)
--   --vars '{load: 2}'  -> second load too  (events 1-3 RE-EMITTED with changed amounts + new 4-6)
-- Deduped to the latest load per event_id so each run sees exactly one row per event. event_time is
-- declared so the microbatch mart can filter this input per daily batch.
{{ config(event_time='event_ts') }}

select
    event_id,
    customer_id,
    amount,
    status,
    event_ts
from {{ ref('raw_events') }}
where load <= {{ var('load', 2) }}
qualify row_number() over (partition by event_id order by load desc) = 1
