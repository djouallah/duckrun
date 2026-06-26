-- incremental_strategy: merge. Re-scans the whole window each run and upserts on event_id, so a
-- re-emitted key is UPDATED (events 1-3 move to their load-2 amounts) and new keys are inserted.
{{ config(incremental_strategy='merge', unique_key='event_id') }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
