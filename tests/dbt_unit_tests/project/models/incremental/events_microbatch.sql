-- incremental_strategy: microbatch -- dbt drives one delete+insert per event_time (daily) batch,
-- filtering the stg_events input to each batch window. The harness bounds the window with
-- --event-time-start/--event-time-end (a plain build would otherwise batch every day from `begin`
-- to today), processing day 1 (events 1-3) then day 2 (events 4-6).
{{ config(
    incremental_strategy='microbatch',
    event_time='event_ts',
    begin='2024-01-01',
    batch_size='day'
) }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
