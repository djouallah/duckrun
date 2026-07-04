-- Exercises merge_materialize_source (review #14): the model SQL is staged into a DuckDB temp table
-- ONCE, so the cardinality guard and delta_rs's source collection see identical rows and the SQL is
-- not re-evaluated 2-3x. The merge result must be identical to events_merge (upsert on event_id).
{{ config(incremental_strategy='merge', unique_key='event_id', merge_materialize_source=true) }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
