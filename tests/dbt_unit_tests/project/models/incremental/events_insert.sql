-- incremental_strategy: insert. Inserts only keys not already present (idempotent append / dedupe),
-- so re-emitted keys 1-3 KEEP their original amounts and only the genuinely new keys 4-6 are added.
{{ config(incremental_strategy='insert', unique_key='event_id') }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
