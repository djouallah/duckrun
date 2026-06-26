-- incremental_strategy: delete+insert. Deletes target rows whose event_id is in the batch, then
-- inserts the whole batch -- so re-emitted keys 1-3 are REPLACED with their load-2 amounts (same
-- final state as merge here; the two differ only when the batch carries duplicate keys).
{{ config(incremental_strategy='delete+insert', unique_key='event_id') }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
