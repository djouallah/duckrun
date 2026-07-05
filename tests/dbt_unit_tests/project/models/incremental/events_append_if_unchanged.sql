-- incremental_strategy: append_if_unchanged -- an optimistic append that commits only if the table
-- version has not moved since the read began (CAS), else fails. With no concurrent writer the row
-- outcome matches append: the is_incremental() guard appends only the new keys 4-6.
{{ config(incremental_strategy='append_if_unchanged') }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
{% if is_incremental() %}
where event_id > (select max(event_id) from {{ this }})
{% endif %}
