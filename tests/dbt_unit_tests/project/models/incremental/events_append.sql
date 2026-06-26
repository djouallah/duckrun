-- incremental_strategy: append (no unique_key) -- a blind append. The is_incremental() guard limits
-- later runs to genuinely new keys (event_id beyond the current max), so we append 4-6 without
-- re-appending 1-3 and without dedup. This model also carries the project's incremental unit tests
-- (_unit_tests.yml), exercising the is_incremental() branch via overrides + a `this` input.
{{ config(incremental_strategy='append') }}

select event_id, customer_id, amount, status, event_ts
from {{ ref('stg_events') }}
{% if is_incremental() %}
where event_id > (select max(event_id) from {{ this }})
{% endif %}
