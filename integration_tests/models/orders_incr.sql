-- Standard dbt `incremental` materialization, Delta-backed.
-- First run overwrites; later runs merge (upsert) on unique_key.
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}

select
    order_id,
    customer,
    amount,
    status
from {{ ref('stg_orders') }}
{% if is_incremental() %}
-- only new/changed rows on incremental runs (exercises is_incremental + {{ this }})
where order_id not in (select order_id from {{ this }} where status = 'cancelled')
{% endif %}
