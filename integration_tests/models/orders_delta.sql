-- Standard dbt `table` materialization, Delta-backed (overwrite each run).
{{ config(materialized='table') }}

select
    status,
    count(*)      as order_count,
    sum(amount)   as total_amount
from {{ ref('stg_orders') }}
group by status
