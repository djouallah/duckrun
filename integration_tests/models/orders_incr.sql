-- Incremental Delta table: first run overwrites, later runs merge on unique_key.
{{ config(materialized='delta', incremental=true, unique_key='order_id') }}

select
    order_id,
    customer,
    amount,
    status
from {{ ref('stg_orders') }}
