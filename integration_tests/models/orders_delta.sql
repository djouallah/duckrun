-- Full-refresh Delta table: DuckDB runs the SQL, delta_rs materializes it.
{{ config(materialized='delta') }}

select
    status,
    count(*)      as order_count,
    sum(amount)   as total_amount
from {{ ref('stg_orders') }}
group by status
