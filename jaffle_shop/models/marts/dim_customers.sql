{#-- Customer dimension: a `table` materialization, so delta_rs overwrites the whole Delta
     table every run. Exercises duckrun's Delta table-write path on the canonical schema. --#}
{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(*) as number_of_orders
    from orders
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    co.first_order,
    co.most_recent_order,
    coalesce(co.number_of_orders, 0) as number_of_orders
from customers c
left join customer_orders co using (customer_id)
