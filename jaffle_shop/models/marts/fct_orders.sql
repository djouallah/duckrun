-- depends_on: {{ ref('raw_orders_increment') }}
{#-- Order fact, incrementally merged into Delta on order_id.

     Pass 1 (full refresh): the table does not exist yet, so is_incremental() is false and
     all 99 canonical orders are written.

     Pass 2 (jaffle_increment=true): the table exists, so is_incremental() is true and only
     the late-arriving slice (the ids in raw_orders_increment) is selected and handed to the
     Delta merge. Order 99 is UPDATEd in place (placed -> completed) and orders 100/101 are
     INSERTed — the merge correctness the gating tests assert. --#}
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_payments as (
    select
        order_id,
        sum(amount) as amount
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    coalesce(p.amount, 0) as amount
from orders o
left join order_payments p using (order_id)

{% if is_incremental() %}
where o.order_id in (select id from {{ ref('raw_orders_increment') }})
{% endif %}
