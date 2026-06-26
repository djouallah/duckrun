-- Staging over raw orders: derive a simple business flag (an order of $100+ is "large").
select
    order_id,
    customer_id,
    amount,
    status,
    amount >= 100 as is_large_order
from {{ ref('raw_orders') }}
