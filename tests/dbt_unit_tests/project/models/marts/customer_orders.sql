-- One row per customer: order count + total spend + whether they ever placed a large order.
-- LEFT JOIN so a customer with no orders still appears (0 / 0 / false).
select
    c.customer_id,
    c.first_name,
    count(o.order_id)                          as num_orders,
    coalesce(sum(o.amount), 0)                 as total_amount,
    coalesce(bool_or(o.is_large_order), false) as has_large_order
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o
    on o.customer_id = c.customer_id
group by 1, 2
