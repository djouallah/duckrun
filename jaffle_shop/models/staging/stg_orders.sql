{#-- On the increment pass (jaffle_increment=true) we fold a small late-arriving batch into
     the order stream. raw_orders_increment carries two brand-new orders (100, 101) and a
     restatement of an existing one (order 99: placed -> completed). The increment wins on
     overlapping ids, so stg_orders stays unique on order_id and downstream fct_orders can
     prove a real upsert (UPDATE matched + INSERT new) via the Delta merge. --#}
{% if var('jaffle_increment', false) %}

with unioned as (
    select id, user_id, order_date, status, 0 as _priority from {{ ref('raw_orders') }}
    union all
    select id, user_id, order_date, status, 1 as _priority from {{ ref('raw_orders_increment') }}
),

deduped as (
    select
        *,
        row_number() over (partition by id order by _priority desc) as _rn
    from unioned
)

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from deduped
where _rn = 1

{% else %}

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from {{ ref('raw_orders') }}

{% endif %}
