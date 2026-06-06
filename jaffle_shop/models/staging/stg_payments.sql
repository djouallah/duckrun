{#-- Payments for the two inserted increment orders (100, 101) arrive alongside them on the
     increment pass. Ids are disjoint from the base set, so a plain union is safe. --#}
select
    id as payment_id,
    order_id,
    payment_method,
    amount
from {{ ref('raw_payments') }}

{% if var('jaffle_increment', false) %}
union all
select
    id as payment_id,
    order_id,
    payment_method,
    amount
from {{ ref('raw_payments_increment') }}
{% endif %}
