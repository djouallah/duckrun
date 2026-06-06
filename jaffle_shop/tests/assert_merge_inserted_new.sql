-- Orders 100 and 101 exist only in the increment; a correct merge INSERTs them. Returns a
-- row (fails) for any expected new order id missing from fct_orders after the merge pass.
{{ config(tags=['merge_assertion']) }}

with expected as (
    select 100 as order_id
    union all
    select 101 as order_id
)

select e.order_id
from expected e
where e.order_id not in (select order_id from {{ ref('fct_orders') }})
