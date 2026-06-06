-- After the incremental merge pass, fct_orders must hold the 99 base orders plus the two
-- INSERTed increment orders (100, 101) = 101 rows. Order 99 is an UPDATE, not a new row, so
-- it must not inflate the count. Returns a row (test fails) on any other total.
{{ config(tags=['merge_assertion']) }}

select count(*) as n
from {{ ref('fct_orders') }}
having count(*) <> 101
