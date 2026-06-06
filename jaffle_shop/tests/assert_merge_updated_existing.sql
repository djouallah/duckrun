-- Order 99 was written as 'placed' in pass 1 and restated to 'completed' in the increment.
-- A correct Delta merge UPDATEs the matched row in place. Returns a row (fails) if the
-- update did not land — i.e. if the merge only inserted and left the old value behind.
{{ config(tags=['merge_assertion']) }}

select order_id, status
from {{ ref('fct_orders') }}
where order_id = 99
  and status <> 'completed'
