-- Inherited dbt-duckdb 'view' materialization (proves dependency macro-loading works).
select
    order_id,
    customer,
    amount,
    status
from {{ ref('raw_orders') }}
