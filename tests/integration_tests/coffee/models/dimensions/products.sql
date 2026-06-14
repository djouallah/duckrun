-- Dedup the SCD2 dim_products to a current, unique-key product table: keep the latest validity
-- window per product_id (row_number over to_date desc). This makes the fact join 1:1 and gives any
-- downstream merge a legitimate key.
select product_id, name, category, subcategory, standard_cost, standard_price
from (
    select *, row_number() over (partition by product_id order by to_date desc) as rn
    from {{ ref('dim_products') }}
) where rn = 1
