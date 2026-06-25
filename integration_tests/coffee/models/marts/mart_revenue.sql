-- Revenue by product category x season. The fact joins 1:1 to the unique-key `products`, so the
-- summed revenue reconciles exactly to fact_sales.sales_amount (only rounding differs).
select p.category, f.season,
       count(*) as order_lines,
       round(sum(f.sales_amount), 2) as revenue
from {{ ref('fact_sales') }} f
join {{ ref('products') }} p on p.product_id = f.product_id
group by 1, 2
