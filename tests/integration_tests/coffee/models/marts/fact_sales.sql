{{ config(partition_by='region') }}
-- Generate COFFEE_ROWS coffee-shop order lines (random product/location/qty/discount/date) and join
-- them 1:1 to the dims, then write partitioned by region. Products and locations are dense-indexed
-- (row_number) and sampled by index so every order line joins exactly once — no rows dropped.
with prods as (
        select *, row_number() over (order by product_id) as prn from {{ ref('products') }}
     ),
     locs as (
        select location_id, region, row_number() over (order by record_id) as lrn
        from {{ ref('dim_locations') }}
     ),
     raw as (
        select i as order_line_id,
               1 + (i / 3)::int as order_id,
               floor(random() * (select count(*) from prods))::int + 1 as prn,
               floor(random() * (select count(*) from locs))::int  + 1 as lrn,
               (random() * 4)::int + 1 as quantity,
               case when random() < 0.8 then 0 else (random() * 14)::int + 1 end as discount_percentage,
               date '2023-01-01' + (random() * 364)::int as order_date
        from range(1, {{ env_var('COFFEE_ROWS', '1000000') | int + 1 }}) t(i)
     )
select r.order_id, r.order_line_id, r.order_date,
       case when month(r.order_date) in (12, 1, 2) then 'Winter'
            when month(r.order_date) in (3, 4, 5)  then 'Spring'
            when month(r.order_date) in (6, 7, 8)  then 'Summer'
            else 'Fall' end as season,
       l.location_id, l.region, p.name as product_name, r.quantity,
       round(p.standard_price * ((100 - r.discount_percentage) / 100.0) * r.quantity, 2) as sales_amount,
       r.discount_percentage, p.product_id
from raw r
join prods p on p.prn = r.prn
join locs  l on l.lrn = r.lrn
