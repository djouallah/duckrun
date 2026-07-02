-- Gold lakehouse (a THIRD catalog). JOINs bronze_raw (lh_bronze) with silver_clean (default/silver)
-- in one query — three separate Delta roots, all visible and joinable at once. Proves cross-catalog
-- reads compose (not just a single ref): the classic dimension/fact join across lakehouses.
{{ config(materialized='table', database='lh_gold') }}

select
    b.id,
    b.amount            as bronze_amount,
    s.double_amount     as silver_double_amount
from {{ ref('bronze_raw') }} b
join {{ ref('silver_clean') }} s
  on b.id = s.id
order by b.id
