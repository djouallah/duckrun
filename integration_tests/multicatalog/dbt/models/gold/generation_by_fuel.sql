-- GOLD lakehouse (lh_gold). JOINs raw_generation (BRONZE) with clean_generation (SILVER) — three
-- separate Lakehouses in one query — to report, per fuel, the raw MW alongside the cleaned carbon
-- estimate. Proves cross-catalog reads compose, and the result lands under the GOLD root.
{{ config(materialized='table') }}

select
    s.fuel,
    round(sum(r.mw), 1)              as total_mw,
    round(sum(s.est_tonnes_co2), 3)  as total_tonnes_co2
from {{ ref('raw_generation') }} r
join {{ ref('clean_generation') }} s
  on s.gen_date = r.gen_date and s.duid = upper(r.duid)
group by s.fuel
order by total_mw desc
