-- SILVER lakehouse (the default catalog). Reads raw_generation from the BRONZE lakehouse — a
-- cross-lakehouse ref() — and tidies it: uppercased DUID, a carbon estimate per reading.
{{ config(materialized='table') }}

select
    gen_date,
    upper(duid)                             as duid,
    fuel,
    mw,
    mw * case lower(fuel)
             when 'black coal'  then 900
             when 'natural gas' then 490
             else 0                          -- wind/solar/etc. are zero-carbon
         end / 1000.0                        as est_tonnes_co2
from {{ ref('raw_generation') }}
