{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
  )
}}
{% if is_incremental() %}
  -- Batch: a ~5% sample key-shifted past the max key so nothing matches — every row INSERTs.
  -- Stamped with a future l_shipdate (2035) as a verifiable marker. Still a MERGE (no matches).
  with mx as (select max(l_orderkey) as m from {{ this }})
  select * replace ((l_orderkey + (select m from mx)) as l_orderkey, DATE '2035-06-15' as l_shipdate)
  from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  -- Chained: build on the mixed-upsert result.
  select * from {{ ref('mixed_upsert') }}
{% endif %}
