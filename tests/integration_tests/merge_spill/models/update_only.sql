{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
  )
}}
{% if is_incremental() %}
  -- Batch: a ~5% sample of existing keys (no shift) → 100% match. Marked l_quantity = -2. The
  -- heaviest merge join; the row count must NOT change and the sampled rows must carry the marker.
  select * replace (-2.0::decimal(15,2) as l_quantity)
  from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  -- Chained: build on the insert-only result.
  select * from {{ ref('insert_only') }}
{% endif %}
