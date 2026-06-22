{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
    merge_update_set_expressions={
      'l_quantity': 'CAST(-9 AS DECIMAL(15,2))',
      'l_returnflag': "CASE WHEN DBT_INTERNAL_SOURCE.l_discount > 0.05 THEN 'H' ELSE 'L' END"
    }
  )
}}
{% if is_incremental() %}
  -- A 100%-match UPDATE driven by merge_update_set_expressions: every batch row keeps its key, and the
  -- merge sets l_quantity = -9 and re-derives l_returnflag with a CASE expression over the source —
  -- exercising the arbitrary-expression / CASE update path (not a plain column copy) at scale. The
  -- non-listed columns are still copied from source, so the row count must NOT change.
  select * from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  select * from {{ ref('mixed_upsert') }} using sample 50 percent (bernoulli)
{% endif %}
