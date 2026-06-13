{{
  config(
    materialized='incremental',
    incremental_strategy='append',
  )
}}
{% if is_incremental() %}
  -- Batch: a ~5% sample key-shifted past the max key, appended (no target scan, no key join) —
  -- far cheaper than a MERGE of the same row count. Here for the head-to-head comparison.
  with mx as (select max(l_orderkey) as m from {{ this }})
  select * replace ((l_orderkey + (select m from mx)) as l_orderkey)
  from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  -- Chained: build on the idempotent re-merge result.
  select * from {{ ref('idempotent_remerge') }}
{% endif %}
