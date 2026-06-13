{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
  )
}}
{% if is_incremental() %}
  -- Batch: a ~5% sample of the current table, UNCHANGED. A correct MERGE of rows that already hold
  -- these keys and values is idempotent — nothing must change. The runner runs this twice.
  select * from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  -- Chained: build on the update-only result.
  select * from {{ ref('update_only') }}
{% endif %}
