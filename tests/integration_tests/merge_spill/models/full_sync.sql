{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
    merge_clauses={
      'when_matched': [{'action': 'update', 'mode': 'by_name'}],
      'when_not_matched_by_source': [{'action': 'delete'}]
    }
  )
}}
{% if is_incremental() %}
  -- A full-dimension SYNC from a fresh source — delta_rs WHEN NOT MATCHED BY SOURCE via merge_clauses:
  --   source = ~95% of the table (every key EXCEPT a deterministic 5% 'departed' slice), marked -5.
  --   matched keys → UPDATE; keys the source no longer carries (the 5% slice) → BY-SOURCE DELETE.
  -- This is the heaviest merge shape: the WHOLE target is scanned and anti-joined against a LARGE
  -- source, so it stresses the by-source path's memory far harder than a small incremental batch.
  select * replace (-5.0::decimal(15,2) as l_quantity) from {{ this }} where l_orderkey % 20 <> 0
{% else %}
  select * from {{ ref('mixed_upsert') }} using sample 50 percent (bernoulli)
{% endif %}
