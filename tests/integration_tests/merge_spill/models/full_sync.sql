{#
  full_sync — the heaviest merge shape: matched UPDATE + WHEN NOT MATCHED BY SOURCE DELETE.
  The source is HALF the table (far bigger than a normal incremental batch), so set
  merge_streamed_exec=true: stream the source instead of collecting it for pruning stats, so it
  isn't materialized whole. by-source must scan the entire target regardless, so no pruning benefit
  is lost, and streaming keeps the big source from blowing past the spill cap. The documented config
  for a large-source merge.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
    merge_streamed_exec=true,
    merge_clauses={
      'when_matched': [{'action': 'update', 'mode': 'by_name'}],
      'when_not_matched_by_source': [{'action': 'delete'}]
    }
  )
}}
{% if is_incremental() %}
  -- A full-dimension SYNC from a fresh source — delta_rs WHEN NOT MATCHED BY SOURCE via merge_clauses:
  --   source = ~50% of the table (every ODD orderkey), marked -5; the EVEN orderkeys are 'departed'.
  --   matched keys (odd) → UPDATE; keys the source no longer carries (even) → BY-SOURCE DELETE.
  -- The whole target is scanned and anti-joined against a ~50% (streamed) source — the heaviest merge
  -- shape here, and the one that proves the by-source path stays within RAM (spills) at scale.
  select * replace (-5.0::decimal(15,2) as l_quantity) from {{ this }} where l_orderkey % 2 <> 0
{% else %}
  select * from {{ ref('mixed_upsert') }} using sample 50 percent (bernoulli)
{% endif %}
