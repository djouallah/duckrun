{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
    merge_clauses={
      'when_matched': [
        {'action': 'delete', 'condition': 'DBT_INTERNAL_SOURCE.l_quantity = -7'},
        {'action': 'update', 'mode': 'by_name'}
      ],
      'when_not_matched': [
        {'action': 'insert', 'mode': 'by_name'}
      ]
    }
  )
}}
{% if is_incremental() %}
  -- A CDC change-set applied in ONE merge — the full delta_rs clause set through dbt merge_clauses:
  --   • tombstones (l_quantity = -7) on a deterministic ~1% slice (l_orderkey % 100 = 7)  → matched DELETE
  --   • a ~3% random sample OUTSIDE that slice, marked -8                                  → matched UPDATE
  --   • a ~2% random sample, key-shifted past max + a 2036 shipdate marker                 → NOT MATCHED INSERT
  -- The matched DELETE clause (condition on the source marker) runs BEFORE the catch-all UPDATE, so a
  -- tombstoned row is removed, not updated. Sampling {{ this }} (pinned to the read version) keeps it
  -- single-snapshot. Source keys stay unique: the delete slice (%100=7) and the update sample (%100<>7)
  -- are disjoint, and inserts are shifted past max so they never collide with an existing key.
  with mx as (select max(l_orderkey) as m from {{ this }}),
       upd_s as (select * from {{ this }} using sample 3 percent (bernoulli)),
       ins_s as (select * from {{ this }} using sample 2 percent (bernoulli))
  select * replace (-7.0::decimal(15,2) as l_quantity) from {{ this }} where l_orderkey % 100 = 7
  union all
  select * replace (-8.0::decimal(15,2) as l_quantity) from upd_s where l_orderkey % 100 <> 7
  union all
  select * replace ((l_orderkey + (select m from mx)) as l_orderkey, DATE '2036-01-01' as l_shipdate)
  from ins_s
{% else %}
  -- Seed a big (sampled) base off the chain head; terminal branch — feeds nothing downstream.
  select * from {{ ref('mixed_upsert') }} using sample 50 percent (bernoulli)
{% endif %}
