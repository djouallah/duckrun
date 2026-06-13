{{
  config(
    materialized='incremental',
    incremental_strategy='safeappend',
  )
}}
{% if is_incremental() %}
  -- Batch: a ~5% sample key-shifted past the max key, via safeappend — a plain append that commits
  -- only if the table version is unchanged since it was read (it is here — single writer). Same
  -- cheap append, version-guarded; no target scan.
  with mx as (select max(l_orderkey) as m from {{ this }})
  select * replace ((l_orderkey + (select m from mx)) as l_orderkey)
  from {{ this }} using sample 5 percent (bernoulli)
{% else %}
  -- Chained: build on the append result.
  select * from {{ ref('append_only') }}
{% endif %}
