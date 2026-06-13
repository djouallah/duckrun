{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_orderkey', 'l_linenumber'],
    merge_max_spill_size=var('merge_max_spill_size', none),
  )
}}
{% if is_incremental() %}
  -- Batch: a ~1% sample of the current table — ~80% kept on their existing keys (UPDATE) and ~20%
  -- key-shifted past the max key (INSERT). Every batch row is marked l_quantity = -1 so the merge's
  -- effect is verifiable. Sampling {{ this }} (pinned to the read version) keeps it single-snapshot.
  with s as (select *, random() as _r from {{ this }} using sample 1 percent (bernoulli)),
       mx as (select max(l_orderkey) as m from {{ this }})
  select * exclude (_r) replace (-1.0::decimal(15,2) as l_quantity)
  from s where _r >= 0.2
  union all
  select * exclude (_r) replace ((l_orderkey + (select m from mx)) as l_orderkey, -1.0::decimal(15,2) as l_quantity)
  from s where _r < 0.2
{% else %}
  -- First run: seed this table by STREAMING the generated parquet straight from the source — a
  -- duckrun catalog view over read_parquet, not a 120M-row in-memory stage. generate_data must have
  -- produced the parquet first (the bench runs it before this model).
  select * from {{ source('tpch', 'lineitem') }}
{% endif %}
