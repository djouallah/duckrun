{{
  config(materialized='incremental', incremental_strategy='insert', unique_key='id')
}}

-- insert = insert-ONLY upsert: the batch inserts a NEW key (id=11) and a MATCHED key (id=1) that is
-- skipped (insert-only never updates an existing row). Routes through the fenced merge_delta
-- (insert_only=True). The race test records its real OCC behaviour under a concurrent writer.
{% if is_incremental() %}
select id::bigint as id, value::bigint as value from (values (1, 111), (11, 110)) as t(id, value)
{% else %}
select i::bigint as id, (i * 10)::bigint as value from range(1, 11) as t(i)   -- seed ids 1..10
{% endif %}
