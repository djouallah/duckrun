{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]}
  )
}}

-- dbt-duckdb's PORTABLE spelling of insert-only (#20): a merge whose matched clause does nothing.
-- It must resolve to exactly what events_insert.sql (incremental_strategy='insert') does — the same
-- rows AND the same cheap plain-append commit, not a delta_rs merge.
{% if is_incremental() %}
select id::bigint as id, value::bigint as value from (values (1, 111), (11, 110)) as t(id, value)
{% else %}
select i::bigint as id, (i * 10)::bigint as value from range(1, 11) as t(i)   -- seed ids 1..10
{% endif %}
