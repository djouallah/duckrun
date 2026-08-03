-- Exercises sort_by='auto' (the dbt spelling of SORTED BY AUTO): a hash-scattered 3-value category
-- (so arrival order is unsorted) next to a unique id (the grain-stop must keep it out of the key).
-- The profiler must pick category and the write must land PHYSICALLY clustered by it. The var lets
-- the test also drive the case-insensitive spelling and the 'auto'-inside-a-list rejection.
{{ config(materialized='table', sort_by=var('auto_sort_by', 'auto')) }}

select
    r as id,
    ['east', 'west', 'north'][(1 + hash(r) % 3)::int] as category,
    (r * 7) % 100 as amount
from range(200) t(r)
