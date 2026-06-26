{{
  config(materialized = 'table')
}}

-- A full-rebuild `table` model is UNFENCED by design: every run overwrites the whole table
-- (last-writer-wins), so a concurrent writer that commits mid-run is replaced wholesale. The race
-- test documents that boundary — a full refresh is not, and should not be, snapshot-fenced. Each run
-- re-selects the same ten rows (value = id * 10).
select i::bigint as id, (i * 10)::bigint as value
from range(1, 11) as t(i)
