-- Exercises the `sort_by` write config (review #6): the input rows are deliberately shuffled, and
-- sort_by must write them PHYSICALLY ordered by sort_key (long RLE runs / dictionary locality for
-- Direct Lake), deterministically, regardless of preserve_insertion_order=false. A trailing ORDER BY
-- in model SQL is intentionally NOT relied on here.
{{ config(materialized='table', sort_by=['sort_key']) }}

select * from (
  values (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')
) as t(sort_key, label)
