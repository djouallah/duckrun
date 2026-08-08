-- dbt-duckdb 1.11 canonicalized the spellings as sorted_by/partitioned_by (aliasing duckrun's
-- sort_by/partition_by both ways). This model uses ONLY the canonical spellings, so a regression
-- back to the duckrun-only names would leave it unpartitioned and unsorted — the mirror test
-- reads the Delta table to prove both configs reached the writer.
{{ config(materialized='table', sorted_by=['sort_key'], partitioned_by=['bucket']) }}

select * from (
  values (3, 'b'), (1, 'a'), (5, 'a'), (2, 'b'), (4, 'a')
) as t(sort_key, bucket)
