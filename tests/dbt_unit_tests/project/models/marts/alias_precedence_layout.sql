-- Upstream (dbt-duckdb 1.11) alias precedence: the canonical partitioned_by wins over a legacy
-- partition_by unless it is none or '' — and an EMPTY column list is a hard compiler error, never
-- a silent fall-through to the legacy key or a silently unpartitioned table. The vars let the
-- mirror test drive each case, including the expected failures, without a project of its own.
{{ config(materialized='table',
          partitioned_by=var('alias_partitioned_by', none),
          partition_by=var('alias_partition_by', none)) }}

select * from (values (1, 'a'), (2, 'b'), (3, 'a')) as t(id, bucket)
