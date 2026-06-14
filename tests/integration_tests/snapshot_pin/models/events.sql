{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
  )
}}

{% if is_incremental() %}

-- INCREMENTAL BATCH (every run after the first): update id=1 (-> 111). The table stays 10 rows so
-- the with-pin / without-pin final tables line up row for row.
--
-- The WHERE clause self-references {{ this }} on purpose. duckrun pins this read to the snapshot
-- captured at the START of the run, by registering {{ this }} as
--     delta_scan('<location>', version => vB)
-- (the duckdb-delta `version => N` param, which is why duckdb is pinned to the 1.5.4 floor). So
-- this SELECT sees the target exactly as it was at vB, NOT whatever a concurrent writer may have
-- committed since the run began. The merge that follows is likewise pinned to vB, so its commit is
-- validated against the window (vB, HEAD] — read and write agree on one snapshot (Spark MERGE
-- parity). `value < 0` never matches, so every batch row survives the filter: the point is simply
-- that the read goes through the pinned snapshot.
select id::bigint as id, value::bigint as value
from (values (1, 111)) as t(id, value)
where id not in (select id from {{ this }} where value < 0)

{% else %}

-- SEED (first run only): ten rows, value = id * 10. This creates the table at version v0.
select i::bigint as id, (i * 10)::bigint as value
from range(1, 11) as t(i)

{% endif %}
