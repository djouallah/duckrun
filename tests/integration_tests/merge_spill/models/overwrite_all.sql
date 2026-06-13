{{ config(materialized='table') }}
-- Overwrite (no merge): a plain table materialization replaces the whole table every run — here
-- with a ~5% sample of the chain's final state. Also far cheaper than a MERGE (no target scan or
-- key join). Last in the chain (destructive), so it doesn't feed anything downstream.
select * from {{ ref('safeappend_only') }} using sample 5 percent (bernoulli)
