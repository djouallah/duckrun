{{ config(materialized='incremental', database='lh_bronze', unique_key='id', incremental_strategy='merge') }}

{% if is_incremental() %}
-- Incremental load: re-emit id=1 with a changed amount and add id=3. A merge upserts these into
-- the EXISTING bronze table, so id=2 (untouched) must survive. If cross-catalog discovery failed to
-- find this table under the bronze root, is_incremental() would be false and this would overwrite,
-- dropping id=2 — which the test asserts against.
select * from (values (1, 111), (3, 300)) as t(id, amount)
{% else %}
select * from (values (1, 100), (2, 200)) as t(id, amount)
{% endif %}
