-- BRONZE lakehouse (lh_bronze). Raw generation readings per DUID — incremental merge, so a second
-- run upserts by (date, duid) into the table discovered under the BRONZE root (cross-catalog
-- is_incremental). Self-contained sample data so the demo needs no external source.
{{ config(materialized='incremental', unique_key=['gen_date', 'duid']) }}

{% if is_incremental() %}
select * from (values
    (date '2024-01-02', 'BW01', 'Black coal', 480.0),   -- re-emitted next day, new value
    (date '2024-01-02', 'WIND1', 'Wind',      210.0),
    (date '2024-01-02', 'GAS1', 'Natural gas', 150.0)
) as t(gen_date, duid, fuel, mw)
{% else %}
select * from (values
    (date '2024-01-01', 'BW01', 'Black coal', 500.0),
    (date '2024-01-01', 'WIND1', 'Wind',      200.0),
    (date '2024-01-01', 'GAS1', 'Natural gas', 300.0)
) as t(gen_date, duid, fuel, mw)
{% endif %}
