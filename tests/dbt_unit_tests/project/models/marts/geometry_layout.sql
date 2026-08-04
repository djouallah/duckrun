-- Exercises the per-model write-geometry configs END TO END THROUGH DBT: max_row_group_size must
-- reach the deltalake writer. This is the hop that silently broke in 0.4.43 — the plugin honored
-- the values (_geometry_config -> WriterProperties) but _delta_core.sql's delta_config dict never
-- carried them, so every dbt model got the adaptive layout while the unit test on the parser and
-- the parquet_layout CI (which pins the engine seam directly) both stayed green.
-- 10 rows under a 3-row ceiling = 4 row groups; the adaptive default would write 1.
{{ config(materialized='table', max_row_group_size=3, target_file_size_mb=1) }}

select range as id, 'row_' || range as label from range(10)
