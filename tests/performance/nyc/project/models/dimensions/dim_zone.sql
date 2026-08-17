-- The 265 TLC taxi zones, from the lookup download_nyc_taxi.py lands under parquet_raw/zone/.
-- Small and slowly-changing, so it is rebuilt whole rather than merged -- delete+insert on the key
-- matches how dim_date is written.
--
-- Parquet, not the CSV TLC serves: the downloader converts it once so that all three dialects read
-- it with one plain statement. That is for Spark's benefit rather than DuckDB's -- see the spark
-- copy's header -- but the whole point of a shared landing format is that no dialect gets its own.
--
-- LocationID is an INTEGER key, which is why this dataset needs no whitespace assertion: the
-- assert_duid_has_no_whitespace pathology (T-SQL pads on comparison, DuckDB and Spark do not) can
-- only bite a STRING join key. If a string key is ever added here it needs all three dialect
-- copies, exactly as DUID has.
{{ config(
    materialized='incremental',
    unique_key='LocationID',
    incremental_strategy='delete+insert'
) }}

SELECT
  CAST(LocationID AS INT) AS LocationID,
  CAST(Borough AS VARCHAR) AS Borough,
  CAST(Zone AS VARCHAR) AS Zone,
  CAST(service_zone AS VARCHAR) AS service_zone
FROM read_parquet('{{ get_parquet_archive_path() }}/zone/taxi_zone_lookup.parquet')
