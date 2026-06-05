{{ config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='delete+insert'
) }}

SELECT
  CAST(date AS DATE) as date,
  CAST(EXTRACT(year FROM date) AS INT) as year,
  CAST(EXTRACT(month FROM date) AS INT) as month
FROM (
  SELECT unnest(generate_series(
    CAST('2018-04-01' AS DATE),
    CAST('2026-12-31' AS DATE),
    INTERVAL 1 DAY
  )) as date
)
{% if is_incremental() %}
WHERE date NOT IN (SELECT date FROM {{ this }})
{% endif %}
