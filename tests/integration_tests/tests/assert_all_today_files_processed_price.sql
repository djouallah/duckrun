{{ config(tags=['heavy']) }}
-- Test: All downloaded intraday price files should be processed in fct_price_today
-- Returns rows where a downloaded file is missing from fct_price_today

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'price_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT file
    FROM {{ ref('fct_price_today') }}
  )
