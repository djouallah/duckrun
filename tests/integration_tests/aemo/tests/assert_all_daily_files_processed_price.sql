{{ config(tags=['heavy']) }}
-- Test: All downloaded daily files should be processed in fct_price
-- Returns rows where a downloaded file is missing from fct_price

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'daily'
  AND csv_filename NOT IN (
    SELECT DISTINCT daily_file
    FROM {{ ref('fct_price') }}
    WHERE daily_file IS NOT NULL
  )
