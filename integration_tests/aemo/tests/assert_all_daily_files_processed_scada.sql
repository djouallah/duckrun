{{ config(tags=['heavy']) }}
-- Test: All downloaded daily files should be processed in fct_scada
-- Returns rows where a downloaded file is missing from fct_scada

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'daily'
  AND csv_filename NOT IN (
    SELECT DISTINCT daily_file
    FROM {{ ref('fct_scada') }}
    WHERE daily_file IS NOT NULL
  )
