{{ config(tags=['heavy']) }}
-- Test: All downloaded intraday SCADA files should be processed in fct_scada_today
-- Returns rows where a downloaded file is missing from fct_scada_today

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'scada_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT file
    FROM {{ ref('fct_scada_today') }}
  )
