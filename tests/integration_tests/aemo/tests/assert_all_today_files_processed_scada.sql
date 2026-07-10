{{ config(tags=['heavy']) }}
-- Test: All downloaded intraday SCADA files should be merged into fct_scada
-- Returns rows where a downloaded intraday file is missing from fct_scada.intraday_file.
-- (A file whose every row was superseded by a same-run daily merge could be absent — rare at CI
--  volume, and this is heavy-tagged.)

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'scada_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT intraday_file
    FROM {{ ref('fct_scada') }}
    WHERE intraday_file IS NOT NULL
  )
