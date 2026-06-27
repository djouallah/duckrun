{{ config(tags=['heavy']) }}
-- Test: All downloaded intraday price files should be merged into fct_price
-- Returns rows where a downloaded intraday file is missing from fct_price.intraday_file.
-- (A file whose every row was superseded by a same-run daily merge could be absent — rare at CI
--  volume, and this is heavy-tagged.)

SELECT
  csv_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'price_today'
  AND csv_filename NOT IN (
    SELECT DISTINCT intraday_file
    FROM {{ ref('fct_price') }}
    WHERE intraday_file IS NOT NULL
  )
