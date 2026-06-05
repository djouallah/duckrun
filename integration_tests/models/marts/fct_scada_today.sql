{{ config(
    materialized='incremental',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
    pre_hook="SET VARIABLE scada_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'scada_today'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} LIMIT {{ env_var('process_limit', '1000') }}))"
) }}

{% set csv_archive_path = get_csv_archive_path() %}

WITH scada_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('scada_today_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I': 'VARCHAR',
      'DISPATCH': 'VARCHAR',
      'UNIT_SCADA': 'VARCHAR',
      'xx': 'VARCHAR',
      'SETTLEMENTDATE': 'timestamp',
      'DUID': 'VARCHAR',
      'SCADAVALUE': 'double',
      'LASTCHANGED': 'timestamp'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND SCADAVALUE != 0
)

SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(LASTCHANGED AS TIMESTAMPTZ) AS LASTCHANGED,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM scada_staging
