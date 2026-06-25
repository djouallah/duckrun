-- Lineage only: the intraday SCADA CSVs are read below via read_csv with the file list from
-- stg_csv_archive_log; this records the source dependency and draws the node in the DAG.
-- depends_on: {{ source('aemo', 'scada_today') }}

{{ config(
    materialized='incremental',
    incremental_strategy='insert',
    unique_key=['file'],
    partition_by=['month_key'],
    incremental_predicates=["target.month_key = source.month_key"],
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
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR,
  -- Monthly partition key (YYYYMM): low-cardinality column delta_rs prunes the merge on
  -- (target.month_key = source.month_key), so each merge only touches the batch's months.
  CAST(YEAR(SETTLEMENTDATE) AS INT) * 100 + CAST(MONTH(SETTLEMENTDATE) AS INT) AS month_key
FROM scada_staging
