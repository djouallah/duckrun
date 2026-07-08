-- Lineage only: both feeds are read below via read_csv with the file lists from
-- stg_csv_archive_log; these record the source dependencies and draw the nodes in the DAG.
-- depends_on: {{ source('aemo', 'daily_report') }}
-- depends_on: {{ source('aemo', 'scada_today') }}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['DUID', 'SETTLEMENTDATE', 'INTERVENTION'],
    partition_by=['month_key'],
    incremental_predicates=['target.month_key = source.month_key'],
    merge_update_condition='source.source_priority >= target.source_priority',
    merge_exclude_columns=['intraday_file'],
    pre_hook=[
      "SET VARIABLE scada_daily_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'daily'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT daily_file FROM {{ this }} WHERE daily_file IS NOT NULL){% endif %} ORDER BY source_filename LIMIT {{ env_var('process_limit', '200') }}))",
      "SET VARIABLE scada_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'scada_today'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT intraday_file FROM {{ this }} WHERE intraday_file IS NOT NULL){% endif %} ORDER BY source_filename LIMIT {{ env_var('process_limit', '200') }}))"
    ]
) }}

{#-- ONE SCADA table fed by TWO sources, and the canonical merge example in this suite:

    * Intraday Dispatch_SCADA (source_priority = 0) INSERTS preliminary actual-MW rows as 5-min
      dispatch intervals arrive. Only DUID / SETTLEMENTDATE / INITIALMW are known; the rich daily
      columns are NULL.
    * The daily settled DUNIT file (source_priority = 1) MERGES over them — the UPDATE branch —
      overwriting INITIALMW with the settled value and backfilling every daily-only column.
    * Intraday-only keys (small / non-scheduled DUIDs never in settlement) are never matched, so
      they survive as preliminary rows — the merge's unmatched leftovers.

    Why each merge knob is here:
    - unique_key is the BUSINESS key (no `file`) so a settled row matches the preliminary one and
      UPDATES it instead of inserting a duplicate.
    - merge_update_condition keeps daily authoritative: a late intraday row (priority 0) can never
      overwrite a settled daily row (priority 1); daily overwrites intraday, daily re-settles daily,
      intraday revises intraday.
    - merge_exclude_columns preserves intraday_file when daily updates a row, so the file-tracking
      below still sees that intraday file as processed (merge overwrites every other column, which
      is why a single `file` column can't drive the incremental filter).
    - The source is deduped to one row per key with QUALIFY (daily wins, then latest file) — this is
      exactly the fix the duplicate-source-key cardinality guard recommends. --#}

{%- set check_files_query -%}
SELECT
  (SELECT COUNT(*) FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'daily'
   {%- if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT daily_file FROM {{ this }} WHERE daily_file IS NOT NULL){% endif %})
  +
  (SELECT COUNT(*) FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'scada_today'
   {%- if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT intraday_file FROM {{ this }} WHERE intraday_file IS NOT NULL){% endif %}) AS cnt
{%- endset -%}

{%- if execute and flags.WHICH == 'run' -%}
  {%- set files_result = run_query(check_files_query) -%}
  {%- set has_files = files_result and files_result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_files = true -%}
{%- endif -%}

{% if has_files %}
WITH scada_daily AS (
  SELECT *
  FROM read_csv(
    getvariable('scada_daily_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I': 'VARCHAR',
      'UNIT': 'VARCHAR',
      'XX': 'VARCHAR',
      'VERSION': 'VARCHAR',
      'SETTLEMENTDATE': 'VARCHAR',
      'RUNNO': 'VARCHAR',
      'DUID': 'VARCHAR',
      'INTERVENTION': 'VARCHAR',
      'DISPATCHMODE': 'VARCHAR',
      'AGCSTATUS': 'VARCHAR',
      'INITIALMW': 'VARCHAR',
      'TOTALCLEARED': 'VARCHAR',
      'RAMPDOWNRATE': 'VARCHAR',
      'RAMPUPRATE': 'VARCHAR',
      'LOWER5MIN': 'VARCHAR',
      'LOWER60SEC': 'VARCHAR',
      'LOWER6SEC': 'VARCHAR',
      'RAISE5MIN': 'VARCHAR',
      'RAISE60SEC': 'VARCHAR',
      'RAISE6SEC': 'VARCHAR',
      'MARGINAL5MINVALUE': 'VARCHAR',
      'MARGINAL60SECVALUE': 'VARCHAR',
      'MARGINAL6SECVALUE': 'VARCHAR',
      'MARGINALVALUE': 'VARCHAR',
      'VIOLATION5MINDEGREE': 'VARCHAR',
      'VIOLATION60SECDEGREE': 'VARCHAR',
      'VIOLATION6SECDEGREE': 'VARCHAR',
      'VIOLATIONDEGREE': 'VARCHAR',
      'LOWERREG': 'VARCHAR',
      'RAISEREG': 'VARCHAR',
      'AVAILABILITY': 'VARCHAR',
      'RAISE6SECFLAGS': 'VARCHAR',
      'RAISE60SECFLAGS': 'VARCHAR',
      'RAISE5MINFLAGS': 'VARCHAR',
      'RAISEREGFLAGS': 'VARCHAR',
      'LOWER6SECFLAGS': 'VARCHAR',
      'LOWER60SECFLAGS': 'VARCHAR',
      'LOWER5MINFLAGS': 'VARCHAR',
      'LOWERREGFLAGS': 'VARCHAR',
      'RAISEREGAVAILABILITY': 'VARCHAR',
      'RAISEREGENABLEMENTMAX': 'VARCHAR',
      'RAISEREGENABLEMENTMIN': 'VARCHAR',
      'LOWERREGAVAILABILITY': 'VARCHAR',
      'LOWERREGENABLEMENTMAX': 'VARCHAR',
      'LOWERREGENABLEMENTMIN': 'VARCHAR',
      'RAISE6SECACTUALAVAILABILITY': 'VARCHAR',
      'RAISE60SECACTUALAVAILABILITY': 'VARCHAR',
      'RAISE5MINACTUALAVAILABILITY': 'VARCHAR',
      'RAISEREGACTUALAVAILABILITY': 'VARCHAR',
      'LOWER6SECACTUALAVAILABILITY': 'VARCHAR',
      'LOWER60SECACTUALAVAILABILITY': 'VARCHAR',
      'LOWER5MINACTUALAVAILABILITY': 'VARCHAR',
      'LOWERREGACTUALAVAILABILITY': 'VARCHAR'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND UNIT = 'DUNIT' AND VERSION = '3'
),

scada_intraday AS (
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
      'SETTLEMENTDATE': 'VARCHAR',
      'DUID': 'VARCHAR',
      'SCADAVALUE': 'VARCHAR',
      'LASTCHANGED': 'VARCHAR'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND SCADAVALUE != '0'
),

-- Daily settled DUNIT rows: full schema, authoritative (source_priority = 1).
daily_rows AS (
  SELECT
    UNIT,
    DUID,
    CAST(VERSION AS DOUBLE) AS VERSION,
    CAST(RUNNO AS DOUBLE) AS RUNNO,
    CAST(INTERVENTION AS DOUBLE) AS INTERVENTION,
    CAST(DISPATCHMODE AS DOUBLE) AS DISPATCHMODE,
    CAST(AGCSTATUS AS DOUBLE) AS AGCSTATUS,
    CAST(INITIALMW AS DOUBLE) AS INITIALMW,
    CAST(TOTALCLEARED AS DOUBLE) AS TOTALCLEARED,
    CAST(RAMPDOWNRATE AS DOUBLE) AS RAMPDOWNRATE,
    CAST(RAMPUPRATE AS DOUBLE) AS RAMPUPRATE,
    CAST(LOWER5MIN AS DOUBLE) AS LOWER5MIN,
    CAST(LOWER60SEC AS DOUBLE) AS LOWER60SEC,
    CAST(LOWER6SEC AS DOUBLE) AS LOWER6SEC,
    CAST(RAISE5MIN AS DOUBLE) AS RAISE5MIN,
    CAST(RAISE60SEC AS DOUBLE) AS RAISE60SEC,
    CAST(RAISE6SEC AS DOUBLE) AS RAISE6SEC,
    CAST(MARGINAL5MINVALUE AS DOUBLE) AS MARGINAL5MINVALUE,
    CAST(MARGINAL60SECVALUE AS DOUBLE) AS MARGINAL60SECVALUE,
    CAST(MARGINAL6SECVALUE AS DOUBLE) AS MARGINAL6SECVALUE,
    CAST(MARGINALVALUE AS DOUBLE) AS MARGINALVALUE,
    CAST(VIOLATION5MINDEGREE AS DOUBLE) AS VIOLATION5MINDEGREE,
    CAST(VIOLATION60SECDEGREE AS DOUBLE) AS VIOLATION60SECDEGREE,
    CAST(VIOLATION6SECDEGREE AS DOUBLE) AS VIOLATION6SECDEGREE,
    CAST(VIOLATIONDEGREE AS DOUBLE) AS VIOLATIONDEGREE,
    CAST(LOWERREG AS DOUBLE) AS LOWERREG,
    CAST(RAISEREG AS DOUBLE) AS RAISEREG,
    CAST(AVAILABILITY AS DOUBLE) AS AVAILABILITY,
    CAST(RAISE6SECFLAGS AS DOUBLE) AS RAISE6SECFLAGS,
    CAST(RAISE60SECFLAGS AS DOUBLE) AS RAISE60SECFLAGS,
    CAST(RAISE5MINFLAGS AS DOUBLE) AS RAISE5MINFLAGS,
    CAST(RAISEREGFLAGS AS DOUBLE) AS RAISEREGFLAGS,
    CAST(LOWER6SECFLAGS AS DOUBLE) AS LOWER6SECFLAGS,
    CAST(LOWER60SECFLAGS AS DOUBLE) AS LOWER60SECFLAGS,
    CAST(LOWER5MINFLAGS AS DOUBLE) AS LOWER5MINFLAGS,
    CAST(LOWERREGFLAGS AS DOUBLE) AS LOWERREGFLAGS,
    CAST(RAISEREGAVAILABILITY AS DOUBLE) AS RAISEREGAVAILABILITY,
    CAST(RAISEREGENABLEMENTMAX AS DOUBLE) AS RAISEREGENABLEMENTMAX,
    CAST(RAISEREGENABLEMENTMIN AS DOUBLE) AS RAISEREGENABLEMENTMIN,
    CAST(LOWERREGAVAILABILITY AS DOUBLE) AS LOWERREGAVAILABILITY,
    CAST(LOWERREGENABLEMENTMAX AS DOUBLE) AS LOWERREGENABLEMENTMAX,
    CAST(LOWERREGENABLEMENTMIN AS DOUBLE) AS LOWERREGENABLEMENTMIN,
    CAST(RAISE6SECACTUALAVAILABILITY AS DOUBLE) AS RAISE6SECACTUALAVAILABILITY,
    CAST(RAISE60SECACTUALAVAILABILITY AS DOUBLE) AS RAISE60SECACTUALAVAILABILITY,
    CAST(RAISE5MINACTUALAVAILABILITY AS DOUBLE) AS RAISE5MINACTUALAVAILABILITY,
    CAST(RAISEREGACTUALAVAILABILITY AS DOUBLE) AS RAISEREGACTUALAVAILABILITY,
    CAST(LOWER6SECACTUALAVAILABILITY AS DOUBLE) AS LOWER6SECACTUALAVAILABILITY,
    CAST(LOWER60SECACTUALAVAILABILITY AS DOUBLE) AS LOWER60SECACTUALAVAILABILITY,
    CAST(LOWER5MINACTUALAVAILABILITY AS DOUBLE) AS LOWER5MINACTUALAVAILABILITY,
    CAST(LOWERREGACTUALAVAILABILITY AS DOUBLE) AS LOWERREGACTUALAVAILABILITY,
    1 AS source_priority,
    {{ parse_filename('filename') }} AS daily_file,
    CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
    CAST(SETTLEMENTDATE AS DATE) AS DATE,
    CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
    CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
      + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
  FROM scada_daily
),

-- Intraday preliminary rows: only the columns SCADA carries; UNION ALL BY NAME fills every
-- daily-only column with NULL. INTERVENTION is 0 (SCADA has no intervention concept).
intraday_rows AS (
  SELECT
    DUID,
    CAST(SCADAVALUE AS DOUBLE) AS INITIALMW,
    CAST(0 AS DOUBLE) AS INTERVENTION,
    0 AS source_priority,
    {{ parse_filename('filename') }} AS intraday_file,
    CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
    CAST(SETTLEMENTDATE AS DATE) AS DATE,
    CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
    CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
      + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
  FROM scada_intraday
),

unioned AS (
  SELECT * FROM daily_rows
  UNION ALL BY NAME
  SELECT * FROM intraday_rows
)

SELECT
  *,
  -- Authoritative source file for the surviving row (daily once settled, else the intraday file).
  COALESCE(daily_file, intraday_file) AS file
FROM unioned
-- One row per business key for the merge source: daily beats intraday, then latest file wins.
-- This is the dedup the duplicate-source-key cardinality guard requires.
QUALIFY row_number() OVER (
  PARTITION BY DUID, SETTLEMENTDATE, INTERVENTION
  ORDER BY source_priority DESC, COALESCE(daily_file, intraday_file) DESC
) = 1
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
