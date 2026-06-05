{% set csv_archive_path = get_csv_archive_path() %}

{# Check if there are new DUIDs not in the existing table #}
{%- set check_new_duids_query -%}
  SELECT count(*) as cnt FROM (
    SELECT DUID FROM read_csv('{{ csv_archive_path }}/duid/duid_data.csv') WHERE length(DUID) > 2
    UNION
    SELECT "Facility Code" AS DUID FROM read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ) source_duids
  WHERE DUID NOT IN (SELECT DUID FROM {{ this }})
{%- endset -%}

{%- if execute and is_incremental() and flags.WHICH == 'run' -%}
  {%- set result = run_query(check_new_duids_query) -%}
  {%- set has_new_duids = result and result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_new_duids = true -%}
{%- endif -%}

{#-- Delta state is written by delta_rs and {{ this }} is a read-only delta_scan view, so we
     can't DELETE+rebuild in place. dim_duid is small (a few CSVs), so materialize as a table:
     delta_rs overwrites the whole dimension every run. is_incremental() is then always false,
     so has_new_duids resolves true above and the full-rebuild branch below runs. --#}
{{ config(
    materialized='table'
) }}

-- Ensure download runs first by depending on stg_csv_archive_log
-- {{ ref('stg_csv_archive_log') }}

{% if has_new_duids %}
WITH
  states AS (
    SELECT 'WA1' AS RegionID, 'Western Australia' AS State
    UNION ALL SELECT 'QLD1', 'Queensland'
    UNION ALL SELECT 'NSW1', 'New South Wales'
    UNION ALL SELECT 'TAS1', 'Tasmania'
    UNION ALL SELECT 'SA1', 'South Australia'
    UNION ALL SELECT 'VIC1', 'Victoria'
  ),

  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      first(Participant) AS Participant
    FROM
      read_csv('{{ csv_archive_path }}/duid/duid_data.csv')
    WHERE
      length(DUID) > 2
    GROUP BY
      DUID
  ),

  wa_facilities AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM
      read_csv_auto('{{ csv_archive_path }}/duid/facilities.csv')
  ),

  wa_energy AS (
    SELECT *
    FROM read_csv_auto('{{ csv_archive_path }}/duid/WA_ENERGY.csv', header = 1)
  ),

  duid_wa AS (
    SELECT
      wa_facilities.DUID,
      wa_facilities.Region,
      wa_energy.Technology AS FuelSourceDescriptor,
      wa_facilities.Participant
    FROM wa_facilities
    LEFT JOIN wa_energy ON wa_facilities.DUID = wa_energy.DUID
  ),

  duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
  ),

  geo AS (
    SELECT
      duid,
      max(latitude) as latitude,
      max(longitude) as longitude
    FROM read_csv('{{ csv_archive_path }}/duid/geo_data.csv')
    WHERE latitude IS NOT NULL
    GROUP BY duid
  )

SELECT
  a.DUID,
  first(a.Region) AS Region,
  first(UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2))) AS FuelSourceDescriptor,
  first(a.Participant) AS Participant,
  first(states.State) AS State,
  first(geo.latitude) AS latitude,
  first(geo.longitude) AS longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.duid = geo.duid
GROUP BY a.DUID
{% else %}
-- No new DUIDs found, return empty result to keep existing data
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
