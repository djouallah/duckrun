-- The wide raw fact under layout test — 591.7M rows over 17 columns of skewed categoricals and
-- money measures. Ported from djouallah/direct-lake-parquet-layout
-- models/nyc/duckdb/marts/fct_trips.sql; the model body, config and pre_hook are verbatim so this
-- benchmark builds the SAME table the layout runs measure. See that file's header for the full
-- reasoning behind the two deliberate oddities kept here:
--   * incremental_strategy='append': TLC trip records have NO natural unique key (duplicate trips
--     are a documented feature), the only candidate (file) is 3M rows per file, and duckrun
--     refuses a non-unique merge key on both write paths. The file-selection pre_hook guards the
--     write; assert_fct_trips_matches_archive_log is the detector.
--   * pickup_date is a stored DATE column: Direct Lake cannot relate a DATETIME to a DATE
--     dimension key, and the near-contiguous values RLE well under the default sort.
-- The benchmark itself always runs `dbt build --full-refresh`, so the append path never actually
-- increments here — the overwrite branch (and its SORTED BY AUTO profile) is what gets measured.
{%- set pending_files_query -%}
SELECT file_stem FROM {{ ref('stg_parquet_archive_log') }}
WHERE source_type = 'yellow'
{%- if is_incremental() %}
AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(pending_files_query) -%}
  {%- set pending_files = files_result.columns[0].values() | list if files_result else [] -%}
{%- else -%}
  {#-- Parse time: unknowable. `none` means "assume there is work", so the model renders its real
       body rather than the no-op branch. --#}
  {%- set pending_files = none -%}
{%- endif -%}
{%- set has_files = pending_files is none or pending_files | length > 0 -%}

{%- set cols = nyc_trip_columns() -%}

{#-- Same dispatch knobs as the layout repo: DUCKDB_SORTED gates sorting entirely, DUCKDB_SORT_BY
     is 'auto' (duckrun's picker — must stay a SCALAR) or a comma-separated column list, and the
     geometry knobs default to duckrun's adaptive 'auto'. nyc_bench.py forwards them, so a
     dispatch can sweep layouts exactly as the layout repo does. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    sort_by=(('auto' if env_var('DUCKDB_SORT_BY', 'auto').lower() == 'auto'
              else env_var('DUCKDB_SORT_BY', 'auto').split(','))
             if env_var('DUCKDB_SORTED', 'true') == 'true' else none),
    max_row_group_size=(none if env_var('DUCKDB_ROW_GROUP_SIZE', 'auto').lower() == 'auto'
                        else env_var('DUCKDB_ROW_GROUP_SIZE', 'auto') | int),
    target_file_size_mb=(none if env_var('DUCKDB_FILE_SIZE_MB', 'auto').lower() == 'auto'
                         else env_var('DUCKDB_FILE_SIZE_MB', 'auto') | int),
    pre_hook="SET VARIABLE nyc_yellow_paths = (SELECT COALESCE(NULLIF(list('{{ get_parquet_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_parquet_archive_log') }} WHERE source_type = 'yellow'{% if is_incremental() %} AND file_stem NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{#-- A plain read: the archive is HOMOGENEOUS by construction (the downloader rewrites every month
     to the canonical 17-column schema before uploading), so no union_by_name and no schema
     merging. The CASTs are no-ops over already-canonical data, kept as the explicit declaration
     of the stored types. --#}
WITH trips AS (
  SELECT *
  FROM read_parquet(
    getvariable('nyc_yellow_paths'),
    filename = 1,
    hive_partitioning = false
  )
)

SELECT
  {%- for name in cols %}
  CAST({{ name }} AS {{ nyc_trip_type(name, 'duckdb') }}) AS {{ name }},
  {%- endfor %}
  CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
  {{ parse_filename('filename') }} AS file
FROM trips
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
