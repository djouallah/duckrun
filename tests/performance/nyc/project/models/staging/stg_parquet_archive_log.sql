-- Staging table over the archive log the layout repo's downloader writes to the landing lakehouse
-- (Files/parquet_raw_archive_log.parquet). Ported verbatim (duckdb leg). Insert-only merge keyed
-- on (source_type, source_filename); the WHERE keeps the merge source to just the unlogged rows,
-- and reading {{ this }} in the body puts the read and the commit on one snapshot.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['source_type', 'source_filename'],
    schema='landing'
) }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    file_stem,
    columns
FROM read_parquet('{{ get_root_path() }}/parquet_raw_archive_log.parquet')
{% if is_incremental() %}
WHERE (source_type, source_filename) NOT IN (SELECT source_type, source_filename FROM {{ this }})
{% endif %}
