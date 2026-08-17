{#-- Root of the NYC TLC parquet archive in the landing lakehouse Files section. FILES_PATH is
     forwarded by nyc_bench.py; the landing itself is written by
     djouallah/direct-lake-parquet-layout's downloader and is persistent — this project only
     READS it. --#}
{%- macro get_root_path() -%}
{{ env_var('FILES_PATH', '/tmp') | trim }}
{%- endmacro -%}

{%- macro get_parquet_archive_path() -%}
{{ get_root_path() ~ '/parquet_raw' }}
{%- endmacro -%}
