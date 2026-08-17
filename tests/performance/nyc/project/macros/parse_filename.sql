{#-- Extract the file "stem": the last path segment, then everything before the first '.'.
    e.g. '.../parquet_raw/yellow/yellow_tripdata_2019-06.parquet' -> 'yellow_tripdata_2019-06'.
    Ported verbatim from the layout repo; only the DuckDB branch runs here. --#}
{% macro parse_filename(filepath) %}
  {%- if target.type == 'fabric' -%}
    {%- set fn -%}RIGHT({{ filepath }}, CHARINDEX('/', REVERSE({{ filepath }}) + '/') - 1){%- endset -%}
    CAST(LEFT({{ fn }}, CHARINDEX('.', {{ fn }} + '.') - 1) AS VARCHAR(256))
  {%- elif target.type == 'fabricspark' -%}
    substring_index(element_at(split({{ filepath }}, '/'), -1), '.', 1)
  {%- else -%}
    split_part(split_part({{ filepath }}, '/', -1), '.', 1)
  {%- endif -%}
{% endmacro %}
