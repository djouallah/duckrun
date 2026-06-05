{% macro parse_filename(filepath) %}
    split_part(split_part({{ filepath }}, '/', -1), '.', 1)
{% endmacro %}
