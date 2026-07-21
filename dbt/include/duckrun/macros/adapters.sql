{#
  Adapter-level overrides. Only what duckrun must change on top of dbt-duckdb lives here.
#}

{% macro duckrun__drop_relation(relation) -%}
  {%- if duckrun__is_native_catalog(relation.database) -%}
    {#-- DuckDB's Iceberg catalog rejects DROP ... CASCADE ("not supported for Iceberg tables",
         first live integration_tests_iceberg run — dbt-duckdb's default drop always cascades).
         Nothing in such a catalog holds dependents duckrun creates, so a plain drop is
         equivalent. Every other relation keeps dbt-duckdb's drop untouched. --#}
    {% call statement('drop_relation', auto_begin=False) -%}
      drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
  {%- else -%}
    {{ duckdb__drop_relation(relation) }}
  {%- endif -%}
{% endmacro %}
