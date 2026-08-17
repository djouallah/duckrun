{#-- Ported verbatim from the layout repo: target.schema (DBT_SCHEMA) is the redirect lever.
       - DBT_SCHEMA == 'mart'      -> +schema verbatim (landing, mart) — the layout repo's layout
       - DBT_SCHEMA=anything_else  -> '<DBT_SCHEMA>_<+schema>' (nyc_landing, nyc_mart)
     The benchmark defaults DBT_SCHEMA to 'nyc' (profiles.yml), so it never collides with the
     aemo CI's landing/mart schemas in the shared `duckrun` lakehouse. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.schema == 'mart' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema ~ '_' ~ (custom_schema_name | trim) }}
    {%- endif -%}
{%- endmacro %}
