{%- macro get_root_path() -%}
{{ env_var('FILES_PATH', '/tmp') | trim }}
{%- endmacro -%}
