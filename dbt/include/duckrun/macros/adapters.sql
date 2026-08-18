{#
  Column introspection, pinned to the catalog (issue #59).

  dbt-duckdb 1.11.0 rewrote duckdb__get_columns_in_relation from this information_schema query
  to ``describe {{ relation }}`` (to spare DuckLake an information_schema scan). Under duckrun
  every relation is a view over delta_scan(...), so each DESCRIBE re-binds that view: the delta
  extension LISTs _delta_log, reads _last_checkpoint and GETs commit JSONs — one remote log
  replay per column introspection, measured at +19% storage metadata round-trips per run on
  OneLake (issue #59, strace bisect). The catalog answer is identical for every relation duckrun
  registers (a view's columns are resolved when the view is created), so pin dbt-duckdb 1.10.1's
  catalog-only SQL under the duckrun__ prefix, which adapter.dispatch resolves before duckdb__ on
  either dbt-duckdb version — no dependency cap needed.

  The one case the catalog cannot answer — a delta_scan view never registered in this session
  (``dbt run-operation`` skips cache population) — is already handled in Python:
  impl.get_columns_in_relation binds that one view on an empty result and asks again (issue #24).
#}
{% macro duckrun__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from system.information_schema.columns
      where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and lower(table_schema) = '{{ relation.schema | lower }}'
      {% endif %}
      {% if relation.database %}
      and lower(table_catalog) = '{{ relation.database | lower }}'
      {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}
