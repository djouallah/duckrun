{#
  Standard dbt `table` materialization, backed by Delta Lake.
  Overwrites the Delta table on every run.
#}
{% materialization table, adapter='duckrun' %}
  {% set result = duckrun__build_delta(sql, pre_hooks, post_hooks, false) %}
  {{ return(result) }}
{% endmaterialization %}
