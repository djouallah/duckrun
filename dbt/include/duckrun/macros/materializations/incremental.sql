{#
  Standard dbt `incremental` materialization, backed by Delta Lake.

  First run (or --full-refresh, or table missing) overwrites. Later runs apply
  config('incremental_strategy'):
    - merge  (default when unique_key set): upsert on unique_key
    - insert : insert only new keys (idempotent append / dedupe), requires unique_key
    - append (default when no unique_key): blind append
#}
{% materialization incremental, adapter='duckrun' %}
  {% set result = duckrun__build_delta(sql, pre_hooks, post_hooks, true) %}
  {{ return(result) }}
{% endmaterialization %}
