{#
  Standard dbt `incremental` materialization, backed by Delta Lake.

  First run (or --full-refresh, or table missing) overwrites. Later runs apply
  config('incremental_strategy'):
    - merge  (default when unique_key set): upsert on unique_key
    - insert : insert only new keys (idempotent append / dedupe), requires unique_key
    - delete+insert: delete target rows whose unique_key is in the batch, then insert the WHOLE
      batch (duplicates preserved) — one atomic replaceWhere; requires unique_key. Differs from
      merge: merge updates matched rows and rejects duplicate source keys.
    - append (default when no unique_key): blind append
    - safeappend: optimistic append — commit only if the table version has not moved
      since the write began, else fail (no dedup; that's the model SQL's job)
    - microbatch: delete+insert the model's event_time window per dbt-driven batch
#}
{% materialization incremental, adapter='duckrun', supported_languages=['sql', 'python'] %}
  {%- if model['language'] == 'python' -%}
    {#-- Stage the python model here (not in a helper) so dbt's submit_python_job
         depth-2 macro guard (materialization -> statement) is satisfied. --#}
    {%- set p = duckrun__delta_paths() -%}
    {%- do adapter.create_schema(p['tmp']) -%}
    {% call statement('stage_model', language='python') -%}
      {{- create_table_as(False, p['tmp'], compiled_code, 'python') }}
    {%- endcall %}
  {%- endif -%}
  {% set result = duckrun__build_delta(sql, pre_hooks, post_hooks, true) %}
  {{ return(result) }}
{% endmaterialization %}
