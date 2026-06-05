{#
  `delta` materialization — explicit alias for the Delta-backed table/incremental flow.

  Equivalent to `table`, but honors config(incremental=true) to opt into the
  incremental path. Prefer the standard `table` / `incremental` materializations;
  this name is kept for convenience and backward compatibility.
#}
{% materialization delta, adapter='duckrun', supported_languages=['sql', 'python'] %}
  {%- if model['language'] == 'python' -%}
    {#-- Stage the python model here (not in a helper) so dbt's submit_python_job
         depth-2 macro guard (materialization -> statement) is satisfied. --#}
    {%- set p = duckrun__delta_paths() -%}
    {%- do adapter.create_schema(p['tmp']) -%}
    {% call statement('stage_model', language='python') -%}
      {{- create_table_as(False, p['tmp'], compiled_code, 'python') }}
    {%- endcall %}
  {%- endif -%}
  {% set result = duckrun__build_delta(sql, pre_hooks, post_hooks, config.get('incremental', false)) %}
  {{ return(result) }}
{% endmaterialization %}
