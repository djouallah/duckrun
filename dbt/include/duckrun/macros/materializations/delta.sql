{#
  `delta` materialization — explicit alias for the Delta-backed table/incremental flow.

  Equivalent to `table`, but honors config(incremental=true) to opt into the
  incremental path. Prefer the standard `table` / `incremental` materializations;
  this name is kept for convenience and backward compatibility.
#}
{% materialization delta, adapter='duckrun' %}
  {% set result = duckrun__build_delta(sql, pre_hooks, post_hooks, config.get('incremental', false)) %}
  {{ return(result) }}
{% endmaterialization %}
