{#
  Shared flow for duckrun's Delta-backed materializations (table / incremental / delta).

  DuckDB executes the model SQL into a staging view; the delta-write plugin reads that
  view and materializes a Delta table; the model relation becomes a delta_scan view so
  downstream ref() resolves.
#}

{% macro duckrun__build_delta(model_sql, pre_hooks, post_hooks, is_incremental) %}

  {%- set target_relation = this.incorporate(type='view') -%}
  {#-- Fully-qualified staging relation so the plugin's cursor can resolve it
       regardless of the connection's default catalog/schema. --#}
  {%- set tmp_relation = api.Relation.create(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier ~ '__duckrun_tmp',
        type='view') -%}

  {#-- Resolve the Delta location: explicit config wins, else <root_path>/<schema>/<id> --#}
  {%- set location = config.get('location') -%}
  {%- if not location -%}
    {%- set root_path = target.root_path -%}
    {%- if not root_path -%}
      {{ exceptions.raise_compiler_error(
          "duckrun: model '" ~ model.name ~ "' needs config(location=...) or a 'root_path' in the profile.") }}
    {%- endif -%}
    {%- set location = root_path ~ '/' ~ target_relation.schema ~ '/' ~ target_relation.identifier -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {%- do adapter.create_schema(target_relation) -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- 1. Stage the model SQL as a DuckDB view so the plugin can read it as Arrow --#}
  {% call statement('stage_model') -%}
    create or replace view {{ tmp_relation }} as {{ model_sql }}
  {%- endcall %}

  {#-- Commit so the staging view is visible to the plugin's cursor (cross-cursor
       isolation otherwise hides uncommitted DDL). --#}
  {{ adapter.commit() }}

  {%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}

  {#-- 2. Hand off to the delta-write plugin (store -> write_deltalake / merge) --#}
  {%- set delta_config = {
      'incremental': is_incremental,
      'incremental_strategy': config.get('incremental_strategy'),
      'full_refresh': should_full_refresh(),
      'unique_key': config.get('unique_key'),
      'partition_by': config.get('partition_by'),
      'merge_schema': config.get('merge_schema', false),
      'storage_options': config.get('storage_options'),
  } -%}
  {% do adapter.store_relation('duckrun', tmp_relation, columns, location, 'delta', delta_config) %}

  {#-- 3. Replace the staging view with a delta_scan view as the model relation --#}
  {% call statement('drop_stage') -%}
    drop view if exists {{ tmp_relation }}
  {%- endcall %}

  {% call statement('main') -%}
    create or replace view {{ target_relation }} as
      select * from delta_scan('{{ location }}')
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}
