{#
  Shared flow for duckrun's Delta-backed materializations (table / incremental / delta).

  Two modes:

  - delta_scan mode (default): DuckDB executes the model SQL into a staging view; the
    delta-write plugin reads that view and materializes a Delta table; the model relation
    becomes a `delta_scan` view so downstream ref() resolves.

  - attach state mode (target.delta_attach set): dbt's `database` is a read-only
    `delta_classic` attach, so `{{ this }}` already resolves to the existing Delta table.
    We must NOT create inside that read-only catalog: stage in the writable in-memory
    `memory` catalog, write the Delta table via delta_rs, and skip the relation view — the
    delta_classic attach surfaces the (latest) table from the directory on every query.
#}

{% macro duckrun__build_delta(model_sql, pre_hooks, post_hooks, is_incremental) %}

  {%- set target_relation = this.incorporate(type='view') -%}

  {#-- attach state mode when the dbt database is a read-only delta_classic attach. --#}
  {%- set attach_mode = target.delta_attach -%}
  {%- set stage_db = 'memory' if attach_mode else target_relation.database -%}

  {#-- Fully-qualified staging relation so the plugin's cursor can resolve it regardless
       of the connection's default catalog/schema. In attach mode it lives in the writable
       `memory` catalog (the attach catalog is read-only). --#}
  {%- set tmp_relation = api.Relation.create(
        database=stage_db,
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

  {#-- Create the schema we actually write into. In attach mode that's the writable
       staging schema in `memory`; the read-only delta_classic catalog must be left alone. --#}
  {%- if attach_mode -%}
    {%- do adapter.create_schema(tmp_relation) -%}
  {%- else -%}
    {%- do adapter.create_schema(target_relation) -%}
  {%- endif -%}

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

  {#-- 3. Drop the staging view --#}
  {% call statement('drop_stage') -%}
    drop view if exists {{ tmp_relation }}
  {%- endcall %}

  {#-- 4. Surface the model relation.
       - delta_scan mode: (re)create a delta_scan view as the model relation.
       - attach mode: nothing to create — the read-only delta_classic catalog exposes the
         table from <location> on every query (PIN_SNAPSHOT off => always latest). The
         `main` statement is still required by dbt, so run a no-op. --#}
  {%- if attach_mode -%}
    {% call statement('main') -%}
      select 1
    {%- endcall %}
  {%- else -%}
    {% call statement('main') -%}
      create or replace view {{ target_relation }} as
        select * from delta_scan('{{ location }}')
    {%- endcall %}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {%- if not attach_mode -%}
    {% do persist_docs(target_relation, model) %}
  {%- endif -%}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}
