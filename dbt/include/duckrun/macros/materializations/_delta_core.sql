{#
  Shared flow for duckrun's Delta-backed materializations (table / incremental / delta).

  State lives in Delta Lake. DuckDB executes the model into a staging relation; the
  delta-write plugin (delta_rs) materializes the Delta table; the model relation then becomes
  a `delta_scan` view so downstream ref() and the model's own `{{ this }}` resolve. A brand
  new table is just a `create or replace view` — instantly visible to the rest of the run,
  with no attach/re-attach. Cross-process state comes from run-start discovery in the adapter
  (list_relations_without_caching rebuilds these views from disk).

  Python models: dbt's `submit_python_job` may only be called directly from a
  materialization macro (depth-2 guard), so python staging is done in the materialization
  wrappers (table/incremental/delta.sql) via `duckrun__stage_python()`, and this macro
  skips its own staging for python.
#}

{#-- Compute the relations/location used by both the materialization wrappers (python
     staging) and duckrun__build_delta. Deterministic; returns values only (no SQL). --#}
{% macro duckrun__delta_paths() %}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set is_py = (model['language'] == 'python') -%}
  {%- set tmp_relation = api.Relation.create(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier ~ '__duckrun_tmp',
        type=('table' if is_py else 'view')) -%}
  {%- set location = config.get('location') -%}
  {%- if not location -%}
    {%- set root_path = target.root_path -%}
    {%- if not root_path -%}
      {{ exceptions.raise_compiler_error(
          "duckrun: model '" ~ model.name ~ "' needs config(location=...) or a 'root_path' in the profile.") }}
    {%- endif -%}
    {%- set location = root_path ~ '/' ~ target_relation.schema ~ '/' ~ target_relation.identifier -%}
  {%- endif -%}
  {{ return({'target': target_relation, 'tmp': tmp_relation, 'location': location}) }}
{% endmacro %}


{% macro duckrun__build_delta(model_sql, pre_hooks, post_hooks, is_incremental) %}

  {%- set language = model['language'] -%}
  {%- set p = duckrun__delta_paths() -%}
  {%- set target_relation = p['target'] -%}
  {%- set tmp_relation = p['tmp'] -%}
  {%- set location = p['location'] -%}

  {#-- Pre-register {{ this }} as a delta_scan view when the Delta table already exists on
       disk, so pre-hooks and the model's own SQL (is_incremental self-reference) can read the
       current state. Disk discovery already reports the relation as existing in dbt's cache
       (so is_incremental() is true); this creates the matching physical view on the run-phase
       connection (views created during cache population don't survive to model run). --#}
  {%- if adapter.delta_table_exists(location) -%}
    {%- do adapter.create_schema(target_relation) -%}
    {% call statement('register_this') -%}
      create or replace view {{ target_relation }} as select * from delta_scan('{{ location }}')
    {%- endcall %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#-- Create the schema we write into. For python the staging table (and its schema) was
       already created in the materialization wrapper. --#}
  {%- if language != 'python' -%}
    {%- do adapter.create_schema(target_relation) -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- 1. Stage the model SQL as a DuckDB view so the plugin can read it as Arrow.
       (python models are already staged as a table by the materialization wrapper) --#}
  {%- if language != 'python' -%}
    {% call statement('stage_model') -%}
      create or replace view {{ tmp_relation }} as {{ model_sql }}
    {%- endcall %}
  {%- endif -%}

  {#-- Commit so the staging relation is visible to the plugin's cursor (cross-cursor
       isolation otherwise hides uncommitted DDL). --#}
  {{ adapter.commit() }}

  {%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}

  {#-- microbatch batch bounds: dbt sets model.batch (a BatchContext) per batch run; the
       values are UTC datetimes. Render them as naive 'YYYY-MM-DD HH:MM:SS' for the plugin's
       delete+insert window. None for every non-microbatch model. --#}
  {%- set _batch = model.get('batch') -%}
  {%- set _batch_start = _batch.get('event_time_start').strftime('%Y-%m-%d %H:%M:%S') if _batch and _batch.get('event_time_start') else none -%}
  {%- set _batch_end = _batch.get('event_time_end').strftime('%Y-%m-%d %H:%M:%S') if _batch and _batch.get('event_time_end') else none -%}

  {#-- 2. Hand off to the delta-write plugin (store -> write_deltalake / merge) --#}
  {%- set delta_config = {
      'incremental': is_incremental,
      'incremental_strategy': config.get('incremental_strategy'),
      'full_refresh': should_full_refresh(),
      'unique_key': config.get('unique_key'),
      'partition_by': config.get('partition_by'),
      'merge_schema': config.get('merge_schema', false),
      'storage_options': config.get('storage_options'),
      'merge_update_columns': config.get('merge_update_columns'),
      'merge_exclude_columns': config.get('merge_exclude_columns'),
      'merge_max_spill_size': config.get('merge_max_spill_size'),
      'merge_streamed_exec': config.get('merge_streamed_exec'),
      'incremental_predicates': config.get('incremental_predicates') or config.get('predicates'),
      'on_schema_change': config.get('on_schema_change', 'ignore'),
      'event_time': config.get('event_time'),
      'batch_start': _batch_start,
      'batch_end': _batch_end,
      'invocation_id': invocation_id,
  } -%}
  {% do adapter.store_relation('duckrun', tmp_relation, columns, location, 'delta', delta_config) %}

  {#-- 3. Drop the staging relation (a table for python, a view for sql) --#}
  {% call statement('drop_stage') -%}
    {% if language == 'python' %}drop table if exists {{ tmp_relation }}{% else %}drop view if exists {{ tmp_relation }}{% endif %}
  {%- endcall %}

  {#-- 4. Surface the model relation as a delta_scan view over the freshly written Delta
       table. A brand-new table becomes visible to the rest of the run right here. --#}
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
