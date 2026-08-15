{#
  `external` materialization — write the model to a real parquet/csv/json FILE and surface it as
  a view over that file. Unlike every other duckrun materialization, nothing here touches Delta:
  DuckDB's own `COPY ... TO` writes the bytes.

  This is dbt-duckdb's macro, near-verbatim. dbt resolves a materialization by exact adapter name
  with only a `default` fallback (unlike `adapter.dispatch`, which walks duckrun -> duckdb ->
  default because the plugin declares dependencies=['duckdb']), so dbt-duckdb's
  `{% materialization external, adapter="duckdb" %}` is unreachable from `type: duckrun` and an
  external model errored. Copying it under adapter='duckrun' is the whole fix — every helper it
  calls (external_location, render_write_options, write_to_file, store_relation, create_table_as)
  and every adapter method (external_root, external_write_options, external_read_location) is
  already dbt-duckdb's, inherited.

  Keep this a copy: divergence from upstream is a compatibility bug, not an improvement. The one
  intentional difference is that upstream's stray `)` after the `location` line (which renders a
  literal `)` into the output) is not reproduced.

  Note `location` means something different here than in the Delta materializations: for
  table/incremental it is the Delta table path (duckrun__delta_location); here it is the output
  FILE path, defaulting to `{{ external_root }}/{{ identifier }}.{{ format }}`.

  The model relation is a DuckDB view over the file, and disk discovery only rebuilds Delta
  tables — so, exactly as upstream, a run that doesn't rebuild an external model needs
  `on-run-start: "{{ register_upstream_external_models() }}"` to resolve it.
#}
{% materialization external, adapter='duckrun', supported_languages=['sql', 'python'] %}

  {%- set location = render(config.get('location', default=external_location(this, config))) -%}
  {%- set rendered_options = render_write_options(config) -%}

  {%- set format = config.get('format') -%}
  {%- set allowed_formats = ['csv', 'parquet', 'json'] -%}
  {%- if format -%}
      {%- if format not in allowed_formats -%}
          {{ exceptions.raise_compiler_error("Invalid format: " ~ format ~ ". Allowed formats are: " ~ allowed_formats | join(', ')) }}
      {%- endif -%}
  {%- else -%}
    {%- set format = location.split('.')[-1].lower() if '.' in location else 'parquet' -%}
    {%- set format = format if format in allowed_formats else 'parquet' -%}
  {%- endif -%}

  {%- set write_options = adapter.external_write_options(location, rendered_options) -%}
  {%- set read_location = adapter.external_read_location(location, rendered_options) -%}
  {%- set parquet_read_options = config.get('parquet_read_options', {'union_by_name': False}) -%}
  {%- set json_read_options = config.get('json_read_options', {'auto_detect': True}) -%}
  {%- set csv_read_options = config.get('csv_read_options', {'auto_detect': True}) -%}

  -- set language - python or sql
  {%- set language = model['language'] -%}

  {%- set target_relation = this.incorporate(type='view') %}

  -- Continue as normal materialization
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set temp_relation =  make_intermediate_relation(this.incorporate(type='table'), suffix='__dbt_tmp') -%}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation, suffix='__dbt_int') -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_temp_relation = load_cached_relation(temp_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {#-- A NATIVE DuckDB table, not a Delta one: duckrun's cursor only rewrites `create table ... as
       <select>` into a Delta write when the body is an unqualified select, and create_table_as
       always wraps it as `as ( ... )`. See delta_dml._create_as. --#}
  {% call statement('create_table', language=language) -%}
    {{- create_table_as(False, temp_relation, compiled_code, language) }}
  {%- endcall %}

  -- check if relation is empty
  {%- set count_query -%}
    select count(*) as row_count from {{ temp_relation }}
  {%- endset -%}
  {%- set row_count = run_query(count_query) -%}

  -- if relation is empty, write a non-empty table with column names and null values
  {% call statement('main', language='sql') -%}
    {% if row_count[0][0] == 0 %}
    insert into {{ temp_relation }} values (
      {%- for col in get_columns_in_relation(temp_relation) -%}
      NULL,
      {%- endfor -%}
    )
    {% endif %}
  {%- endcall %}

  -- write a temp relation into file
  {{ write_to_file(temp_relation, location, write_options) }}

-- create a view on top of the location
  {% call statement('main', language='sql') -%}
    {% if format == 'json' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_json('{{ read_location }}'
        {%- for key, value in json_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
        -- if relation is empty, filter by all columns having null values
        {% if row_count[0][0] == 0 %}
          where 1
          {%- for col in get_columns_in_relation(temp_relation) -%}
            {{ '' }} AND "{{ col.column }}" is not NULL
          {%- endfor -%}
        {% endif %}
      );
    {% elif format == 'parquet' %}
      create or replace view {{ intermediate_relation }} as (
        select * from read_parquet('{{ read_location }}'
        {%- for key, value in parquet_read_options.items() -%}
          , {{ key }}=
          {%- if value is string -%}
            '{{ value }}'
          {%- else -%}
            {{ value }}
          {%- endif -%}
        {%- endfor -%}
        )
        -- if relation is empty, filter by all columns having null values
        {% if row_count[0][0] == 0 %}
          where 1
          {%- for col in get_columns_in_relation(temp_relation) -%}
            {{ '' }} AND "{{ col.column }}" is not NULL
          {%- endfor -%}
        {% endif %}
      );
    {% elif format == 'csv' %}
    create or replace view {{ intermediate_relation }} as (
      select * from read_csv('{{ read_location }}'
      {%- for key, value in csv_read_options.items() -%}
        , {{ key }}=
        {%- if value is string -%}
          '{{ value }}'
        {%- else -%}
          {{ value }}
        {%- endif -%}
      {%- endfor -%}
      )
      -- if relation is empty, filter by all columns having null values
      {% if row_count[0][0] == 0 %}
        where 1
        {%- for col in get_columns_in_relation(temp_relation) -%}
          {{ '' }} AND "{{ col.column }}" is not NULL
        {%- endfor -%}
      {% endif %}
    );
    {% endif %}
  {%- endcall %}

  -- cleanup
  {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}
  {#-- The one deviation from upstream: drop the staging relation with a bare `drop table if
       exists` instead of drop_relation, which appends CASCADE. A python model is staged by
       py_write_table as `create table … as select …` — the one form duckrun's cursor rewrites into
       a delta_rs write (delta_dml._create_as) — so by now the relation is a delta_scan VIEW, and
       the CASCADE form matches neither the router's `drop table` pattern (which would tombstone it)
       nor a DuckDB view ("Existing object is of type View, trying to drop type Table"). Without
       CASCADE both cases work: a SQL model's native staging table is dropped by DuckDB, a python
       model's Delta staging table is tombstoned. duckrun's own materializations drop the same way
       (_delta_core.sql). --#}
  {% call statement('drop_stage') -%}
    drop table if exists {{ temp_relation }}
  {%- endcall %}

  -- register table into glue
  {%- set plugin_name = config.get('plugin') -%}
  {%- set glue_register = config.get('glue_register', default=false) -%}
  {%- set partition_columns = config.get('partition_columns', []) -%}
  {% if plugin_name is not none or glue_register is true %}
    {% if glue_register %}
      {# legacy hack to set the glue database name, deprecate this #}
      {%- set plugin_name = 'glue|' ~ config.get('glue_database', 'default') -%}
    {% endif %}
    {% do store_relation(plugin_name, target_relation, location, format, config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
