{#
  duckrun `seed` materialization, backed by Delta Lake.

  dbt-core's default seed loads the CSV into a NATIVE in-memory DuckDB table (`create table` +
  `COPY`) that lives only in the building connection. So a fresh process (`dbt docs generate`, or a
  `dbt build --select <subtree>` that doesn't select the seed) can't see it, and a compile-time
  `run_query`/`ref()` against the seed fails ("Table ... does not exist"). Everything else in duckrun
  persists to Delta and is rediscovered from disk at run start (list_relations_without_caching) — a
  seed must too, or it's the one stateless relation in an otherwise stateless-across-processes adapter.

  So we load the CSV into a native *staging* table (reusing dbt-duckdb's fast typed-create + COPY, so
  `column_types`/`delimiter`/`quote_columns` behave exactly as on dbt-duckdb), hand it to the
  delta-write plugin (delta_rs) exactly like a `table` model, then surface the seed as a `delta_scan`
  view. The staging table is named `<seed>__duckrun_tmp` — NOT `<seed>` — so its `drop table` can't be
  taken by delta_dml for a drop of the freshly-written Delta table at the seed's own location (which
  would tombstone it); the tmp location never holds a Delta table. This mirrors _delta_core's
  python-model staging.
#}
{% materialization seed, adapter='duckrun' %}

  {%- set agate_table = load_agate_table() -%}
  {#-- dbt reads this back (e.g. `dbt show`, logging of seed rows). --#}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set tmp_relation = api.Relation.create(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier ~ '__duckrun_tmp',
        type='table') -%}

  {#-- Delta location: config(location=...) wins, else root_path/<schema>/<seed> (same as a model). --#}
  {%- set location = config.get('location') -%}
  {%- if not location -%}
    {%- set root_path = target.root_path -%}
    {%- if not root_path -%}
      {{ exceptions.raise_compiler_error(
          "duckrun: seed '" ~ model.name ~ "' needs config(location=...) or a 'root_path' in the profile.") }}
    {%- endif -%}
    {%- set location = root_path ~ '/' ~ target_relation.schema ~ '/' ~ target_relation.identifier -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {%- do adapter.create_schema(target_relation) -%}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- 1. Stage the seed CSV as a NATIVE DuckDB table: a typed CREATE (honoring column_types) then a
        fast COPY (the dbt-duckdb seed path). The `__duckrun_tmp` name and the `or replace` keep it
        native (delta_dml passes both through) and clobber any leftover from a same-process re-run. --#}
  {%- set column_override = config.get('column_types', {}) -%}
  {%- set quote_seed_column = config.get('quote_columns', None) -%}
  {%- set delimiter = config.get('delimiter', ',') -%}
  {% call statement('create_seed_tmp') -%}
    create or replace table {{ tmp_relation }} (
      {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set col_type = column_override.get(col_name, inferred_type) -%}
        {{ adapter.quote_seed_column(col_name, quote_seed_column) }} {{ col_type }}{%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
  {%- endcall %}
  {% call statement('load_seed_tmp') -%}
    copy {{ tmp_relation }} from '{{ adapter.get_seed_file_path(model) }}' (format csv, header true, delimiter '{{ delimiter }}')
  {%- endcall %}

  {#-- Commit so the staged relation is visible to the plugin's cursor (cross-cursor isolation
        otherwise hides uncommitted DDL) — same as _delta_core. --#}
  {{ adapter.commit() }}

  {%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}

  {#-- 2. Hand the staged rows to the delta-write plugin: a plain overwrite, exactly like `table`. --#}
  {%- set delta_config = {
      'incremental': false,
      'full_refresh': should_full_refresh(),
      'storage_options': config.get('storage_options'),
      'partition_by': config.get('partition_by'),
  } -%}
  {% do adapter.store_relation('duckrun', tmp_relation, columns, location, 'delta', delta_config) %}

  {#-- 3. Drop the native staging table. Its location holds no Delta table, so delta_dml lets this be
        a real native drop (NOT a tombstone of the seed's Delta table). --#}
  {% call statement('drop_seed_tmp') -%}
    drop table if exists {{ tmp_relation }}
  {%- endcall %}

  {#-- 4. Surface the seed as a delta_scan view over the freshly written Delta table — same read
        surface as every model, so ref()/{{ this }} resolve in this run and after rediscovery. --#}
  {% call statement('main') -%}
    create or replace view {{ target_relation }} as
      select * from delta_scan('{{ location | replace("'", "''") }}')
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {#-- persist_docs: COMMENT ON the in-run view (dbt-duckdb macros) + write descriptions into the
        Delta metadata so a later docs-generate (a fresh process that rebuilds the view from disk)
        still reports them — see impl._apply_delta_comments. Mirrors _delta_core. --#}
  {% do persist_docs(target_relation, model) %}
  {%- set _relation_docs = model.description if (config.persist_relation_docs() and model.description) else none -%}
  {%- set _column_docs = {} -%}
  {%- if config.persist_column_docs() and model.columns -%}
    {%- for _cn, _col in model.columns.items() -%}
      {%- if _col.description -%}
        {%- do _column_docs.update({_col.name: _col.description}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- if _relation_docs or _column_docs -%}
    {% do adapter.persist_delta_docs(location, _relation_docs, _column_docs) %}
  {%- endif -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
