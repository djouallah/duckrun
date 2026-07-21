{#
  dbt `snapshot` materialization, backed by Delta Lake.

  Why duckrun needs its own: dbt's default snapshot materialization runs `create table` /
  `merge` as SQL DDL/DML against the warehouse. On duckrun the warehouse is Delta + an
  in-memory DuckDB session, so the default snapshot lands only in the in-memory catalog and is
  LOST across processes — a snapshot appears to work within one `dbt` invocation but never
  persists (real SCD2 is cross-invocation). This materialization persists the snapshot to Delta
  via the same store path the table/incremental materializations use.

  How it maps onto duckrun's supported merge (no SQL MERGE, no merge_clauses):

    * First run / --full-refresh: `build_snapshot_table` SELECT -> overwrite the Delta table.
    * Subsequent runs: `snapshot_staging_table` (dbt's own SCD2 change detection) produces the
      'insert' rows (new versions, fresh dbt_scd_id) and the 'update'/'delete' rows (close the
      open version, carry the new dbt_valid_to under the *existing* dbt_scd_id). We project away
      the dbt_change_type / dbt_unique_key helper columns and MERGE on dbt_scd_id with
      merge_update_columns=[dbt_valid_to]:
        - close rows  -> matched     -> update dbt_valid_to    (only that column)
        - insert rows -> not matched -> insert the new version
      The change_type partition is implicit: closes share the open version's scd_id (so they
      match), inserts get a brand-new scd_id (so they don't). This reproduces dbt's
      default__snapshot_merge_sql exactly with the merge controls delta_rs can express.
#}
{% materialization snapshot, adapter='duckrun' %}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set p = duckrun__delta_paths() -%}
  {%- set target_relation = p['target'] -%}
  {%- set tmp_relation = p['tmp'] -%}
  {%- set location = p['location'] -%}
  {%- set _loc_sql = location | replace("'", "''") -%}

  {#-- Version/existence of the Delta table, captured before the model reads anything (one log open
       via delta_state, not two), so the merge can pin OCC to it (single snapshot for the staging
       read and the merge commit). --#}
  {%- set _delta_state = adapter.delta_state(location) -%}
  {%- set read_version = _delta_state['version'] -%}
  {%- set exists = _delta_state['exists'] -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {%- do adapter.create_schema(target_relation) -%}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", model['config'], exists) %}

  {% if not exists or should_full_refresh() %}

    {#-- First run (or full refresh): build the initial snapshot and overwrite the Delta table. --#}
    {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
    {{ check_time_data_types(build_sql) }}

    {% call statement('stage_model') -%}
      create or replace view {{ tmp_relation }} as {{ build_sql }}
    {%- endcall %}
    {{ adapter.commit() }}
    {%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}
    {% do adapter.store_relation('duckrun', tmp_relation, columns, location, 'delta', {
        'incremental': false,
        'full_refresh': true,
        'invocation_id': invocation_id,
    }) %}
    {% call statement('drop_stage') -%}
      drop view if exists {{ tmp_relation }}
    {%- endcall %}

  {% else %}

    {% set snapshot_cols = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() %}
    {{ adapter.assert_valid_snapshot_target_given_strategy(target_relation, snapshot_cols, strategy) }}

    {#-- Pin the existing snapshot read to read_version so the staging change-detection sees one
         consistent version (matches the merge's OCC pin below). --#}
    {% call statement('register_this') -%}
      create or replace view {{ target_relation }} as
        select * from delta_scan('{{ _loc_sql }}'{% if read_version is not none %}, version => {{ read_version }}{% endif %})
    {%- endcall %}

    {% set staging_sql = snapshot_staging_table(strategy, sql, target_relation) %}
    {{ check_time_data_types(staging_sql) }}
    {% call statement('stage_model') -%}
      create or replace view {{ tmp_relation }} as {{ staging_sql }}
    {%- endcall %}
    {{ adapter.commit() }}

    {#-- Drop dbt's staging-only helper columns; the merge source must match the snapshot table. --#}
    {% set remove_columns = ['dbt_change_type', 'DBT_CHANGE_TYPE', 'dbt_unique_key', 'DBT_UNIQUE_KEY'] %}
    {% if unique_key | is_list %}
      {% for key in strategy.unique_key %}
        {% do remove_columns.append('dbt_unique_key_' + loop.index|string) %}
        {% do remove_columns.append('DBT_UNIQUE_KEY_' + loop.index|string) %}
      {% endfor %}
    {% endif %}
    {% set source_columns = adapter.get_columns_in_relation(tmp_relation)
                                 | rejectattr('name', 'in', remove_columns) | list %}
    {% set col_csv = source_columns | map(attribute='name') | join(', ') %}

    {%- set merge_src = api.Relation.create(
          database=target_relation.database,
          schema=target_relation.schema,
          identifier=target_relation.identifier ~ '__duckrun_snap_src',
          type='view') -%}
    {% call statement('stage_merge_src') -%}
      create or replace view {{ merge_src }} as select {{ col_csv }} from {{ tmp_relation }}
    {%- endcall %}
    {{ adapter.commit() }}
    {%- set columns = adapter.get_columns_in_relation(merge_src) -%}

    {#-- Merge on dbt_scd_id; matched (closes) update only dbt_valid_to, unmatched (new versions)
         insert. read_version pins OCC to the version the staging read.

         on_schema_change='append_new_columns' matches dbt's default snapshot, which unconditionally
         appends new source columns (get_missing_columns -> create_columns). It evolves the Delta
         schema additively when a column appears upstream (new versions carry it), never drops
         columns, and is a no-op when nothing was added. Hardcoded, not config.get(): dbt's
         SnapshotConfig.on_schema_change defaults to 'ignore', so a passthrough would silently keep
         dropping columns. The added column is evolved as a metadata-only commit BEFORE the merge
         (engine.merge_delta_clauses), so it reads NULL on the already-closed version — the close row's
         update touches only dbt_valid_to — matching dbt-core's default snapshot exactly. --#}
    {% do adapter.store_relation('duckrun', merge_src, columns, location, 'delta', {
        'incremental': true,
        'incremental_strategy': 'merge',
        'unique_key': snapshot_cols.dbt_scd_id,
        'merge_update_columns': [snapshot_cols.dbt_valid_to],
        'read_version': read_version,
        'dbt_believes_exists': true,
        'full_refresh': false,
        'on_schema_change': 'append_new_columns',
        'invocation_id': invocation_id,
    }) %}

    {% call statement('drop_stage') -%}
      drop view if exists {{ merge_src }}; drop view if exists {{ tmp_relation }}
    {%- endcall %}

  {% endif %}

  {#-- Surface the snapshot as a delta_scan view over the freshly written HEAD. --#}
  {%- do adapter.create_schema(target_relation) -%}
  {% call statement('main') -%}
    create or replace view {{ target_relation }} as select * from delta_scan('{{ _loc_sql }}')
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
