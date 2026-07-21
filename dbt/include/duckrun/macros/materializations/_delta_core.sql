{#
  Shared flow for duckrun's Delta-backed materializations (table / incremental / delta).

  State lives in Delta Lake. DuckDB executes the model into a staging relation; the
  delta-write plugin (delta_rs) materializes the Delta table; the model relation then becomes
  a `delta_scan` view so downstream ref() and the model's own `{{ this }}` resolve. A brand
  new table is just a `create or replace view` — instantly visible to the rest of the run,
  with no attach/re-attach. Cross-process state comes from run-start discovery in the adapter
  (list_relations_without_caching rebuilds these views from disk).

  Python models: dbt's `submit_python_job` may only be called directly from a
  materialization macro (depth-2 guard), so python staging is INLINED in each materialization
  wrapper (table/incremental/delta.sql) — a shared helper would add a macro-stack level and
  trip the guard — and this macro skips its own staging for python.
#}

{#-- Resolve the Delta write location for `relation`: config(location=...) wins, else the write
     root by the relation's database (the default catalog uses target.root_path, a `+database:
     <alias>` naming a declared catalog uses that catalog's root) + /<schema>/<identifier>.
     Shared by duckrun__delta_paths and the seed materialization so the resolution can't drift. --#}
{% macro duckrun__delta_location(relation) %}
  {%- set location = config.get('location') -%}
  {%- if not location -%}
    {%- set _db = relation.database -%}
    {%- set _catalogs = target.catalog_locations or {} -%}
    {%- if _db and _db != target.database and _db in _catalogs -%}
      {%- set root_path = _catalogs[_db] -%}
    {%- else -%}
      {%- set root_path = target.root_path -%}
    {%- endif -%}
    {%- if not root_path -%}
      {{ exceptions.raise_compiler_error(
          "duckrun: '" ~ model.name ~ "' needs config(location=...), a 'root_path' in the profile, or a matching entry in the profile's 'catalogs:' for database '" ~ _db ~ "'.") }}
    {%- endif -%}
    {%- set location = root_path ~ '/' ~ relation.schema ~ '/' ~ relation.identifier -%}
  {%- endif -%}
  {{ return(location) }}
{% endmacro %}


{#-- persist_docs: COMMENT ON the in-run DuckDB view (dbt-duckdb macros), then ALSO write the
     descriptions into the Delta table's own metadata so they survive a later `dbt docs generate`
     (a fresh process rebuilds the views from disk via list_relations_without_caching, which
     re-applies these as COMMENT ON — see impl._apply_delta_comments). Shared by the Delta core
     and the seed materialization. --#}
{% macro _duckrun__persist_docs(target_relation, location) %}
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
{% endmacro %}


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
  {%- set location = duckrun__delta_location(target_relation) -%}
  {{ return({'target': target_relation, 'tmp': tmp_relation, 'location': location}) }}
{% endmacro %}


{% macro duckrun__build_delta(model_sql, pre_hooks, post_hooks, is_incremental) %}

  {%- set language = model['language'] -%}
  {%- set p = duckrun__delta_paths() -%}
  {%- set target_relation = p['target'] -%}
  {%- set tmp_relation = p['tmp'] -%}
  {%- set location = p['location'] -%}

  {#-- Capture the target's Delta version AND existence NOW, before pre-hooks or the model read
       `{{ this }}`, in a SINGLE log open (delta_state) instead of two — so `append_if_unchanged` can pin to
       the version (if any writer commits during this build, the optimistic append fails rather than
       appending against a newer version and risking a duplicate), and the plugin gets the "table
       exists" belief for its contradiction guard. read_version is None when the table doesn't exist
       yet (first run overwrites anyway). --#}
  {%- set _delta_state = adapter.delta_state(location) -%}
  {%- set read_version = _delta_state['version'] -%}

  {#-- Pre-register {{ this }} as a delta_scan view when the Delta table already exists on
       disk, so pre-hooks and the model's own SQL (is_incremental self-reference) can read the
       current state. Disk discovery already reports the relation as existing in dbt's cache
       (so is_incremental() is true); this creates the matching physical view on the run-phase
       connection (views created during cache population don't survive to model run). --#}
  {#-- dbt's relation cache resolved is_incremental() from run-start disk discovery; capture the
       same "table exists" belief here so the plugin can detect a contradiction (discovery said
       it exists, but the Delta table can't be opened at store time → a transient storage error,
       NOT a real absence) and refuse to overwrite an incremental dataset. --#}
  {%- set dbt_believes_exists = _delta_state['exists'] -%}

  {#-- Does the model read `{{ this }}` (a read-modify-append on itself, e.g. an incremental append
       whose watermark is `max(ts) from {{ this }}`)? The compiled `model_sql` contains the rendered
       `{{ this }}` when it does. If so, the `append` strategy is fenced to `vB` (the version read):
       a concurrent commit since the build started fails it loudly instead of appending a duplicate —
       the automatic append_if_unchanged behavior, no strategy to pick. A plain append of new data
       (no `{{ this }}` read) stays unfenced. A missed match degrades to a plain append (the old
       behavior), so detection erring low is safe. --#}
  {%- set reads_self = dbt_believes_exists and language != 'python' and model_sql and (this | string) in model_sql -%}
  {#-- Pin the self-reference view to the captured `vB` (delta_scan version => N, requires the
       duckdb-delta version param). This makes the model's `is_incremental()` read of `{{ this }}`
       resolve at EXACTLY the version the write commit will validate OCC against — one snapshot for
       the read and the commit (single-snapshot MERGE semantics). Applies to every incremental
       strategy (merge / microbatch / append_if_unchanged), all of which self-reference `{{ this }}`. When
       read_version is None (brand-new table) there is nothing to pin — scan HEAD. --#}
  {%- if dbt_believes_exists -%}
    {%- do adapter.create_schema(target_relation) -%}
    {%- set _loc_sql = location | replace("'", "''") -%}
    {% call statement('register_this') -%}
      create or replace view {{ target_relation }} as select * from delta_scan('{{ _loc_sql }}'{% if read_version is not none %}, version => {{ read_version }}{% endif %})
    {%- endcall %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#-- Create the schema we write into. For python the staging table (and its schema) was
       already created in the materialization wrapper. --#}
  {%- if language != 'python' -%}
    {%- do adapter.create_schema(target_relation) -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#-- Model contracts (config(contract={enforced:true})). duckrun materializes via delta_rs
       writes, not SQL DDL, so dbt-core's create-table-with-constraints path never runs — we
       enforce here instead. Preflight column equivalence (name/type/count of the model SQL vs
       the yaml contract) is the standard dbt check; reusing get_assert_columns_equivalent keeps
       the error formatting identical. NOT NULL is enforced at write time in the plugin (a guard
       query on the staged rows), so collect the not-null column names and thread them through.
       check / primary_key / foreign_key / unique constraints are not enforceable in a delta_rs
       write (no constraint DDL), so — per dbt's NOT_ENFORCED convention (the standard adapter
       fixtures declare pk/check on every contract model and expect a green run) — they emit a
       warning instead of silently passing, unless the constraint opts out via
       warn_unenforced/warn_unsupported: false. --#}
  {%- set contract_config = config.get('contract') -%}
  {%- set not_null_columns = [] -%}
  {%- set unenforced_constraints = [] -%}
  {%- if contract_config and contract_config.enforced and language != 'python' -%}
    {{ get_assert_columns_equivalent(model_sql) }}
    {%- for _cname, _col in (model.get('columns') or {}).items() -%}
      {%- for _c in (_col.get('constraints') or []) -%}
        {%- if _c.get('type') == 'not_null' -%}
          {%- do not_null_columns.append(_col['name']) -%}
        {%- elif _c.get('warn_unenforced', true) and _c.get('warn_unsupported', true) -%}
          {%- do unenforced_constraints.append(_c.get('type') ~ ' on ' ~ _col['name']) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- for _c in (model.get('constraints') or []) -%}
      {%- if _c.get('warn_unenforced', true) and _c.get('warn_unsupported', true) -%}
        {%- do unenforced_constraints.append('model-level ' ~ _c.get('type')) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if unenforced_constraints -%}
      {%- do exceptions.warn("duckrun enforces this contract's column shape and not_null, but cannot enforce: "
          ~ (unenforced_constraints | join(', '))
          ~ ". These stay metadata-only (delta_rs has no constraint DDL); enforce them as dbt tests, "
          ~ "or set warn_unenforced: false on the constraint to silence this warning.") -%}
    {%- endif -%}
  {%- endif -%}

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
      'read_version': read_version,
      'reads_self': reads_self,
      'dbt_believes_exists': dbt_believes_exists,
      'not_null_columns': not_null_columns,
      'full_refresh': should_full_refresh(),
      'unique_key': config.get('unique_key'),
      'partition_by': config.get('partition_by'),
      'sort_by': config.get('sort_by'),
      'merge_schema': config.get('merge_schema', false),
      'storage_options': config.get('storage_options'),
      'merge_update_columns': config.get('merge_update_columns'),
      'merge_exclude_columns': config.get('merge_exclude_columns'),
      'merge_update_condition': config.get('merge_update_condition'),
      'merge_insert_condition': config.get('merge_insert_condition'),
      'merge_on_using_columns': config.get('merge_on_using_columns'),
      'merge_update_set_expressions': config.get('merge_update_set_expressions'),
      'merge_returning_columns': config.get('merge_returning_columns'),
      'merge_clauses': config.get('merge_clauses'),
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
      select * from delta_scan('{{ location | replace("'", "''") }}')
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do _duckrun__persist_docs(target_relation, location) %}

  {{ return({'relations': [target_relation]}) }}

{% endmacro %}


{#-- True when `db` names a catalog DuckDB owns natively (a profile `attach:` entry, or the one
     synthesized from `format: iceberg`). Those catalogs hold real DuckDB/Iceberg tables, not Delta
     directories, so duckrun's materializations step aside and let dbt-duckdb's own — the engine that
     already knows how to write them — run instead. --#}
{% macro duckrun__is_native_catalog(db) %}
  {{ return(db is not none and db in (target.native_catalog_names or [])) }}
{% endmacro %}
