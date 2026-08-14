{#
  duckrun snapshot-timestamp overrides.

  dbt's snapshot materialization stamps its SCD2 metadata columns (dbt_valid_from,
  dbt_valid_to, dbt_updated_at, and the dbt_scd_id hash input) via snapshot_get_time /
  snapshot_string_as_time. duckrun declares dependencies=["duckdb"], so those dispatch to
  dbt-duckdb, whose implementations cast to a bare ``::timestamp`` — DuckDB TIMESTAMP (no time
  zone), which lands in Delta as TIMESTAMP_NTZ. Microsoft Fabric's SQL endpoint does not support
  the Delta timestampNtz type, so the snapshot table errors when read through the SQL endpoint
  (issue #9).

  ``adapter.dispatch`` resolves ``duckrun__x`` before ``duckdb__x``, so these overrides win with
  no project-side change and keep the time zone: DuckDB now() is TIMESTAMP WITH TIME ZONE, which
  writes as a Delta ``timestamp`` (isAdjustedToUTC=true) that Fabric accepts.

  These macros cover the snapshot metadata columns; since issue #42 the same NTZ problem is
  fixed for USER columns too, at the engine write seam (engine.coerce_naive_timestamps): a naive
  TIMESTAMP model column is coerced to UTC-adjusted ``timestamp`` by default, with
  ``timestamp_ntz: true`` (model config) / ``DUCKRUN_TIMESTAMP_NTZ=1`` (env) restoring the old
  written-verbatim behavior.
#}

{% macro duckrun__snapshot_get_time() -%}
  now()
{%- endmacro %}

{% macro duckrun__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamptz" -%}
    {{ return(result) }}
{%- endmacro %}
