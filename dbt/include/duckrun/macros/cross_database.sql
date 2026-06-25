{#
  duckrun cross-database macro overrides.

  duckrun runs on DuckDB, so dispatched dbt / dbt_utils macros should produce DuckDB-correct SQL.
  dbt-duckdb leaves some of these at dbt-core's ``default__`` implementation (lowest-common-denominator
  SQL) and relies on projects branching on ``target.type == 'duckdb'`` to special-case DuckDB. But
  duckrun is its OWN adapter type (``target.type == 'duckrun'``, NOT 'duckdb' — and it can't claim to
  be 'duckdb' without dbt loading the dbt-duckdb adapter instead of duckrun), so those
  ``target.type == 'duckdb'`` branches don't fire on duckrun.

  Instead, duckrun supplies the correct DuckDB behaviour here as ``duckrun__`` dispatch overrides.
  ``adapter.dispatch('x')`` resolves ``duckrun__x`` before ``default__x``, so these win for ANY
  project with no project-side change — the boring, dbt-native way to "treat duckrun like DuckDB".
#}

{% macro duckrun__safe_cast(field, type) %}
    {#- dbt-core's default__safe_cast is a STRICT cast (raises on a bad value); DuckDB's real "safe"
        cast is try_cast (NULL on failure). A bare cast(x as int) over alphanumeric data (e.g. an
        HCPCS code like '4004F') throws, so use try_cast — the semantics safe_cast actually promises. -#}
    try_cast({{ field }} as {{ type }})
{% endmacro %}
