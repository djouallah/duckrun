{#-- duckrun catalog: report Delta-backed relations as BASE TABLE.

  Every duckrun model is physically a DuckDB *view* over `delta_scan('<location>')`, so the stock
  dbt-duckdb catalog (duckdb_views() -> 'VIEW') reports them as views — which is dishonest: they are
  Delta *tables*, and `dbt docs generate` / is_incremental() treat them as tables. We override the
  catalog so a view whose definition reads from `delta_scan(...)` is reported as `BASE TABLE`, while
  genuine `view`-materialized models (no delta_scan) stay `VIEW`. Comments (table + column) come
  through unchanged from WS4's COMMENT ON, which get_catalog already reads.

  Stats are intentionally not synthesized here: the duckrun conformance catalog fixtures use
  `no_stats()`, so row/byte counts from the Delta log aren't required to pass — and computing them
  per relation would re-open every table during docs generate. (If stats are wanted later, pull
  num_records / size_bytes from DeltaTable.get_add_actions and cache per build.)
#}
{% macro duckrun__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    with relations AS (
      select
        t.table_name
        , t.database_name
        , t.schema_name
        , 'BASE TABLE' as table_type
        , t.comment as table_comment
      from duckdb_tables() t
      WHERE t.database_name = '{{ database }}'
      UNION ALL
      SELECT v.view_name as table_name
      , v.database_name
      , v.schema_name
      -- A delta_scan view is a Delta table surfaced for reads; report it as a table, not a view.
      , case when v.sql ilike '%delta_scan(%' then 'BASE TABLE' else 'VIEW' end as table_type
      , v.comment as table_comment
      from duckdb_views() v
      WHERE v.database_name = '{{ database }}'
    )
    select
        '{{ database }}' as table_database,
        r.schema_name as table_schema,
        r.table_name,
        r.table_type,
        r.table_comment,
        c.column_name,
        c.column_index as column_index,
        c.data_type as column_type,
        c.comment as column_comment,
        NULL as table_owner
    FROM relations r JOIN duckdb_columns() c ON r.schema_name = c.schema_name AND r.table_name = c.table_name
    WHERE (
        {%- for schema in schemas -%}
          upper(r.schema_name) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
    ORDER BY
        r.schema_name,
        r.table_name,
        c.column_index
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
