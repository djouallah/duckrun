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

  The whole-schema entry point (get_catalog) and the per-relation one (get_catalog_relations)
  differ ONLY in their WHERE filter, so they share one body (_duckrun__catalog_sql) and each
  wrapper builds just its filter — a fix to the catalog shape lands in both automatically.
#}
{% macro _duckrun__catalog_sql(database, where_sql) -%}
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
      -- Structural match on the exact shape duckrun registers (`... as select * from delta_scan('...`),
      -- not a bare substring test — a genuine `view` model that merely MENTIONS delta_scan in a
      -- string literal or comment must stay a VIEW.
      , case when regexp_matches(v.sql, 'as\s+select\s+\*\s+from\s+delta_scan\s*\(', 'i')
             then 'BASE TABLE' else 'VIEW' end as table_type
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
    -- join on database too: an attached DB can hold a same-named table in the same schema, and
    -- without this the columns of that shadow relation would bleed into the result.
    FROM relations r JOIN duckdb_columns() c ON r.database_name = c.database_name AND r.schema_name = c.schema_name AND r.table_name = c.table_name
    WHERE (
        {{ where_sql }}
    )
    ORDER BY
        r.schema_name,
        r.table_name,
        c.column_index
{%- endmacro %}


{% macro duckrun__get_catalog(information_schema, schemas) -%}
  {#-- Scope to the catalog dbt asked about (matches duckrun__get_catalog_relations), so
       `dbt docs generate` reports each attached catalog's relations under its own database. --#}
  {%- set database = information_schema.database -%}
  {%- set filters = [] -%}
  {%- for schema in schemas -%}
    {%- do filters.append("upper(r.schema_name) = upper('" ~ schema ~ "')") -%}
  {%- endfor -%}
  {%- call statement('catalog', fetch_result=True) -%}
    {{ _duckrun__catalog_sql(database, filters | join(' or ')) }}
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}


{#-- duckrun catalog, scoped to a specific set of relations (the `get_catalog_relations`
  capability dbt uses when it can ask for just the relations it cares about — see
  BaseAdapter._get_one_catalog_by_relations). Same delta_scan-view → BASE TABLE rule via the shared
  body; the WHERE filters to the passed relations (by schema, and by identifier when one is given)
  instead of to whole schemas. Without this, dbt falls back to default__get_catalog_relations,
  which just raises "not implemented for duckrun". #}
{% macro duckrun__get_catalog_relations(information_schema, relations) -%}
  {%- set database = information_schema.database -%}
  {%- set filters = [] -%}
  {%- for relation in relations -%}
    {%- set f = "upper(r.schema_name) = upper('" ~ relation.schema ~ "')" -%}
    {%- if relation.identifier -%}
      {%- set f = f ~ " and upper(r.table_name) = upper('" ~ relation.identifier ~ "')" -%}
    {%- endif -%}
    {%- do filters.append("(" ~ f ~ ")") -%}
  {%- endfor -%}
  {%- call statement('catalog', fetch_result=True) -%}
    {{ _duckrun__catalog_sql(database, filters | join(' or ')) }}
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
