{#
  Shared helpers for reading the DIGen-generated TPC-DI source files with DuckDB.

  The source files are headerless and typed, so we read them with an explicit
  column list (positional) rather than read_csv_auto. This mirrors the Snowflake
  reference project's TXT_PIPE / TXT_CSV / TXT_FIXED_WIDTH file formats.

  `path` is relative to var('tpcdi_dir') and may be a glob, e.g.
  'Batch1/Date.txt' or 'Batch[23]/Trade.txt'.
  `columns` is a Jinja/DuckDB dict literal: {'accountid': 'BIGINT', ...} — order
  is significant (positional mapping).
#}

{% macro _abspath(path) -%}
  {{ var('tpcdi_dir') }}/{{ path }}
{%- endmacro %}

{# Pipe-delimited files (most .txt sources). No quoting, backslash escape.
   null_padding tolerates rows with fewer trailing fields than declared. #}
{% macro read_pipe(path, columns, with_filename=false) -%}
  read_csv(
    '{{ _abspath(path) }}',
    delim = '|',
    header = false,
    quote = '',
    escape = '',
    nullstr = '',
    all_varchar = false,
    null_padding = true,
    columns = {{ columns }}
    {%- if with_filename %}, filename = true{% endif %}
  )
{%- endmacro %}

{# Comma-delimited files (Prospect.csv, HR.csv). DIGen emits these UNQUOTED — fields
   never embed a comma (column counts are fixed) and stray '"' / '\' occur as literal
   data, so we disable quoting/escaping. Treating '"' as a quote char makes DuckDB's
   dialect sniffer choke on those stray quotes ("Error when sniffing"). #}
{% macro read_csvfile(path, columns, with_filename=false) -%}
  read_csv(
    '{{ _abspath(path) }}',
    delim = ',',
    header = false,
    quote = '',
    escape = '',
    nullstr = '',
    all_varchar = false,
    null_padding = true,
    columns = {{ columns }}
    {%- if with_filename %}, filename = true{% endif %}
  )
{%- endmacro %}

{# Fixed-width FINWIRE files: one string column per line, parsed downstream.
   chr(30) (ASCII record separator) never appears in the text, so each line
   becomes a single 'line' field (only the newline record-delimiter splits rows). #}
{% macro read_fixed(path) -%}
  read_csv(
    '{{ _abspath(path) }}',
    delim = chr(30),
    header = false,
    quote = '',
    escape = '',
    columns = {'line': 'VARCHAR'}
  )
{%- endmacro %}

{# Extract the batch number (1/2/3) from a read_csv filename column. #}
{% macro batchid_from_filename() -%}
  try_cast(regexp_extract(filename, 'Batch([0-9])', 1) as int)
{%- endmacro %}

{# Snowflake sk_dateid / sk_timeid helpers (integer YYYYMMDD / HHMMSS). #}
{% macro sk_dateid(ts) -%}
  (extract(year from {{ ts }}) * 10000 + extract(month from {{ ts }}) * 100 + extract(day from {{ ts }}))
{%- endmacro %}

{% macro sk_timeid(ts) -%}
  (extract(hour from {{ ts }}) * 10000 + extract(minute from {{ ts }}) * 100 + extract(second from {{ ts }}))
{%- endmacro %}

{# webbed XML helpers (used by stg_customermgmt).
   `cm(xpath)` pulls the first text match of a namespace-agnostic XPath from the
   per-Action fragment `node`. Empty match -> NULL (list[1] of an empty list). #}
{% macro cm(xpath) -%}
  xml_extract_text(node, $x${{ xpath }}$x$)[1]
{%- endmacro %}

{# Concatenate a phone from its parts exactly as the reference CustomerMgmtRaw does. #}
{% macro cm_phone(ctry, area, local, ext) -%}
  case when nullif({{ local }}, '') is not null then
    concat(
      case when nullif({{ ctry }}, '') is not null then concat('+', {{ ctry }}, ' ') else '' end,
      case when nullif({{ area }}, '') is not null then concat('(', {{ area }}, ') ') else '' end,
      {{ local }}, coalesce({{ ext }}, ''))
  end
{%- endmacro %}

{# Map the raw 4-letter status codes to their long form (used in many models). #}
{% macro status_longform(col) -%}
  case {{ col }}
    when 'ACTV' then 'Active'
    when 'CMPT' then 'Completed'
    when 'CNCL' then 'Canceled'
    when 'PNDG' then 'Pending'
    when 'SBMT' then 'Submitted'
    when 'INAC' then 'Inactive'
    else null
  end
{%- endmacro %}
