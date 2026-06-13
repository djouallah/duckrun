-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Delta state is written by delta_rs and {{ this }} is a read-only delta_scan view, so the
     old TRUNCATE+append rebuild can't run in place. Materialize as a table: delta_rs overwrites
     with the full picture (daily + today) every run. is_incremental() is then always false, so
     the full-refresh branch at the bottom runs. --#}
{#-- merge_schema: this table fully overwrites every run, and now carries a `year` column joined
     from the dim_calendar source. Schema evolution lets the overwrite add `year` to an existing
     Delta table instead of failing on the column-count mismatch. --#}
{{ config(
    materialized='table',
    schema='mart',
    merge_schema=true
) }}

{% if is_incremental() %}

{%- set has_new_daily_query -%}
SELECT
  (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
  (SELECT COUNT(DISTINCT date) FROM {{ this }}) as summary_days
{%- endset -%}

{%- if execute and flags.WHICH == 'run' -%}
  {%- set result = run_query(has_new_daily_query) -%}
  {%- set has_new_daily = result and result.rows[0][0] > result.rows[0][1] -%}
{%- else -%}
  {%- set has_new_daily = true -%}
{%- endif -%}

{% if has_new_daily %}

-- New daily data found: full rebuild from daily
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}) as cutoff,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL
)

SELECT
  ds.date,
  ds.time,
  ds.DUID,
  CAST(ds.mw AS DECIMAL(18, 4)) AS mw,
  CAST(ds.price AS DECIMAL(18, 4)) AS price,
  cal.year,
  ds.cutoff
FROM daily_summary ds
LEFT JOIN {{ source('aemo', 'dim_calendar') }} cal ON ds.date = cal.date

{% else %}

-- No new daily data: append intraday after cutoff
WITH max_cutoff AS (
  SELECT MAX(cutoff) as cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.DATE as date,
    s.SETTLEMENTDATE,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
)

SELECT
  id.date,
  CAST(strftime(id.SETTLEMENTDATE, '%H%M') AS INT) AS time,
  id.DUID,
  CAST(id.mw AS DECIMAL(18, 4)) AS mw,
  CAST(id.price AS DECIMAL(18, 4)) AS price,
  cal.year,
  CAST(MAX(id.SETTLEMENTDATE) OVER () AS TIMESTAMPTZ) AS cutoff
FROM incremental_data id
LEFT JOIN {{ source('aemo', 'dim_calendar') }} cal ON id.date = cal.date

{% endif %}

{% else %}

-- Full refresh from daily + today data
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL

  UNION ALL

  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  ds.date,
  ds.time,
  ds.DUID,
  CAST(ds.mw AS DECIMAL(18, 4)) AS mw,
  CAST(ds.price AS DECIMAL(18, 4)) AS price,
  cal.year,
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary ds
LEFT JOIN {{ source('aemo', 'dim_calendar') }} cal ON ds.date = cal.date
ORDER BY ds.date

{% endif %}
