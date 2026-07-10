{#-- fct_scada and fct_price now each hold the settled (daily) AND preliminary (intraday) rows in a
    single merged table — one row per business key — so the old daily/intraday/cutoff stitching that
    lived here is gone. The summary is now a plain star join. Materialized as a table: Delta state is
    a read-only delta_scan view, so it's fully overwritten every run; merge_schema lets the overwrite
    add the joined `year` column to an existing table without a column-count mismatch. --#}
{{ config(
    materialized='table',
    schema='mart',
    merge_schema=true
) }}

WITH summary AS (
  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
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
  sm.date,
  sm.time,
  sm.DUID,
  CAST(sm.mw AS DECIMAL(18, 4)) AS mw,
  CAST(sm.price AS DECIMAL(18, 4)) AS price,
  cal.year,
  CAST((SELECT MAX(SETTLEMENTDATE) FROM {{ ref('fct_scada') }}) AS TIMESTAMP) AS cutoff
FROM summary sm
LEFT JOIN {{ source('aemo', 'dim_calendar') }} cal ON sm.date = cal.date
ORDER BY sm.date
