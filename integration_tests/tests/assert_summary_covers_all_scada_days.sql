{{ config(tags=['heavy']) }}
-- Summary should have at least as many distinct days as scada
-- Tagged heavy: with CI's small process_limit, some ingested daily files
-- may have all INTERVENTION=1 rows or unmapped DUIDs and produce zero
-- summary rows for that date. The assertion only makes sense at full data
-- volume.
SELECT
  scada_days,
  summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
    (SELECT COUNT(DISTINCT date) FROM {{ ref('fct_summary') }}) as summary_days
)
WHERE scada_days > summary_days
