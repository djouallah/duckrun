-- Merge-correctness gate: the unified fct_price is built by a keyed MERGE, so the daily settled
-- rows must UPDATE the preliminary intraday rows in place rather than duplicate them. If the merge
-- ever double-inserted, a business key would appear more than once. Returns offending keys (empty
-- result = pass) — this is the core assertion that the merge upserts instead of appending.

SELECT
  REGIONID,
  SETTLEMENTDATE,
  INTERVENTION,
  count(*) AS n
FROM {{ ref('fct_price') }}
GROUP BY REGIONID, SETTLEMENTDATE, INTERVENTION
HAVING count(*) > 1
