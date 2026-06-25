-- Mixed upsert (~1% sample): ~80% of the sampled rows kept on their existing keys (UPDATE) and
-- ~20% key-shifted past the max key (INSERT). Every batch row is marked l_quantity = -1 so the
-- merge's effect is verifiable. The batch is materialised into a TEMP table first, so the random()
-- split is evaluated exactly once (a single, stable snapshot of the pre-merge table).
CREATE OR REPLACE TEMP TABLE _batch AS
  WITH s AS (SELECT *, random() AS _r FROM {schema}.mixed_upsert USING SAMPLE 1 PERCENT (bernoulli)),
       mx AS (SELECT max(l_orderkey) AS m FROM {schema}.mixed_upsert)
  SELECT * EXCLUDE (_r) REPLACE (-1.0::decimal(15,2) AS l_quantity)
  FROM s WHERE _r >= 0.2
  UNION ALL
  SELECT * EXCLUDE (_r) REPLACE ((l_orderkey + (SELECT m FROM mx)) AS l_orderkey, -1.0::decimal(15,2) AS l_quantity)
  FROM s WHERE _r < 0.2;

MERGE INTO {schema}.mixed_upsert AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
