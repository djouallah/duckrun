-- Insert-only (~5% sample): key-shifted past the max key so nothing matches — every row INSERTs.
-- Stamped with a future l_shipdate (2035) as a verifiable marker. Still a MERGE (no matches).
CREATE OR REPLACE TEMP TABLE _batch AS
  WITH mx AS (SELECT max(l_orderkey) AS m FROM {schema}.insert_only)
  SELECT * REPLACE ((l_orderkey + (SELECT m FROM mx)) AS l_orderkey, DATE '2035-06-15' AS l_shipdate)
  FROM {schema}.insert_only USING SAMPLE 5 PERCENT (bernoulli);

MERGE INTO {schema}.insert_only AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
