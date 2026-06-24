-- Append (no merge): a ~5% sample key-shifted past the max key, appended — no target scan, no key
-- join. Far cheaper than a MERGE of the same row count; here for the head-to-head comparison.
CREATE OR REPLACE TEMP TABLE _batch AS
  WITH mx AS (SELECT max(l_orderkey) AS m FROM {schema}.append_only)
  SELECT * REPLACE ((l_orderkey + (SELECT m FROM mx)) AS l_orderkey)
  FROM {schema}.append_only USING SAMPLE 5 PERCENT (bernoulli);

INSERT INTO {schema}.append_only SELECT * FROM _batch;
