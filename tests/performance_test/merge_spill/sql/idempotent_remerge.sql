-- Idempotent re-merge (~5% sample, UNCHANGED): a correct MERGE of rows that already hold these keys
-- and values changes nothing. The runner runs this op twice — the second run is the one measured,
-- and the row count must be identical before and after.
CREATE OR REPLACE TEMP TABLE _batch AS
  SELECT * FROM {schema}.idempotent_remerge USING SAMPLE 5 PERCENT (bernoulli);

MERGE INTO {schema}.idempotent_remerge AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
