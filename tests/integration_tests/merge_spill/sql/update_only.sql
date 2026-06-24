-- Update-only (~5% sample): existing keys, no shift → 100% match. Marked l_quantity = -2. The
-- heaviest merge join shape; the row count must NOT change and the sampled rows must carry the marker.
CREATE OR REPLACE TEMP TABLE _batch AS
  SELECT * REPLACE (-2.0::decimal(15,2) AS l_quantity)
  FROM {schema}.update_only USING SAMPLE 5 PERCENT (bernoulli);

MERGE INTO {schema}.update_only AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
