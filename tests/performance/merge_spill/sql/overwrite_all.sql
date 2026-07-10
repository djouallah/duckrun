-- Overwrite (no merge): a plain Delta overwrite (CREATE OR REPLACE TABLE) replaces the whole table
-- with a ~5% sample of the chain tail. Also far cheaper than a MERGE (no target scan or key join).
-- Terminal — feeds nothing downstream.
CREATE OR REPLACE TABLE {schema}.overwrite_all AS
  SELECT * FROM {schema}.append_if_unchanged_only USING SAMPLE 5 PERCENT (bernoulli);
