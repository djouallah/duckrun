-- append_if_unchanged (no merge): the same cheap key-shifted append, but committed only if the table version
-- is unchanged since it was read (single writer here, so it commits). There is no raw-SQL append_if_unchanged
-- form, so this file only builds the batch — the runner appends _batch via the DataFrame write API
-- (mode 'append_if_unchanged'), which is the same engine path with the version-guard fence.
CREATE OR REPLACE TEMP TABLE _batch AS
  WITH mx AS (SELECT max(l_orderkey) AS m FROM {schema}.append_if_unchanged_only)
  SELECT * REPLACE ((l_orderkey + (SELECT m FROM mx)) AS l_orderkey)
  FROM {schema}.append_if_unchanged_only USING SAMPLE 5 PERCENT (bernoulli);
