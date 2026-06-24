-- Expression update (~5% sample, 100% match): a MERGE whose UPDATE SET is arbitrary expressions +
-- CASE, not a plain column copy. Sets l_quantity = -9 and re-derives l_returnflag with a CASE over
-- the source — exercising the arbitrary-expression / CASE update path. The sample is OF the table,
-- so every batch row matches an existing key and the row count must NOT change.
CREATE OR REPLACE TEMP TABLE _batch AS
  SELECT * FROM {schema}.expr_update USING SAMPLE 5 PERCENT (bernoulli);

MERGE INTO {schema}.expr_update AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET
    l_quantity = CAST(-9 AS DECIMAL(15,2)),
    l_returnflag = CASE WHEN source.l_discount > 0.05 THEN 'H' ELSE 'L' END;
