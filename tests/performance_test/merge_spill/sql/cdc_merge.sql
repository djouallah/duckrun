-- CDC change-set applied in ONE merge — the full delta_rs clause set:
--   * tombstones (l_quantity = -7) on a deterministic ~1% slice (l_orderkey % 100 = 7) → matched DELETE
--   * a ~3% random sample OUTSIDE that slice, marked -8                                 → matched UPDATE
--   * a ~2% random sample, key-shifted past max + a 2036 shipdate marker                → NOT MATCHED INSERT
-- The matched DELETE clause (condition on the source marker) is listed BEFORE the catch-all UPDATE, so
-- a tombstoned row is removed, not updated. Source keys stay unique: the delete slice (%100=7) and the
-- update sample (%100<>7) are disjoint, and inserts are shifted past max so they never collide.
CREATE OR REPLACE TEMP TABLE _batch AS
  WITH mx AS (SELECT max(l_orderkey) AS m FROM {schema}.cdc_merge),
       upd_s AS (SELECT * FROM {schema}.cdc_merge USING SAMPLE 3 PERCENT (bernoulli)),
       ins_s AS (SELECT * FROM {schema}.cdc_merge USING SAMPLE 2 PERCENT (bernoulli))
  SELECT * REPLACE (-7.0::decimal(15,2) AS l_quantity) FROM {schema}.cdc_merge WHERE l_orderkey % 100 = 7
  UNION ALL
  SELECT * REPLACE (-8.0::decimal(15,2) AS l_quantity) FROM upd_s WHERE l_orderkey % 100 <> 7
  UNION ALL
  SELECT * REPLACE ((l_orderkey + (SELECT m FROM mx)) AS l_orderkey, DATE '2036-01-01' AS l_shipdate) FROM ins_s;

MERGE INTO {schema}.cdc_merge AS target USING _batch AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED AND source.l_quantity = -7 THEN DELETE
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
