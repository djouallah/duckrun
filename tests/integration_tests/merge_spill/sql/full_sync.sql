-- Full-dimension sync — the heaviest merge shape: matched UPDATE + WHEN NOT MATCHED BY SOURCE DELETE.
-- The source is ~50% of the table (every ODD orderkey), marked -5; the EVEN orderkeys are 'departed'.
-- Matched keys (odd) → UPDATE; keys the source no longer carries (even) → BY-SOURCE DELETE. The whole
-- target is scanned and anti-joined against the ~50% source — the shape that proves the by-source path
-- stays within RAM (spills) at scale. The source is left as an inline subquery (not a TEMP table) so
-- the big ~50% relation is streamed into the merge rather than materialised whole.
MERGE INTO {schema}.full_sync AS target
  USING (SELECT * REPLACE (-5.0::decimal(15,2) AS l_quantity) FROM {schema}.full_sync WHERE l_orderkey % 2 <> 0) AS source
  ON target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED BY SOURCE THEN DELETE;
