-- Full-dimension sync — the heaviest merge shape: matched UPDATE + WHEN NOT MATCHED BY SOURCE DELETE.
-- This file only BUILDS the source (~50% of the table, every ODD orderkey, marked -5; the EVEN
-- orderkeys are 'departed'). The runner then applies the by-source MERGE via the DataFrame builder with
-- streamed_exec=True — there is no raw-SQL way to set streamed_exec, and a by-source merge MUST stream
-- its big source: it scans the whole target regardless (no pruning to gain), and collecting the ~50%
-- source whole would build a non-spillable hash and OOM. Matched keys (odd) → UPDATE; keys the source
-- no longer carries (even) → BY-SOURCE DELETE.
CREATE OR REPLACE TEMP TABLE _src AS
  SELECT * REPLACE (-5.0::decimal(15,2) AS l_quantity) FROM {schema}.full_sync WHERE l_orderkey % 2 <> 0;
