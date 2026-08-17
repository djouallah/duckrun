-- Every landed month appears in fct_trips exactly as many times as its parquet footer said, and no
-- month is missing. Ported verbatim from the layout repo (duckdb leg) — the one assertion on this
-- table, since TLC trip records carry no natural unique key to assert a grain on. Catches a month
-- inserted twice (stored count a multiple of the logged one), a partial write (below it), and a
-- month landed but never built (missing).

WITH stored AS (
  SELECT file, COUNT(*) AS n
  FROM {{ ref('fct_trips') }}
  GROUP BY file
),
landed AS (
  SELECT file_stem, row_count
  FROM {{ ref('stg_parquet_archive_log') }}
  WHERE source_type = 'yellow'
)
SELECT
  landed.file_stem,
  landed.row_count AS logged_rows,
  stored.n AS stored_rows
FROM landed
LEFT JOIN stored ON stored.file = landed.file_stem
WHERE stored.n IS NULL OR stored.n <> landed.row_count
