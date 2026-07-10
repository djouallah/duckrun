# MERGE benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate + manual
dispatch), newest at the bottom. Written by the `merge-spill` job; the live scorecard is in
[merge-benchmark.md](merge-benchmark.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | Rows | Peak RSS | Wall | OK |
|------|-----|--------|--------|----------|----|------|----------|------|----|
