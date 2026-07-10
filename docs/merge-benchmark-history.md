# MERGE benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate + manual
dispatch), newest at the bottom. Written by the `merge-spill` job; the live scorecard is in
[merge-benchmark.md](merge-benchmark.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | Rows | Peak RSS | Wall | OK |
|------|-----|--------|--------|----------|----|------|----------|------|----|
| 2026-07-10 | [#26](https://github.com/djouallah/duckrun/actions/runs/29089013898) | e491eaa | 1.5.4 | 1.5.0 | 1.0 | 6.0M | 2,763 MB | 136s | ✅ |
| 2026-07-10 | [#27](https://github.com/djouallah/duckrun/actions/runs/29089555777) | eea1f4e | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,436 MB | 765s | ✅ |
