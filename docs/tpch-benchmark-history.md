# TPC-H benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate SF=10 + manual
dispatch), newest at the bottom. Written by the `tpch-stress` job; the live scorecard is in
[tpch.md](tpch.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | CPU | Ingest | Queries | OK |
|------|-----|--------|--------|----------|----|-----|--------|---------|----|
| 2026-07-10 | [#26](https://github.com/djouallah/duckrun/actions/runs/29089013898) | e491eaa | 1.5.4 | 1.5.0 | 10 | 4 | 62.9s | 26.3s | ✅ |
| 2026-07-10 | [#27](https://github.com/djouallah/duckrun/actions/runs/29089555777) | eea1f4e | 1.5.4 | 1.5.0 | 100 | 4 | 657.3s | 449.8s | ✅ |
| 2026-07-11 | [#65](https://github.com/djouallah/duckrun/actions/runs/29154652744) | 1ce3e01 | 1.5.4 | 1.5.0 | 10 | 4 | 70.8s | 27.6s | ✅ |
