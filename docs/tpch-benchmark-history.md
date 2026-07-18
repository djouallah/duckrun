# TPC-H benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate SF=10 + manual
dispatch), newest at the bottom. Written by the `tpch-stress` job; the live scorecard is in
[tpch.md](tpch.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | CPU | Ingest | Queries | OK |
|------|-----|--------|--------|----------|----|-----|--------|---------|----|
| 2026-07-10 | [#26](https://github.com/djouallah/duckrun/actions/runs/29089013898) | e491eaa | 1.5.4 | 1.5.0 | 10 | 4 | 62.9s | 26.3s | ✅ |
| 2026-07-10 | [#27](https://github.com/djouallah/duckrun/actions/runs/29089555777) | eea1f4e | 1.5.4 | 1.5.0 | 100 | 4 | 657.3s | 449.8s | ✅ |
| 2026-07-11 | [#65](https://github.com/djouallah/duckrun/actions/runs/29154652744) | 1ce3e01 | 1.5.4 | 1.5.0 | 10 | 4 | 70.8s | 27.6s | ✅ |
| 2026-07-12 | [#66](https://github.com/djouallah/duckrun/actions/runs/29181964514) | 28dc3de | 1.5.4 | 1.5.0 | 10 | 4 | 64.5s | 26.9s | ✅ |
| 2026-07-16 | [#67](https://github.com/djouallah/duckrun/actions/runs/29472085561) | 4e13452 | 1.5.4 | 1.5.0 | 10 | 4 | 67.2s | 27.2s | ✅ |
| 2026-07-16 | [#68](https://github.com/djouallah/duckrun/actions/runs/29483601234) | 1a586eb | 1.5.4 | 1.5.0 | 10 | 4 | 56.9s | 22.8s | ✅ |
| 2026-07-16 | [#69](https://github.com/djouallah/duckrun/actions/runs/29498442215) | 7edd813 | 1.5.4 | 1.5.0 | 10 | 4 | 65.9s | 27.5s | ✅ |
| 2026-07-17 | [#70](https://github.com/djouallah/duckrun/actions/runs/29546579275) | 4b9b8ee | 1.5.4 | 1.5.0 | 10 | 4 | 71.7s | 27.4s | ✅ |
| 2026-07-17 | [#71](https://github.com/djouallah/duckrun/actions/runs/29581344055) | c621564 | 1.5.4 | 1.5.0 | 10 | 4 | 68.2s | 27.2s | ✅ |
| 2026-07-17 | [#72](https://github.com/djouallah/duckrun/actions/runs/29583748763) | 2e39d0e | 1.5.4 | 1.5.0 | 10 | 4 | 63.1s | 25.5s | ✅ |
| 2026-07-18 | [#74](https://github.com/djouallah/duckrun/actions/runs/29631634129) | 8afc522 | 1.5.4 | 1.5.0 | 10 | 4 | 73.8s | 28.5s | ✅ |
