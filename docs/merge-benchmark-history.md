# MERGE benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate + manual
dispatch), newest at the bottom. Written by the `merge-spill` job; the live scorecard is in
[merge-benchmark.md](merge-benchmark.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | Rows | Peak RSS | Wall | OK |
|------|-----|--------|--------|----------|----|------|----------|------|----|
| 2026-07-10 | [#26](https://github.com/djouallah/duckrun/actions/runs/29089013898) | e491eaa | 1.5.4 | 1.5.0 | 1.0 | 6.0M | 2,763 MB | 136s | ✅ |
| 2026-07-10 | [#27](https://github.com/djouallah/duckrun/actions/runs/29089555777) | eea1f4e | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,436 MB | 765s | ✅ |
| 2026-07-11 | [#65](https://github.com/djouallah/duckrun/actions/runs/29154652744) | 1ce3e01 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,426 MB | 915s | ✅ |
| 2026-07-12 | [#66](https://github.com/djouallah/duckrun/actions/runs/29181964514) | 28dc3de | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,724 MB | 892s | ✅ |
| 2026-07-16 | [#67](https://github.com/djouallah/duckrun/actions/runs/29472085561) | 4e13452 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,353 MB | 884s | ✅ |
| 2026-07-16 | [#68](https://github.com/djouallah/duckrun/actions/runs/29483601234) | 1a586eb | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,500 MB | 1142s | ✅ |
| 2026-07-16 | [#69](https://github.com/djouallah/duckrun/actions/runs/29498442215) | 7edd813 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,722 MB | 897s | ✅ |
| 2026-07-17 | [#70](https://github.com/djouallah/duckrun/actions/runs/29546579275) | 4b9b8ee | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,553 MB | 800s | ✅ |
| 2026-07-17 | [#71](https://github.com/djouallah/duckrun/actions/runs/29581344055) | c621564 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,220 MB | 1208s | ✅ |
| 2026-07-17 | [#72](https://github.com/djouallah/duckrun/actions/runs/29583748763) | 2e39d0e | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,616 MB | 976s | ✅ |
| 2026-07-18 | [#74](https://github.com/djouallah/duckrun/actions/runs/29631634129) | 8afc522 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 10,091 MB | 1175s | ✅ |
| 2026-07-19 | [#75](https://github.com/djouallah/duckrun/actions/runs/29679884940) | 1b7d537 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,252 MB | 1124s | ✅ |
