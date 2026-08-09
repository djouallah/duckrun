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
| 2026-07-19 | [#75](https://github.com/djouallah/duckrun/actions/runs/29679884940) | 1b7d537 | 1.5.4 | 1.5.0 | 10 | 4 | 75.5s | 28.4s | ✅ |
| 2026-07-20 | [#76](https://github.com/djouallah/duckrun/actions/runs/29717661545) | f9c7e8f | 1.5.4 | 1.5.0 | 10 | 4 | 62.3s | 22.8s | ✅ |
| 2026-07-20 | [#77](https://github.com/djouallah/duckrun/actions/runs/29726831351) | c101f04 | 1.5.4 | 1.5.0 | 10 | 4 | 57.3s | 22.0s | ✅ |
| 2026-07-21 | [#78](https://github.com/djouallah/duckrun/actions/runs/29796715631) | 4afacd6 | 1.5.4 | 1.5.0 | 10 | 4 | 67.2s | 27.0s | ✅ |
| 2026-07-21 | [#79](https://github.com/djouallah/duckrun/actions/runs/29798145834) | df930b0 | 1.5.4 | 1.5.0 | 10 | 4 | 56.5s | 21.9s | ✅ |
| 2026-07-21 | [#80](https://github.com/djouallah/duckrun/actions/runs/29829512178) | d39b08c | 1.5.4 | 1.5.0 | 10 | 4 | 68.8s | 29.1s | ✅ |
| 2026-07-21 | [#81](https://github.com/djouallah/duckrun/actions/runs/29835353920) | bf110d2 | 1.5.4 | 1.5.0 | 10 | 4 | 68.9s | 27.1s | ✅ |
| 2026-07-21 | [#82](https://github.com/djouallah/duckrun/actions/runs/29873370204) | ada8e77 | 1.5.4 | 1.5.0 | 10 | 4 | 67.7s | 27.8s | ✅ |
| 2026-07-25 | [#83](https://github.com/djouallah/duckrun/actions/runs/30153413359) | 2badb11 | 1.5.5 | 1.5.0 | 10 | 4 | 67.3s | 27.5s | ✅ |
| 2026-07-27 | [#84](https://github.com/djouallah/duckrun/actions/runs/30258196683) | a12610f | 1.5.5 | 1.5.0 | 10 | 4 | 66.2s | 26.9s | ✅ |
| 2026-07-29 | [#85](https://github.com/djouallah/duckrun/actions/runs/30498361969) | 6d7b5dd | 1.5.5 | 1.5.0 | 10 | 4 | 66.2s | 27.3s | ✅ |
| 2026-07-30 | [#86](https://github.com/djouallah/duckrun/actions/runs/30505267954) | 2692e4c | 1.5.5 | 1.5.0 | 10 | 4 | 65.6s | 23.6s | ✅ |
| 2026-07-30 | [#87](https://github.com/djouallah/duckrun/actions/runs/30525504647) | 192c3ee | 1.5.5 | 1.5.0 | 10 | 4 | 57.9s | 22.3s | ✅ |
| 2026-07-31 | [#88](https://github.com/djouallah/duckrun/actions/runs/30594553958) | af9e801 | 1.5.5 | 1.5.0 | 10 | 4 | 71.3s | 28.5s | ✅ |
| 2026-08-02 | [#89](https://github.com/djouallah/duckrun/actions/runs/30750513533) | ee8fade | 1.5.5 | 1.5.0 | 10 | 4 | 65.5s | 30.6s | ✅ |
| 2026-08-03 | [#28](https://github.com/djouallah/duckrun/actions/runs/30777796492) | 3b62fc8 | 1.5.5 | 1.5.0 | 100 | 4 | 683.4s | 480.7s | ✅ |
| 2026-08-03 | [#90](https://github.com/djouallah/duckrun/actions/runs/30782232941) | 28f5ca1 | 1.5.5 | 1.5.0 | 10 | 4 | 66.5s | 28.9s | ✅ |
| 2026-08-03 | [#29](https://github.com/djouallah/duckrun/actions/runs/30787288654) | 2a68d60 | 1.5.5 | 1.5.0 | 100 | 4 | 682.4s | 475.8s | ✅ |
| 2026-08-03 | [#91](https://github.com/djouallah/duckrun/actions/runs/30788694619) | 2a68d60 | 1.5.5 | 1.5.0 | 10 | 4 | 64.8s | 29.7s | ✅ |
| 2026-08-03 | [#92](https://github.com/djouallah/duckrun/actions/runs/30810956744) | f17aaaa | 1.5.5 | 1.5.0 | 10 | 4 | 71.5s | 30.5s | ✅ |
| 2026-08-04 | [#93](https://github.com/djouallah/duckrun/actions/runs/30874190571) | 52a6062 | 1.5.5 | 1.5.0 | 10 | 4 | 54.0s | 21.9s | ✅ |
| 2026-08-04 | [#94](https://github.com/djouallah/duckrun/actions/runs/30920579835) | 92f4f22 | 1.5.5 | 1.5.0 | 10 | 4 | 66.3s | 29.3s | ✅ |
| 2026-08-04 | [#95](https://github.com/djouallah/duckrun/actions/runs/30959757609) | 9645e1a | 1.5.5 | 1.5.0 | 10 | 4 | 66.1s | 28.8s | ✅ |
| 2026-08-05 | [#96](https://github.com/djouallah/duckrun/actions/runs/30981560637) | 206a6db | 1.5.5 | 1.5.0 | 10 | 4 | 62.9s | 27.8s | ✅ |
| 2026-08-06 | [#97](https://github.com/djouallah/duckrun/actions/runs/31069832866) | b825720 | 1.5.5 | 1.5.0 | 10 | 4 | 65.6s | 27.8s | ✅ |
| 2026-08-07 | [#98](https://github.com/djouallah/duckrun/actions/runs/31158636537) | 2cfc057 | 1.5.5 | 1.5.0 | 10 | 4 | 66.1s | 28.0s | ✅ |
| 2026-08-09 | [#99](https://github.com/djouallah/duckrun/actions/runs/31311123572) | 3f7a0c5 | 1.5.5 | 1.5.0 | 10 | 4 | 67.7s | 28.9s | ✅ |
