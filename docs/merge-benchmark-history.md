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
| 2026-07-20 | [#76](https://github.com/djouallah/duckrun/actions/runs/29717661545) | f9c7e8f | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,152 MB | 1030s | ✅ |
| 2026-07-20 | [#77](https://github.com/djouallah/duckrun/actions/runs/29726831351) | c101f04 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,617 MB | 1038s | ✅ |
| 2026-07-21 | [#78](https://github.com/djouallah/duckrun/actions/runs/29796715631) | 4afacd6 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,225 MB | 730s | ✅ |
| 2026-07-21 | [#79](https://github.com/djouallah/duckrun/actions/runs/29798145834) | df930b0 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,576 MB | 898s | ✅ |
| 2026-07-21 | [#80](https://github.com/djouallah/duckrun/actions/runs/29829512178) | d39b08c | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,245 MB | 1012s | ✅ |
| 2026-07-21 | [#81](https://github.com/djouallah/duckrun/actions/runs/29835353920) | bf110d2 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,469 MB | 892s | ✅ |
| 2026-07-21 | [#82](https://github.com/djouallah/duckrun/actions/runs/29873370204) | ada8e77 | 1.5.4 | 1.5.0 | 10.0 | 60.0M | 9,304 MB | 827s | ✅ |
| 2026-07-25 | [#83](https://github.com/djouallah/duckrun/actions/runs/30153413359) | 2badb11 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 10,164 MB | 1031s | ✅ |
| 2026-07-27 | [#84](https://github.com/djouallah/duckrun/actions/runs/30258196683) | a12610f | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,190 MB | 1076s | ✅ |
| 2026-07-29 | [#85](https://github.com/djouallah/duckrun/actions/runs/30498361969) | 6d7b5dd | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 10,615 MB | 1122s | ✅ |
| 2026-07-30 | [#86](https://github.com/djouallah/duckrun/actions/runs/30505267954) | 2692e4c | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,333 MB | 1045s | ✅ |
| 2026-07-30 | [#87](https://github.com/djouallah/duckrun/actions/runs/30525504647) | 192c3ee | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,338 MB | 1233s | ✅ |
| 2026-07-31 | [#88](https://github.com/djouallah/duckrun/actions/runs/30594553958) | af9e801 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,437 MB | 909s | ✅ |
| 2026-08-02 | [#89](https://github.com/djouallah/duckrun/actions/runs/30750513533) | ee8fade | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 10,323 MB | 911s | ✅ |
| 2026-08-03 | [#28](https://github.com/djouallah/duckrun/actions/runs/30777796492) | 3b62fc8 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,803 MB | 1061s | ✅ |
| 2026-08-03 | [#90](https://github.com/djouallah/duckrun/actions/runs/30782232941) | 28f5ca1 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,613 MB | 1078s | ✅ |
| 2026-08-03 | [#29](https://github.com/djouallah/duckrun/actions/runs/30787288654) | 2a68d60 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,382 MB | 1044s | ✅ |
| 2026-08-03 | [#91](https://github.com/djouallah/duckrun/actions/runs/30788694619) | 2a68d60 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,428 MB | 825s | ✅ |
| 2026-08-03 | [#92](https://github.com/djouallah/duckrun/actions/runs/30810956744) | f17aaaa | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,592 MB | 935s | ✅ |
| 2026-08-04 | [#93](https://github.com/djouallah/duckrun/actions/runs/30874190571) | 52a6062 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,643 MB | 1020s | ✅ |
| 2026-08-04 | [#94](https://github.com/djouallah/duckrun/actions/runs/30920579835) | 92f4f22 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,611 MB | 1085s | ✅ |
| 2026-08-04 | [#95](https://github.com/djouallah/duckrun/actions/runs/30959757609) | 9645e1a | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 9,499 MB | 798s | ✅ |
| 2026-08-05 | [#96](https://github.com/djouallah/duckrun/actions/runs/30981560637) | 206a6db | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 8,727 MB | 997s | ✅ |
| 2026-08-06 | [#97](https://github.com/djouallah/duckrun/actions/runs/31069832866) | b825720 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 8,926 MB | 1221s | ✅ |
| 2026-08-07 | [#98](https://github.com/djouallah/duckrun/actions/runs/31158636537) | 2cfc057 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 8,641 MB | 802s | ✅ |
| 2026-08-09 | [#99](https://github.com/djouallah/duckrun/actions/runs/31311123572) | 3f7a0c5 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 8,814 MB | 812s | ✅ |
| 2026-08-11 | [#100](https://github.com/djouallah/duckrun/actions/runs/31488266797) | 0797118 | 1.5.5 | 1.5.0 | 10.0 | 60.0M | 8,593 MB | 966s | ✅ |
