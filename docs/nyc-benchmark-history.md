# NYC benchmark history

A permanent, append-only log — one row per `nyc_bench` dispatch, newest at the bottom. Written by
`tests/performance/nyc/nyc_bench.py`: the 591.7M-row fct_trips (17 columns, skewed categoricals)
built with `SORTED BY AUTO` by duckrun@the-commit on an 8-vcore Fabric notebook. `Profile` is the
sort-key profiler's own cost (its scans over the staged table were ~2/3 of the build on 0.4.54);
`Rows/RG` is the landed average rows per row group (the 16M Direct Lake segment band is the
target; 0.4.54 landed 21.7M).

| Date | Run | Commit | duckrun | DuckDB | delta_rs | Rows | fct_trips | Profile | Files | Rows/RG | MB | OK |
|------|-----|--------|---------|--------|----------|------|-----------|---------|-------|---------|----|----|
| 2026-08-17 | [#1](https://github.com/djouallah/duckrun/actions/runs/32002906444) | b07d65d | 0.4.54 | 1.5.5 | 1.5.0 | ? | 192s | ? | ? | ? | ? | ❌ |
| 2026-08-17 | [#2](https://github.com/djouallah/duckrun/actions/runs/32003630603) | f85a38b | 0.4.54 | 1.5.5 | 1.5.0 | 591,729,858 | 4366s | 20 scans / 3163s | 40 | 14,793,246 | 6512 | ✅ |
| 2026-08-17 | [#3](https://github.com/djouallah/duckrun/actions/runs/32014674495) | d067a55 | 0.4.54 | 1.5.5 | 1.5.0 | 591,729,858 | 794s | ? | 32 | 18,491,558 | 6560 | ✅ |
| 2026-08-17 | [#5](https://github.com/djouallah/duckrun/actions/runs/32016992949) | 736e6b5 | 0.4.54 | 1.5.5 | 1.5.0 | ? | 1623s | 19 scans / 86s | ? | ? | ? | ❌ |
| 2026-08-17 | [#6](https://github.com/djouallah/duckrun/actions/runs/32019720102) | f9b9d66 | 0.4.54 | 1.5.5 | 1.5.0 | 591,729,858 | 1530s | 19 scans / 84s | 43 | 13,761,159 | 6483 | ✅ |
