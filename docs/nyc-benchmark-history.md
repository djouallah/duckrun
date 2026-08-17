# NYC benchmark history

A permanent, append-only log — one row per `nyc_bench` dispatch, newest at the bottom. Written by
`tests/performance/nyc/nyc_bench.py`: the 591.7M-row fct_trips (17 columns, skewed categoricals)
built with `SORTED BY AUTO` by duckrun@the-commit on an 8-vcore Fabric notebook. `Profile` is the
sort-key profiler's own cost (its scans over the staged table were ~2/3 of the build on 0.4.54);
`Rows/RG` is the landed average rows per row group (the 16M Direct Lake segment band is the
target; 0.4.54 landed 21.7M).

| Date | Run | Commit | duckrun | DuckDB | delta_rs | Rows | fct_trips | Profile | Files | Rows/RG | MB | OK |
|------|-----|--------|---------|--------|----------|------|-----------|---------|-------|---------|----|----|
