# TPC-H benchmark history

A permanent, append-only log — one row per `local_stress_tests` run (release gate SF=10 + manual
dispatch), newest at the bottom. Written by the `tpch-stress` job; the live scorecard is in
[tpch.md](tpch.md).

| Date | Run | Commit | DuckDB | delta_rs | SF | CPU | Ingest | Queries | OK |
|------|-----|--------|--------|----------|----|-----|--------|---------|----|
