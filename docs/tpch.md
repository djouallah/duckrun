# TPC-H benchmark

The TPC-H benchmark generates TPC-H with `tpchgen-cli`, ingests all 8 tables from Parquet into
Delta through duckrun's write path (`conn.read.parquet(...).write.saveAsTable(...)`), then runs the
22 TPC-H queries through `conn.sql` over `delta_scan` — timing each. A fast **SF=1** smoke runs on
every push as a guard ([`cores.yml`](../.github/workflows/cores.yml)); the heavy scorecard below is
the **SF=100** run from [`local_stress_tests.yml`](../.github/workflows/local_stress_tests.yml)
(manual dispatch on the big-disk runner), committed to `main`.

It is a **coverage + cost** check, not a speed contest: the ingestion time is duckrun's write
path, but the 22 query times are DuckDB reading Delta with no second engine to compare against —
so read them as "the whole schema loads and all 22 queries run at this scale", not a "duckrun is
fast" claim.

<!-- TPCH:START -->

## 🐤 TPC-H benchmark — duckrun on Delta Lake

**What this checks:** duckrun ingests the full TPC-H schema (8 tables) from Parquet into Delta through its write path (`conn.read.parquet(...).write.saveAsTable(...)`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is duckrun's write path; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 544.4s** &middot; **run 22 queries in 456.6s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (duckrun write path)
| Table | Rows | Write (s) |
|---|---:|---:|
| `nation` | 25 | 0.97 |
| `region` | 5 | 0.01 |
| `customer` | 15,000,000 | 11.86 |
| `supplier` | 1,000,000 | 0.94 |
| `lineitem` | 600,037,902 | 372.51 |
| `orders` | 150,000,000 | 90.73 |
| `partsupp` | 80,000,000 | 54.93 |
| `part` | 20,000,000 | 12.42 |
| **Total** | **866,037,932** | **544.37** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 18.816 |
| Q02 | 3.915 |
| Q03 | 11.980 |
| Q04 | 7.913 |
| Q05 | 22.632 |
| Q06 | 6.011 |
| Q07 | 16.280 |
| Q08 | 19.226 |
| Q09 | 32.982 |
| Q10 | 14.542 |
| Q11 | 1.963 |
| Q12 | 8.605 |
| Q13 | 19.895 |
| Q14 | 11.449 |
| Q15 | 9.740 |
| Q16 | 3.607 |
| Q17 | 120.950 |
| Q18 | 22.122 |
| Q19 | 14.268 |
| Q20 | 19.024 |
| Q21 | 65.033 |
| Q22 | 5.665 |
| **Total** | **456.62** |

<!-- TPCH:END -->
