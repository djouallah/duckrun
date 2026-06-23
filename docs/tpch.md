# TPC-H benchmark

The `tpch-smoke` job in [`cores.yml`](../.github/workflows/cores.yml) generates TPC-H at scale
factor **10** with `tpchgen-cli`, ingests all 8 tables from Parquet into Delta through duckrun's
write path (`conn.read.parquet(...).write.saveAsTable(...)`), then runs the 22 TPC-H queries
through `conn.sql` over `delta_scan` — timing each. It runs on a standard GitHub-hosted runner on
every push, and the scorecard below is rendered live and committed on every push to `main`.

It is a **coverage + cost** check, not a speed contest: the ingestion time is duckrun's write
path, but the 22 query times are DuckDB reading Delta with no second engine to compare against —
so read them as "the whole schema loads and all 22 queries run at SF=10", not a "duckrun is fast"
claim.

<!-- TPCH:START -->

## 🐤 TPC-H benchmark — duckrun on Delta Lake

**What this checks:** duckrun ingests the full TPC-H schema (8 tables) from Parquet into Delta through its write path (`conn.read.parquet(...).write.saveAsTable(...)`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is duckrun's write path; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 56.6s** &middot; **run 22 queries in 33.4s** &middot; SF 10 &middot; 86.6M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **10** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (duckrun write path)
| Table | Rows | Write (s) |
|---|---:|---:|
| `nation` | 25 | 0.99 |
| `region` | 5 | 0.01 |
| `customer` | 1,500,000 | 1.62 |
| `supplier` | 100,000 | 0.13 |
| `lineitem` | 59,986,052 | 37.56 |
| `orders` | 15,000,000 | 8.88 |
| `partsupp` | 8,000,000 | 5.71 |
| `part` | 2,000,000 | 1.68 |
| **Total** | **86,586,082** | **56.58** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 1.994 |
| Q02 | 0.447 |
| Q03 | 1.179 |
| Q04 | 0.758 |
| Q05 | 1.829 |
| Q06 | 0.610 |
| Q07 | 1.391 |
| Q08 | 1.697 |
| Q09 | 2.697 |
| Q10 | 1.840 |
| Q11 | 0.216 |
| Q12 | 0.823 |
| Q13 | 1.793 |
| Q14 | 1.058 |
| Q15 | 0.757 |
| Q16 | 0.433 |
| Q17 | 3.455 |
| Q18 | 2.136 |
| Q19 | 1.560 |
| Q20 | 1.578 |
| Q21 | 4.442 |
| Q22 | 0.746 |
| **Total** | **33.44** |

<!-- TPCH:END -->
