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

> **Ingest 8 tables in 56.0s** &middot; **run 22 queries in 31.4s** &middot; SF 10 &middot; 86.6M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **10** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (duckrun write path)
| Table | Rows | Write (s) |
|---|---:|---:|
| `nation` | 25 | 0.89 |
| `region` | 5 | 0.01 |
| `customer` | 1,500,000 | 1.56 |
| `supplier` | 100,000 | 0.13 |
| `lineitem` | 59,986,052 | 37.20 |
| `orders` | 15,000,000 | 8.83 |
| `partsupp` | 8,000,000 | 5.78 |
| `part` | 2,000,000 | 1.62 |
| **Total** | **86,586,082** | **56.02** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 1.867 |
| Q02 | 0.446 |
| Q03 | 1.142 |
| Q04 | 0.738 |
| Q05 | 1.816 |
| Q06 | 0.566 |
| Q07 | 1.301 |
| Q08 | 1.605 |
| Q09 | 2.528 |
| Q10 | 1.751 |
| Q11 | 0.197 |
| Q12 | 0.758 |
| Q13 | 1.708 |
| Q14 | 1.010 |
| Q15 | 0.653 |
| Q16 | 0.396 |
| Q17 | 2.887 |
| Q18 | 1.978 |
| Q19 | 1.517 |
| Q20 | 1.465 |
| Q21 | 4.310 |
| Q22 | 0.783 |
| **Total** | **31.42** |

<!-- TPCH:END -->
