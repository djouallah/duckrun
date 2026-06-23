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

> **Ingest 8 tables in 53.7s** &middot; **run 22 queries in 31.2s** &middot; SF 10 &middot; 86.6M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **10** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (duckrun write path)
| Table | Rows | Write (s) |
|---|---:|---:|
| `nation` | 25 | 0.94 |
| `region` | 5 | 0.01 |
| `customer` | 1,500,000 | 1.52 |
| `supplier` | 100,000 | 0.12 |
| `lineitem` | 59,986,052 | 35.34 |
| `orders` | 15,000,000 | 8.75 |
| `partsupp` | 8,000,000 | 5.49 |
| `part` | 2,000,000 | 1.59 |
| **Total** | **86,586,082** | **53.74** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 1.818 |
| Q02 | 0.464 |
| Q03 | 1.147 |
| Q04 | 0.767 |
| Q05 | 1.753 |
| Q06 | 0.563 |
| Q07 | 1.273 |
| Q08 | 1.571 |
| Q09 | 2.523 |
| Q10 | 1.712 |
| Q11 | 0.214 |
| Q12 | 0.795 |
| Q13 | 1.709 |
| Q14 | 1.031 |
| Q15 | 0.682 |
| Q16 | 0.395 |
| Q17 | 2.954 |
| Q18 | 1.969 |
| Q19 | 1.469 |
| Q20 | 1.465 |
| Q21 | 4.219 |
| Q22 | 0.735 |
| **Total** | **31.23** |

<!-- TPCH:END -->
