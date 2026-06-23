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

```
┌──────────────────────────────────┐
│ ingest 8 tables      56.8s       │
│ run 22 queries      32.6s        │
│ SF 10  -  86.6M rows  -  4 cores │
└──────────────────────────────────┘
```

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **10** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (duckrun write path)
| Table | Rows | Write (s) |
|---|---:|---:|
| `nation` | 25 | 0.81 |
| `region` | 5 | 0.01 |
| `customer` | 1,500,000 | 1.56 |
| `supplier` | 100,000 | 0.13 |
| `lineitem` | 59,986,052 | 37.64 |
| `orders` | 15,000,000 | 8.98 |
| `partsupp` | 8,000,000 | 6.03 |
| `part` | 2,000,000 | 1.62 |
| **Total** | **86,586,082** | **56.79** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 1.875 |
| Q02 | 0.453 |
| Q03 | 1.178 |
| Q04 | 0.781 |
| Q05 | 1.997 |
| Q06 | 0.573 |
| Q07 | 1.353 |
| Q08 | 1.565 |
| Q09 | 2.659 |
| Q10 | 1.788 |
| Q11 | 0.211 |
| Q12 | 0.804 |
| Q13 | 1.805 |
| Q14 | 1.047 |
| Q15 | 0.704 |
| Q16 | 0.413 |
| Q17 | 3.178 |
| Q18 | 2.055 |
| Q19 | 1.495 |
| Q20 | 1.519 |
| Q21 | 4.358 |
| Q22 | 0.766 |
| **Total** | **32.58** |

<!-- TPCH:END -->
