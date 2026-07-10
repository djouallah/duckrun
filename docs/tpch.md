# TPC-H benchmark

The TPC-H benchmark generates TPC-H with `tpchgen-cli`, registers all 8 tables as Delta **in place**
via a zero-copy convert (delta-rs `convert_to_deltalake`, which writes
only the `_delta_log`, never rewriting the parquet), then runs the 22 TPC-H queries through
`conn.sql` over `delta_scan` — timing each. A fast **SF=1** smoke runs on every push as a guard
([`cores.yml`](../.github/workflows/cores.yml)); the heavy scorecard below is the **SF=100** run from
[`local_stress_tests.yml`](../.github/workflows/local_stress_tests.yml) (manual dispatch on the
big-disk runner), committed to `main`.

It is a **coverage + cost** check, not a speed contest: the ingestion time is the (near-free)
convert cost, and the 22 query times are DuckDB reading Delta with no second engine to compare
against — so read them as "the whole schema loads and all 22 queries run at this scale", not a
"duckrun is fast" claim. (We measured rewriting sorted with fine row groups and a native DuckDB file
too; zero-rewrite convert was cheapest to load and fastest to query, so it's the arm kept.)

<!-- TPCH:START -->

## 🐤 TPC-H benchmark — duckrun on Delta Lake

**What this checks:** duckrun registers the full TPC-H schema (8 tables) as Delta in place via `DeltaTable.convertToDelta` (zero-copy — writes only the `_delta_log`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is the (near-free) convert; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 62.9s** &middot; **run 22 queries in 26.3s** &middot; SF 10 &middot; 86.6M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **10** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 1.24 |
| `region` | 5 | 0.02 |
| `customer` | 1,500,000 | 2.29 |
| `supplier` | 100,000 | 0.14 |
| `lineitem` | 59,986,052 | 42.79 |
| `orders` | 15,000,000 | 10.32 |
| `partsupp` | 8,000,000 | 4.31 |
| `part` | 2,000,000 | 1.81 |
| **Total** | **86,586,082** | **62.92** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 1.910 |
| Q02 | 0.340 |
| Q03 | 0.830 |
| Q04 | 0.659 |
| Q05 | 1.411 |
| Q06 | 0.368 |
| Q07 | 1.004 |
| Q08 | 1.225 |
| Q09 | 2.306 |
| Q10 | 1.189 |
| Q11 | 0.158 |
| Q12 | 0.679 |
| Q13 | 1.627 |
| Q14 | 0.534 |
| Q15 | 0.467 |
| Q16 | 0.310 |
| Q17 | 2.784 |
| Q18 | 1.782 |
| Q19 | 0.908 |
| Q20 | 1.176 |
| Q21 | 4.027 |
| Q22 | 0.652 |
| **Total** | **26.35** |

<!-- TPCH:END -->
