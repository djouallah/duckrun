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

> **Ingest 8 tables in 657.3s** &middot; **run 22 queries in 449.8s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.81 |
| `region` | 5 | 0.02 |
| `customer` | 15,000,000 | 17.17 |
| `supplier` | 1,000,000 | 1.55 |
| `lineitem` | 600,037,902 | 459.69 |
| `orders` | 150,000,000 | 122.01 |
| `partsupp` | 80,000,000 | 41.76 |
| `part` | 20,000,000 | 14.33 |
| **Total** | **866,037,932** | **657.33** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 20.732 |
| Q02 | 3.396 |
| Q03 | 12.285 |
| Q04 | 7.371 |
| Q05 | 20.722 |
| Q06 | 4.293 |
| Q07 | 14.360 |
| Q08 | 28.802 |
| Q09 | 47.316 |
| Q10 | 16.455 |
| Q11 | 2.190 |
| Q12 | 9.198 |
| Q13 | 17.145 |
| Q14 | 17.423 |
| Q15 | 6.655 |
| Q16 | 3.145 |
| Q17 | 97.048 |
| Q18 | 25.004 |
| Q19 | 16.110 |
| Q20 | 16.395 |
| Q21 | 57.363 |
| Q22 | 6.425 |
| **Total** | **449.83** |

<!-- TPCH:END -->
