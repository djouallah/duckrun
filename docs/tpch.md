# TPC-H benchmark

The TPC-H benchmark generates TPC-H with `tpchgen-cli`, registers all 8 tables as Delta **in place**
via `DeltaTable.convertToDelta` (delta-rs `convert_to_deltalake` — a zero-copy convert that writes
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

> **Ingest 8 tables in 0.1s** &middot; **run 22 queries in 678.3s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.03 |
| `region` | 5 | 0.00 |
| `customer` | 15,000,000 | 0.01 |
| `supplier` | 1,000,000 | 0.01 |
| `lineitem` | 600,037,902 | 0.07 |
| `orders` | 150,000,000 | 0.02 |
| `partsupp` | 80,000,000 | 0.01 |
| `part` | 20,000,000 | 0.01 |
| **Total** | **866,037,932** | **0.15** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 22.004 |
| Q02 | 6.058 |
| Q03 | 31.265 |
| Q04 | 9.201 |
| Q05 | 39.129 |
| Q06 | 8.656 |
| Q07 | 26.598 |
| Q08 | 45.391 |
| Q09 | 67.968 |
| Q10 | 25.414 |
| Q11 | 4.589 |
| Q12 | 11.976 |
| Q13 | 17.858 |
| Q14 | 24.032 |
| Q15 | 22.107 |
| Q16 | 3.920 |
| Q17 | 116.213 |
| Q18 | 29.987 |
| Q19 | 32.435 |
| Q20 | 38.173 |
| Q21 | 86.901 |
| Q22 | 8.375 |
| **Total** | **678.25** |

<!-- TPCH:END -->
