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

> **Ingest 8 tables in 0.3s** &middot; **run 22 queries in 669.8s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.15 |
| `region` | 5 | 0.00 |
| `customer` | 15,000,000 | 0.01 |
| `supplier` | 1,000,000 | 0.01 |
| `lineitem` | 600,037,902 | 0.07 |
| `orders` | 150,000,000 | 0.02 |
| `partsupp` | 80,000,000 | 0.01 |
| `part` | 20,000,000 | 0.01 |
| **Total** | **866,037,932** | **0.26** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 23.994 |
| Q02 | 5.930 |
| Q03 | 30.526 |
| Q04 | 9.474 |
| Q05 | 38.490 |
| Q06 | 8.841 |
| Q07 | 26.019 |
| Q08 | 45.224 |
| Q09 | 62.773 |
| Q10 | 24.535 |
| Q11 | 4.565 |
| Q12 | 11.493 |
| Q13 | 17.611 |
| Q14 | 23.987 |
| Q15 | 22.168 |
| Q16 | 3.898 |
| Q17 | 116.652 |
| Q18 | 29.723 |
| Q19 | 32.493 |
| Q20 | 37.581 |
| Q21 | 85.524 |
| Q22 | 8.277 |
| **Total** | **669.78** |

<!-- TPCH:END -->
