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

> **Ingest 8 tables in 554.8s** &middot; **run 22 queries in 483.5s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.85 |
| `region` | 5 | 0.01 |
| `customer` | 15,000,000 | 11.80 |
| `supplier` | 1,000,000 | 1.02 |
| `lineitem` | 600,037,902 | 395.67 |
| `orders` | 150,000,000 | 87.49 |
| `partsupp` | 80,000,000 | 45.28 |
| `part` | 20,000,000 | 12.65 |
| **Total** | **866,037,932** | **554.76** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 19.615 |
| Q02 | 3.562 |
| Q03 | 13.129 |
| Q04 | 8.023 |
| Q05 | 22.161 |
| Q06 | 5.442 |
| Q07 | 15.853 |
| Q08 | 26.355 |
| Q09 | 46.990 |
| Q10 | 17.599 |
| Q11 | 2.481 |
| Q12 | 12.797 |
| Q13 | 17.138 |
| Q14 | 17.868 |
| Q15 | 7.025 |
| Q16 | 3.414 |
| Q17 | 113.512 |
| Q18 | 28.619 |
| Q19 | 18.501 |
| Q20 | 17.206 |
| Q21 | 60.209 |
| Q22 | 5.978 |
| **Total** | **483.48** |

<!-- TPCH:END -->
