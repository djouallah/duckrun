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

> **Ingest 8 tables in 748.6s** &middot; **run 22 queries in 491.8s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 1.16 |
| `region` | 5 | 0.02 |
| `customer` | 15,000,000 | 16.63 |
| `supplier` | 1,000,000 | 1.77 |
| `lineitem` | 600,037,902 | 517.44 |
| `orders` | 150,000,000 | 147.35 |
| `partsupp` | 80,000,000 | 48.86 |
| `part` | 20,000,000 | 15.34 |
| **Total** | **866,037,932** | **748.57** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 23.371 |
| Q02 | 3.819 |
| Q03 | 14.444 |
| Q04 | 8.056 |
| Q05 | 22.894 |
| Q06 | 4.583 |
| Q07 | 15.880 |
| Q08 | 30.600 |
| Q09 | 52.500 |
| Q10 | 18.407 |
| Q11 | 2.249 |
| Q12 | 9.436 |
| Q13 | 18.515 |
| Q14 | 18.130 |
| Q15 | 7.239 |
| Q16 | 3.476 |
| Q17 | 95.344 |
| Q18 | 33.437 |
| Q19 | 18.473 |
| Q20 | 18.083 |
| Q21 | 64.978 |
| Q22 | 7.873 |
| **Total** | **491.79** |

<!-- TPCH:END -->
