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

> **Ingest 8 tables in 7.5s** &middot; **run 22 queries in 5.4s** &middot; SF 1 &middot; 8.7M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **1** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 1.00 |
| `region` | 5 | 0.01 |
| `customer` | 150,000 | 0.24 |
| `supplier` | 10,000 | 0.02 |
| `lineitem` | 6,001,215 | 4.33 |
| `orders` | 1,500,000 | 1.15 |
| `partsupp` | 800,000 | 0.57 |
| `part` | 200,000 | 0.19 |
| **Total** | **8,661,245** | **7.51** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 0.414 |
| Q02 | 0.120 |
| Q03 | 0.202 |
| Q04 | 0.190 |
| Q05 | 0.329 |
| Q06 | 0.093 |
| Q07 | 0.233 |
| Q08 | 0.310 |
| Q09 | 0.373 |
| Q10 | 0.252 |
| Q11 | 0.053 |
| Q12 | 0.173 |
| Q13 | 0.355 |
| Q14 | 0.114 |
| Q15 | 0.090 |
| Q16 | 0.071 |
| Q17 | 0.361 |
| Q18 | 0.411 |
| Q19 | 0.221 |
| Q20 | 0.208 |
| Q21 | 0.742 |
| Q22 | 0.102 |
| **Total** | **5.42** |

<!-- TPCH:END -->
