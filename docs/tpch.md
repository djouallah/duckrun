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
"duckrun is fast" claim.
Every run also appends one line to the [full run history](tpch-benchmark-history.md).

<!-- TPCH:START -->

## 🐤 TPC-H benchmark — duckrun on Delta Lake

**What this checks:** duckrun registers the full TPC-H schema (8 tables) as Delta in place via `conn.convert_to_delta` (zero-copy — writes only the `_delta_log`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is the (near-free) convert; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 682.4s** &middot; **run 22 queries in 475.8s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.5 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convert_to_delta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 1.36 |
| `region` | 5 | 0.01 |
| `customer` | 15,000,000 | 15.73 |
| `supplier` | 1,000,000 | 1.67 |
| `lineitem` | 600,037,902 | 481.43 |
| `orders` | 150,000,000 | 123.87 |
| `partsupp` | 80,000,000 | 43.54 |
| `part` | 20,000,000 | 14.83 |
| **Total** | **866,037,932** | **682.44** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 21.882 |
| Q02 | 3.860 |
| Q03 | 12.705 |
| Q04 | 7.794 |
| Q05 | 21.326 |
| Q06 | 4.446 |
| Q07 | 15.013 |
| Q08 | 30.560 |
| Q09 | 47.686 |
| Q10 | 18.202 |
| Q11 | 2.239 |
| Q12 | 10.322 |
| Q13 | 18.268 |
| Q14 | 17.751 |
| Q15 | 8.362 |
| Q16 | 3.405 |
| Q17 | 93.534 |
| Q18 | 41.148 |
| Q19 | 13.454 |
| Q20 | 18.006 |
| Q21 | 58.874 |
| Q22 | 7.004 |
| **Total** | **475.84** |

<!-- TPCH:END -->
