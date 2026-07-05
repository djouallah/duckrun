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

**What this checks:** duckrun registers the full TPC-H schema (8 tables) as Delta in place via a zero-copy convert (writes only the `_delta_log`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is the (near-free) convert; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 552.3s** &middot; **run 22 queries in 471.3s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convertToDelta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.80 |
| `region` | 5 | 0.01 |
| `customer` | 15,000,000 | 10.83 |
| `supplier` | 1,000,000 | 1.02 |
| `lineitem` | 600,037,902 | 391.07 |
| `orders` | 150,000,000 | 92.79 |
| `partsupp` | 80,000,000 | 42.89 |
| `part` | 20,000,000 | 12.91 |
| **Total** | **866,037,932** | **552.33** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 18.798 |
| Q02 | 3.247 |
| Q03 | 11.684 |
| Q04 | 7.666 |
| Q05 | 22.029 |
| Q06 | 5.379 |
| Q07 | 14.710 |
| Q08 | 25.166 |
| Q09 | 56.736 |
| Q10 | 16.520 |
| Q11 | 2.188 |
| Q12 | 12.835 |
| Q13 | 17.874 |
| Q14 | 18.867 |
| Q15 | 6.967 |
| Q16 | 3.643 |
| Q17 | 99.174 |
| Q18 | 24.349 |
| Q19 | 16.858 |
| Q20 | 17.344 |
| Q21 | 63.211 |
| Q22 | 6.028 |
| **Total** | **471.27** |

<!-- TPCH:END -->
