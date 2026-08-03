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
Every run also appends one line to the [full run history](tpch-benchmark-history.md).

<!-- TPCH:START -->

## 🐤 TPC-H benchmark — duckrun on Delta Lake

**What this checks:** duckrun registers the full TPC-H schema (8 tables) as Delta in place via `conn.convert_to_delta` (zero-copy — writes only the `_delta_log`), then runs the 22 TPC-H queries through `conn.sql` over `delta_scan`. The **ingestion** time is the (near-free) convert; the **query** times are DuckDB reading Delta — there is no second engine to race here, so read them as "the whole schema loads and all 22 queries run at this scale", not a *duckrun is fast* claim.

> **Ingest 8 tables in 683.4s** &middot; **run 22 queries in 480.7s** &middot; SF 100 &middot; 866.0M rows &middot; 4 cores

### Setup
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.5 &middot; delta_rs 1.5.0 |
| Scale factor | **100** |
| Runner | GitHub-hosted &middot; 4 cores |

### Ingestion — Parquet → Delta (zero-copy convert_to_delta)
| Table | Rows | Convert (s) |
|---|---:|---:|
| `nation` | 25 | 0.79 |
| `region` | 5 | 0.01 |
| `customer` | 15,000,000 | 15.53 |
| `supplier` | 1,000,000 | 1.63 |
| `lineitem` | 600,037,902 | 478.46 |
| `orders` | 150,000,000 | 122.01 |
| `partsupp` | 80,000,000 | 49.12 |
| `part` | 20,000,000 | 15.83 |
| **Total** | **866,037,932** | **683.39** |

### Queries — 22 TPC-H over `delta_scan`
| Query | Duration (s) |
|:---|---:|
| Q01 | 23.122 |
| Q02 | 3.768 |
| Q03 | 12.503 |
| Q04 | 7.732 |
| Q05 | 21.368 |
| Q06 | 5.395 |
| Q07 | 15.364 |
| Q08 | 26.078 |
| Q09 | 48.729 |
| Q10 | 19.585 |
| Q11 | 2.312 |
| Q12 | 14.586 |
| Q13 | 18.620 |
| Q14 | 19.226 |
| Q15 | 7.906 |
| Q16 | 3.501 |
| Q17 | 94.590 |
| Q18 | 26.071 |
| Q19 | 18.036 |
| Q20 | 16.688 |
| Q21 | 68.688 |
| Q22 | 6.786 |
| **Total** | **480.65** |

<!-- TPCH:END -->
