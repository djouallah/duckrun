# Incremental MERGE benchmark

The `merge-spill` job in [`local_stress_tests.yml`](../.github/workflows/local_stress_tests.yml) builds a large TPCH `lineitem`
fact table (the release gate runs scale factor **20**, ~120M rows) and runs four merge
shapes against it — mixed upsert, insert-only, update-only, and an idempotent re-merge —
plus a plain `append`, `safeappend`, and `overwrite` of the same batch for comparison, on a
single machine with duckrun's shipping memory defaults (per-merge DuckDB `memory_limit` +
delta_rs `max_spill_size` + target pruning). It runs on a standard **GitHub-hosted runner
(~16 GB RAM)** — no beefy hardware — proving the merges stay within that RAM and apply every
UPDATE/INSERT correctly, and lets you compare a MERGE's cost against a plain write of the same
batch. It gates every release; the latest scorecard is rendered live below.

<!-- MERGE:START -->

## 🔀 Incremental MERGE test — duckrun on Delta Lake (via dbt)

**What this checks:** that duckrun MERGEs incremental batches into a large Delta *fact* table **through the dbt path** — a chain of `ref`-ed incremental models (the materialization's read-pin + the plugin's spill cap) — applying UPDATEs and INSERTs correctly without being OOM-killed, and how the same shape compares against a plain `append` / `safeappend` / `overwrite` (which never scan the target).

### Setup (the inputs)
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Target fact table | TPCH `lineitem`, scale factor **20.0** → **119,994,608 rows** |
| Primary key (merge on) | `(l_orderkey, l_linenumber)` |
| Effective memory | 14923 MB (runner RAM, no artificial limit) |
| Merge spill cap | 8954 MB — delta_rs `max_spill_size` |

### The operations (a chain — each builds on the previous via `ref`)
1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE, ~20% key-shifted → INSERT.
2. **Insert-only (~5% sample):** key-shifted past max key, future `l_shipdate` (2035) → all INSERT.
3. **Update-only (~5% sample):** existing keys, no shift → 100% match; row count unchanged.
4. **Idempotent re-merge:** re-merge unchanged rows → nothing changes.
5. **Append (no merge):** the batch appended — no target scan/join (far cheaper).
6. **Safeappend (no merge):** same cheap append, version-guarded against concurrent writers.
7. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.

### Results (row counts in millions; peak RSS is the `dbt run` child's, per op)
| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Peak RSS | Time |
|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|---:|
| Mixed upsert | 1.2M | 1.0M | 0.2M | 120.0M | 120.2M | 120.2M | ✅ | ✅ | 4,945 MB | 196.6s |
| Insert-only (future shipdate) | 6.0M | 0.0M | 6.0M | 120.2M | 126.2M | 126.2M | ✅ | ✅ | 3,608 MB | 34.9s |
| Update-only (100% match) | 6.3M | 6.3M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 6,746 MB | 202.7s |
| Idempotent re-merge | 0.0M | 0.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 6,308 MB | 157.4s |
| Append (no merge) | 6.3M | 0.0M | 6.3M | 126.2M | 132.6M | 132.6M | ✅ | ✅ | 1,564 MB | 32.4s |
| Safeappend (no merge) | 6.6M | 0.0M | 6.6M | 132.6M | 139.2M | 139.2M | ✅ | ✅ | 1,547 MB | 34.8s |
| Overwrite (no merge) | 7.0M | 0.0M | 7.0M | 139.2M | 7.0M | 7.0M | ✅ | ✅ | 1,668 MB | 31.2s |

**Result: ✅ all operations correct.** The chain tail reached **139,183,841 rows**, peak memory **6,746 MB** — duckrun stayed within the runner's RAM and every update/insert landed through the dbt path.

<!-- MERGE:END -->
