# Incremental MERGE benchmark

The `merge-spill` job in [`local_stress_tests.yml`](../.github/workflows/local_stress_tests.yml) builds a large TPCH `lineitem`
fact table (the release gate runs scale factor **10**, ~60M rows) and runs four merge
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
| Effective memory | 14917 MB (runner RAM, no artificial limit) |
| Merge spill cap | 8950 MB — delta_rs `max_spill_size` |

### The operations (a chain — each builds on the previous via `ref`)
1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE, ~20% key-shifted → INSERT.
2. **Insert-only (~5% sample):** key-shifted past max key, future `l_shipdate` (2035) → all INSERT.
3. **Update-only (~5% sample):** existing keys, no shift → 100% match; row count unchanged.
4. **Idempotent re-merge:** re-merge unchanged rows → nothing changes.
5. **CDC merge (full clause set):** one MERGE that DELETEs a tombstoned slice, UPDATEs a sample, and INSERTs key-shifted rows — matched-delete + matched-update + not-matched-insert via `merge_clauses`.
6. **Full sync (by-source delete):** matched rows UPDATEd, keys a ~50% (streamed) source no longer carries DELETEd via `WHEN NOT MATCHED BY SOURCE` — the heaviest shape (whole-target anti-join); `merge_streamed_exec` keeps the big source from materializing.
7. **Expression update:** a 100%-match UPDATE whose SET is an arbitrary expression + `CASE` (`merge_update_set_expressions`), not a plain column copy.
8. **Append (no merge):** the batch appended — no target scan/join (far cheaper).
9. **Safeappend (no merge):** same cheap append, version-guarded against concurrent writers.
10. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.

_Operations 5–7 exercise delta-rs's full MERGE clause set and run on the LOCAL stress gate only; the OneLake path-smoke job skips them._

### Results (row counts in millions; peak RSS is the `dbt run` child's, per op)
| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Peak RSS | Time |
|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|---:|
| Mixed upsert | 1.2M | 1.0M | 0.2M | 120.0M | 120.2M | 120.2M | ✅ | ✅ | 5,003 MB | 193.5s |
| Insert-only (future shipdate) | 6.0M | 0.0M | 6.0M | 120.2M | 126.2M | 126.2M | ✅ | ✅ | 3,429 MB | 32.2s |
| Update-only (100% match) | 6.3M | 6.3M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 5,771 MB | 140.2s |
| Idempotent re-merge | 0.0M | 0.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 6,497 MB | 226.5s |
| CDC merge (delete+update+insert) | 3.6M | 1.8M | 1.2M | 60.1M | 60.7M | 60.7M | ✅ | ✅ | 4,812 MB | 103.7s |
| Full sync (update + by-source delete) | 30.1M | 30.1M | 0.0M | 60.1M | 30.1M | 30.1M | ✅ | ✅ | 10,561 MB | 52.3s |
| Expression update (set_expressions + CASE) | 3.0M | 3.0M | 0.0M | 60.1M | 60.1M | 60.1M | ✅ | ✅ | 5,191 MB | 96.2s |
| Append (no merge) | 6.3M | 0.0M | 6.3M | 126.2M | 132.6M | 132.6M | ✅ | ✅ | 1,560 MB | 31.7s |
| Safeappend (no merge) | 6.6M | 0.0M | 6.6M | 132.6M | 139.2M | 139.2M | ✅ | ✅ | 1,617 MB | 35.0s |
| Overwrite (no merge) | 7.0M | 0.0M | 7.0M | 139.2M | 7.0M | 7.0M | ✅ | ✅ | 1,630 MB | 31.6s |

**Result: ✅ all operations correct.** The chain tail reached **139,180,007 rows**, peak memory **10,561 MB** — duckrun stayed within the runner's RAM and every update/insert landed through the dbt path.

<!-- MERGE:END -->
