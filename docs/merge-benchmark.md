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
| Target fact table | TPCH `lineitem`, scale factor **10.0** → **59,986,052 rows** |
| Primary key (merge on) | `(l_orderkey, l_linenumber)` |
| Effective memory | 14904 MB (runner RAM, no artificial limit) |
| Merge spill cap | 8942 MB — delta_rs `max_spill_size` |

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
| Mixed upsert | 0.6M | 0.5M | 0.1M | 60.0M | 60.1M | 60.1M | ✅ | ✅ | 4,466 MB | 71.0s |
| Insert-only (future shipdate) | 3.0M | 0.0M | 3.0M | 60.1M | 63.1M | 63.1M | ✅ | ✅ | 2,681 MB | 15.5s |
| Update-only (100% match) | 3.2M | 3.2M | 0.0M | 63.1M | 63.1M | 63.1M | ✅ | ✅ | 5,465 MB | 69.9s |
| Idempotent re-merge | 0.0M | 0.0M | 0.0M | 63.1M | 63.1M | 63.1M | ✅ | ✅ | 4,628 MB | 107.6s |
| CDC merge (delete+update+insert) | 1.8M | 0.9M | 0.6M | 30.1M | 30.4M | 30.4M | ✅ | ✅ | 3,713 MB | 41.8s |
| Full sync (update + by-source delete) | 15.0M | 15.0M | 0.0M | 30.0M | 15.0M | 15.0M | ✅ | ✅ | 6,027 MB | 26.4s |
| Expression update (set_expressions + CASE) | 1.5M | 1.5M | 0.0M | 30.0M | 30.0M | 30.0M | ✅ | ✅ | 4,632 MB | 36.1s |
| Append (no merge) | 3.2M | 0.0M | 3.2M | 63.1M | 66.3M | 66.3M | ✅ | ✅ | 1,133 MB | 15.7s |
| Safeappend (no merge) | 3.3M | 0.0M | 3.3M | 66.3M | 69.6M | 69.6M | ✅ | ✅ | 1,155 MB | 18.7s |
| Overwrite (no merge) | 3.5M | 0.0M | 3.5M | 69.6M | 3.5M | 3.5M | ✅ | ✅ | 1,129 MB | 16.1s |

**Result: ✅ all operations correct.** The chain tail reached **69,582,509 rows**, peak memory **6,027 MB** — duckrun stayed within the runner's RAM and every update/insert landed through the dbt path.

<!-- MERGE:END -->
