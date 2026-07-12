# Incremental MERGE benchmark

The `merge-spill` job in [`local_stress_tests.yml`](../.github/workflows/local_stress_tests.yml) builds a large TPCH `lineitem`
fact table (the release gate runs scale factor **10**, ~60M rows) and runs several merge
shapes against it — mixed upsert, insert-only, update-only, idempotent re-merge, and the full
delta-rs clause set (CDC delete+update+insert, by-source delete, expression update) — plus a
plain `append` and `overwrite` of the same batch for comparison, all through the
**connection API** (`duckrun.connect()` + `conn.sql(...)` — no dbt), on a single machine with
duckrun's shipping memory defaults (per-merge DuckDB `memory_limit` + delta_rs `max_spill_size`
+ target pruning). It runs on a standard **GitHub-hosted runner (~16 GB RAM)** — no beefy
hardware — proving the merges stay within that RAM and apply every UPDATE/INSERT/DELETE
correctly, and lets you compare a MERGE's cost against a plain write of the same batch. It gates
every release; the latest scorecard is rendered live below.

<!-- MERGE:START -->

## 🔀 Incremental MERGE test — duckrun on Delta Lake (via the connection API)

**What this checks:** that duckrun MERGEs incremental batches into a large Delta *fact* table **through the connection API** — a chain of `conn.sql(...)` MERGEs (the delta_rs spill cap + the per-merge DuckDB memory pin) — applying UPDATEs and INSERTs correctly without being OOM-killed, and how the same shape compares against a plain `append` / `append_if_unchanged` / `overwrite` (which never scan the target).

### Setup (the inputs)
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.4 &middot; delta_rs 1.5.0 |
| Target fact table | TPCH `lineitem`, scale factor **10.0** → **59,986,052 rows** |
| Primary key (merge on) | `(l_orderkey, l_linenumber)` |
| Effective memory | 15061 MB (runner RAM, no artificial limit) |
| Merge spill cap | 9037 MB — delta_rs `max_spill_size` |

### The operations (a chain — each builds on the previous)
1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE, ~20% key-shifted → INSERT.
2. **Insert-only (~5% sample):** key-shifted past max key, future `l_shipdate` (2035) → all INSERT.
3. **Update-only (~5% sample):** existing keys, no shift → 100% match; row count unchanged.
4. **Idempotent re-merge:** re-merge unchanged rows → nothing changes.
5. **CDC merge (full clause set):** one MERGE that DELETEs a tombstoned slice, UPDATEs a sample, and INSERTs key-shifted rows — `WHEN MATCHED … THEN DELETE` + `WHEN MATCHED THEN UPDATE SET *` + `WHEN NOT MATCHED THEN INSERT *`.
6. **Full sync (by-source delete):** matched rows UPDATEd, keys a ~50% (inline-subquery) source no longer carries DELETEd via `WHEN NOT MATCHED BY SOURCE` — the heaviest shape (whole-target anti-join).
7. **Expression update:** a 100%-match UPDATE whose SET is an arbitrary expression + `CASE` over the source, not a plain column copy.
8. **Append (no merge):** the batch appended — no target scan/join (far cheaper).
9. **Append #2 (no merge):** a second plain append of new data (the version-guard verb was removed; a read-modify-append on the SAME table is auto-fenced instead).
10. **Overwrite (no merge):** the table replaced by the batch — also no target scan/join.

_Operations 5–7 exercise delta-rs's full MERGE clause set and run on the LOCAL stress gate only; the OneLake path-smoke job skips them._

### Results (row counts in millions; peak RSS is the process's, per op)
| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Peak RSS | Time |
|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|---:|
| Mixed upsert | 0.6M | 0.5M | 0.1M | 60.0M | 60.1M | 60.1M | ✅ | ✅ | 4,430 MB | 186.2s |
| Insert-only (future shipdate) | 3.0M | 0.0M | 3.0M | 60.1M | 63.1M | 63.1M | ✅ | ✅ | 5,416 MB | 12.5s |
| Update-only (100% match) | 3.2M | 3.2M | 0.0M | 63.1M | 63.1M | 63.1M | ✅ | ✅ | 7,097 MB | 214.3s |
| Idempotent re-merge | 0.0M | 0.0M | 0.0M | 63.1M | 63.1M | 63.1M | ✅ | ✅ | 6,954 MB | 163.3s |
| CDC merge (delete+update+insert) | 1.8M | 0.9M | 0.6M | 30.1M | 30.4M | 30.4M | ✅ | ✅ | 6,514 MB | 88.0s |
| Full sync (update + by-source delete) | 15.0M | 15.0M | 0.0M | 30.1M | 15.0M | 15.0M | ✅ | ✅ | 9,724 MB | 107.9s |
| Expression update (set expressions + CASE) | 1.5M | 1.5M | 0.0M | 30.1M | 30.1M | 30.1M | ✅ | ✅ | 7,421 MB | 81.0s |
| Append (no merge) | 3.2M | 0.0M | 3.2M | 63.1M | 66.3M | 66.3M | ✅ | ✅ | 5,292 MB | 13.2s |
| Append #2 (plain) (no merge) | 3.3M | 0.0M | 3.3M | 66.3M | 69.6M | 69.6M | ✅ | ✅ | 8,291 MB | 15.0s |
| Overwrite (no merge) | 3.5M | 0.0M | 3.5M | 69.6M | 3.5M | 3.5M | ✅ | ✅ | 5,829 MB | 11.1s |

**Result: ✅ all operations correct.** The chain tail reached **69,584,660 rows**, peak memory **9,724 MB** — duckrun stayed within the runner's RAM and every update/insert landed through the connection API.

<!-- MERGE:END -->
