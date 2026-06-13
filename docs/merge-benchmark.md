# Incremental MERGE benchmark

The [`merge-spill`](../.github/workflows/merge.yml) workflow builds a large TPCH `lineitem`
fact table (the release gate runs scale factor **20**, ~120M rows) and runs four merge
shapes against it — mixed upsert, insert-only, update-only, and an idempotent re-merge —
plus a plain `append`, `safeappend`, and `overwrite` of the same batch for comparison, on a
single machine with duckrun's shipping memory defaults (per-merge DuckDB `memory_limit` +
delta_rs `max_spill_size` + target pruning). It runs on a standard **GitHub-hosted runner
(~16 GB RAM)** — no beefy hardware — proving the merges stay within that RAM and apply every
UPDATE/INSERT correctly, and lets you compare a MERGE's cost against a plain write of the same
batch. It gates every release; the latest scorecard is rendered live below.

<!-- MERGE:START -->

## 🔀 Incremental MERGE test — duckrun on Delta Lake

**What this checks:** that duckrun MERGEs incremental batches into a large Delta *fact* table on one machine — across four merge shapes — applying UPDATEs and INSERTs correctly without being OOM-killed, and how the same batch compares against a plain `append` / `safeappend` / `overwrite` on that same table (which never scan the target).

### Setup (the inputs)
| | |
|---|---|
| Engine | duckrun &middot; DuckDB 1.5.1 &middot; delta_rs 1.5.0 |
| Target fact table | TPCH `lineitem`, scale factor **20.0** → **119,994,608 rows** |
| Primary key (merge on) | `(l_orderkey, l_linenumber)` |
| Effective memory | 14927 MB (runner RAM, no artificial limit) |
| DuckDB `memory_limit` | 12.4 GiB — set by duckrun (cgroup-aware) |
| Merge spill cap | 8956 MB — delta_rs `max_spill_size` |

### The operations (run in order, on the same growing table)
1. **Mixed upsert (~1% sample):** ~80% existing keys → UPDATE (randomized measures), ~20% key-shifted → INSERT. _Expect:_ rows grow by the inserts; updated rows carry the new measures.
2. **Insert-only (~5% sample):** key-shifted past the max key so nothing matches, stamped with a future `l_shipdate` (2035). _Expect:_ every row INSERTed; exactly that many rows carry the 2035 date.
3. **Update-only (~5% sample):** existing keys, randomized measures, no key shift → 100% match. _Expect:_ row count unchanged; rows carry the new measures.
4. **Idempotent re-merge:** re-run scenario 3's exact batch. _Expect:_ a correct MERGE is idempotent — nothing changes (same row count, same values).
5. **Append (no merge):** the same batch appended to the table. _Expect:_ rows grow by the batch — far faster than a MERGE, because an append only lands files (no target scan/join) and DuckDB streams the source.
6. **Safeappend (no merge):** the same batch via `safeappend` — a plain append that commits only if the table version is unchanged since it was read (it is here). _Expect:_ same cheap append, now version-guarded against concurrent writers.
7. **Overwrite (no merge):** the same batch overwriting the table. _Expect:_ the table is replaced by the batch — also far faster than a MERGE (no target scan/join).

### Results (row counts in millions)
| Operation | Increment | Updates | Inserts | Before | After | Expected | Count ✓ | Values ✓ | Time |
|---|---:|---:|---:|---:|---:|---:|:---:|:---:|---:|
| Mixed upsert | 1.2M | 1.0M | 0.2M | 120.0M | 120.2M | 120.2M | ✅ | ✅ | 150.1s |
| Insert-only (future shipdate) | 6.0M | 0.0M | 6.0M | 120.2M | 126.2M | 126.2M | ✅ | ✅ | 5.0s |
| Update-only (100% match) | 6.0M | 6.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 158.3s |
| Idempotent re-merge | 6.0M | 6.0M | 0.0M | 126.2M | 126.2M | 126.2M | ✅ | ✅ | 172.3s |
| Append (no merge) | 6.0M | 0.0M | 6.0M | 126.2M | 132.2M | 132.2M | ✅ | ✅ | 4.3s |
| Safeappend (no merge) | 6.0M | 0.0M | 6.0M | 132.2M | 138.2M | 138.2M | ✅ | ✅ | 4.2s |
| Overwrite (no merge) | 6.0M | 0.0M | 6.0M | 138.2M | 6.0M | 6.0M | ✅ | ✅ | 4.1s |

_The last three rows are the same batch as a plain `append` / `safeappend` / `overwrite` — compare their time against the merges above to see the cost a MERGE pays to scan & join the target._

**Result: ✅ all operations correct.** Target grew to **126,231,331 rows** across the merges, peak memory **6,507 MB** — duckrun stayed within the runner's RAM and every update/insert landed as expected.

<!-- MERGE:END -->
