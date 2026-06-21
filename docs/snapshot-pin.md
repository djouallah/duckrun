# DuckDB snapshot pin — what it fixes

On every incremental run, duckrun captures the target table's Delta version `vB` *before* the
model runs, then anchors both sides of the run to it:

- **Read pin** — the model's `{{ this }}` self-reference is registered as
  `delta_scan('<location>', version => vB)` (the duckdb-delta `version => N` param, which is why
  duckdb is pinned to the 1.5.4 floor), so the model SQL sees the table as of `vB`, not a drifted
  HEAD.
- **Write pin** — the merge target is opened at the same `vB`, and delta-rs validates the commit
  over the window `(vB, HEAD]`. A foreign commit inside that window makes the merge refuse.

Together this is single-snapshot MERGE semantics: the read and the write agree on one version
of the table. Lakehouses don't guarantee a single writer (a job double-fires, two pipelines touch
the same table), so this matters in practice.

The `snapshot-pin` job in [`cores.yml`](../.github/workflows/cores.yml) proves it through a **real
`dbt run`**: it runs the same concurrent-writer race twice against an incremental MERGE model —
once *without* the pin (the old behaviour: read/commit against HEAD) and once *with* it — and walks
the table's Delta versions so you can see, version by version, where the unpinned path silently
loses data and the pinned path catches it. The full scenario is in
[`tests/integration_tests/snapshot_pin/`](../tests/integration_tests/snapshot_pin/); the assertions
that gate every change live in `test_snapshot_pin.py`. The latest scorecard is rendered live below.

<!-- SNAPSHOT-PIN:START -->

## 🔒 DuckDB snapshot pin — version by version ✅

**What this proves (through a real `dbt run`):** the same concurrent-writer race is run twice against an incremental MERGE model — the only difference is whether the merge is pinned to `vB` (the version captured at the start of the run) or left to read/commit against HEAD. The table's Delta versions tell the story: a writer commits `id=1 = 999` (`v1`) while the model is mid-run.

### WITHOUT the pin (merge vs HEAD — old behaviour) — merge anchored to `v1` (HEAD)

| Delta version | rows (`id`→`value`) | what produced it |
|:---:|---|---|
| `v0` | 1→10, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 | `dbt run` #1 — seed (ids 1..10) |
| `v1` | 1→999, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 | concurrent writer: `update id=1 -> 999` &nbsp;**← merge anchored HERE (HEAD) — NO pin** |
| `v2` | 1→111, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 | `dbt run` #2 — incremental MERGE (batch: update id=1 -> 111) |

**Final state:** 1→111, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 → ❌ WRONG — merge committed v2 against HEAD; `id=1 = 999` silently LOST (no error raised)

### WITH the pin (duckrun today) — merge pinned to `vB = v0`

| Delta version | rows (`id`→`value`) | what produced it |
|:---:|---|---|
| `v0` | 1→10, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 | `dbt run` #1 — seed (ids 1..10) &nbsp;**← merge pinned HERE (vB) — read + commit** |
| `v1` | 1→999, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 | concurrent writer: `update id=1 -> 999` |
| `v2` | *— never written* | **MERGE refused** — pinned to `v0`; HEAD is now `v1`, and the check `(v0, v1]` catches the concurrent `v1` |

**Final state:** 1→999, 2→20, 3→30, 4→40, 5→50, 6→60, 7→70, 8→80, 9→90, 10→100 → ✅ RIGHT — merge REFUSED — no v2 written; run failed loudly; `id=1 = 999` preserved

### 🔴 Final tables — same code, same race, 1 row differs

| id | WITH pin | WITHOUT pin | difference |
|:---:|:---:|:---:|:---:|
| `1` | **999** | **111** | ⬅️ **DIFFERENT** |
| `2` | 20 | 20 | same |
| `3` | 30 | 30 | same |
| `4` | 40 | 40 | same |
| `5` | 50 | 50 | same |
| `6` | 60 | 60 | same |
| `7` | 70 | 70 | same |
| `8` | 80 | 80 | same |
| `9` | 90 | 90 | same |
| `10` | 100 | 100 | same |

> **Read it like this** (here `vB = v0`, and HEAD = `v1`, the concurrent writer's commit): without the pin, `v2` overwrote `id=1 = 999` with `111` — the concurrent writer's change vanished and the run reported success. With the pin, the merge checks the range after its pinned version up to the current HEAD — `(v0, v1]` — sees the concurrent `v1` there, and **refused**: no `v2`, the run fails loudly, and `id=1 = 999` is still there. Re-run and it merges cleanly on top of `v1`.

> ✅ **The pin fixes a real correctness bug:** silent data loss becomes a safe, loud failure.

<!-- SNAPSHOT-PIN:END -->
