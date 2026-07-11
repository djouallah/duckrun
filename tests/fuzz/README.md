# fuzz/ — background differential fuzzing (NOT a PR gate)

These are **soak tests**: long, randomized, and **nondeterministic**, so they don't belong on
the per-PR path. sqlsmith generates a fresh corpus every run even with a fixed `--seed` (its
generation isn't fully seed-stable), so the finding count varies run to run and can't be a
red/green gate. Run them by hand or on a schedule (nightly / manual dispatch), not on every push.

## fuzz_replay.py

DuckDB's own `sqlsmith` fuzzer as the generator, each generated statement replayed **differentially**
through `duckrun.connect().sql()` vs plain DuckDB (threads=1) as the oracle. Replaying through
`.sql()` — instead of `CALL sqlsmith()` on a raw connection — is the point: it exercises the Python
routing seam, not the C++ engine.

```
python tests/fuzz/fuzz_replay.py --queries 400 --seed 99
```

Needs the `sqlsmith` DuckDB extension (the script `INSTALL`s + `LOAD`s it itself).

### Reading the results

The stats buckets split into two very different kinds of divergence:

- **`result_diff` / `state_diff` / `persistence_diff`** — the ones that matter: the engine returned
  **different data**, left tables in a different state, or lost writes across a reconnect. Chase these.
  Note the common false positive: a query with an unordered `LIMIT … OFFSET` (or a `LIMIT` scalar
  subquery) is genuinely nondeterministic — a Delta table read via `delta_scan` has a different
  physical row order than a native DuckDB table — and the heuristic can miss it when a window
  `OVER (… ORDER BY …)` masks the top-level lack of `ORDER BY`. Confirm determinism before filing.
- **`error_mismatch`** — the engine raised where DuckDB succeeded (or vice versa). Usually **not** a
  bug: it's duckrun failing loud on exotic syntax delta_rs can't route (`RETURNING`, `VALUES (…,
  DEFAULT)`, `TABLESAMPLE SYSTEM (n)` on a view, …). Fail-loud ≠ wrong-answer. Only a divergence that
  produces or persists wrong data is a correctness defect.
