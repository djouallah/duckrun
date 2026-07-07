# duckrun-slt: a sqllogictest-style conformance suite for duckrun

A black-box logic test suite for any engine exposing `connect(path)` → `con.sql(q)`
over Delta Lake storage on a local path. It knows nothing about duckrun internals —
which is the point: it exists specifically to shake out regex-based SQL routing.

**933 records, all expected results validated against plain DuckDB 1.5.4 as a
semantics oracle** (via `duckrun_shim.py`). Any failure against duckrun is therefore
either a routing/rewrite bug, a Delta storage bug, or a deliberate deviation from
DuckDB semantics — never a wrong expectation in the suite.

## Run it

```bash
# against duckrun
python run_all.py tests/

# against the DuckDB oracle (should always be all-green)
DUCKRUN_MODULE=duckrun_shim python run_all.py tests/

# one file, keep the database dir for inspection
python runner.py tests/05_adversarial_parser.slt --db /tmp/inspect_me --stop-on-fail

# optional multi-writer OCC accounting check
python stress_concurrency.py duckrun
```

Each `.slt` file runs against a fresh temp directory. Exit code 1 on any failure.

## The files

| file | records | what it hunts |
|---|---|---|
| `01_ddl.slt` | 132 | lifecycle, CTAS, CREATE OR REPLACE, keyword/unicode/quoted identifiers, 20-table loop |
| `02_types.slt` | 74 | integer boundaries, DECIMAL(38,10), NaN/±Inf/-0.0, pre-1970 & year-9999 temporal, empty-string-vs-NULL, 30k strings, BLOB, LIST/STRUCT round-trips |
| `03_dml.slt` | 152 | insert/update/delete lifecycle, self-referencing inserts, delete-all-and-resurrect, 100 single-row commits then churn |
| `04_queries.slt` | 45 | windows, QUALIFY, recursive CTEs, GROUPING SETS, `NOT IN` with NULLs, correlated subqueries, ALL/ANY, DISTINCT ON |
| `05_adversarial_parser.slt` | 83 | **the regex killers** — see below |
| `06_delta_semantics.slt` | 101 | every write re-verified after `reconnect`: log replay, checkpoint pressure, schema evolution, all-NULL stats, min/max pruning boundaries |
| `07_errors.slt` | 43 | every failure must raise cleanly; failed multi-row inserts, CTAS, UPDATE, DELETE must commit *nothing* and must not wedge the session |
| `08_stress.slt` | 303 | 100k rows, self-joins, 150 one-row commits, a 120-commit insert/update/delete churn loop, durability re-checked after reconnect |

## What `05_adversarial_parser.slt` specifically targets

Every statement is plain valid SQL. A regex router misclassifying any of them
must still produce exactly correct results:

- `INSERT INTO sales VALUES (1, 'INSERT INTO sales SELECT * FROM sales')` — keywords and the target name inside string literals
- line/block comments containing `DELETE FROM sales`, `UPDATE sales SET …`, including comments *before* the statement keyword
- self-referencing inserts hidden behind double-nested subqueries (must be non-blind)
- `INSERT INTO sales WITH sales AS (…) SELECT * FROM sales` — the CTE shadows the target
- `sale` / `sales` / `sales_backup` — prefix-named tables, no write leakage
- `iNsErT\n InTo\n sales` with tabs and newlines between keywords
- `$$…$$` dollar-quoting, doubled quotes, semicolons inside strings, raw newlines inside string literals mid-statement
- tables/columns literally named `"insert"`, `"from"`, `"where"`, `"select from where"`, `"range"`
- `INSERT … BY NAME`, positional column reordering, `DEFAULT`, `RETURNING`, trailing semicolons
- FROM-first syntax (`FROM t SELECT count(*)`) and bare `FROM t`
- CTE-prefixed `DELETE` and `UPDATE`
- one trap the suite itself fell into during construction: the note-copying
  self-referencing insert means a later `DELETE … WHERE note = '…'` removes **two**
  rows, not one — pinned in the expectations

## Dialect

Close to DuckDB's sqllogictest, with results one value per line, row-major:

```
statement ok | error | maybe        (maybe = optional feature, outcome reported)
query <types> [rowsort|valuesort]   then ----, then expected values
loop i 0 150 … endloop              ${i} substitution, nestable
reconnect                           close + reopen the same path (durability check)
```

Formatting: `NULL`, `(empty)`, `true`/`false`, floats as `%.3f` (`NaN`/`Infinity`),
bytes as `0x<hex>`, `\n`/`\t` escaped in strings, `Decimal` keeps scale.

`statement maybe` records a per-feature supported/unsupported line instead of
failing — so a run doubles as a capability matrix (time travel, OPTIMIZE, VACUUM,
MERGE, column rename/drop, MAP/UUID/ENUM types, PARTITIONED BY, transactions, …).
Crucially, tables touched by `maybe` statements are re-verified afterwards:
unsupported is fine, *corrupting* is not.

## Design choices worth knowing

- **`reconnect` is the storage arbiter.** In-session correctness can come from a
  cache; only a reopened connection proves the Delta log is right. Reconnects are
  deliberately placed mid-loop, after deletes, after failed statements, and after
  optional maintenance ops.
- **Every multi-row query is order-pinned** — explicit `ORDER BY` or `rowsort` —
  so Delta file ordering never causes flaky results.
- **The runner forces materialization on `statement` records**, so engines with
  lazy relations can't turn runtime errors into silent successes.
- **Failed writes are atomicity tests, not just error tests**: a multi-row insert
  whose third row fails must leave zero rows, verified both in-session and after
  reconnect.
