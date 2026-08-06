---
hide:
  - navigation
---

# Debug a dbt model in a notebook

`duckrun.dbt_project()` compiles a model with dbt and runs it on duckrun, handing you a DuckDB
**relation** — real types, and lazy.

```python
from duckrun import dbt_project

p = dbt_project("dbt/", target="dev")      # returns immediately; parses on first use

p.show("orders_enriched")                  # DuckDBPyRelation
p.sql("select * from {{ ref('stg_orders') }} where year = 2026")
p.compiled("orders_enriched")              # the SQL text

p.ctes("orders_enriched")                  # ['base', 'allocated', 'final']
p.cte("orders_enriched", "allocated")      # run only as far as that CTE
```

`.pl()` / `.arrow()` / `.df()` / `.filter()` / `.limit()` / `.count()` / `.fetchall()` come from the
relation itself, so there is no DataFrame API to learn here — and no DataFrame library in duckrun's
dependencies. Which also means `.pl()` needs polars installed and `.df()` needs pandas; a
`ModuleNotFoundError` from one of those is DuckDB asking for the library you chose, not a duckrun
problem. `.arrow()` and `.fetchall()` always work.

## Why not `dbt show`

`dbt show` truncates, and `dbt show --output json` serializes through agate: the schema is gone by
the time the rows reach you. Decimals arrive as strings or floats, dates as strings, and the
DataFrame library has to guess a type per column from Python objects. On a wide model the mixed
inference failures become their own debugging session. No amount of better inference fixes it —
the type information was discarded two layers earlier. It is also eager: `--limit -1` pulls the
whole result through JSON before you can filter it.

duckrun does not have that problem, because the adapter runs DuckDB **in-process**. A model's
compiled SQL is ordinary DuckDB SQL whose `ref()`s resolve to `delta_scan` views, so dbt never has
to *execute* anything to give you a debuggable result — it only has to **compile**. duckrun
executes.

```python
rel = p.show("orders_enriched")
rel.types                                  # real DuckDB types, end to end
rel.filter("customer = 'X'").limit(100).pl()   # pushes INTO the delta_scan; nothing read before
```

## `cte()` — the part that actually solves the problem

When a model runs clean but returns nonsense, the move is always the same: run the CTEs one at a
time and find where the row count or a key goes wrong. Done by hand that means copying blocks out
of the compiled SQL, twenty times, getting it slightly wrong once.

```python
p.ctes("orders_enriched")                  # ['base', 'allocated', 'final']

p.cte("orders_enriched", "base").count("*")        # -> 41233, as expected
p.cte("orders_enriched", "allocated").count("*")   # -> 38902, the join drops rows
p.cte("orders_enriched", "allocated").filter("share is null").pl()
```

The rewrite keeps the `WITH` list up to and including the named CTE and selects from it. The CTE
text is spliced out of the compiled SQL **verbatim** — nothing is re-parsed or re-generated — so
what runs is character-for-character what dbt produced, comments and formatting included. You debug
the query, not a rendering of it.

Real types at every intermediate step are what make comparing two steps trustworthy.

## Which `is_incremental()` branch you are looking at

A model that branches on `is_incremental()` has **two** compiled forms, and the compiled SQL cannot
tell you which one you have — rendering erases the `{% if %}`, and what is left looks like an
ordinary query either way.

```sql
{{ config(materialized='incremental', unique_key='id') }}
{% if is_incremental() %}
select * from {{ ref('stg') }} where ts > (select max(ts) from {{ this }})
{% else %}
select * from {{ ref('stg') }}
{% endif %}
```

Read the incremental branch as if it were the table's contents and you go looking for rows that
were never meant to be there. So duckrun says which one it compiled:

```
>>> p.show("orders_incremental")
[duckrun] orders_incremental: is_incremental() = True -- this is the incremental branch,
          i.e. the rows a run would write INTO the existing table, not the table's
          contents. The other branch: incremental=False
```

Compile the other branch to compare them side by side:

```python
p.compiled("orders_incremental", incremental=False)   # dbt's --full-refresh
p.last_compile.incremental                            # True / False / None
```

`None` means the model does not branch at all, and nothing is printed — the hint stays rare so it
stays worth reading. The answer comes from dbt itself (compile both ways, compare), not from a
re-implementation of dbt's rule, so it cannot drift from what a real run would do.

`incremental=True` cannot be forced: `is_incremental()` is true whenever the target table exists and
`--full-refresh` was not passed, so it is already the default.

## Ephemeral models

dbt has no standalone compiled form for an ephemeral model — it injects it as a CTE named
`__dbt__cte__<name>` into whatever selects from it. So it shows up in the CTE list of its consumer,
and both routes below work:

```python
p.ctes("mart")                             # ['__dbt__cte__stg_clean', 'base', 'final']
p.cte("mart", "__dbt__cte__stg_clean")
p.sql("select * from {{ ref('stg_clean') }}")
```

On a project with a staging layer they can dominate the list — eighteen of thirty, on the project
this was tested against. `ephemeral=False` leaves them out when what you want is the model's own
steps:

```python
p.ctes("mart", ephemeral=False)            # ['base', 'final']
```

It hides them from the listing only. They really are in the compiled SQL, and slicing at one is
often exactly right: it shows a staging model in the context of the model that consumes it.

## Read-only

The session cannot write. That is **structural**, not a setting: its cursor is a
`DuckrunDebugCursor`, which has no route to delta_rs anywhere in its class hierarchy, so a write
lands on the read-only `delta_scan` view and DuckDB refuses it.

```python
p.sql("delete from {{ ref('stg_orders') }}")
# DuckrunReadOnlyError: read-only debug session: this statement writes.
```

Scratch objects stay allowed, because they are how you take a model apart:

```python
p.sql("create temp table candidates as select * from {{ ref('stg_orders') }} where amount > 1000")
p.sql("create or replace view v_check as select customer, count(*) from candidates group by 1")
```

They live only in the in-memory DuckDB catalog and never reach the lakehouse. duckrun has no `view`
materialization at all — the only things it writes are Delta tables, through delta_rs.

One exception, and it is dbt's rather than duckrun's: a `create` statement that refs an **ephemeral**
model cannot work. dbt injects an ephemeral model by prepending `with __dbt__cte__<name> as (...)`
to the query, and a `WITH` clause only parses in front of a `SELECT`. Build the relation first
instead — same result, and it reads better:

```python
rel = p.sql("select * from {{ ref('stg_orders') }} where amount > 1000")
rel.create("candidates")            # or rel.create_view("candidates")
```

`.create()` goes through DuckDB's relation API rather than the session cursor, so it does not pass
the read-only check — and does not need to: duckrun's route to delta_rs lives in that cursor, so the
relation API has no way to reach the lakehouse. What it makes is a plain DuckDB table, the same kind
of scratch object `create temp table` already gives you.

!!! warning "Read-only covers what the session executes — not dbt's compile"

    Compiling is a real `dbt compile`, on dbt's own connection. A macro that runs SQL at compile
    time (`{% if execute %}` with `run_query(...)`) still runs, exactly as it does under
    `dbt compile`. duckrun does not make that worse, but "read-only" is a promise about the
    statements *you* run through the session, not a claim that compiling is side-effect free.

## Selectors

`model` is handed to dbt untouched, so any dbt selector works and keeps working as dbt's selection
syntax grows:

```python
p.show("orders_enriched")
p.show("path:models/marts/orders_enriched.sql")
p.show("tag:daily")                        # …if it resolves to exactly one model
```

A selector matching several nodes lists them rather than picking one, since guessing would give a
silently wrong answer.

The tests on a model are not "several nodes". dbt hands them back alongside it (indirect selection),
and duckrun keeps the model — but a test named outright still resolves, which turns a failing test
into something you can read with real types instead of re-deriving its SQL by hand:

```python
p.show("not_null_orders_enriched_id").pl()
p.show("not_null_orders_enriched_id").limit(20)     # lazy: nothing read until you ask
```

## Editing while you debug

The parsed manifest is kept warm — a re-parse costs seconds on a real project — but it is checked
against the project's files on every call (a few milliseconds) and re-parsed the moment anything
changed:

```
[duckrun] dbt project parsed (orders_enriched.sql changed, 2.4s)
```

That matters more than the speed: dbt re-compiles from the code it parsed *earlier*, so a warm
manifest would otherwise hand back the SQL from before your last edit, silently. `p.reload()`
forces a re-parse for something the file check cannot see, such as an `env_var` changing in the
kernel.

## The connection

The session runs on a `DuckrunEnvironment` built from your profile — reusing the one dbt just used
when the credentials match. Secrets, `ATTACH`ed catalogs, catalog aliases and the lazy `delta_scan`
bind are therefore identical to a real run **by construction**: it is the run path minus the write,
not a reconstruction of it. In a multi-Lakehouse project (`catalogs:`) that distinction is the
difference between reading the right Lakehouse and a copy of the adapter's logic that has quietly
drifted.

Reuse also keeps it to one DuckDB instance. Each pins `memory_limit` to a large share of available
RAM, so a second environment beside dbt's is an OOM in a Fabric notebook, not a style question.

## Reference

| Call | Returns |
| --- | --- |
| `dbt_project(project_dir=".", target=None, profiles_dir=None)` | a `DbtProject` |
| `p.show(model, incremental=None)` | `DuckDBPyRelation` |
| `p.sql(query)` | `DuckDBPyRelation` — `ref()`/`source()` rendered; `None` for a statement with no result set |
| `p.compiled(model, incremental=None)` | the compiled SQL, as text — `print()` it |
| `p.ctes(model, incremental=None, ephemeral=True)` | list of CTE names, in order; `ephemeral=False` leaves out the injected ones |
| `p.cte(model, name, incremental=None)` | `DuckDBPyRelation` for that step |
| `p.reload()` | re-parses now |
| `p.last_compile` | `.model` `.sql` `.incremental` `.full_refresh` `.cte` `.node_id` |

`project_dir` is the directory holding `dbt_project.yml`. `profiles.yml` is looked for next to it,
then in `DBT_PROFILES_DIR`, then `~/.dbt` — pass `profiles_dir` only if it is somewhere else. A
wrong directory or a typo'd target fails at `dbt_project(...)`, not several cells later inside a
`show()` where it would look like the model being broken.
