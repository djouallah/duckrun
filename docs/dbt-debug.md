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
relation, so there is no DataFrame library in duckrun's dependencies: `.pl()` needs polars and
`.df()` needs pandas installed; `.arrow()` and `.fetchall()` always work.

## Why not `dbt show`

`dbt show` truncates, and `--output json` serializes through agate: decimals arrive as strings or
floats, dates as strings, and the DataFrame library guesses a type per column. It is also eager.
duckrun runs DuckDB in-process, so dbt only has to **compile**: a model's compiled SQL is DuckDB SQL
whose `ref()`s resolve to `delta_scan` views, and duckrun executes it.

```python
rel = p.show("orders_enriched")
rel.types                                       # real DuckDB types, end to end
rel.filter("customer = 'X'").limit(100).pl()    # pushes into the delta_scan; nothing read before
```

### Models materialized as `view`

A `view` model exists only in the session that built it, and a debug session is a different
process. The session therefore registers the manifest's view models as views, in dependency order,
before the query runs, so a `.filter()` still pushes down through the staging layer. They are
dropped whenever the project is re-parsed, so an edited `view` model is never read through its old
definition.

## `cte()` — run one step

When a model runs clean but returns nonsense, run the CTEs one at a time and find where the row
count or a key goes wrong:

```python
p.ctes("orders_enriched")                  # ['base', 'allocated', 'final']

p.cte("orders_enriched", "base").count("*")        # -> 41233, as expected
p.cte("orders_enriched", "allocated").count("*")   # -> 38902, the join drops rows
p.cte("orders_enriched", "allocated").filter("share is null").pl()
```

The rewrite keeps the `WITH` list up to and including the named CTE and selects from it. The CTE
text is spliced out of the compiled SQL verbatim, so what runs is character-for-character what dbt
produced.

## Which `is_incremental()` branch you are looking at

A model that branches on `is_incremental()` has two compiled forms, and the compiled SQL cannot tell
you which one you have. duckrun says:

```
>>> p.show("orders_incremental")
[duckrun] orders_incremental: is_incremental() = True -- this is the incremental branch,
          i.e. the rows a run would write INTO the existing table, not the table's
          contents. The other branch: incremental=False
```

```python
p.compiled("orders_incremental", incremental=False)   # dbt's --full-refresh
p.last_compile.incremental                            # True / False / None
```

`None` means the model does not branch, and nothing is printed. The answer comes from dbt itself
(compile both ways, compare), not a re-implementation of its rule. `incremental=True` cannot be
forced: it is already the default whenever the target table exists.

## Ephemeral models

dbt injects an ephemeral model as a CTE named `__dbt__cte__<name>` into whatever selects from it,
so it shows up in the consumer's CTE list and both routes work:

```python
p.ctes("mart")                             # ['__dbt__cte__stg_clean', 'base', 'final']
p.cte("mart", "__dbt__cte__stg_clean")
p.sql("select * from {{ ref('stg_clean') }}")
p.ctes("mart", ephemeral=False)            # ['base', 'final'] — hides them from the listing only
```

## Read-only

The session cannot write: its cursor has no route to delta-rs, so a write lands on the read-only
`delta_scan` view and DuckDB refuses it. `COPY … TO` and `EXPORT DATABASE` are refused too, since
they write files wherever the session's credentials reach. Scratch objects stay allowed; they live in
the in-memory catalog and never reach the lakehouse.

```python
p.sql("delete from {{ ref('stg_orders') }}")
# DuckrunReadOnlyError: read-only debug session: this statement writes.

p.sql("create temp table candidates as select * from {{ ref('stg_orders') }} where amount > 1000")
p.sql("create or replace view v_check as select customer, count(*) from candidates group by 1")
```

A `create` that refs an **ephemeral** model cannot work, because dbt prepends a `WITH` clause that
only parses in front of a `SELECT`; build the relation first and call `rel.create("candidates")`
(or `create_view`), which makes the same kind of scratch table.

!!! warning "Read-only covers what the session executes — not dbt's compile"

    Compiling is a real `dbt compile` on dbt's own connection. A macro that runs SQL at compile
    time (`{% if execute %}` with `run_query(...)`) still runs, exactly as under `dbt compile`.

## Selectors

`model` is handed to dbt untouched, so any dbt selector works:

```python
p.show("orders_enriched")
p.show("path:models/marts/orders_enriched.sql")
p.show("tag:daily")                        # …if it resolves to exactly one model
```

A selector matching several nodes lists them rather than picking one. A model's tests come back
alongside it and are ignored, but a test named outright resolves, so a failing test can be read with
real types:

```python
p.show("not_null_orders_enriched_id").limit(20)
```

## Editing while you debug

The parsed manifest is kept warm but checked against the project's files on every call and re-parsed
the moment anything changed, so you never get the SQL from before your last edit. `p.reload()`
forces a re-parse for something the file check cannot see, such as an `env_var` changing in the
kernel.

```
[duckrun] dbt project parsed (orders_enriched.sql changed, 2.4s)
```

## The connection

The session runs on a `DuckrunEnvironment` built from your profile, reusing the one dbt just used
when the credentials match. Secrets, `ATTACH`ed catalogs, catalog aliases and the lazy `delta_scan`
bind are therefore identical to a real run. Reuse also keeps it to one DuckDB instance; each pins
`memory_limit` to a large share of RAM, so a second one beside dbt's is an OOM in a Fabric notebook.

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
then in `DBT_PROFILES_DIR`, then `~/.dbt`; pass `profiles_dir` only if it is elsewhere. A wrong
directory or target fails at `dbt_project(...)`, not later inside a `show()`.
