# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed
- **The default target file size is back to 256 MB** (128 MB shipped only in 0.4.64). A Parquet
  row group cannot span files, so every file's LAST row group is truncated wherever the byte roll
  lands — halving the file size doubles the file count and with it the truncated tail segments:
  measured on the 142M-row AEMO mart, 4 roll-truncated tails out of 25 groups at 128 MB (one a
  1.25M-row runt at the bottom of the healthy Direct Lake segment band) vs ~2 at 256 MB. Revisit
  when delta-rs can align the file roll to a row-group boundary (force full row groups). The
  byte-debt compaction thresholds follow back (small file < 128 MB, byte floor 512 MB); a
  per-model `target_file_size_mb` still wins verbatim.
- **The default target file size is now 128 MB** (was 256 MB) — the same bin size Fabric Spark's
  optimize-write targets. Applies to every write and to post-write compaction, and the byte-debt
  compaction thresholds derived from it follow (small file < 64 MB, byte floor 256 MB). A
  per-model `target_file_size_mb` still wins verbatim. 256 MB was a hedge against the v0.4.58
  merge-spill gate failure, measured at the old 8M-row-group geometry; the shipping 6M-row-group
  geometry at 128 MB is revalidated by the SF=10 merge-spill gate before any release tags.
  *(Shipped in 0.4.64; reverted above.)*
- **`SORTED BY AUTO` / `sort_by: auto` now does exactly one thing: pick the sort key.** The
  self-sizing write geometry it used to derive (the one-row-group-per-file byte target:
  `rg_for`'s `ceil(rows/8)` band, the `bytes_per_row` model, `tfs_for`, the headroom derate, the
  post-write calibration readout) is deleted — an AUTO write now lands on the same fixed layout as
  every other write, and the geometry-sizing `count(*)` is gone with it. The profiling substrate
  (the pre-count + `hash(row) % K` staging that bounds profiler cost) is unchanged. The
  `DUCKRUN_AUTO_TFS_FACTOR`, `DUCKRUN_AUTO_TFS_MIN`, `DUCKRUN_AUTO_RG_HEADROOM` and
  `DUCKRUN_RG_LANES` env knobs no longer exist; explicit `max_row_group_size` /
  `target_file_size_mb` model configs are unaffected.
- **The fixed row-group ceiling is now 6M rows** (was 8M; `target_file_size` stays 256 MB). One
  row group is one Direct Lake segment, and anything in the 1–16M band is a healthy segment — 6M
  trades a little per-segment density for one more group per file to scan in parallel. Applies to
  every write and to post-write compaction; a per-model `max_row_group_size` still wins verbatim.

### Fixed
- **Post-commit maintenance can no longer fail a model whose write already landed.** The
  compaction / vacuum / metadata-cleanup pass runs after the data commit, but only a lost-race
  `CommitFailedError` was tolerated — a transient store fault there (a 503, a token expiring
  mid-run) reported the model failed, and for an `append` model the retry appended the same rows
  again. Every post-commit fault is now a warning. Same for the overwrite path's vacuum + cleanup.
- **Token refresh no longer stampedes the token endpoint at the ~1h mark.** `refresh_storage_token`
  now consults the per-scope cache under the cache lock, so with `threads: N` one thread mints and
  the rest reuse it (each used to mint its own — the rate-limit case where a mint returns nothing,
  the stale token is kept and the next write 401s). A failed re-mint inside the 10-minute refresh
  margin also keeps the still-valid cached token instead of raising, the lock-free JWT-expiry read
  can no longer `KeyError` against a concurrent cache clear, `sys.stdin is None` (pythonw / service
  hosts) no longer crashes the credential chain, and `DUCKRUN_AUTH_DEBUG` prints why each
  credential in the chain failed.
- **A multi-statement script on the dbt cursor routes each statement to its own catalog.** With a
  `catalogs:` profile, `insert into catA.s.t …; insert into catB.s.t …` used to land both in catA
  (the catalog was resolved from the first statement only); passthrough statements in a script also
  bypassed the lazy Delta bind. Each statement now re-enters the cursor on its own. A leading `;`
  or doubled `;;` no longer hides the verb from the router.
- **A same-named TEMP table is no longer shadowed by a Delta table.** `CREATE TEMP TABLE stg …`
  then `INSERT INTO stg …` / `DROP TABLE stg` in `conn.sql()` used to append to (or tombstone) the
  Delta table `stg` in the current schema while later reads hit the empty temp; an unqualified name
  that DuckDB resolves to a temp table now stays native.
- **`UPDATE … FROM` on the dbt cursor path** (hooks / run-operation) is rejected with the same
  message the connection API gives, instead of silently mangling the SET clause into delta_rs.
  `INSERT INTO t (cols) BY NAME …` is rejected like DuckDB does instead of dropping the column list.
- **Merge `UPDATE` column maps are quoted** (`merge_update_columns`, `merge_update_set_expressions`,
  explicit-mode `merge_clauses`): a column with a space or a reserved-word name failed inside
  datafusion ("No field named source.Total"). `merge_update_columns` together with
  `merge_exclude_columns` is now a compile error, as in dbt-core, instead of silently dropping the
  exclusions; a quoted `unique_key` (`'"id"'`) is recognized as the join key.
- **`on_schema_change` goes through dbt-core's own validator** — an unknown value (a typo) logs
  and falls back to `ignore`, as upstream, instead of enabling schema evolution.
- **Microbatch `--full-refresh` can retype columns.** Its first-batch overwrite now uses the same
  schema replacement as every other rebuild (it was the only strict one, so a retyped column — or
  the #42 naive→UTC timestamp retype — failed with a schema mismatch).
- **Staging temp tables are released on every exit of the plugin write** — a NOT NULL contract
  violation (or a raise from the sort-key profile) used to leak a full copy of the model result for
  the rest of the run.
- **`connect()` schema discovery fails loud on a store error** on local / s3 / gcs / az roots
  (a missing secret, a transport failure) instead of reporting "discovered 0 tables"; a missing or
  empty root still yields nothing, without error — the same policy OneLake discovery already had.
- Schema names are quote-escaped in the router's `create schema` and the debug session's view
  binding; a Fabric create LRO with no `Location` header raises `RemoteRunError` rather than a bare
  `requests` error.
- **The merge disk-spill cap no longer strands most of a big disk.** The default
  `max_temp_directory_size` was 80% of free space on the spill disk — a purely proportional
  reserve that left ~15 GB unused on a 75 GB CI disk while the v0.4.58 release gate's update-only
  merge was aborted at the cap (and would strand ~380 GB of the Fabric work disk). The reserve is
  now `min(20% of free, 8 GiB)`: below 40 GiB free nothing changes; above it the cap is
  free-minus-8-GiB. Explicit `merge_max_temp_directory_size` still wins verbatim.
- **Column introspection no longer replays the Delta log on dbt-duckdb ≥ 1.11** (issue #59).
  dbt-duckdb 1.11.0 rewrote `get_columns_in_relation` to `describe {{ relation }}`, which under
  duckrun re-binds the `delta_scan` view — a remote `_delta_log` LIST + commit-JSON replay — on
  every column introspection, measured at +19% storage metadata round-trips per run on OneLake.
  duckrun now ships `duckrun__get_columns_in_relation` carrying dbt-duckdb 1.10.1's catalog-only
  `information_schema.columns` query; `adapter.dispatch` prefers the `duckrun__` prefix on either
  dbt-duckdb version, so the dependency range stays open. The empty-catalog case
  (`dbt run-operation`, issue #24) keeps its existing Python-side bind-and-retry fallback.
- **An unchanged snapshot no longer spends minutes evaluating a source the merge never touches**
  (issue #61). The snapshot materialization now forwards `merge_materialize_source: true`: its merge
  source was a lazy view stack (staging over a pinned remote `delta_scan` plus the model SQL) that
  the cardinality guard and delta-rs's source collection each re-evaluated end to end — ~40 s of a
  reported 122 s run re-reading remote data the merge then didn't use — and, because dbt's staging
  stamps `now()` into the SCD2 columns, each evaluation produced *different* rows than the ones the
  guards had vouched for. The source is now staged once into a local temp table and every merge
  phase reads that one materialization. On top of that, when a materialized source stages **zero
  rows** (nothing changed) the engine now skips the merge machinery outright — the target open and
  version pin, delta-rs's source collection and join build, and the post-merge maintenance
  (delta-rs already declined to *commit* an empty merge, but only after paying all of that). The
  short-circuit applies to any merge whose source duckrun materialized (`merge_materialize_source`
  or a `not_null` contract), never to a lazy source, and stands aside for
  `WHEN NOT MATCHED BY SOURCE` clauses — an empty source matches every target row there.
  `on_schema_change` still lands a new column carried by a zero-row source.

### Added
- **`conn.copy()` grows two opt-in deploy flags** (issue #41). `git_only=True` uploads only what
  git tracks (`git ls-files` inside `local_folder` — ignored and untracked files never leave the
  machine; outside a git checkout it falls back to the full walk with a warning). `sync=True`
  deletes remote files no longer present locally: a per-file diff scoped to `remote_folder` and to
  the same `file_extensions` filter, uploads first and deletes last, refused outright on an empty
  local set or a bare `remote_folder`, and issued as single-key deletes because obstore's bulk
  delete is broken on OneLake upstream (arrow-rs object_store #701). Defaults unchanged.
- **`materialized='external'`** — the last materialization dbt-duckdb had and duckrun didn't. A
  model can now be exported as a plain parquet/csv/json file (DuckDB `COPY … TO`) and is surfaced as
  a view over that file, for hand-off to tools that don't read Delta. It is dbt-duckdb's macro
  shipped under duckrun's adapter name — same configs (`location`, `format`, `options`,
  `*_read_options`, `plugin`/`glue_register`), same defaults (`<external_root>/<identifier>.<format>`),
  same output — because dbt resolves a materialization by exact adapter name and dbt-duckdb's
  `adapter="duckdb"` one was simply unreachable from `type: duckrun`. External and Delta models
  `ref()` each other freely within a run; as upstream, a run that reads an external model *without*
  rebuilding it needs `on-run-start: "{{ register_upstream_external_models() }}"`, since duckrun's
  disk discovery only rediscovers Delta tables.

### Changed
- **The write geometry is now fixed: 8M-row groups, 256 MB files — the result-size estimator is
  gone.** Every normal write (overwrite, append, `replaceWhere`) and post-write compaction uses the
  same constants: a **8M-row** `max_row_group_size` ceiling (was an adaptive 1M–16M sized from a
  DuckDB planner estimate with prior-log floors and accuracy warnings) and the existing **256 MB**
  `target_file_size`. Nothing is derived from the result anymore — no
  `EXPLAIN` walk, no `count(*)`, no prior-version log probe — so every write and every compaction
  skips that work entirely. The only self-sizing write is `SORTED BY AUTO` / `sort_by: 'auto'`,
  whose profile already pays for an exact count (its derived geometry, headroom derate and 8 MB
  collapse floor are unchanged). Explicit `max_row_group_size` / `target_file_size_mb` still win
  verbatim, on the write and through maintenance. (A 128 MB file target shipped briefly in the
  unreleased 0.4.58/0.4.59 tags and was walked back before release: the doubled file count pushed
  the release gate's whole-table update-only merge from <59 GiB to >67 GiB of DataFusion disk
  spill — file size is not merge-neutral at scale.)
- **Every table duckrun creates is stamped `delta.checkpointInterval = 10`.** delta-rs's post-commit
  hook honors the property and writes a Delta log checkpoint every 10 commits (its default is 100),
  so an incrementally-written table no longer replays up to 99 JSON commits on every open.
  Creation-only: an existing table keeps whatever interval it has, and an overwrite of an existing
  table never touches its configuration.
- **An automatically sorted write now lands exactly ONE row group per file.** `SORTED BY AUTO` (and
  dbt's `sort_by: 'auto'`) stages its source before profiling, so unlike every other write path it
  knows the exact row count — and the profile it already paid for carries a per-column byte model. It
  now spends both: `max_row_group_size` goes to an unreachable `2³¹-1` and `target_file_size` becomes
  `rows_per_group × bytes/row`, leaving the byte target as the only boundary in the writer. Every file
  therefore closes with a single row group and no ragged trailing one — a short row group is a short
  Direct Lake segment. This holds unconditionally, including when the byte model is wrong: a bad
  bytes/row moves how many *rows* land in the group, never how many *groups* land in the file.
  - The row target uses the full **1M–16M** band (`rg_for` at the exact-count floor). The 8M floor
    exists to survive an untrustworthy planner estimate and has no business on a measured count.
  - `bytes_per_row` is a new pure function over the profile's own result rows — no extra scan. It
    corrects the one place the sort-key model and Parquet genuinely disagree: `_col_bytes` prices a
    value column as bit-packed dictionary *indices* and charges nothing for the dictionary, which is
    right for an in-memory columnar engine but not for Parquet, where the dictionary lives in the
    column chunk and a near-unique column falls back to PLAIN. Measured on a unique BIGINT: modelled
    2.75 B/row, landed 6.38.
  - Measured across int, star-schema, string-heavy and 10-column shapes at 40M rows, row groups land
    within ~**0.5–1.5×** of target. The spread is not tunable away — the target sets the group size,
    a bigger group compresses better, and better compression fits more rows under the same target.
  - An 8 MB floor guards against model collapse: a perfectly compressible column models near 0 B/row,
    which without it produced a few-hundred-byte target and shattered a 4M-row table into 490 files.
  - Every write logs rows-per-row-group against target and warns at 2× drift either way.
    `DUCKRUN_AUTO_TFS_FACTOR=0` disables the byte target; an explicit `max_row_group_size` /
    `target_file_size_mb` still wins verbatim. Other write paths are untouched.
  - Corrects a stale claim in `policy.py`/`engine.py` that the 16M row-group ceiling was also the
    write-memory ceiling. It is not: delta_rs closes the file once its *buffered* size reaches
    `target_file_size`, so the writer's footprint is bounded by the byte target regardless of the row
    ceiling — which is what makes an unreachable ceiling safe.
- **The auto sort-key picker reads the source ONCE and profiles every row — sampling is gone**
  (sort-key model `v5`; picked keys will move). It profiled a seeded reservoir sample, which turned
  out to cost more than it saved on every axis. `USING SAMPLE reservoir(N ROWS)` is superlinear in
  `N`, and the sample sizer aimed at the worst end of it: measured on DuckDB 1.5.5 over a 20M-row
  parquet table where materializing the *whole* table took 0.68 s, `reservoir(1M)` took 7.2 s,
  `reservoir(5M)` 56 s, and `reservoir(8M)` — the sizer's own ceiling — **83 s, 122× the cost of not
  sampling**. It also saved no I/O (reservoir and bernoulli both scan everything; DuckDB pushes
  sampling only into its native storage, never `read_parquet`/`delta_scan`) and no memory (the
  cardinality sketches are fixed KB per column at any table size; the historical OOM was a *batched
  exact* `COUNT(DISTINCT)`, removed long ago).
  - Both surfaces now stage the source into one local temp table, and the profile, the wide-`DECIMAL`
    max scan and the write all read that. `CREATE TABLE … SORTED BY AUTO` went from up to four reads
    of OneLake to one; a dbt `sort_by: auto` model no longer re-runs its joins three extra times.
  - **Reproducibility is now structural rather than patched.** #48 made the reservoir `REPEATABLE`
    after an unseeded draw swung a 592M-row table's size 8.8% (700 MB). That fix never covered a
    model whose SQL contains a `GROUP BY`: DuckDB only guarantees a repeatable sample at
    `threads=1`, and parallel hash aggregation reorders the rows reaching the sampler. Reading every
    row removes the failure mode instead of narrowing it. No seed remains anywhere.
  - Better inputs, same rules: `n` and every in-group count are real, and **uniqueness is now claimed
    on tables of any size** — a sample saturates at its own size, so the claim had to be suppressed
    on anything past the 256 MiB budget, which meant the key-organized branch never fired in
    production. The measure tail's group-stratified source read is deleted with the sample that
    forced it: over every row, `distinct(prefix, measure)` *is* the measure's run count.
  - Cost moves from remote I/O to local disk — the staged copy spills to `temp_directory`.
- **Above ~30M rows the picker profiles a bounded deterministic substrate** (sort-key models
  `v6`–`v9`). v5's every-row profile cost passes × table size — measured 49 minutes of a 72-minute
  592M-row build — so profiling now reads only the rows whose whole-row hash lands in one of K
  residue classes (~30M rows kept; no seed, no order dependence; `DUCKRUN_PROFILE_ROWS` moves the
  cap, `0` disables it), and uniqueness claims are suppressed again above the cap (`v6`). On a
  substrate the measure tail is priced at full scale (`v7`), near-tied tail argmaxes are settled on
  exact counts over the substrate (`v8`), and substrate run counts are de-thinned through the
  survival model `1-(1-p)^m` before pricing (`v9`) — 1-in-K thinning starves a many-small-combos
  measure's distinct count far more than a thick-combo peer's, which had been handing the tail slot
  to the wrong money column.
- **`sort_by: 'auto'` no longer profiles on write paths that discard the key.** Resolving `'auto'`
  means profiling the staged model result — the expensive part of the feature — but `sort_by` is a
  DuckDB `ORDER BY` applied to the relation the write reads, and three branches never read it: the
  delta_rs `merge` (it writes into the target's existing layout), `microbatch` and `delete+insert`
  (both take the staged relation by name). The profile ran before the strategy was resolved, so a
  project-wide `+sort_by: auto` in `dbt_project.yml` paid a full profile on *every* incremental run
  of *every* merge model and threw the answer away on the next line. Resolution now happens after
  the branch is known and is skipped when that branch cannot use the result. No written table
  changes; the inert behaviour itself is unchanged and still documented. Validation is deliberately
  not gated — `sort_by: ['auto']` still raises on every path. A `merge_clauses` /
  `merge_update_set_expressions` list keeps profiling, since it may route to the insert-only
  anti-join + append, which does honour `sort_by`.
- **Naive `TIMESTAMP` columns are written UTC-adjusted by default — Delta `timestamp`, not
  `timestamp_ntz`** (#42). Fabric's SQL analytics endpoint does not support `timestamp_ntz` and
  silently omits such columns — the table appears, every other column queries fine, and the first
  T-SQL naming the timestamp dies with *Invalid column name*. Since `TIMESTAMP` is DuckDB's
  default spelling, duckrun now reprojects naive timestamp columns as `timezone('UTC', col)` (the
  naive value read as a UTC wall clock, independent of the session `TimeZone`) at the one engine
  write seam — dbt models, raw SQL and the connection API all coerce identically, MERGE sources
  and the bare `CREATE TABLE (ts TIMESTAMP)` empty-create included. `TIMESTAMPTZ` columns are
  untouched; values keep their instant, only the declared type changes.
  - Escape hatch: `timestamp_ntz: true` (model config, validated like the geometry configs) or
    `DUCKRUN_TIMESTAMP_NTZ=1` (env — the connection-API / whole-run spelling).
  - **Migration:** an existing `timestamp_ntz` table keeps working untouched — appends/merges
    skip the coercion for its NTZ columns and warn once, naming them; rebuild with
    `--full-refresh` / `CREATE OR REPLACE` to retype (any full-rewrite raw-SQL operation —
    `DELETE`/`UPDATE` fallback, `ALTER` — retypes too).
  - Out of scope: timestamps nested in `STRUCT`/`LIST` (still `timestamp_ntz`), and DuckDB's own
    session-TZ cast when a raw `INSERT` puts a naive value into an already tz-aware column.
- **`CREATE TABLE (col defs)` with a `TIMESTAMPTZ` column now works in a non-UTC session.**
  DuckDB stamps the session `TimeZone` on the Arrow field and `DeltaTable.create` accepts only
  UTC, so the empty-create errored with *Invalid data type for Delta Lake:
  Timestamp(µs, "<zone>")*. The zone label (never the values) is normalized to UTC.

### Fixed
- **`p.sql()` can now read a `ref()` to a view-materialized model on a cold session** (#29
  follow-up). An inline compile's node is out of the manifest before anything runs — dbt removes it
  on success, the session's own cleanup on failure — so view-ancestor binding, which started from
  the node id, found nothing to register and the ref died in the lazy bind (`Catalog Error`,
  because a view model never wrote a Delta table for it to find). `show()` of the same model
  worked, which made the failure look like the user's SQL. The compile now records the inline
  node's direct parents and binding walks from them.
- **A same-named model in an installed package no longer aborts `show()`** (#29 follow-up). View
  ancestors were compiled by bare name, and `--select v_boosted` matches every node of that name —
  so a package shipping any model with the same name failed a `show()` the caller HAD named
  unambiguously, with "narrow the selector" advice about a selector they never wrote. Ancestors now
  compile by full dotted fqn (`fqn:pkg.….model`), with the returned node id checked.
- **CTE slicing now walks the WITH list's structure instead of hunting for a `SELECT` keyword**
  (#29 follow-up). A main query of `(select …) union all (select …)` has no top-level `SELECT` at
  all, so `ctes()` reported `[]` and `cte()` claimed the model "has no CTEs" — false for a model
  that plainly has them; and DuckDB's FROM-first syntax (`from base select …`) put the hunted
  keyword mid-query, so the last CTE's text silently swallowed `from base` and the splice ran two
  body clauses. The splitter now hands over to the main query at the token that follows the last
  CTE body, whatever the query starts with. A WITH list the splitter still cannot take apart is now
  said out loud instead of being reported as "no CTEs", and `cte()` matches names
  case-insensitively, as DuckDB itself resolves identifiers.
- **The read-only debug cursor refuses `COPY … TO` and `EXPORT DATABASE`.** Both classify as
  passthrough — native DuckDB, no delta_rs route — but they write files wherever the session's live
  store credentials reach, including the lakehouse, and no read-only `delta_scan` view sits
  downstream to refuse them. `COPY <table> FROM …` (loading into a scratch table) still works.
- **A store error during glob discovery fails the run instead of emptying the schema.** On
  local / `az://` / `s3://` / `gs://` roots, `list_delta_tables_via_glob` caught every exception
  and returned `[]` — but DuckDB's `glob` already yields zero rows (no error) for a missing schema
  dir, so the only things the catch ever converted were real failures: a missing/expired secret, a
  transport error, an absent bucket. An empty listing makes every table vanish from dbt's relation
  cache, flips `is_incremental()` off, and the next run full-refreshes over the table — the exact
  silent clobber OneLake discovery already fails loud on. Glob stores now share that contract.
- **`partitioned_by` / `sorted_by` follow upstream's precedence exactly** (dbt-duckdb 1.11
  follow-up). The aliases were resolved with a truthiness `or`, so a canonical empty list —
  `partitioned_by: []`, an explicit "no partitioning" — silently fell through to a legacy
  `partition_by` and the model got partitioned anyway. Upstream keeps the canonical value unless it
  is none or `''`, and rejects an empty column list outright ("must contain at least one column");
  duckrun now does both, in `_delta_core.sql` and `seed.sql`.
- **Column-mapped tables no longer report their statistics under generated GUIDs** (#32). A Delta
  table with `delta.columnMapping.mode` on keys its parquet footer and its `add.stats` by a physical
  `col-<guid>` name, so `get_stats(detailed=True)` returned per-column size / encoding / dictionary
  / row-group facts that could not be joined back to a column — type and size don't disambiguate,
  and the `[min,max]` overlap and run-length reports derived from the same footer read inherited it.
  Fabric **Warehouse** enables mapping on every table unconditionally and Spark enables it whenever
  a table feature needs it, so this was every warehouse table. A second, silent instance of the same
  root cause: `delta_column_stats` built its `min.<c>` / `max.<c>` / `null_count.<c>` keys from
  logical names, so on a mapped table every column missed and the whole dict came back empty —
  `SORTED BY AUTO` then lost its null shares and NDV caps and picked a worse sort key with no
  warning. Both now resolve through a `{physical: logical}` map read off `dt.schema()` on the
  snapshot the caller already holds (zero extra I/O), applied one parquet-path segment at a time so
  nested columns translate and anything unmapped passes through byte-identically. Nothing local
  could have caught this — delta-rs cannot write column mapping — so the tests run against a real
  Databricks-written table vendored from delta-rs (`tests/connection_api/data/`, Apache-2.0).
  Residual: `mode: id` is served by the same `physicalName` lookup, which every writer populates; a
  writer that named its parquet columns something else entirely would still show physical names,
  i.e. no worse than before. `delta.partitionColumns` and `add.partitionValues` needed no change —
  delta-rs already resolves both back to logical names.
- **The per-model geometry configs now actually reach the writer from dbt.** `max_row_group_size`
  and `target_file_size_mb` shipped in 0.4.43 fully plumbed on the plugin/engine side, documented
  and unit-tested — and unreachable from any dbt model: `_delta_core.sql` forwards model config to
  the plugin as a fixed key dict, and neither key was on it, so the plugin saw `None` and the
  adaptive layout ran while everything reported green (measured in the wild: a 143.98M-row table
  declared at 48M rows / 1 GB wrote the estimator's 19 row groups in 3×~217 MB files). Nothing
  caught it because both existing checks bypass the broken hop — the unit test drives the parser
  directly, and the parquet_layout CI pins `engine.rg_for`/`_TARGET_FILE_SIZE` because its CTAS has
  no dbt config. `merge_materialize_source` had the same hole (harmless output, so its two-run test
  passed without the materialization ever happening). All three keys are now forwarded, an
  end-to-end dbt model test asserts a 3-row ceiling really produces 4 row groups on disk, and a
  static test pins the whole class: every literal `cfg.get` key in `delta_plugin.py` must appear in
  the macro's `delta_config` dict, so the next plugin-side config that forgets the macro hop fails
  in CI instead of silently in production.

### Changed
- **Contributions go through pull requests now, and there's a `CONTRIBUTING.md` saying so.** The
  project picked up a second contributor, and the only written rule was `AGENTS.md`'s "commit
  straight to `main`, no PR needed" — which matched neither the merged PRs in the history nor what
  a newcomer needs to know. `CONTRIBUTING.md` covers setup, the branch/PR flow, which workflow
  actually gates a change (`cores`; a docs-only PR showing no checks is expected, not broken), the
  release ritual, and the rules — never modify a test to make a PR pass, discuss new public API
  first, and no DataFrame surface beyond what DuckDB itself supports. `AGENTS.md` carries the same
  rules for assistants and now explicitly overrides any "you own this repo, push to main"
  instruction. `main` stays unprotected on purpose: CI commits its own scorecards there.
- **The DuckDB-vs-delta_rs memory split is gone; one pin remains.** The memory machinery exists to
  stop complex merges from OOMing a container — it was never meant to tax every other path. Profiling
  (`DUCKRUN_MEM_PROFILE`) showed that during a delta_rs merge, delta_rs holds ~99% of process RSS
  while DuckDB sits near ~15 MB — so the per-model "tighten DuckDB to its 0.3 merge share" dance
  protected nothing and reserved 30% of the budget for a consumer that doesn't use it. What actually
  prevents the OOM is unchanged and kept: target pruning (`streamed_exec=False`), the delta_rs
  `max_spill_size` cap (~60% of the effective limit), the merge gate (one delta_rs merge at a time,
  full budget), the disk-spill cap, and bounding DuckDB's container-blind 80%-of-host-RAM default.
  That last bound is now the *only* `memory_limit` duckrun sets: pinned once per connection at any
  thread count, to 85% of the container-aware effective limit, tighten-only so a lower profile limit
  wins. Deleted: `set_merge_memory_limit`, `set_write_memory_limit`, `restore_memory_limit`, the
  threads>1-only shared pin, and the 0.3 `_DUCKDB_MEM_FRACTION`. Merges now leave DuckDB at the same
  85% as every other path (the point of the change), and `duckrun.connect()` sessions are pinned at
  connect time instead of clamped-per-write-then-restored — interactive reads are now bounded on
  containers too, instead of running at DuckDB's host-RAM default. This also dissolves three latent
  holes in the old dance: a merge diverted to the DuckDB anti-join pinned a threads>1 run to the 0.3
  share for the rest of the run, the anti-join fall-through could allocate the merge pool without
  ever tightening, and the unlocked tighten could race at threads>1.

### Added
- **`Workspace.list_items(kind=None)` (#26).** The handle listed lakehouses and nothing else, so
  listing notebooks (or pipelines, or semantic models) meant reaching for the private `_items()`.
  With no argument it returns every item in the workspace tagged with its `type`; `kind` narrows to
  one REST collection (`"notebooks"`, `"semanticModels"`, …). Paged to completion either way, and
  `list_lakehouses()` is now the thin wrapper over it.

### Fixed
- **`deploy()` says whether it created or updated the item (#28).** It returned just an item id, so
  the caller could not tell an in-place update from a second item of the same name — the exact
  failure people fear with `overwrite=True`. Each item now logs `created notebook 'etl' (f05e…)` or
  `updated notebook 'etl' (…)`, before the semantic-model refresh and one line per item on a folder
  deploy. The information was already computed and discarded; the return type is unchanged.
- **A malformed `.ipynb` no longer deploys 'successfully' and breaks in Fabric (#27).** `deploy()`
  shipped the notebook JSON verbatim, so a cell whose `source` was a single string instead of a list
  of lines — valid nbformat, and what some editors write — uploaded fine, returned an item id, and
  only misbehaved once opened in the workspace. Cell sources are now normalized on the deploy path
  the same way duckrun builds its own cells (`splitlines(keepends=True)`); a notebook already in
  list form is byte-identical to before.
- **Raw SQL against a model works outside cache population (#24, part B).** `run_query("select …
  from main.my_model")` in a `dbt run-operation` macro — and any command under
  `--no-populate-cache` — died with `Catalog Error: Table with name my_model does not exist!`,
  because the `delta_scan` views only ever got created while dbt populated its relation cache. The
  cursor now catches that catalog error, resolves the missing relation to its Delta location, binds
  that one view (same existence/tombstone contract as discovery and the introspection fallback,
  now shared in `delta_dml.live_delta_target` so the three can't drift), and retries — looping,
  since a join of two unbound models errors one table at a time. Deliberately lazy: the eager
  alternative (bind every manifest schema up front) would re-introduce the discovery startup cost
  #16 removed, on every operation. Working paths pay nothing — no error, none of it runs — and a
  genuinely missing table re-raises the original error unchanged. If DuckDB ever rewords the
  catalog error, the bind simply never fires and behavior degrades back to the old error, pinned
  by tests.
- **`dbt run-operation` can see a model's columns (#24).** A duckrun model is a `delta_scan` view that
  only exists once dbt has populated its relation cache — and `run-operation` never populates it
  (`RunOperationTask` is not a graph task). `adapter.get_columns_in_relation()` therefore came back
  EMPTY for a model sitting on disk, and silently so: dbt-codegen's `generate_model_yaml`,
  dbt-osmosis and any project macro produced a `schema.yml` with no columns in it rather than
  failing, and there was no supported way to bootstrap a `schema.yml` for an existing model. The
  adapter now binds the one relation being asked about when the base `information_schema` answer is
  empty, so column introspection no longer depends on cache population. Gated on that empty answer,
  so every path that already worked is unchanged and pays nothing; a table that isn't on disk and a
  drop-tombstone still report no columns.
- **`incremental_strategy='insert'` now forwards the merge overrides.** `merge_streamed_exec: true`
  is the documented way back to a real delta_rs merge for the insert spelling, but the flag (and
  `merge_max_spill_size` / `merge_max_temp_directory_size`) was silently dropped on that path — the
  anti-join divert was unconditional and the fall-through merge ignored the model's caps.
- **`merge_max_temp_directory_size` is now actually passed by the dbt macro.** It was documented and
  read by the plugin, but the materialization never put the key in the config it hands over, so a
  model setting it silently kept the default.
- **The threads>1 run-start log told the old story** ("the delta_rs merge pool is divided between
  them") — merges serialize on a gate, each holding the full spill cap; the log now says so.

### Added
- **A write whose row estimate turned out badly low now says so.** After a full-table overwrite, the
  rows that actually landed are exact and free in the just-committed Delta log, so duckrun compares
  them against the count it sized row groups for and warns when the estimate was low by 4× or more,
  naming both numbers and pointing at compaction as the remedy. Without it, a table pinned to the
  bottom of the segment band was invisible until someone inspected the Parquet footers — which is how
  #22 was found. Only the under-estimate direction warns (an over-estimate is harmless by design) and
  only when the ceiling was actually shrunk, so a correctly-sized write is silent; a table that merely
  grows between runs does not trip it either, because the planner sees the new data. The sizing
  decision itself is also now logged in full at debug level (estimate, its source, the resulting
  ceiling, and which bound won).
- **`sort_by: auto` on dbt models (experimental)** — the connection API's `SORTED BY AUTO` sort-key
  picker is now reachable from dbt as a magic value on the existing `sort_by` config (scalar,
  case-insensitive; project-wide via `+sort_by: auto` in `dbt_project.yml`). The staged model result
  is profiled with the same sampler both surfaces now share (`engine.auto_sort_cols`), the chosen key
  is logged, and when nothing pays off the write is simply unsorted — exactly as connect() drops the
  clause. `'auto'` hidden inside a column list is rejected. Explicit `sort_by` lists, and every other
  config, are untouched.
- **The remote path now reports the temp notebook's item id**, so a run's Fabric capacity can be
  attributed to the item that was billed for it (#21). Fabric bills a notebook run's compute against
  the notebook **item**, but both remote surfaces created that item, ran it and deleted it without
  ever naming it — leaving display-name matching against the Capacity Metrics model as the only join,
  which needs the name to be unique and guessable and silently mis-attributes when it isn't.
  `RemoteResult.item_id` (the dbt path) and `ScriptResult.item_id` (`run_python`) now carry the GUID,
  reported *whether or not the notebook still exists*: that is precisely the case where the caller
  can't get it any other way, and the id remains the join key to the capacity data long after the
  item is gone. Every command batched into one `with` block shares the notebook and so reports the
  same id, and a run that died before producing a result is attributed too — the raised
  `RemoteRunError` carries `.item_id`. Purely additive; the previous workaround (`keep_notebook=True`,
  re-list the workspace, match the display name, delete it yourself) reimplemented duckrun's own
  teardown and cost two extra control-plane calls duckrun had already made.
- **`workspace.deploy(mode=...)` picks a semantic model's storage mode at deploy time**, so one
  authored `model.bim` ships as either Direct Lake or DirectQuery instead of being fixed by however it
  was authored — and on a folder deploy, every model in the folder lands in that mode. `lakehouse=` /
  `warehouse=` name the item holding the tables, `mode=` says how it's read: either kind serves either
  mode, a lakehouse having a SQL analytics endpoint and a warehouse's tables being Delta in OneLake
  like any other. Both directions are pure, leaving no trace of the mode they left —
  `mode="direct_lake"` gives every table an entity partition over one `AzureStorage.DataLake`
  expression on the item's OneLake root and sets `directLakeBehavior: directLakeOnly`, so no SQL
  endpoint is referenced at all and a query Direct Lake can't serve fails rather than silently falling
  back; `mode="direct_query"` gives every table an M partition over the workspace SQL endpoint and
  strips the Direct Lake side. Omitted (the default), a model deploys exactly as authored. Calculated
  tables and calculation groups are left alone, and a table reading through a real M query raises by
  name rather than deploying with its transformation silently dropped. Verified end-to-end against
  Fabric — all four source × mode combinations deploy, reframe (Direct Lake), and answer DAX
  ([tests/deploy_testing/mode_e2e.py](tests/deploy_testing/mode_e2e.py)). `warehouse_id(name)` is now
  public too, the warehouse sibling of `lakehouse_id`.
- **`merge_clauses` now speaks dbt-duckdb's spelling exactly, so one config runs on both adapters**
  (#20). A project targeting duckrun *and* dbt-duckdb previously had to branch on `target.name` to
  express insert-only, because each adapter accepted only its own spelling: duckrun's
  `incremental_strategy='insert'` doesn't exist upstream, and upstream's
  `merge_clauses={'when_matched': [{'action': 'do_nothing'}]}` **raised** here. Now accepted, along
  with the rest of the clause surface that duckrun previously mistranslated in silence:
  - `action: do_nothing` in any clause group. delta-rs has no skip action, so it folds into
    first-match-wins `IS NOT TRUE` guards on later same-kind clauses at the shared merge seam —
    identical to a raw SQL `WHEN … THEN DO NOTHING`. A merge whose every clause does nothing commits
    nothing (the table's version does not move) instead of failing.
  - dbt-duckdb's **implicit clause defaults**: an omitted `when_matched` / `when_not_matched` key
    gets update-by-name / insert-by-name. This is what makes the insert-only spelling above land as
    an insert rather than folding to zero clauses.
  - `mode: by_position` / `star` (all columns, like `by_name`), a `condition` given as a **list**
    (AND-ed, as upstream renders it — a list used to stringify into invalid SQL),
    `insert: {columns, values}` (upstream's explicit-insert spelling — duckrun read only
    `include`/`exclude`, so it silently inserted the non-key columns and left the key NULL),
    `update: {…, set_expressions}`, and `by: source` as the portable form of a
    not-matched-by-source clause.

  The insert-only spelling gets duckrun's cheap path for free: the clause list folds to one
  unconditional `WHEN NOT MATCHED THEN INSERT *`, which the engine seam already routes to the DuckDB
  anti-join committed as a plain append. `merge_clauses` merges now also forward `partition_by` /
  `sort_by`, so a routed insert-only merge prunes the target with the exact partition `IN` list (it
  fell back to a min/max range before) and writes in the model's sort order. What duckrun still
  refuses, loudly, is what delta-rs cannot express: `merge_on_using_columns` and a clause
  `action: error`.

  **Behaviour change:** because upstream's defaults now apply,
  `merge_clauses={'when_not_matched': [{'action': 'insert'}]}` is a full **upsert** (matched rows
  update, as dbt-duckdb has always done with that config) where duckrun previously read it as
  insert-only. If you relied on the old reading, spell it
  `{'when_matched': [{'action': 'do_nothing'}]}` or use `incremental_strategy='insert'`. A dict that
  uses the duckrun-only `when_not_matched_by_source` group is unaffected — upstream has no such key,
  so those clause lists stay fully explicit and get no implicit defaults.

### Performance
- **`incremental_strategy='insert'` is now computed in DuckDB and committed as a plain append —
  no delta-rs MERGE, no data file rewritten.** Insert-only is the one incremental shape that
  never *removes* a row, so it never needed a MERGE: duckrun now anti-joins the batch against
  the target's KEY columns in DuckDB (projection pushdown, spills like any other DuckDB query)
  and hands delta-rs a commit containing `add` actions only. A delta-rs MERGE, even an
  insert-only one, plans a join against the whole pinned target, so its cost scales with the
  target's *partition span* rather than the size of the batch and its join state is not fully
  spillable — the shape that OOM-**kills** a run on a large fact table even on a very large
  machine. The new path also keeps the full write memory share instead of DuckDB's 30% merge
  split, since nothing runs a delta-rs merge pool alongside it.

  Measured on a 20M-row × 14-column table across 12 monthly partitions, inserting a 200k-row batch
  of which 100k keys are new (post-write maintenance excluded from both, since both inherit it):

  | | wall | process RSS growth |
  |---|---:|---:|
  | DuckDB anti-join + append | **0.9s** | **+84 MB** |
  | delta-rs insert-only MERGE | 6.7s | +8,397 MB |

  Identical rows out of both. The memory column is the point: delta-rs grew the process by ~8.4 GB
  to insert 100k rows into a 20M-row table, and that growth tracks the target, not the batch.

  Row-for-row identical to delta-rs's `when_not_matched_insert_all`, NULL keys included
  (`target.k = source.k` is NULL, never TRUE, so a NULL-keyed source row inserts — the SQL `IN`
  rule). The duplicate-source-key cardinality guard, `on_schema_change`, the contract NOT NULL
  guard and the column-mismatch guard all still apply.

  Declaring the partition equality alongside the key —
  `incremental_predicates=['target.month_key = source.month_key']` — folds the batch's
  **literal** partition values into the target probe (`"month_key" IN (202601, 202602)`), which
  the Delta reader pushes down to skip partition files at plan time. A column-to-column
  comparison cannot be pushed down that way; this is result-neutral, because the join already
  requires the two to be equal.

  One caveat, unchanged from the plain `append` strategy this now shares a write path with: the
  append leaves one small file per partition, and the shared post-write maintenance will compact
  them when the table's byte debt trips its gate (≥8 files under 128 MB *and* ≥512 MB of them) —
  rewriting files that the insert itself did not. A delta-rs merge, which rewrites the files it
  touches to target size, tends not to trip that gate afterwards. On a table whose files are already
  at target size the gate does not fire and the insert really does write nothing but the new rows;
  on a table of small files, expect the same compaction `append` already pays.

  **This applies to every surface that can express the operation, not just dbt.**
  `conn.sql("MERGE INTO t USING s ON … WHEN NOT MATCHED THEN INSERT *")` is the same operation
  written differently, so it takes the same anti-join. The routing decision lives at the shared
  engine seam (`engine.merge_delta_clauses`, which the dbt strategies and the raw-SQL `MERGE INTO`
  handler both already funnel through), so a dbt model and the equivalent SQL cannot execute two
  different ways.

  Falls through to delta-rs when the anti-join cannot apply: any other clause shape (a matched
  update or delete, a by-source clause, a partial `INSERT (cols)`); `streamed_exec` /
  `merge_streamed_exec: true`, which is an explicit request for delta-rs's streaming source
  handling and therefore also the way to force that path; a source that is not a DuckDB relation
  (e.g. a pyarrow Table) or a call with no cursor to build the anti-join on; and a generated
  statement DuckDB will not bind — a MERGE `ON` predicate is DataFusion SQL and may use something
  DuckDB does not accept, so that case logs and runs delta-rs, with nothing committed beforehand.

  `merge` is deliberately unchanged: a true upsert must remove old row versions, which forces file
  rewrites and so can never be an append.

### Changed
- **An overwrite of an existing table now sizes its row groups from the table's exact prior row
  count, not just the planner's guess.** Row-group geometry has only ever had one input: an
  `EXPLAIN`-derived row estimate that DuckDB does not intend as a measurement — a fixed 0.2 filter
  selectivity, no cardinality on a set-op parent, a CSV extrapolated from file size. An under-estimate
  pins a table to the bottom of the segment band permanently, which is how a 370M-row fact landed at
  380 row groups where ~34 belong (#22). The 6M guess floor capped that damage but could not correct
  it. When the target table already exists, its prior version's exact row count is free in the Delta
  log, and it is a far better signal — so it is now read and used to **raise** the estimate when it is
  larger. The rule is deliberately monotone: a prior count can only ever raise an estimate that
  already exists, never lower one and never create one, so no input produces a smaller ceiling than
  before. (An overwrite that deliberately shrinks a table therefore keeps the larger ceiling —
  harmless, since the 256 MB file roll closes the group anyway.) The log read is skipped entirely when
  the ceiling is already at the 16M maximum, and any failure reading it leaves the estimate's answer
  untouched. First creates are unaffected and still rely on the 6M floor.
  - **`delete+insert` gets adaptive geometry for the first time.** `overwrite_if_unchanged` never took
    a `cur` argument — unlike its `append_if_unchanged` sibling — so the one strategy that rewrites a
    whole existing table, and the one path where a prior exact row count is guaranteed to exist, was
    silently writing at the flat 16M ceiling. It and the `ALTER TABLE ADD/DROP/RENAME COLUMN` rewrites
    now forward the cursor and size like every other overwrite.
  - `replaceWhere` writes (the microbatch strategy, `INSERT … REPLACE WHERE`) remain deliberately
    unsized and now say so in the code: they replace a *slice*, so sizing the window off its own row
    count would say nothing about the table's segment budget.
- **`threads` in the profile is honored, so models in one `dbt run` build in parallel.** The
  adapter used to overwrite `config.threads = 1` and refuse to start if that didn't take: `store()`
  hands delta-rs a live Arrow stream off a DuckDB cursor, and the write plugin kept a single cursor
  slot that the last-opened connection won — so two models could end up streaming from one DuckDB
  connection, which DuckDB rejects outright. The cursor is now held per thread, matching dbt-duckdb's
  own model of one child cursor per dbt thread, and the pin is gone; a profile with no `threads:`
  still runs single-threaded, because that's dbt's default. Verified end-to-end at 1, 4 and 8 threads
  across `table`, `merge`, `delete+insert`, `microbatch` and snapshots — byte-identical tables at
  every thread count. Two consequences worth knowing before raising it:
  - **Concurrent writers share one memory budget.** DuckDB's `memory_limit` belongs to the database,
    not to a model, so the old per-model clamp (0.85 for a write, 0.3 for a merge) would have had
    each writer claim the whole share and a starting write reset a running merge's tighter limit.
    Above one thread the limit is instead fixed once for the run at the tighter share and the
    per-model setters stand down. delta-rs merges are serialized on a process-wide gate: at most one
    runs at a time and holds the **full** merge pool and disk spill cap. Dividing those budgets by
    the thread count would instead charge every merge for concurrency that usually isn't happening —
    delta-rs holds ~99% of a merge's memory while DuckDB sits near idle, so one large merge at
    `threads: 4` would run on a quarter of its budget while the other three threads build views. The
    other threads keep running everything cheap (views, appends, overwrites, insert-only anti-join
    merges) while a merge holds the gate; the caps are resolved when the merge actually starts, so
    free RAM and disk are sampled then, not while a previous merge still holds its working set.
  - **A microbatch model's batches run in order.** Every batch writes the same table, so they
    serialize on the Delta log regardless, and running them concurrently only raced them to rebuild
    the model's read view (a DuckDB catalog write-write conflict). Different *models* still run in
    parallel.
- **Token acquisition, secret minting and catalog `ATTACH` are serialized across threads**, so N
  models starting at once resolve one OneLake token between them instead of each minting its own and
  re-issuing `CREATE SECRET` against the shared DuckDB instance. The discovery pools are also sized
  down by the thread count, so a wide project no longer multiplies into 32 × N in-flight requests.
- **An `insert` batch that adds nothing now writes no commit at all** — the Delta version does
  not move, where a delta-rs MERGE committed a no-op version. Re-running an already-loaded
  backlog leaves no log churn. The operation recorded in history is `WRITE`, not `MERGE`.
- **The `insert` append is always fenced.** The anti-join reads the target, making this a
  read-modify-append: it commits only if the table version is unchanged since the model started,
  and raises `CommitFailedError` otherwise. A writer that committed in between would have made
  the anti-join stale and let a duplicate through. This does not depend on the `reads_self`
  heuristic that gates the plain `append` strategy.

### Fixed
- **At `threads > 1`, a run that never merges no longer gets capped at the merge share.** DuckDB's
  `memory_limit` is global to the instance, so above one thread duckrun pins it once at connection
  setup rather than letting each model set its own. That pin took the *tightest* share any path could
  ask for — `_DUCKDB_MEM_FRACTION` (0.3) — on the reasoning that we cannot know in advance which
  models will merge. The cost was not, as the code claimed, that the write path "spills to disk
  sooner": a project that runs no delta-rs merge at all reserved 60% of the box for a pool that was
  never allocated and capped DuckDB at 30%. Measured on a 29 GiB node at `threads: 4` — pinned to
  8.72 GiB, and the build died with roughly 20 GiB idle (`failed to pin block of size 256.0 KiB
  (8.7 GiB/8.7 GiB used)`). It also silently defeated the `insert` strategy's deliberate choice *not*
  to take the merge share, which held only at `threads: 1`. The pin is now the **write** share, and
  the merge share is applied lazily by the first delta-rs merge, staying for the rest of the run. A
  run with no merge keeps the full write budget; a run with merges is unchanged from its first merge
  onward. The one-way tighten is what keeps it safe — nothing raises the limit back out from under a
  concurrent merge — and `memory_limit` is checked on new allocations, so lowering it mid-run makes a
  running query spill rather than fail.
- **A Delta log where only some files carry statistics no longer reports a silently-low row count.**
  The free row count compaction sizes from is `sum(num_records)` over the log, and SQL `sum` ignores
  NULLs — so a table whose files were not all written with statistics answered with a *fraction* of
  its rows, and compaction, which trusts that count at the finer 1M floor, sized its row groups off
  that fraction. Such a log now reports unknown, which keeps the 16M ceiling: an unknown count costs
  nothing, a silently-low one pins the table to the bottom of the segment band — the same failure as
  #22, reached from the other direction. delta-rs always writes `num_records`, so this only affects
  tables written by something else. An empty table still reports unknown, as before.
- **The row estimator no longer registers its probe under a fixed name.** Two writes sharing one
  DuckDB connection — the notebook API driven from user threads, or the adapter's shared-connection
  fallback — could unregister each other's relation mid-estimate. The name is now per thread and per
  relation.
- **The overwrite row estimate no longer drops every `UNION ALL` branch but the first (#22).** A
  `UNION` plan node carries no estimated cardinality of its own while each branch carries one, and
  the walk that reads the plan returned the first child that yielded a number — so an N-feed union
  reported roughly 1/N of its rows, and under-reporting is the direction that shrinks the row-group
  ceiling. Unlike the planner's own guesses this one was ours to get right: DuckDB reports every
  branch correctly. A union parent now sums its branches, which for `UNION ALL` is exact (verified
  2-feed, 3-feed and nested). Nothing else sums: DuckDB collapses `UNION` (distinct), `EXCEPT` and
  `INTERSECT` into a single-child projection over a hash aggregate so they never reach that branch,
  and a join's inputs must never be summed — each of those stays a correct upper bound on its own
  result, which is the harmless direction.
- **A bad planner row estimate can no longer pin a large table's row groups to the bottom of the
  segment band (#22).** The row-group ceiling for a full-table overwrite is `ceil(rows / 8)`, and
  those `rows` come from DuckDB's planner. That number is not a measurement: DuckDB applies a fixed
  0.2 selectivity guess to filters and anti/semi joins, a set-operation parent carries no cardinality
  of its own, and a CSV's row count is extrapolated from *file size*, so a compressed source is off
  by its compression ratio. Measured, these are 2x–5x each and compound — a two-feed union with a
  filter estimated 9.3x low. The failure is one-sided: an over-estimate is harmless (it caps at 16M
  and the file roll decides), while an under-estimate collapses the ceiling onto the floor. A 370M-row
  fact built that way landed at **380 row groups of ~974k rows** where ~34 belong, and no later pass
  fixed it. duckrun does not try to out-guess the planner — instead an estimate is now floored at
  **6M** rows instead of 1M, capping the worst case at ~68 groups. An *exact* row count, which
  compaction and `VACUUM <table>` read for free from the Delta log, keeps the 1M floor and is
  unchanged, so compacting a table still restores the finer geometry. The cost is that the ~8-lane
  target for a small table is only reached above ~48M rows.
- **Appends now get the read-layout profile instead of delta_rs defaults (#22).** An append was
  written with no writer properties and no 256 MB file target, on the reasoning that appends are
  transient increments which threshold-gated compaction folds into the read layout later. That fold
  never happens for the tables it matters most for: compaction fires on small-file **byte debt**, so
  an append-only fact whose files are already a healthy size never trips the trigger and keeps
  delta_rs's 1,048,576-row groups and 100 MB files permanently. Measured on a 40M-row append: 2 files
  / 39 row groups / 1,025,641 rows each before, 1 file / 3 row groups / 16M rows each after. This
  costs nothing on a small increment, because the row-group size is a ceiling an append that never
  reaches it never pays. Appends are still not *sized* — they keep the 16M ceiling and let the file
  roll pick the group, since an increment knows nothing about the table it lands in. This also covers
  an insert-only merge (`incremental_strategy='insert'`, or `merge_clauses` with
  `when_matched: do_nothing`), which is routed to a DuckDB anti-join and committed as a plain append.
  A merge that genuinely reaches delta_rs still writes with delta_rs defaults, deliberately: that
  path is the OOM-prone one and must not also take on a 16M-row write buffer.
- **`get_stats` no longer overstates `total_rows` on a table with deletion vectors.** A parquet
  footer still counts rows a DV has logically removed, so summing `parquet_file_metadata.num_rows`
  reported the *physical* row count — a Fabric Warehouse table read as 144,349,058 rows against a
  real 143,876,534. `total_rows` is now the logical count, matching `SELECT COUNT(*)`. The DV total
  is read only when the table's protocol declares the `deletionVectors` reader feature, so the
  overwhelming majority of tables (delta-rs rewrites files rather than emitting a DV) pay nothing,
  and `describe detail`, which reports no row count, opts out. Inline (`storageType:"i"`, the bitmap
  base85-encoded in the log JSON) and file-based DVs both work. `avg_row_group` deliberately stays
  *physical* — it describes the parquet layout — so on a DV table `avg_row_group * num_row_groups`
  exceeds `total_rows` by the deleted count. Verified against three Fabric-written tables; the cost
  of the DV read is a known [limitation](docs/limitations.md#microsoft-fabric--onelake).
- **OneLake discovery no longer claims a non-Delta directory is a table (#19).** `abfss://`
  can't be globbed, so discovery lists directory *names* over REST — and a directory holding
  parquet but no `_delta_log` (an interrupted write) was cached as an existing relation. That
  made `is_incremental()` true for a table with no queryable view, and the model failed with
  `Catalog Error: Table with name X does not exist!` pointing at its own compiled SQL rather
  than at discovery; `duckrun.connect()`, which confirms the log, disagreed about the same store
  in the same job. Both discovery paths (per-schema and the cross-schema prefetch) now share one
  filter that confirms the `_delta_log` — only for a directory delta-rs already failed to open,
  so a healthy schema pays no extra round trip, and only a positive "no log" answer drops a
  relation (no token, or a probe that errors, keeps it: a relation wrongly dropped would flip
  `is_incremental()` off and clobber the table).

## [0.4.30]

### Performance
- **Read-only startup on a multi-schema OneLake project, round 2 (#16): ~28s → ~8s wall clock
  (`dbt show` of one small model, ~80 tables over 8 schemas, residential connection; most of
  the remainder is dbt parse + connection open).** 0.4.28 made the per-schema delta-rs opens
  concurrent, but a multi-schema project still paid discovery schema-by-schema (dbt lists every
  manifest schema serially on duckrun's single thread), and the `delta_scan` view registrations
  stayed serial — each `CREATE VIEW` binds at creation, replaying the table's Delta log a second
  time through DuckDB's delta extension (no metadata cache shared with delta-rs). Discovery now
  prefetches the whole cache-population burst in one cross-schema pass
  (`_relations_cache_for_schemas`): all listings, then ONE concurrent open pool for every
  table's log, then ONE concurrent pool for the view binds (each worker on its own raw child
  cursor — catalog and secrets are instance-global). The discovery pools also grew from 8 to 32
  workers (the work is latency-bound, not CPU-bound). Per-schema semantics are unchanged:
  same tombstone hiding, same persisted-docs re-apply, `OneLakeAccessError` still fails loud,
  and the prefetch dies with the burst — a later `list_relations` call is never served stale.

## [0.4.29]

### Added
- **Opt-in Iceberg REST catalog target.** A Fabric lakehouse also serves an Iceberg REST
  catalog, and there DuckDB is the whole engine — listing, schemas, reads and writes. duckrun
  contributes the OneLake token, the Azure storage secret and the `ATTACH`, then stays out of
  the way (no `delta_scan` views, no delta-rs routing). Delta stays the default; Iceberg must
  be asked for: `duckrun.connect(path, format='iceberg')` / `conn.attach(path,
  format='iceberg')` on the connection API, `format: iceberg` on a dbt profile catalog —
  models on such a catalog are materialized by dbt-duckdb's own macros.

### Fixed
- **Windows regression (since 0.4.22): OneLake reads failed with "SSL peer certificate or SSH
  remote key was not OK".** Off a Fabric notebook duckrun forced DuckDB's azure transport to
  `curl` (a fix for Linux runners), but on Windows the azure extension's libcurl has no CA
  bundle, so every TLS handshake failed — `dbt show`/snapshots/any `delta_scan` read over
  abfss. Windows now keeps DuckDB's default transport (WinHTTP, which trusts the system cert
  store); curl remains the off-Fabric default elsewhere and `AZURE_TRANSPORT_OPTION_TYPE`
  still overrides. (#16 regression report)

### CI
- A `windows-latest` job in the OneLake integration suite runs the full coffee scenario
  (Delta writes + `delta_scan` reads) against live OneLake, so the Windows transport path
  can't silently regress again.
- The parity suite is opt-in: it auto-runs only when the parity harness itself changes,
  no longer on every adapter/macro commit.

## [0.4.28]

### Performance
- **`dbt show` / read-only startup on a multi-catalog OneLake project: ~30s → a few seconds**
  (#16). dbt populates its relation cache for every schema before any command; discovery did
  one serial Delta-log replay per table plus a create-schema per relation. The log opens now
  run concurrently through a small thread pool and the schema is created once per schema.
- Cross-surface redundancy pass: OneLake DFS calls pool one shared HTTP session; the Azure
  secret mint is guarded per (connection, token) instead of re-minted per statement; JWT
  expiry is decoded once per token; `conn.sql()` writes no longer re-run full catalog
  discovery after every statement (other processes' tables surface at `connect()` or
  `conn.refresh()`).

### Fixed
- Progress prints are ASCII-safe and streams are hardened against cp1252 Windows consoles
  (#15).

## [0.4.27]

### Added
- **OneLake shorthand everywhere**: `<workspace>/<item>` (names or GUIDs, e.g.
  `myws/mylh.Lakehouse`) is accepted wherever a full `abfss://` URL was —
  `duckrun.connect()` / `conn.attach()`, dbt `root_path` and plugin sources, and
  `RemoteRunner` profiles + forwarded env.

## [0.4.26]

### Added
- `ws.create_warehouse()`, and `folder=` placement on `create_lakehouse` /
  `create_warehouse`.
- `deploy()`: `warehouse=` repoints DirectQuery semantic models (with `sql_endpoint()` to
  resolve the target), nested pipeline activities are reached, and a plain folder of loose
  files deploys without the git-integration layout.

### Fixed
- Direct Lake detection for the post-deploy refresh (DirectQuery models no longer get a
  spurious reframe).

## [0.4.25]

### Added
- `ws.run_python(script)` — run an arbitrary Python script on Fabric compute, with workspace
  folder support (`ScriptResult` in the public API).
- The remote dbt log streams live during `RemoteRunner` jobs instead of `InProgress`
  heartbeats.

### Fixed
- Remote job polling outlives the ~1h token and the 1h poll cap (long remote builds no
  longer die mid-poll).

## [0.4.24]

### Added
- **Folder deploy**: `ws.deploy("fabric_items")` now accepts a folder in the Fabric
  git-integration layout (`name.ItemType/` subfolders with `.platform` files) and deploys every
  item in it — variable libraries, notebooks, semantic models, then pipelines — returning
  `{displayName: item id}`. Names come from each item's `.platform`; a pipeline's notebook
  activities are auto-pointed at the folder's sole notebook; `lakehouse=` / `variables=` /
  `overwrite=` apply per item exactly as in a single-file deploy.
- **Workspace download**: `ws.download(folder)` is the mirror — it exports the workspace's
  deployable items (variable libraries, notebooks as ipynb, semantic models as TMSL, pipelines)
  to disk in the same git-integration layout, `.platform` files included, so a downloaded folder
  redeploys unchanged. `name=` grabs one item; existing local item folders are skipped unless
  `overwrite=True`.

### Fixed
- **A contract's unenforceable constraints now warn instead of passing silently.** With
  `contract.enforced: true`, `check` / `primary_key` / `foreign_key` / `unique` constraints have
  no Delta-write equivalent; the run previously stayed green with no signal. It still passes
  (dbt's NOT_ENFORCED convention) but now emits a warning naming each unenforced constraint;
  `warn_unenforced: false` on a constraint silences it. Column shape and `not_null` are enforced
  as before.
- **A reserved-word or mixed-case `event_time` no longer breaks microbatch.** The column is now
  quoted in both the batch-window re-filter and the `replaceWhere` predicate, matching the
  identifier quoting merge keys already had.
- **A rare crash evicting from the per-process maintenance marker cache under concurrent writers
  is closed** (two threads racing to evict the same entry).
- **A dropped table is rebuilt, not merged into.** `conn.sql("drop table x")` tombstones the
  table (a one-column marker; nothing deleted), but a dbt incremental model with that name still
  found "a table" at write time and merged into the marker, dying on its schema. The store path
  now detects the tombstone and takes the full-rebuild branch — drop in the notebook, rebuild
  with dbt, like CREATE after DROP.
- **A failed `CREATE SECRET` can no longer leak the OneLake bearer token into error messages or
  logs** — the statement text DuckDB echoes into the exception is redacted, and the original
  (token-bearing) exception is dropped from the chain.
- **The token cache is tenant-scoped** — a process that switches `AZURE_TENANT_ID` no longer
  reuses the previous tenant's cached token; single-tenant behavior is unchanged.
- **`dbt docs` no longer misreports a genuine view that mentions `delta_scan` in a string
  literal as a table** — the catalog matches the exact passthrough-view shape duckrun registers,
  not a substring.
- **`RemoteRunner(cores=)` and `ws.schedule(...)` validate their inputs up front** (Fabric
  notebook sizes 4/8/16/32/64; 24h `HH:MM` times) instead of failing later with an opaque
  Fabric API error. Decimal narrowing at `SORTED BY AUTO` now also recognizes the `NUMERIC`
  alias and scale-less `DECIMAL(p)`.
- **Fabric control-plane list calls now follow pagination** (`continuationToken`/`continuationUri`).
  A workspace or tenant with more items than one page previously resolved names against page one
  only, so `lakehouse_id` / workspace resolution could report "not found" for items that exist.
- **Long remote runs no longer 401 at the finish line**: the result read, notebook teardown, and
  semantic-model refresh loop re-acquire a near-expiry token after waits that can consume the
  token's ~1h life.
- **A failed secondary `attach()` rolls back cleanly** (DuckDB `DETACH` + registry removal) instead
  of leaving a phantom half-registered catalog that broke `refresh()` and blocked re-attaching.
- **DataFrame `update()` now rejects unknown SET columns loudly with no commit**, matching the
  raw-SQL `UPDATE` guard (delta-rs silently writes a no-op commit for a typo'd column).
- **Predicate rewriting is now comment-aware**: identifiers inside `--` and `/* … */` comments in
  merge/incremental predicates are no longer qualified or stripped.
- The `INSERT … VALUES` self-typing probe no longer leaks a DuckDB connection per statement.
- The workspace/remote GUID check is now strict (8-4-4-4-12); a dashed non-GUID string like
  `1-2-3-4-5` is treated as a name and resolved, instead of being classified differently by the
  two surfaces.

### Changed
- **Packaging honesty**: `requires-python >= 3.11` (matching the versions CI actually tests),
  `dbt-duckdb` capped `< 2.0` (the adapter subclasses its internals), `obstore` floored at `>= 0.5`.
- Internal dedup, no behavior change: one fenced compare-and-swap write primitive, one MERGE `ON`
  predicate builder shared by both merge surfaces, one identifier-quoting helper, one
  rewrite-overwrite skeleton behind DELETE/UPDATE fallbacks and the ALTER rewrites, one HTTP
  retry loop shared by the DFS and Fabric REST layers, and shared dbt macros for catalog
  reporting, Delta location resolution, and persist-docs.
- **Fewer Delta-log opens per operation** (each is network round trips on OneLake): a raw-SQL
  DML statement reuses one opened handle for existence + version pin + the operation (a DELETE
  drops from 4 opens to 2), and dbt's run-start discovery serves the tombstone check and the
  persisted-docs read from one open per relation (docs stats likewise).

## [0.4.23]

### Fixed
- **Token-less (pure-OIDC) profiles now authenticate OneLake *reads*, not just writes (issue #10).**
  With a profile that omits `bearer_token` (relying on GitHub OIDC / azure-identity self-acquire),
  duckrun self-acquired a token for the delta-rs *write* and for discovery, but the DuckDB Azure
  secret used for *reads* was minted from the profile's `storage_options` verbatim — empty under
  OIDC — so no read secret was ever created. Every in-model OneLake read (`delta_scan` of
  `{{ this }}`, `read_parquet` of `Files/`, a python model on the connection) then authenticated
  anonymously and failed with an opaque `Unauthorized` (delta_scan falling back to Azure IMDS). The
  adapter now mints the read secret from a self-acquired token at connection open **and** per attached
  catalog, so reads and writes authenticate through the same path.
- **The GitHub-OIDC token exchange retries transient timeouts** (a single 15s timeout no longer loses
  the token), and `with_onelake_token` no longer swallows an acquisition failure on an `abfss://` root
  — a real token-fetch error surfaces with its actionable message instead of a later `Unauthorized`.

### Changed
- **`workspace.deploy(overwrite=True)` updates the item definition in place instead of
  delete-then-recreate.** It now calls Fabric's `updateDefinition`, so the item id and its schedules
  survive, there is no async-delete name-release race, and a stuck/undeletable item can't block a
  redeploy. A genuine deploy/delete failure surfaces loudly (with the response body) rather than being
  swallowed into an opaque `409 Conflict`.

## [0.4.22]

### Added
- **`duckrun.workspace()` — deploy and orchestrate Fabric artifacts.** A workspace handle with
  idempotent `create_lakehouse` / `create_notebook`, plus `ws.deploy(...)` for file artifacts:
  `.ipynb` notebooks, `.bim` semantic models (with refresh, and `lakehouse=` to repoint a Direct
  Lake model at a lakehouse), `pipeline.json` Data Pipelines, and `variables.json` variable
  libraries (`deploy('variables.json', variables={...})`). Deployed items can be run and scheduled:
  `ws.run(name)` runs a notebook/pipeline and waits, and `ws.schedule(name, ...)` sets a Fabric
  cron schedule. See `tests/deploy_testing` for a full-project deploy demo.

### Changed
- **OneLake/Fabric/Power BI auth is now self-acquiring via OIDC — `az login` is no longer required.**
  `azure-identity` is promoted to a **core dependency** (OneLake auth is no longer an optional
  extra), and duckrun mints its own OneLake, Fabric, and Power BI tokens, caching them per scope
  within the process. Both the delta-rs write path and the per-catalog resolvers self-acquire, so a
  CI job needs only `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` (federated OIDC) and drops all explicit
  `az login` + token-plumbing steps.
- **`conn.copy` / `conn.download` stream through obstore across every store.** File transfer is
  streamed via obstore (which now ships with duckrun) uniformly across local, S3, GCS, ADLS, and
  OneLake, so no separate obstore install or direct-obstore usage is needed.

## [0.4.21]

### Fixed
- **MERGE no longer aborts at a hidden 100 GB disk-spill ceiling.** A wide MERGE (one that rewrites
  many partitions) spills to disk, and the DataFusion `DiskManager` under delta-rs caps that spill at
  a flat **100 GB regardless of disk size** — so a large merge failed with *"Resources exhausted … the
  used disk space during the spilling process has exceeded the allowable limit of 100.0 GB"* even on a
  machine with terabytes free. duckrun now sets `max_temp_directory_size` to **~80% of the free space
  on the spill disk** on every merge (both the dbt incremental path and `conn.sql` `MERGE INTO`), so
  the on-disk spill scales to the actual disk instead of a constant. Override per model with
  `merge_max_temp_directory_size`. This is separate from the in-memory `max_spill_size` cap. See
  [Limitations → Memory](docs/limitations.md).

## [0.4.20]

### Fixed
- **`RemoteRunner` now runs the Fabric notebook on the large local work disk, not `/tmp`.** A Fabric
  Python notebook's container disk (`/`, `/tmp`) is a cramped ~19 GiB overlay, while
  `/home/trusted-service-user/work` is a ~135 GiB local disk. RemoteRunner placed the whole run on
  `/tmp` — the project, dbt's `target/`, DuckDB's spill, delta_rs write staging, and `tempfile`
  downloads — so a large build filled it and failed ("No space left on device" / a DuckDB temp-spill
  exhaustion). It now extracts the project and points `TMPDIR` under the work disk (falling back to
  `/tmp` off Fabric), so big remote builds have room.
- **`RemoteRunner` prints the full remote dbt log, not just the last 40 lines.** The notebook captures
  every command's output, but the runner printed only a short tail — for a build+test run that tail
  was entirely the `test` phase, hiding a failing `build` node's error. The whole captured log is now
  printed so a failed remote run is diagnosable.

## [0.4.19]

### Added
- **`RemoteRunner` — run a dbt project on Microsoft Fabric compute.** A drop-in for dbt's
  `dbtRunner` that ships the dbt logic into a temporary Fabric Python notebook, runs the command
  there, streams the log back, and deletes the notebook. Swap `dbtRunner()` for
  `RemoteRunner(cores=8)` to move a build off your laptop/runner onto Fabric, co-located with the
  lakehouse. Only the dbt logic is embedded (with a token-scrubbed `profiles.yml`); external data
  assets stay in OneLake, non-secret `env_var` config is auto-forwarded, and the OneLake token never
  travels (the notebook uses `notebookutils`). See [Remote execution on Fabric](remote.md).

### Fixed
- **The dbt adapter now falls back to `notebookutils` for the OneLake token when the profile has
  none.** Inside a Fabric notebook a read-only command (`dbt test` / `show` / `docs`) discovered
  tables via the OneLake REST list, which needs an explicit bearer token; with no token in the
  profile it found zero tables and failed with "schema does not exist". Discovery now acquires a
  token from the live source (the same fallback `duckrun.connect()` uses), so an in-notebook dbt run
  needs no token in `profiles.yml`.
- **Snapshot metadata columns are now timezone-aware, not `TIMESTAMP_NTZ` (issue #9).** dbt's
  snapshot timestamps (`dbt_valid_from` / `dbt_valid_to` / `dbt_updated_at`) dispatched to
  dbt-duckdb's `snapshot_get_time` / `snapshot_string_as_time`, which cast to a bare
  `::timestamp` and landed in Delta as `timestampNtz` — a type Microsoft Fabric's SQL endpoint
  rejects, so the snapshot table errored when queried through the SQL endpoint. duckrun now ships
  `duckrun__snapshot_get_time` / `duckrun__snapshot_string_as_time` overrides that keep the time
  zone (`now()` → `TIMESTAMPTZ`), writing a Delta `timestamp` Fabric accepts. Only the
  dbt-generated snapshot columns are affected; user model column types are still written verbatim.
  Snapshots created before this fix keep their `timestampNtz` columns on disk — run
  `dbt snapshot --full-refresh` once to rebuild them as timezone-aware.

## [0.4.17]

`conn.sql()` statement routing: multi-statement batches and interior comments are handled
consistently for every verb, not just DML.

### Changed
- **`;`-batches are rejected for every verb, not just DML.** Routing keyed off the leading verb, so a
  read-led batch (`select 1; create table foo as select 2`) slipped past the DML router into raw
  DuckDB and silently made an ephemeral native table. The one-statement-in / one-relation-out guard
  now runs at the top of `sql()`, so it holds for reads too. A `SET`/`PRAGMA; SELECT` one-liner must
  now be two calls on the same connection (session state persists between them).

### Fixed
- **Interior comments no longer skew the DML pre-checks.** The unsupported-DML detection only stripped
  *leading* comments, so a `--` / `/* */` comment could inject a false `;` separator or a false
  `FROM` / `USING` / `RETURNING` boundary and wrongly reject a valid single statement. It now scans
  the fully comment-stripped SQL (quote / dollar-quote aware).

## [0.4.16]

`conn.sql()` DML: several `MERGE` / `UPDATE` / `DELETE` forms that used to fail — or crash — now work.

### Added
- **Columnless `MERGE … WHEN NOT MATCHED THEN INSERT VALUES (…)`.** With no column list the values
  bind positionally to the target's columns, like a native positional `INSERT`.
- **`MERGE … THEN DO NOTHING`.** A valid no-op action; it's folded into first-match-wins guards on the
  later clauses of its kind (an all-`DO NOTHING` merge is a no-op).
- **`WITH … UPDATE` / `WITH … DELETE`.** A leading CTE is evaluated through the DuckDB overwrite
  fallback (with the CTE re-attached to the read) instead of being declined.
- **DuckDB functions and non-correlated subqueries inside `MERGE` predicates and values.** delta_rs
  evaluates a merge's `ON` / `WHEN` / `SET` / `INSERT` expressions in datafusion, which lacks DuckDB's
  function library and can't plan a subquery. Any subexpression that references neither `target` nor
  `source` is now constant-folded in DuckDB first, so `version()`, `current_schema()`, `epoch()`,
  `pi()`, `(select …)` etc. resolve. A wholly-constant predicate folds to a real boolean, so
  `WHEN MATCHED AND 0` no longer trips datafusion's type coercion.
- **`DEFAULT` and subqueries in `INSERT … VALUES`.**

### Fixed
- **No process crash on an unplannable `MERGE` value.** A subquery or `DEFAULT` inside a merge value
  that delta_rs can't plan — including a correlated subquery that survives folding — is now a clean
  `ValueError` instead of a hard Rust panic that took down the process.
- **Self-qualified columns in `UPDATE` / `DELETE`** (`UPDATE t SET t.c = …`) resolve correctly.
- **`RETURNING`** in a `conn.sql()` DML statement raises a clear, actionable error (a Delta write
  commits through the log and can't hand back the affected rows).

### Internal
- sqlsmith differential fuzzer hardening — `RETURNING` filtered from the generated corpus, and
  window-`OVER` `ORDER BY`, unordered `LIMIT`, `TABLESAMPLE`, and clock/transaction functions
  (`transaction_timestamp`, `txid_current`, …) are bucketed as nondeterministic instead of reported as
  false-positive data divergences. A 10,000-statement soak runs clean.

## [0.4.15]

Keep wide `DECIMAL` columns dictionary-encoded for Direct Lake.

### Fixed
- **Wide `DECIMAL` columns stay dictionary-encoded.** A `DECIMAL` with precision > 18 maps to a Parquet
  `FIXED_LEN_BYTE_ARRAY`, which the writer emits as PLAIN (never a dictionary page) — the slow path for
  a Direct Lake read. `CREATE TABLE … SORTED BY AUTO` now narrows such a column to `DECIMAL(18, s)` when
  every value fits the narrower precision, so it keeps a dictionary page; a column that genuinely needs
  the wider precision is left unchanged.

## [0.4.14]

Write-layout tuning for Direct Lake, plus a leaner `MERGE`.

### Changed
- **Adaptive row-group geometry.** A full-table write (`CREATE TABLE AS`, dbt `table` /
  `--full-refresh`) and the post-write compaction now size Parquet row groups from the result's row
  count instead of a flat 16M. A small table shrinks its row groups so it still yields ~8 Direct Lake
  segments (each kept in the 1M–16M-row band) instead of one or two giant ones, so it cold-loads on
  more transcode lanes; a large table is unchanged (16M rows, the ceiling under 2²⁴). Tunable via the
  `DUCKRUN_RG_LANES` env var (default 8).
- **`MERGE` rewrites only the partitions it touches.** The merge auto-injects the source's constant
  partition bounds into the `ON` predicate and prunes matched partitions with `target.p IN (…)` (was
  a `BETWEEN min/max` span), so an incremental merge reads and rewrites far fewer files — less memory,
  faster commit.

### Fixed
- **Adaptive sizing respects `LIMIT`.** A `CREATE TABLE AS … LIMIT n` was sized off the whole source
  (DuckDB's planner estimate can't see a `LIMIT`), producing too few, too-large row groups; it now
  takes an exact count when the plan carries a limit (the limit short-circuits the scan, so it stays
  cheap).

### Internal
- The row-group sizing rule and the sort-key recommender moved into the adapter core (`policy.py` /
  `dbt.adapters.duckrun.sortkey`); the `import duckrun` public surface is unchanged.

## [0.4.11]

Robustness fixes to the SQL-write path, shaken out by a new black-box conformance suite.

### Fixed
- **`INSERT … VALUES` casts each literal to its target column type.** A VALUES list whose columns
  don't share a common type on their own (e.g. `('inf'), (0.0)` — a string next to a decimal) now
  casts per-row to the destination type, the way a native `INSERT` does, instead of failing with
  DuckDB's "Cannot combine types". The lossy-numeric-narrowing guard is unchanged.
- **`INSERT INTO t WITH cte … SELECT …` is routed.** A CTE placed after the target — even one that
  shadows the target's name — is no longer swallowed into the relation name.
- **`INSERT INTO t BY NAME SELECT …`** aligns the source's columns to the target by name (omitted
  target columns are written NULL).
- **A predicate-less `UPDATE` updates every row over a many-file table.** delta-rs 1.5.0's
  full-table `update()` silently updated only some rows of a multi-file table; a no-`WHERE` update
  now goes through a fenced DuckDB-evaluated overwrite (correct, and equal work — it rewrites every
  row anyway).
- **`DROP TABLE <t>` on an already-dropped table raises** like SQL requires; only `DROP TABLE IF
  EXISTS` is a silent no-op. (A drop tombstone is no longer read as "still exists".)

### Internal
- Vendored **`tests/conformance_slt/`** — a black-box, DuckDB-oracle-validated sqllogictest suite
  (933 records) that exercises the SQL router and Delta storage semantics. A pytest wrapper gates it
  on regressions: the oracle must stay all-green and duckrun may fail only a pinned allowlist of
  deliberate deviations (the whitespace-in-name invariant, non-ASCII identifiers, and delta-rs engine
  limits like `RETURNING` / column `DEFAULT`).

## [0.4.1]

Robustness fixes and a cleanup; the frozen 0.4.0 surface is unchanged.

### Fixed
- **Quoted-dot names resolve consistently.** All name-splitting sites now route through one
  quote-aware splitter, so a dot inside a quoted identifier (`"a.b"`, one legal name) is never
  mistaken for a schema/catalog separator.
- **Malformed `CREATE TABLE` fails loud instead of writing garbage.** A mis-spelled layout clause
  (e.g. `SORT BY AUTO` — it's `SORTED BY`) used to leak into the table name and silently create a
  spaces-in-name Delta table (which later broke `get_stats` on abfss). The target is now validated
  before any write.
- **Case-fold collisions in a store fail loud on discovery.** When two tables (or schemas) differ
  only by case — e.g. an external engine wrote both `Foo` and `foo` on a case-sensitive store —
  DuckDB's catalog can expose only one; `connect()` now raises a clear error naming both instead of
  silently hiding one.

### Changed
- **`CREATE TABLE` rejects whitespace in a schema or table name** (even quoted): spaces become
  `%20` in the Delta path and trip abfss globbing, and match no valid OneLake table name anyway. A
  space in an attached-catalog *alias* (not a directory) is still allowed.

### Internal
- Removed the dead `createDataFrame` helper machinery; the sort-key recommender relation is now
  built directly (output columns and types unchanged).

## [0.4.0]

0.4.0 marks the SQL-only surface (`connect()` / `conn.sql()` and the SQL verbs) stable and
frozen. Planned implementation cleanups — parser unification, dead-code removal, case-fold on
discovery — land afterward as internal changes without touching this surface.

### Changed
- **A dropped table is absent on every surface.** A `drop table` tombstone now reports nonexistent
  consistently through one oracle (`_live_table_exists`, reusing the `is_dropped` predicate discovery
  uses): `catalog.tableExists` is `False`, `mode('error').saveAsTable` recreates it (was: raised
  "already exists"), and `mode('ignore')` writes it rather than no-oping. Previously the writer
  disagreed with the SQL/discovery surfaces.
- **Self-overwrite is refused.** `conn.table("t").sort(...).write.mode("overwrite").saveAsTable("t")`
  (an unfenced read-modify-write of a table with a projection of itself) now raises and points at
  `conn.table("t").optimize(...)`, which is snapshot-fenced and measured. A frame from `conn.table`
  carries its table lineage through `.sort()`, so the guard fires even after a sort; writing to any
  other table is unaffected. `.optimize()` on a sorted frame is likewise refused (sorting a frame
  doesn't choose the rewrite key), and no-arg `df.sort()` on a `conn.table` frame now sizes its
  profiling sample from the Delta log's real row width.

### Removed
- **`safeappend`** — the deprecated alias is gone, on both surfaces. `df.write.mode("safeappend")`
  raises (use `mode("append_if_unchanged")`), and `incremental_strategy='safeappend'` is no longer
  accepted (use `append_if_unchanged`). No back-compat shim.
- **`createDataFrame(samplingRatio=, verifySchema=)`** — both were parity-only no-ops and are removed
  from the signature; passing them now raises `TypeError`.
- **`DataFrameWriter.insertInto`** — removed. It was `df.write.mode("append").saveAsTable(name)` (or
  `mode("overwrite")` to replace all rows); use that directly. No shim.
- **Z-order** — removed. `DeltaTable.optimize()` and the internal `engine.optimize` drop their
  `zorder_by` parameter (`optimize()` stays as a plain bin-packing compaction); z-order is gone
  entirely because bit-interleaving destroys the run-length runs a columnar reader relies on.

## [0.3.36] - 2026-07-05

### Changed
- **`conn.table(name).optimize()` is now a maintenance ladder.** The bare call is the *safe button*:
  it compacts small files (a byte trigger bin-packs only partitions carrying real debt) and vacuums,
  commits `dataChange=false`, and **never rewrites row data** — idempotent, safe under concurrent
  writers, schedule-friendly. The profiled sort rewrite moves behind `optimize(rewrite=True)` (auto
  key), an explicit `optimize("a", "b")`, or a scoped `optimize(where=…)`, and now returns a
  `dataChange=true` warning. `optimize(analyze=True)` returns the sort-key recommendation as a
  DataFrame and commits nothing.
- **The full-table sort rewrite is snapshot-fenced.** It commits via `overwrite_if_unchanged` (CAS to
  the version the scan read) instead of a plain overwrite, so a concurrent write fails it loudly
  rather than being clobbered — matching the scoped (`replaceWhere`) path. No unfenced overwrite hole
  is left.
- **Auto sort-key profiler**: a near-unique timestamp is no longer lead-eligible (leading with a
  ~unique temporal grain-stopped the first pick and left an empty key); plus approximate/dynamic
  sampling refinements (dynamic sample size, bounded skew histogram, fully-approximate key selection).

### Removed
- **`df.write.optimize()`** (the write-time layout twin). Every write already lands in the parquet read
  layout, so it added only the sort — land the table then `conn.table(name).optimize(rewrite=True)`.

## [0.3.35] - 2026-07-05

### Changed
- **`optimize` operates on a table, not the session.** Removed the session-level `conn.optimize(name, …)`.
  Compaction and z-order are `DeltaTable.forName(conn, name).optimize()` / `.optimize(zorder_by=[…])`; the
  experimental profiled sort rewrite is `conn.table(name).optimize()` (auto key) or `.optimize("a","b")`.
  The old `sort='experimental'` kwarg on `DeltaTable.optimize()` is gone.
- **Single read-layout writer profile for every file write.** The separate "normal" (ZSTD) and
  "optimize" writer configs are collapsed into one Direct-Lake-friendly profile — SNAPPY, 6M-row groups,
  an **8 MB dictionary page limit** (mid-cardinality columns keep a remappable dictionary; near-unique
  columns overflow to PLAIN — a 128 MB limit instead kept them dictionary-encoded and made a merge reading
  the table materialize 25 GB of dictionaries vs ~4 GB), **data pages bounded to 20k rows** (an unbounded
  page row-count buffers a whole row group as one page on compressible columns — arrow-rs #5797), chunk
  stats, and unique columns written PLAIN — used by append / overwrite / safeappend / compaction / the
  sort-rewrite alike. **MERGE
  is deliberately excluded:** it passes no writer properties and no target file size, so a merge stays
  quick and never rewrites fat files; the threshold-gated post-merge compaction folds merged files up
  into the read layout afterwards.
- **Target file size 1 GB → 256 MB, one row group per file.** A Parquet row group can't span files, so
  a large file-size cap silently truncates the row group (delta-rs closes the file mid-group), leaving
  small, non-uniform Direct Lake column segments; 1 GB also forced the whole-file copy-on-write that blew
  up merges on disk. 256 MB is large enough for a wide fact (lineitem) to reach a full 6M-row segment yet
  far below the 1 GB that hurt merges — and with the dictionary page limit bounded (below), 128/256/512 MB
  all merge in ~16s / ~5 GB (measured), so file size is free to serve the read layout. Applies to every
  file write and to routine post-write compaction.
- **Row group is 6M rows** (was 4M normal / 8M optimize). 6M sits mid-band in Fabric's 1M–16M segment
  guidance while bounding write-time memory (arrow-rs buffers a full uncompressed row group per open
  writer).
- **Auto sort-key profiler drops mostly-null columns** using Delta-log statistics — a column that is
  almost entirely NULL clusters for free and never earns a sort-key slot.
- **TPC-H benchmark ingests through the duckrun write path** (`conn.read.parquet(...).write.saveAsTable`)
  instead of a zero-copy `convert_to_deltalake`, so it exercises the writer and the DuckDB read side of
  the 22 queries end-to-end.

## [0.3.31] - 2026-07-03

### Added
- **`df.sort()` / `df.orderBy()`** — the vanilla Spark DataFrame methods, returning a new *writable*
  DataFrame ordered by a native DuckDB `ORDER BY` (`orderBy` is an alias of `sort`, `ascending=` bool
  or per-column list). Previously these fell through to the raw relation and lost `.write`; now
  `conn.sql(...).sort("a", "b").write…saveAsTable(...)` works and composes with `.partitionBy(...)`.
- **`conn.optimize(name, …)` — experimental sort rewrite.** `conn.optimize(name, sort="experimental")`
  (a one-liner over `DeltaTable.forName(conn, name).optimize(...)`) profiles the table, picks a
  run-length-friendly sort key (partition columns lead but take no key slot; a column functionally
  determined by the key is dropped; measures excluded), and rewrites every file physically sorted with
  the tuned writer properties. Returns the **real measured** on-disk size from the Delta log
  (`sizeBytesBefore` / `sizeBytesAfter` / `savedPct`) — never an estimate. The plain compaction and
  z-order forms are `conn.optimize(name)` / `conn.optimize(name, zorder_by=[...])`.

### Changed
- **Parquet writer properties tuned for columnar / Direct Lake readers** — ZSTD level 3, ~6M-row row
  groups (Power BI segment standard), a 256 MB dictionary-page limit so wide columns stay
  dictionary-encoded (no mid-chunk PLAIN fallback), 8 MB data pages, chunk-level statistics, and a
  ~1 GB target file size. Applied on the initial write **and** on compaction/optimize (compaction
  previously reverted the tuned layout).

## [0.3.30] - 2026-07-03

### Added
- **Storage-neutral Files I/O on the connection API** — `conn.copy()`, `conn.download()`, and
  `conn.list_files()` move loose files to/from any store (local / S3 / GCS / ADLS / OneLake) using
  DuckDB `COPY … (FORMAT BLOB)` over the secret `connect()` already mints. No new dependency; copies
  are byte-verbatim (a `.gz`/`.zst` target is never re-compressed). OneLake enumeration uses the DFS
  REST API (DuckDB can't glob OneLake).
- **`conn.get_stats()`** — per-table Delta statistics (rows, files, row-groups, avg row-group, size,
  VORDER, compression) from the Delta log + parquet footers; `detailed=True` for one row per row
  group. Live files only (tombstoned files excluded).

### Changed
- **`connect()` tolerates any root.** Discovery skips directories that aren't Delta tables (no
  `_delta_log`) instead of hard-failing, so pointing at a Files section or a mixed folder works; a
  genuine unreadable table still fails loud.
- **Unreachable OneLake fails loud.** A wrong-tenant / not-in-workspace store now raises
  `OneLakeAccessError` on both the connection API and the dbt discovery path, instead of silently
  reporting an empty lakehouse.

## [0.3.28] - 2026-07-01

### Fixed
- **Wrong-`deltalake` runtime guard is now exact.** The startup version check only enforced a
  `deltalake >= 1.5.0` floor, but duckrun needs *exactly* 1.5.0 — every newer release breaks
  MERGE-at-scale and batch DELETE. A Microsoft Fabric kernel that keeps a newer `deltalake` loaded
  (installed-but-not-`restartPython()`) previously sailed past the guard and silently ran broken
  merges/deletes; it now raises a loud, actionable error.
- **Single-thread pin is verified, not assumed.** The adapter pins `config.threads = 1` (the Delta
  write path is not thread-safe). If that pin can't take, it now raises instead of silently
  continuing with parallel models that would collide on the shared connection and corrupt tables.
- **`ALTER TABLE … ADD COLUMN <c> <type> NOT NULL`** no longer mis-parses the type: the trailing
  `NOT NULL` is stripped whole instead of leaving `not` glued onto the type name.

### Changed
- Added debug-level traces to two previously-silent best-effort paths (drop-tombstone scan
  failures; the DuckDB-filtered overwrite fallback for `DELETE` predicates with a subquery), so
  they're visible under `--debug`.

## [0.3.27] - 2026-06-26

### Fixed
- **OneLake bearer-token refresh on long-running builds.** A build that outlives the token's ~1h
  lifetime no longer 401s mid-run. The token is captured once at connection-open, so the adapter now
  re-mints it at the universal cursor `execute()` choke point — covering not just per-model writes but
  dbt's test/end-of-run reads, which run on a reused cursor — whenever the JWT is near expiry. The
  fresh token comes from whatever live source is available: a Fabric notebook (`notebookutils`),
  `azure-identity` (Azure CLI / managed identity), or GitHub Actions workload-identity federation. A
  bare static token (`AZURE_STORAGE_TOKEN` with no live credential behind it) still can't self-refresh.

### Changed
- **Adapter version is single-sourced** from the installed package metadata, so it can no longer drift
  from `pyproject.toml`.

## [0.3.26] - 2026-06-26

### Fixed
- **`incremental_strategy='delete+insert'` is now real.** It was silently aliased to `merge`; duckrun
  now performs an actual delete (by `unique_key`) + insert and honors `incremental_predicates`,
  matching dbt-duckdb. Surfaced by the Start Data Engineering parity project.
- **Raw-DML routing hardened.** `INSERT … VALUES` vs `INSERT … SELECT` is detected correctly even when
  a `select` appears inside a string literal, and the statement scanner is dollar-quote-aware, so a
  `;` inside `COMMENT ON … IS $tag$…$tag$` (e.g. Elementary's `persist_docs`) no longer truncates the
  statement.

### Added
- **Multi-statement DML on the dbt-cursor path.** A `delete …; insert …` script (e.g. Elementary's
  delete+insert upsert) is split into its top-level statements (parenthesis- and dollar-quote-aware)
  and each is routed individually — Delta-DML to delta_rs, the rest to the DuckDB cursor. (`conn.sql`
  still runs one statement per call by design.)

## [0.3.23] - 2026-06-23

### Changed
- **`deltalake` hard-pinned to `==1.5.0`.** Every newer release breaks duckrun — `DELETE` is broken
  and OneLake support regresses — and 1.5.0 is the first with the MERGE `max_spill_size` config the
  merge path needs. Do not float until upstream fixes land.
- **`duckdb` upper cap dropped** (`>=1.5.4`, was `>=1.5.4,<1.6.0`). duckdb is only used to read; the
  floor is solely for duckdb-delta's `version =>` pin support, and newer builds read fine.
- **merge-spill recurring gate back to SF=10** (~60M rows). SF=20 (~120M) was verified once in 0.3.22
  (peak 10.5 GB on a 16 GB runner); SF=10 is enough as the per-release gate and keeps release time down.

## [0.3.22] - 2026-06-23

### Added
- **Snapshot-isolated read-modify-write through the `DeltaTable` handle.** `DeltaTable.forName` /
  `forPath` capture the table version once; `merge` / `delete` / `update` through that handle are
  pinned to it and validated under delta-rs OCC, so a conflicting concurrent commit fails loud
  (`CommitFailedError`) instead of silently interleaving. See [docs/snapshot-isolation.md](docs/snapshot-isolation.md).
- **Fenced writer modes** — `mode("append_if_unchanged")` (alias `safeappend`) and
  `mode("overwrite_if_unchanged")`: fail-loud compare-and-swap append / overwrite that commit only
  if the table version hasn't moved since the read.
- **`DeltaTable` maintenance ops** on the connection API — `vacuum`, `optimize`, `restoreToVersion`.
- **Catalog surface fill-in** — `catalog.createTable` (empty managed Delta table from DDL/StructType),
  `refreshTable`, `getTable` / `getDatabase`, `dropTempView`.
- **DataFrame / reader parity** — `df.schema` / `df.printSchema` (Spark shape, DuckDB types), more
  DataFrame actions, `read.schema` (explicit read schema for csv/json), `read.json`.

### Fixed
- Quote-safe identifiers, fail-loud primary authentication, and connection lifecycle on the
  connection API.

### Changed
- **merge-spill release gate restored to SF=20 (~120M rows)** (was SF=10 in 0.3.21).
- CI now also runs on Python 3.12.

## [0.3.21] - 2026-06-22

### Added
- **Full delta-rs `MERGE` parity on the connection API** (`conn.sql` raw `MERGE` + the
  `DeltaTable.merge` builder). Beyond the upsert subset, both surfaces now accept everything delta-rs
  `TableMerger` exposes: `WHEN MATCHED … THEN DELETE`, `WHEN MATCHED … THEN UPDATE SET col = <expr>`
  (arbitrary expressions, incl. `CASE`), `WHEN NOT MATCHED … THEN INSERT (cols) VALUES (<exprs>)`,
  `WHEN NOT MATCHED BY SOURCE … THEN UPDATE/DELETE`, **multiple clauses of the same kind in order**,
  and an arbitrary boolean `ON` predicate (multi-key / range / non-equi). The dbt incremental path and
  its single-snapshot read-pin / OCC concurrency guarantees are unchanged.
- **dbt `merge_clauses` and `merge_update_set_expressions` configs** are now honored — an ordered,
  user-specified clause list and arbitrary `SET col = expr` updates route through the same clause core.
  (`merge_returning_columns` stays rejected — delta-rs `execute()` returns metrics, not rows.)
- **Multiple catalogs in one session.** `conn.attach(path, name=…)` binds a second+ lakehouse root as
  a named catalog, so a single session reads and joins across several lakehouses by three-part
  `catalog.schema.table` name. `read_only` is **per-catalog** — a read-only reference store (e.g. a
  Fabric Warehouse, which is a write-locked Lakehouse) sits next to a writable lakehouse. New catalog
  surface: `catalog.listCatalogs()` / `currentCatalog()` / `setCurrentCatalog(name)`. The primary
  catalog's `name` is derived from the URL (else `data`); `name=` overrides it and is mandatory for a
  GUID-only OneLake path. See [`docs/connection-api.md`](docs/connection-api.md) and the
  [live demo](https://djouallah.github.io/duckrun/multicatalog.html).
- **`conn.createDataFrame(data, schema=None)`** turns in-memory data (list of tuples/scalars, pandas
  `DataFrame`, or pyarrow `Table`/`RecordBatchReader`) into a DataFrame on duckrun's own connection —
  for seeding, demos, or persisting a small result to Delta. No Spark/PySpark dependency.
- **`DeltaTable.convertToDelta(conn, ident, partitionSchema=None)`** — zero-copy conversion of existing
  parquet into Delta (writes a `_delta_log`, no data rewrite).
- **Raw SQL `MERGE` through `conn.sql`** routes to delta-rs (same engine + snapshot pin as the
  `DeltaTable.merge` builder), via the literal `target`/`source` aliases (issue #4).
- **`DeltaTable.history(limit=None)`** — delta-rs commit history (newest-first), to discover versions
  for time travel.

## [0.3.20] - 2026-06-22

### Changed
- **`connect()` is read-only by default.** Every Delta write raises `PermissionError` unless
  `read_only=False` is passed, so an accidental write can't mutate a shared lakehouse. Reads and native
  `CREATE TEMP`/`CREATE VIEW` scratch are always allowed.

### Added
- **`conn.stop()`** closes the underlying DuckDB connection.
- **`df.toArrow()`** returns a streaming `pyarrow.RecordBatchReader` (not a fully-materialized table),
  so large results don't have to fit in memory.

## [0.3.19] - 2026-06-21

### Added
- **`DataFrame.createOrReplaceTempView(name)`** — a native, ephemeral DuckDB view (not Delta, not in
  `conn.catalog`).

### Fixed
- **INSERT fails loud on lossy numeric narrowing** instead of silently truncating values that don't fit
  the target Delta column type (issue #5).

## [0.3.18] - 2026-06-21

### Fixed
- **Cleaner OneLake `delta_scan` errors**, plus a live hint that friendly workspace/lakehouse names hit
  an upstream OneLake read bug — use the GUID form.

## [0.3.17] - 2026-06-21

### Added
- **Storage-neutral `duckrun.connect()` notebook API.** A DataFrame-style surface over DuckDB +
  delta-rs (local / S3 / GCS / ADLS / OneLake) — `conn.sql`, `conn.table`, `conn.read`, `conn.catalog`,
  a `DataFrame` with `.write…saveAsTable()`, and a `DeltaTable` handle (`merge`, `delete`, `update`,
  `version`). See [`docs/connection-api.md`](docs/connection-api.md) and
  [`docs/spark-delta-parity.md`](docs/spark-delta-parity.md).
- **Raw SQL DML through `conn.sql` routes to delta-rs** (`create table as` / `insert` / `update` /
  `delete` / `alter add column` / `drop`), so it works identically on a local path and on OneLake. The
  invariant: every `CREATE TABLE` is Delta-backed; only `CREATE TEMP TABLE` / `CREATE VIEW` stay native.
- **Snapshot pinning by default.** Incremental writes (`merge`, and `mode("safeappend")`) capture the
  target version and validate the commit against it, so a concurrent writer fails loud
  (`CommitFailedError`) rather than silently interleaving (issue #1).
- **Delta-backed dbt snapshots** (`snapshot` materialization via MERGE on `dbt_scd_id`).

### Changed
- **Requires `duckdb` ≥ 1.5.4** (newer than Fabric's bundled stable build) and `deltalake` ≥ 1.5.0;
  `connect()` fails loud with a version guardrail otherwise.

## [0.3.16] - 2026-06-12

### Added
- **dbt sources via the `duckrun` plugin can now read CSV and Parquet, not just Delta.** A source
  with `meta: {plugin: duckrun}` resolves a Delta table (`delta_table_path`), or any `location`
  whose `format` is `csv` / `parquet` / `delta` (inferred from the file extension when `format` is
  omitted). A source declares *location + format* only — CSV parsing is left to `read_csv_auto`'s
  detection; hand-tuned parse options belong in a model's `read_csv(...)`, not the source.

### Fixed
- **Plugin sources failed with `... created by another Connection`.** dbt-duckdb registers the
  plugin's returned `DuckDBPyRelation` and re-registers it on every new per-handle cursor; a
  `DuckDBPyRelation` is bound to its creating connection, so the re-registration threw (and a
  read-only command could miss it entirely). duckrun now registers a plugin source as a
  connection-independent **catalog view** (`CREATE OR REPLACE VIEW … AS delta_scan/read_csv_auto/
  read_parquet(…)`) — the same way it surfaces model Delta tables — so it resolves on every cursor
  and is rebuilt in a fresh process, with no pyarrow and no copying the source into a table.
  Thanks to **Jose Marquez** for reporting the bug.
- **Azure transport for OneLake/ADLS is now set at connection-open**, alongside the bearer-token
  secret in the adapter, instead of relying on a run-only `on-run-start` hook. Read-only commands
  that still open the store — `dbt test` / `show` / `docs generate` — now get the configured
  `azure_transport_option_type` too (driven by `AZURE_TRANSPORT_OPTION_TYPE`; absent → DuckDB's
  default), fixing a OneLake `Problem with the SSL CA cert` failure on `docs generate`.

## [0.3.15] - 2026-06-11

### Fixed
- **Merge: stop silently ignoring valid-but-unsupported config.** A merge config that *passed*
  shape validation but used a key delta-rs can't express (`merge_clauses`,
  `merge_update_set_expressions`, `merge_on_using_columns`) was accepted and then quietly run as a
  plain upsert — a green run that ignored what the user asked for (the same silent-divergence class
  as the WS1 data-loss fix). These keys are now **rejected** with a clear error naming the supported
  alternatives, instead of being dropped.

### Added
- **Merge: honor `merge_update_condition` / `merge_insert_condition`.** These are now applied as
  delta-rs per-clause predicates (gating which matched rows update and which unmatched rows insert),
  rather than ignored.

## [0.3.14] - 2026-06-10

### Fixed
- **Data-loss fix (incremental writes):** `engine.table_exists` swallowed every exception and
  returned `False`, so a *transient* storage error (ADLS/OneLake 503, expired token) at store time
  looked like "no table" and sent an incremental write — already filtered to only-new rows — down
  the overwrite branch, replacing the whole table with just the increment. It now catches only
  `TableNotFoundError`; every other error propagates and fails the run loudly. `delta_version` is
  narrowed the same way (a swallowed error would degrade `safeappend`'s start-of-build pin), and
  `store()` refuses to overwrite when dbt resolved the model as incremental but the table can't be
  opened at store time. Audited every `except Exception` in the adapter to narrow or justify it.

### Added
- **Merge config validation:** invalid merge configs now fail fast with clear messages (ported from
  dbt-duckdb's `validate_merge_config`) before any Delta access, instead of a late generic delta-rs
  "Schema error".
- **Model contracts / constraints:** `config(contract={enforced:true})` now enforces column
  name/type/count (dbt's `assert_columns_equivalent` preflight) and `not null` (a pre-write guard
  on the staged rows that leaves the prior table intact on violation). `check`/`primary_key`/
  `foreign_key` are declared but not enforced against a `delta_scan` view.
- **persist_docs:** model and column descriptions are written into the Delta table's own metadata
  (`set_table_description` / `set_column_metadata`) and re-applied as `COMMENT ON` whenever the view
  is registered, so `dbt docs generate` reports real comments across processes.
- **Catalog:** Delta-backed relations are reported as `BASE TABLE` (not `VIEW`) in `dbt docs
  generate` / `get_catalog`.

### Conformance
- dbt-tests-adapter pass rate raised from 92/135 to 114/135, with a per-push regression gate.

## [0.2.26] - 2026-01-13

### Added
- **`schedule_notebook()`**: Schedule notebooks to run automatically in Microsoft Fabric
  - Supports `interval`, `daily`, `weekly`, and `monthly` schedule types
  - `interval`: Run every X minutes (e.g., `interval_minutes=60` for hourly)
  - `daily`: Run at specific times each day (e.g., `times=["09:00", "18:00"]`)
  - `weekly`: Run on specific days (e.g., `weekdays=["Monday", "Friday"]`)
  - `monthly`: Run on specific day of month (e.g., `day_of_month=1`)
  - `overwrite=False` by default - prevents accidental schedule overwrites
  - Available on connection: `con.schedule_notebook("notebook_name", ...)`

### Note
- Fabric does NOT support traditional cron expressions - uses interval/daily/weekly/monthly instead

## [0.2.17] - 2025-11-01

### Added
- **ZSTD Compression by Default**: All Delta Lake writes now use ZSTD compression instead of Snappy
  - Achieves 30-40% better compression ratios than Snappy
  - Reduces storage costs in OneLake/cloud environments
  - Automatic detection for both PyArrow (0.18.2-0.19.x) and Rust engines (0.20+)
  - Works seamlessly with schema merging, partitioning, and row group optimization

- **Expanded OneLake Connectivity**: Can now connect to multiple Microsoft Fabric item types:
  - Lakehouses (Read/Write)
  - Data Warehouses (Read)
  - Databricks Mirrored Databases (Read)
  - Any OneLake-enabled Fabric item with Delta tables

- **OneLake API Integration**: Now uses OneLake API to List table (no more path parsing)

- **Compression Stats**: Stats now display compression codec information for Delta tables

### Changed
- Refactored writer code to eliminate duplication between `writer.py` and `runner.py`
  - Single source of truth for Delta Lake write configuration
  - Both DataFrame-style API (`.write.saveAsTable()`) and pipeline runner (`run()`) now share the same compression logic


