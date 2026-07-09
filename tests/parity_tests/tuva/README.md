# Parity test — Tuva Health on duckrun

**Goal:** prove that a real, complex dbt+DuckDB project runs on **duckrun** with **zero change
to the project repo** — same SQL, same models, same seeds, same snapshots — and that any gap is
fixed in duckrun, never in the project. This is, in effect, a dbt-duckdb conformance check: if
duckrun is a faithful drop-in, the unmodified project just builds (materializing to Delta Lake).

## The repo under test

- **Repo:** https://github.com/tuva-health/tuva
- **Ref:** `v0.18.0`
- **Project run:** the repo's own `integration_tests/` dbt project (it consumes the repo root as
  the `the_tuva_project` package via `packages.yml: - local: ../`, and its `dbt_project.yml`
  already enables every vertical — claims, clinical, provider attribution, semantic layer, data
  quality — at `synthetic_data_size: small`). Synthetic seed data is pulled from the public
  `tuva-public-resources` S3 bucket.

Nothing in this folder is copied from Tuva. The repo is cloned fresh and run **verbatim**.

## The one thing that is NOT in the repo: the connection

In dbt, the *profile* (warehouse connection) is not part of the project — Tuva keeps it in
`integration_tests/profiles/<warehouse>/profiles.yml` and selects one with `--profiles-dir`.
We do the same: [profiles.yml](profiles.yml) here defines the `default` profile as `type: duckrun`
and is passed via `--profiles-dir`. The Tuva repo is never touched.

> Note: dbt resolves the adapter from `type:` directly (`type: duckdb` → dbt-duckdb,
> `type: duckrun` → duckrun). For an unmodified `type: duckdb` profile to load duckrun, duckrun
> would have to *replace* dbt-duckdb as the `duckdb` adapter — a separate, larger change. Here we
> keep the project files pristine and name duckrun in the external profile.

## How to run

```bash
# 1. Clone the project under test (short path avoids Windows MAX_PATH issues)
git clone --depth 1 --branch v0.18.0 https://github.com/tuva-health/tuva C:/tmp/tv

# 2. Run its OWN integration_tests project on duckrun, pointing at this external profile.
#    WAREHOUSE_PATH is the local Delta root; httpfs + s3_region (in profiles.yml) let the seed
#    post-hooks read the public tuva-public-resources S3 bucket anonymously.
cd C:/tmp/tv/integration_tests
export DBT_PROFILES_DIR="<duckrun>/tests/parity_tests/tuva"   # this folder
export WAREHOUSE_PATH="C:/tmp/tv_wh"
dbt deps
dbt build          # seeds + models + snapshots + tests, everything enabled
```

A green `dbt build` is the parity pass: the unmodified Tuva project built end-to-end on duckrun.

## Caveats

- The authoritative run is Linux CI. A couple of Tuva macros branch on POSIX-style model paths and
  can misfire on Windows (backslash paths) — a project quirk, **not** a duckrun bug. If a failure
  is Windows-path-specific, it is recorded as such, not patched.
