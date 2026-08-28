# Contributing to duckrun

Thanks for being here. duckrun is a small personal project, so the process is short — but it
is a real process now, and it applies to everyone including the maintainer.

## The rules

There is really only one hard rule:

**1. Never modify a test to make a PR pass.** No `skip`, no `xfail`, no weakened assertion,
no editing the expected output. If a test goes red, the fix belongs in the adapter. Some
suites are deliberately vendored referees — the conformance suite (`tests/conformance/`,
`tests/conformance_slt/`) and the MERGE benchmark — and a few of their records are red *by
design*. Those are not yours to edit. If you believe a test is genuinely wrong, say so in
the PR and leave it red; don't quietly change it.

Two softer ones:

**2. Let's talk before you add new public API.** Open an issue, or raise it in the PR
description before writing much. New surface is cheap to discuss and expensive to un-ship.

**3. No DataFrame surface beyond what DuckDB itself supports.** The DataFrame API is a thin
convenience layer over DuckDB — it isn't trying to become a DataFrame engine. If DuckDB has
no equivalent, duckrun shouldn't grow one.

And the smaller conventions:

- Test code lives under `tests/`, never at the repo root — see [`tests/README.md`](tests/README.md).
- Prefer folding a new case into the closest existing test file over adding a new one.
- Never hardcode Azure workspace/lakehouse IDs or tokens; read them from the environment.
- Keep duckrun **boring**. Prefer the dull, idiomatic solution. The one intentional bet is
  the Arrow + delta-rs core, because there is no other way to write Delta.

## Getting set up

```bash
git clone https://github.com/djouallah/duckrun.git
cd duckrun
pip install -e ".[test]"      # ".[conformance]" for the dbt adapter conformance suite
pytest tests
```

Python 3.11+. `pytest tests` is safe offline — the tests that need a live Microsoft Fabric
lakehouse skip cleanly when `WAREHOUSE_PATH` / `ONELAKE_TOKEN` aren't set.
[`tests/README.md`](tests/README.md) maps each test folder to the workflow that runs it, and
lists the env vars for the live ones. Running the heavy suites locally is optional — CI is
the real verification environment.

## Branch and PR flow

Every change lands through a pull request. Nobody pushes to `main` — not contributors, not
the maintainer, not AI agents.

1. Branch off `main`. Prefixes matching the history: `fix/`, `feat/`, `docs/`, `ci/`, `test/`.
2. One logical change per PR. Small PRs get reviewed faster.
3. Push the branch and open the PR. Say what changed and how you know it works.
4. After merge, delete the branch.

Commit messages follow the loose Conventional-Commits style already in the log —
`fix:`, `feat:`, `docs:`, `test:`, `ci:` — and reference the issue as `(#N)` when there is
one.

Two things deliberately stay on `main` and are **not** bugs to fix: CI's own
`[skip ci]` scorecard/benchmark commits, and the release `vX.Y.Z` tag push.

The `legacy` branch is frozen history. Don't check it out, merge it, or push to it.

## Which checks gate your PR

Not every workflow that runs is a gate. The one that matters is **`cores`**
([`.github/workflows/cores.yml`](.github/workflows/cores.yml)) — it is the per-change gate
*and* a release gate. Its jobs:

| Job | What it proves |
| --- | --- |
| `adapter` | dbt adapter internals + the `duckrun.connect()` API suite |
| `conformance` | the official `dbt-tests-adapter` suite, against a recorded baseline |
| `concurrency-correctness` | OCC / concurrent-writer invariants |
| `breaker` (chaos breaker) | failure injection — a regression here blocks the PR |
| `connection-card`, `snapshot-pin` | render the README/docs scorecards |
| `tpch-smoke`, `merge-spill-smoke` | fast sanity passes over the heavy paths |

`cores` is path-filtered (`dbt/**`, `duckrun/**`, `tests/**`, `pyproject.toml`,
`.github/actions/**`), so a **docs-only PR can legitimately show no checks at all**. That's
expected, not a broken PR.

Everything else is informational:

- `integration_tests_onelake` and `aemo` run on PRs but hit a live Fabric lakehouse. They're
  flaky by nature and are **not** gates.
- `parity` ([`.github/workflows/parity.yml`](.github/workflows/parity.yml)) is opt-in and
  explicitly never blocks a release.
- `fuzz`, `parquet_layout`, `deploy_to_fabric` are manual (`workflow_dispatch`) only.

If a non-gating workflow goes red on your PR, mention it — usually it's the environment, not
you.

## Changelog

Add a line under `## [Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md), referencing the issue
number if there is one.

## Releasing (maintainer only)

1. `release: X.Y.Z` commit bumping `version` in `pyproject.toml`. Always a patch bump.
2. Push the tag `vX.Y.Z` on that commit.

[`.github/workflows/publish.yml`](.github/workflows/publish.yml) then runs `version-check`
(the tag must equal `pyproject.toml`'s version, or it hard-fails), gates on `cores` +
`local_stress_tests` + `merge_spill`, and publishes to PyPI via OIDC trusted publishing.
`integration_tests_onelake` and `parity` are deliberately excluded from release gating.

Tags are the source of truth for what shipped — the GitHub *Releases* page is not
maintained.

## AI assistants

If you're working with an AI coding assistant, point it at [`AGENTS.md`](AGENTS.md), which
carries the same rules in a form agents read first. Note in particular that the PR rule here
overrides any general "you own this repo, push straight to main" instruction an assistant
may be carrying.
